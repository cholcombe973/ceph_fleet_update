extern crate api;
extern crate ceph;
extern crate chrono;
extern crate dns_lookup;
extern crate ini;
extern crate init_daemon;
extern crate nix;
extern crate rand;
extern crate reqwest;
extern crate regex;
extern crate semver;
extern crate users;
extern crate uuid;
extern crate walkdir;

use std::error::Error as StdError;
use std::fmt;
use std::fs::{copy, create_dir, File, read_dir, remove_file};
use std::net::IpAddr;
use std::io::{Error, ErrorKind, Write};
use std::io::Result as IOResult;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::SystemTime;
use std::thread;
use std::time::Duration;

use self::api::service::Version as ApiVersion;
use self::api::service::{CephComponent, CephRelease};
use self::ceph::ceph::{connect_to_ceph, ceph_version, disconnect_from_ceph};
use self::ceph::rados::rados_t;
use self::ceph::cmd::{auth_get_key, mgr_auth_add, mgr_dump, Mon, mon_dump, mon_status, OsdOption,
                      osd_unset, osd_set, osd_tree};
use self::dns_lookup::lookup_host;
use self::ini::Ini;
use self::init_daemon::{detect_daemon, Daemon};
use self::nix::unistd::{chown, Gid, Uid};
use self::semver::Version as SemVer;
use self::users::get_user_by_name;
use self::walkdir::WalkDir;

use super::apt;
use super::debian::version::Version;
use super::os_type;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CephType {
    Mds,
    Mgr,
    Mon,
    Osd,
    Rgw,
}

impl fmt::Display for CephType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &CephType::Mgr => write!(f, "Mgr"),
            &CephType::Mds => write!(f, "Mds"),
            &CephType::Mon => write!(f, "Mon"),
            &CephType::Osd => write!(f, "Osd"),
            &CephType::Rgw => write!(f, "Rgw"),
        }
    }
}

impl From<CephComponent> for CephType {
    fn from(c: CephComponent) -> Self {
        match c {
            CephComponent::Mgr => CephType::Mgr,
            CephComponent::Mon => CephType::Mon,
            CephComponent::Osd => CephType::Osd,
            CephComponent::Mds => CephType::Mds,
            CephComponent::Rgw => CephType::Rgw,
        }
    }
}

impl From<CephType> for CephComponent {
    fn from(c: CephType) -> Self {
        match c {
            CephType::Mgr => CephComponent::Mgr,
            CephType::Mon => CephComponent::Mon,
            CephType::Osd => CephComponent::Osd,
            CephType::Mds => CephComponent::Mds,
            CephType::Rgw => CephComponent::Rgw,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CephVersion {
    Dumpling,
    Emperor,
    Firefly,
    Giant,
    Hammer,
    Infernalis,
    Jewel,
    Kraken,
    Luminous,
    Unknown,
}

impl fmt::Display for CephVersion {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &CephVersion::Dumpling => write!(f, "Dumpling"),
            &CephVersion::Emperor => write!(f, "Emperor"),
            &CephVersion::Firefly => write!(f, "Firefly"),
            &CephVersion::Giant => write!(f, "Giant"),
            &CephVersion::Hammer => write!(f, "Hammer"),
            &CephVersion::Infernalis => write!(f, "Infernalis"),
            &CephVersion::Jewel => write!(f, "Jewel"),
            &CephVersion::Kraken => write!(f, "Kraken"),
            &CephVersion::Luminous => write!(f, "Luminous"),
            &CephVersion::Unknown => write!(f, "Unknown"),
        }
    }
}

impl From<CephRelease> for CephVersion {
    fn from(c: CephRelease) -> Self {
        match c {
            CephRelease::Dumpling => CephVersion::Dumpling,
            CephRelease::Emperor => CephVersion::Emperor,
            CephRelease::Firefly => CephVersion::Firefly,
            CephRelease::Giant => CephVersion::Giant,
            CephRelease::Hammer => CephVersion::Hammer,
            CephRelease::Infernalis => CephVersion::Infernalis,
            CephRelease::Jewel => CephVersion::Jewel,
            CephRelease::Kraken => CephVersion::Kraken,
            CephRelease::Luminous => CephVersion::Luminous,
            CephRelease::Unknown => CephVersion::Unknown,
        }
    }
}
/// A server.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CephServer {
    pub addr: String,
    pub id: String,
    pub rank: Option<i64>,
}

#[derive(Debug)]
pub struct CephNode {
    pub os_information: os_type::OSInformation,
}

fn semver_from_debian_version(v: &Version) -> IOResult<SemVer> {
    let mut buff = String::new();
    let mut elements = v.clone().upstream_version.elements;
    buff.push_str(&format!("{}", elements.remove(0).numeric));

    for element in elements {
        buff.push_str(&element.alpha);
        buff.push_str(&format!("{}", element.numeric));
    }
    let vers = semver::Version::parse(&buff).map_err(|e| {
        Error::new(ErrorKind::Other, e)
    })?;
    Ok(vers)
}

fn backup_conf_files() -> IOResult<Vec<PathBuf>> {
    debug!("Backing up /etc/ceph config files to /tmp");
    let mut backed_up = Vec::new();
    for entry in read_dir("/etc/ceph")? {
        let entry = entry?;
        let path = entry.path();
        // Should only be conf files in here
        if !path.is_dir() {
            // cp /etc/ceph/ceph.conf /tmp/ceph.conf
            let f = entry.file_name();
            debug!("cp {} to /tmp/{:?}", path.display(), f);
            copy(path.clone(), format!("/tmp/{}", f.to_string_lossy()))?;
            backed_up.push(PathBuf::from(format!("/tmp/{}", f.to_string_lossy())));
        }
    }
    Ok(backed_up)
}

fn restore_conf_files(files: &Vec<PathBuf>) -> IOResult<()> {
    debug!("Restoring config files {:?} to /etc/ceph", files);
    for f in files {
        copy(
            f.clone(),
            format!("/etc/ceph/{}", f.file_name().unwrap().to_string_lossy()),
        )?;
    }
    Ok(())
}

pub fn discover_topology() -> Result<Vec<(CephServer, CephType)>, String> {
    let mut cluster: Vec<(CephServer, CephType)> = Vec::new();
    let handle = connect_to_ceph("admin", "/etc/ceph/ceph.conf").map_err(
        |e| {
            e.to_string()
        },
    )?;

    let mon_info = mon_dump(handle).map_err(|e| e.to_string())?;
    debug!("mon_info: {:#?}", mon_info);
    for mon in mon_info.mons {
        cluster.push((
            CephServer {
                addr: mon.addr.split(":").next().unwrap().to_string(),
                id: mon.name,
                rank: Some(mon.rank),
            },
            CephType::Mon,
        ));
    }
    let osd_info = osd_tree(handle).map_err(|e| e.to_string())?;
    debug!("osd_info: {:#?}", osd_info);
    for osd in osd_info.nodes.iter().filter(
        |o| o.crush_type == "host".to_string(),
    )
    {
        cluster.push((
            CephServer {
                addr: osd.name.clone(),
                id: osd.id.to_string(),
                rank: None,
            },
            CephType::Osd,
        ))
    }
    disconnect_from_ceph(handle);
    Ok(cluster)
}

fn connect_and_upgrade(
    servers: Vec<CephServer>,
    port: u16,
    new_version: &Version,
    c: &CephType,
    http_proxy: Option<&str>,
    https_proxy: Option<&str>,
    gpg_key: Option<&str>,
    apt_source: Option<&str>,
    force_remove: bool,
) -> Result<(), String> {
    for s in servers {
        // First check if I own this ip address. If so then skip the RPC request
        let owned_ip = match super::owned_ip(&s.addr) {
            Ok(result) => result,
            Err(_) => super::owned_hostname(&s.addr).unwrap_or(false),
        };
        if owned_ip {
            debug!("Upgrading myself");
            let node = CephNode::new();
            match node.upgrade_node(
                &super::version_to_protobuf(new_version),
                CephComponent::from(c.clone()),
                http_proxy,
                https_proxy,
                gpg_key,
                apt_source,
                force_remove,
            ) {
                Ok(_) => {
                    info!("Upgrade succeeded.  Proceeding to next");
                    continue;
                }
                Err(e) => {
                    error!("Upgrade failed: {}", e);
                    continue;
                }
            }
        } else {
            debug!("Connecting to {}:{} to request upgrade", c, s.addr);
            let mut req_socket = super::connect(&s.addr, port).map_err(|e| e.to_string())?;
            debug!("Requesting {} to upgrade", s.addr);
            match super::upgrade_request(
                &mut req_socket,
                super::version_to_protobuf(new_version),
                c,
                http_proxy,
                https_proxy,
                gpg_key,
                apt_source,
                force_remove,
            ) {
                Ok(_) => {
                    info!("Upgrade succeeded.  Proceeding to next");
                    continue;
                }
                Err(e) => {
                    error!("Upgrade failed: {}", e);
                    continue;
                }
            };
        }
    }
    Ok(())
}

///Main function to call which implements the upgrade logic
pub fn roll_cluster(
    new_version: &Version,
    hosts: Vec<(CephServer, CephType)>,
    port: u16,
    http_proxy: Option<&str>,
    https_proxy: Option<&str>,
    gpg_key: Option<&str>,
    apt_source: Option<&str>,
    force_remove: bool,
) -> Result<(), String> {
    // Inspect the cluster health to make sure the mon upgrades were successful
    // Inspect the cluster health to make sure the osd upgrades were successful
    let mons: Vec<CephServer> = hosts
        .iter()
        .filter(|c| c.1 == CephType::Mon)
        .map(|c| c.0.clone())
        .collect();
    let osds: Vec<CephServer> = hosts
        .iter()
        .filter(|c| c.1 == CephType::Osd)
        .map(|c| c.0.clone())
        .collect();
    let mds: Vec<CephServer> = hosts
        .iter()
        .filter(|c| c.1 == CephType::Mds)
        .map(|c| c.0.clone())
        .collect();
    let rgws: Vec<CephServer> = hosts
        .iter()
        .filter(|c| c.1 == CephType::Rgw)
        .map(|c| c.0.clone())
        .collect();
    connect_and_upgrade(
        mons,
        port,
        new_version,
        &CephType::Mon,
        http_proxy,
        https_proxy,
        gpg_key,
        apt_source,
        force_remove,
    )?;
    connect_and_upgrade(
        osds,
        port,
        new_version,
        &CephType::Osd,
        http_proxy,
        https_proxy,
        gpg_key,
        apt_source,
        force_remove,
    )?;
    connect_and_upgrade(
        mds,
        port,
        new_version,
        &CephType::Mds,
        http_proxy,
        https_proxy,
        gpg_key,
        apt_source,
        force_remove,
    )?;
    connect_and_upgrade(
        rgws,
        port,
        new_version,
        &CephType::Rgw,
        http_proxy,
        https_proxy,
        gpg_key,
        apt_source,
        force_remove,
    )?;
    // TODO: How can I do the final checks here??
    let mut finished_hosts: Vec<String> = Vec::new();
    'hosts: for host in hosts {
        let ips: Vec<IpAddr> = lookup_host(&host.0.addr).map_err(|e| e.to_string())?;
        for ip in ips {
            match ip {
                IpAddr::V4(v4_addr) => {
                    if finished_hosts.contains(&v4_addr.to_string()) {
                        info!("Already requested stop from {}.  Skipping", v4_addr);
                        continue 'hosts;
                    }
                }
                IpAddr::V6(v6_addr) => {
                    if finished_hosts.contains(&v6_addr.to_string()) {
                        info!("Already requested stop from {}.  Skipping", v6_addr);
                        continue 'hosts;
                    }
                }
            }
        }

        // Check if this is myself
        let owned_ip = match super::owned_ip(&host.0.addr) {
            Ok(result) => result,
            Err(_) => super::owned_hostname(&host.0.addr).unwrap_or(false),
        };
        if owned_ip {
            // Skip myself
            continue;
        }
        debug!("Requesting {} STOP", host.0.addr);
        let mut req_socket = super::connect(&host.0.addr, port).map_err(
            |e| e.to_string(),
        )?;
        let _ = super::stop_request(&mut req_socket);
        finished_hosts.push(host.0.addr.clone());
    }
    return Ok(());
}

// Edge cases:
// 1. Previous node dies on upgrade, can we retry?
impl CephNode {
    pub fn new() -> Self {
        CephNode { os_information: os_type::current_platform() }
    }

    /// Main entry point for node upgrade procedures
    pub fn upgrade_node(
        &self,
        version: &ApiVersion,
        c: CephComponent,
        http_proxy: Option<&str>,
        https_proxy: Option<&str>,
        gpg_key: Option<&str>,
        apt_source: Option<&str>,
        force_remove: bool,
    ) -> Result<(), String> {
        debug!(
            "Upgrading from {:?} to {:?}",
            apt::get_installed_package_version("ceph")?,
            version
        );
        let from_release = ceph_release().map_err(|e| e.to_string())?;
        debug!("from_release: {}", from_release);

        if let Some(gpg_key) = gpg_key {
            debug!("Adding gpg key: {}", gpg_key);
            apt::get_gpg_key(gpg_key).map_err(|e| e.to_string())?;
        }
        if let Some(apt_source) = apt_source {
            debug!("Adding apt source: {}", apt_source);
            apt::add_source(apt_source).map_err(|e| e.to_string())?;
        }
        // install apt proxy if needed
        if let Some(http_proxy) = http_proxy {
            debug!("apt http_proxy set: {}", http_proxy);
            if let Some(https_proxy) = https_proxy {
                debug!("apt https_proxy set: {}", https_proxy);
                apt::ensure_proxy(http_proxy, https_proxy).map_err(
                    |e| e.to_string(),
                )?;
            }
        }
        // Update our repo information
        apt::apt_update().map_err(|e| e.to_string())?;
        // Upgrade the node
        self.upgrade(&CephType::from(c), &from_release, force_remove)
            .map_err(|e| e.to_string())?;
        return Ok(());
    }

    //basic upgrade things that all nodes need to do
    fn upgrade(
        &self,
        c: &CephType,
        from_release: &CephVersion,
        force_remove: bool,
    ) -> IOResult<()> {
        info!("Beginning upgrade procedure");
        /*
            3 cases:
            1. candidate = 12.2.1   candidate > installed
               installed = 10.0.2   installed == running
               running   = 10.0.2   apt install + restart service

            2. candidate = 10.0.2  candidate == installed == running
               installed = 10.0.2  Do nothing
               running   = 10.0.2

            3. candidate = 12.2.1  candidate == installed
               installed = 12.2.1  installed > running
               running   = 10.0.2  Restart service
         */
        let candidate = apt::get_candidate_package_version("ceph").map_err(|e| {
            Error::new(ErrorKind::Other, e)
        })?;
        let installed = apt::get_installed_package_version("ceph").map_err(|e| {
            Error::new(ErrorKind::Other, e)
        })?;
        let candidate_version = semver_from_debian_version(&candidate)?;
        let installed_version = semver_from_debian_version(&installed)?;
        let running_version = get_running_version(c)?;
        debug!("candidate_version: {}", candidate_version);
        debug!("installed_version: {}", installed_version);
        debug!("running_version: {}", running_version);

        // Case 1
        if candidate_version > installed_version && installed_version == running_version {
            debug!("candidate_version > installed_version && installed_version == running_version");
            //apt install + restart service

            let backed_files = backup_conf_files()?;
            debug!("Backed up conf files: {:?}", backed_files);

            // Try to do these commands for up to a minute and then fail
            for _ in 0..12 {
                if force_remove {
                    //TODO This list of packages should be parsed from a file.  It changes
                    //TODO depending on the ceph version and operating system
                    match apt::apt_remove(vec![
                        "ceph",
                        "ceph-base",
                        "ceph-common",
                        "ceph-mds",
                        "ceph-mon",
                        "ceph-osd",
                        "libcephfs1",
                        "python-cephfs",
                        "python-rados",
                        "python-rbd",
                        "radosgw",
                        "librgw2",
                        "librbd1",
                        "libradosstriper1",
                        "librados2",
                    ]) {
                        Ok(_) => {
                            debug!("apt-remove finished");
                        }
                        Err(e) => {
                            if e.description().contains("Could not get lock") {
                                continue;
                            } else {
                                return Err(e);
                            }
                        }
                    };
                    thread::sleep(Duration::from_secs(5));
                }

                match apt::apt_install(vec!["ceph"]) {
                    Ok(_) => {
                        debug!("apt-install finished");
                        break;
                    }
                    Err(e) => {
                        if e.description().contains("Could not get lock") {
                            continue;
                        } else {
                            return Err(e);
                        }
                    }
                };
            }
            restore_conf_files(&backed_files)?;
            self.finish_upgrade(c, from_release)?;
            return Ok(());
        }

        // Case 2
        if candidate_version == installed_version && installed_version == running_version {
            // Do nothing
            debug!(
                "candidate_version == installed_version && installed_version == running_version"
            );
            return Ok(());
        }
        // Case 3
        if candidate_version == installed_version && installed_version > running_version {
            debug!("candidate_version == installed_version && installed_version > running_version");
            // Restart service
            self.finish_upgrade(c, from_release)?;
            return Ok(());
        }
        Ok(())
    }

    fn finish_upgrade(&self, c: &CephType, from_release: &CephVersion) -> IOResult<()> {
        // Record which release we upgraded to
        let to_release = ceph_release()?;
        let handle = connect_to_ceph("admin", "/etc/ceph/ceph.conf").map_err(
            |e| {
                Error::new(ErrorKind::Other, e)
            },
        )?;
        debug!("Connected to ceph: {:?}", handle);
        match c {
            &CephType::Mgr => {}
            &CephType::Mds => {
                debug!("Performing possible release specific mds changes");
                self.release_specific(
                    &from_release,
                    &to_release,
                    &CephType::Mds,
                    handle,
                )?;
                self.restart_mds()?;
            }
            &CephType::Mon => {
                debug!("Performing possible release specific mon changes");
                self.release_specific(
                    &from_release,
                    &to_release,
                    &CephType::Mon,
                    handle,
                )?;
                self.restart_mon()?;
            }
            &CephType::Osd => {
                debug!("Performing possible release specific osd changes");
                self.release_specific(
                    &from_release,
                    &to_release,
                    &CephType::Osd,
                    handle,
                )?;
                debug!("Setting noout, nodown to prevent cluster rebuilding");
                osd_set(handle, &OsdOption::NoOut, false, false).map_err(
                    |e| {
                        Error::new(ErrorKind::Other, e)
                    },
                )?;
                osd_set(handle, &OsdOption::NoDown, false, false).map_err(
                    |e| {
                        Error::new(ErrorKind::Other, e)
                    },
                )?;
                self.restart_osd()?;
                debug!("Unsetting noout, nodown");
                osd_unset(handle, &OsdOption::NoOut, false).map_err(|e| {
                    Error::new(ErrorKind::Other, e)
                })?;
                osd_unset(handle, &OsdOption::NoDown, false).map_err(|e| {
                    Error::new(ErrorKind::Other, e)
                })?;
            }
            &CephType::Rgw => {
                debug!("Performing possible release specific rgw changes");
                self.release_specific(
                    &from_release,
                    &to_release,
                    &CephType::Rgw,
                    handle,
                )?;
                self.restart_rgw()?;
            }
        };
        Ok(())
    }

    fn restart_service(&self, ceph_type: CephType) -> IOResult<()> {
        let init_daemon = detect_daemon().map_err(|e| Error::new(ErrorKind::Other, e))?;
        match init_daemon {
            Daemon::Systemd => {
                let mut c = Command::new("systemctl");
                c.arg("restart");
                match ceph_type {
                    CephType::Mon => c.arg("ceph-mon.target"),
                    CephType::Mds => c.arg("ceph-mds.target"),
                    CephType::Rgw => c.arg("ceph-rgw.target"),
                    _ => c.arg("ceph-mon.target"),
                };
                debug!("systemctl cmd: {:?}", c);
                let output = c.output()?;
                if !output.status.success() {
                    return Err(Error::last_os_error());
                }
            }
            _ => {
                let mut c = Command::new("restart");
                match ceph_type {
                    CephType::Mon => c.arg("ceph-mon-all"),
                    CephType::Mds => c.arg("ceph-mds-all"),
                    CephType::Rgw => c.arg("ceph-rgw-all"),
                    _ => c.arg("ceph-mon-all"),
                };
                debug!("restart cmd: {:?}", c);
                let output = c.output()?;
                if !output.status.success() {
                    return Err(Error::last_os_error());
                }
            }
        };
        Ok(())
    }

    fn restart_mon(&self) -> IOResult<()> {
        debug!("Restarting Mon");
        self.restart_service(CephType::Mon)?;
        Ok(())
    }

    fn restart_rgw(&self) -> IOResult<()> {
        debug!("Restarting Rgw");
        self.restart_service(CephType::Rgw)?;
        Ok(())
    }

    fn restart_mds(&self) -> IOResult<()> {
        debug!("Restarting Mds");
        self.restart_service(CephType::Mds)?;
        Ok(())
    }

    fn restart_osd(&self) -> IOResult<()> {
        // ask ceph for all the osds that this host owns.
        let hostname = super::get_hostname()?;
        let h = connect_to_ceph("admin", "/etc/ceph/ceph.conf").map_err(
            |e| {
                Error::new(ErrorKind::Other, e)
            },
        )?;
        let osd_tree = ceph::cmd::osd_tree(h).map_err(
            |e| Error::new(ErrorKind::Other, e),
        )?;
        debug!("osd_tree: {:?}", osd_tree);
        // This should filter down to 1 node
        let hosts: Vec<&ceph::cmd::CrushNode> = osd_tree
            .nodes
            .iter()
            .filter(|n| n.crush_type == "host" && n.name == hostname)
            .collect();
        debug!("restart osd hosts: {:?}", hosts);

        match hosts.first() {
            Some(host) => {
                if let Some(ref children) = host.children {
                    debug!("Found osds: {:?} for host: {}", children, hostname);
                    for child in children {
                        self.stop_osd(*child)?;
                        // TODO: Is it necessary disable and enable?
                        // if the upgrade is interrupted with a server crash what should skynet do?
                        self.disable_osd(*child)?;
                        self.enable_osd(*child)?;
                        self.start_osd(*child)?;
                    }
                }
                Ok(())
            }
            None => {
                error!(
                    "Host: {} not found in osd tree.  Unable to determine which osds to restart",
                    hostname
                );
                Err(Error::new(
                    ErrorKind::Other,
                    format!(
                        "No hostname {} found in osd tree.  Can't restart osds",
                        hostname
                    ),
                ))
            }
        }
    }

    ///Stops the specified OSD number.
    fn stop_osd(&self, osd_num: i64) -> IOResult<()> {
        debug!("Stopping osd: {}", osd_num);
        let init_daemon = detect_daemon().map_err(|e| Error::new(ErrorKind::Other, e))?;
        match init_daemon {
            Daemon::Systemd => {
                Command::new("systemctl")
                    .args(&["stop", &format!("ceph-osd@{}", osd_num)])
                    .status()?;
            }
            _ => {
                let cmd = Command::new("service")
                    .args(&["stop", "ceph-osd", &format!("{}", osd_num)])
                    .status()?;
                if !cmd.success() {
                    return Err(::std::io::Error::last_os_error());
                }
            }
        };
        Ok(())
    }

    ///Starts the specified OSD number.
    fn start_osd(&self, osd_num: i64) -> IOResult<()> {
        debug!("Starting osd: {}", osd_num);
        let init_daemon = detect_daemon().map_err(|e| Error::new(ErrorKind::Other, e))?;
        match init_daemon {
            Daemon::Systemd => {
                Command::new("systemctl")
                    .args(&["start", &format!("ceph-osd@{}", osd_num)])
                    .status()?;
            }
            _ => {
                let cmd = Command::new("service")
                    .args(&["start", "ceph-osd", &format!("{}", osd_num)])
                    .status()?;
                if !cmd.success() {
                    return Err(::std::io::Error::last_os_error());
                }
            }
        };
        Ok(())
    }
    ///Disables the specified OSD number.
    ///Ensures that the specified osd will not be automatically started at the
    ///next reboot of the system. Due to differences between init systems,
    ///this method cannot make any guarantees that the specified osd cannot be
    ///started manually.
    fn disable_osd(&self, osd_num: i64) -> IOResult<()> {
        debug!("Disabling osd: {}", osd_num);
        let init_daemon = detect_daemon().map_err(|e| Error::new(ErrorKind::Other, e))?;
        match init_daemon {
            // When running under systemd, the individual ceph-osd daemons run as
            // templated units and can be directly addressed by referring to the
            // templated service name ceph-osd@<osd_num>. Additionally, systemd
            // allows one to disable a specific templated unit by running the
            // 'systemctl disable ceph-osd@<osd_num>' command. When disabled, the
            // OSD should remain disabled until re-enabled via systemd.
            // Note: disabling an already disabled service in systemd returns 0, so
            // no need to check whether it is enabled or not.
            Daemon::Systemd => {
                let output = Command::new("systemctl")
                    .args(&["disable", &format!("ceph-osd@{}", osd_num)])
                    .output()?;
                trace!("disable: {:?}", output);
                if !output.status.success() {
                    return Err(Error::last_os_error());
                }
            }
            Daemon::Upstart => {
                // Neither upstart nor the ceph-osd upstart script provides for
                // disabling the starting of an OSD automatically. The specific OSD
                // cannot be prevented from running manually, however it can be
                // prevented from running automatically on reboot by removing the
                // 'ready' file in the OSD's root directory. This is due to the
                // ceph-osd-all upstart script checking for the presence of this file
                // before starting the OSD.
                let ready_file = PathBuf::from(format!("/var/lib/ceph/osd/ceph-{}/ready", osd_num));
                debug!("Removing upstart osd ready file: {}", ready_file.display());
                if ready_file.exists() {
                    remove_file(ready_file)?;
                }
            }
            _ => {
                return Err(Error::new(
                    ErrorKind::Other,
                    "Unable to detect init daemon.  Cannot disable osd",
                ));
            }
        };

        Ok(())
    }

    ///Starts the specified mgr.
    fn start_mgr(&self, id: &str) -> IOResult<()> {
        debug!("Starting mgr: {}", id);
        let init_daemon = detect_daemon().map_err(|e| Error::new(ErrorKind::Other, e))?;
        match init_daemon {
            Daemon::Systemd => {
                Command::new("systemctl")
                    .args(&["start", &format!("ceph-mgr@{}", id)])
                    .status()?;
            }
            _ => {
                return Err(Error::new(
                    ErrorKind::Other,
                    "Ceph Mgr only works with systemd.  Cannot start mgr",
                ));
            }
        };
        Ok(())
    }

    ///Enables the specified mgr.
    fn enable_mgr(&self, id: &str) -> IOResult<()> {
        debug!("Enabling mgr: {}", id);
        let init_daemon = detect_daemon().map_err(|e| Error::new(ErrorKind::Other, e))?;
        match init_daemon {
            Daemon::Systemd => {
                let output = Command::new("systemctl")
                    .args(&["enable", &format!("ceph-mgr@{}", id)])
                    .output()?;
                trace!("enable: {:?}", output);
                if !output.status.success() {
                    return Err(Error::last_os_error());
                }
            }
            _ => {
                return Err(Error::new(
                    ErrorKind::Other,
                    "Ceph Mgr only works with systemd.  Cannot enable mgr",
                ));
            }
        };
        Ok(())
    }

    ///Enables the specified OSD number.
    ///Ensures that the specified osd_num will be enabled and ready to start
    ///automatically in the event of a reboot.
    ///osd_num: the osd id which should be enabled.
    fn enable_osd(&self, osd_num: i64) -> IOResult<()> {
        debug!("Enabling osd: {}", osd_num);
        let init_daemon = detect_daemon().map_err(|e| Error::new(ErrorKind::Other, e))?;
        match init_daemon {
            Daemon::Systemd => {
                let output = Command::new("systemctl")
                    .args(&["enable", &format!("ceph-osd@{}", osd_num)])
                    .output()?;
                trace!("enable: {:?}", output);
                if !output.status.success() {
                    return Err(Error::last_os_error());
                }
            }
            Daemon::Upstart => {
                // When running on upstart, the OSDs are started via the ceph-osd-all
                // upstart script which will only start the osd if it has a 'ready'
                // file. Make sure that file exists.
                let ready_file = PathBuf::from(format!("/var/lib/ceph/osd/ceph-{}/ready", osd_num));
                debug!("Restoring upstart osd ready file: {}", ready_file.display());
                let mut file = File::create(&ready_file)?;
                file.write_all(b"ready\n")?;
            }
            _ => {
                return Err(Error::new(
                    ErrorKind::Other,
                    "Unable to detect init daemon.  Cannot enable osd",
                ));
            }
        };
        Ok(())
    }

    fn scan_node_for_ceph_processes(&self) -> IOResult<Vec<CephType>> {
        let mut ceph_processes: Vec<CephType> = Vec::new();
        for entry in read_dir(Path::new("/var/run/ceph"))? {
            let entry = entry?;
            let sock_addr_osstr = entry.file_name();
            let file_name = match sock_addr_osstr.to_str() {
                Some(name) => name,
                None => {
                    // Skip files we can't turn into a string
                    debug!(
                        "Could not turn socket file name {:?} into a string.  Skipping",
                        sock_addr_osstr
                    );
                    continue;
                }
            }.trim_right_matches(".asok");
            if file_name.starts_with("ceph-mon") {
                ceph_processes.push(CephType::Mon);
            } else if file_name.starts_with("ceph-osd") {
                ceph_processes.push(CephType::Osd);
            } else if file_name.starts_with("ceph-mds") {
                ceph_processes.push(CephType::Mds);
            } else if file_name.starts_with("ceph-rgw") {
                ceph_processes.push(CephType::Rgw);
            }
        }
        Ok(ceph_processes)
    }

    fn release_specific(
        &self,
        from: &CephVersion,
        to: &CephVersion,
        ceph_type: &CephType,
        handle: rados_t,
    ) -> IOResult<()> {
        if from == &CephVersion::Hammer && to == &CephVersion::Jewel {
            debug!("Hammer -> Jewel specific");
            // For a given ceph daemon type is there anything specific that needs doing before
            // it get restarted?
            match ceph_type {
                &CephType::Mds => {}
                &CephType::Mgr => {}
                &CephType::Mon => {
                    config_set(
                        Path::new("/etc/ceph/ceph.conf"),
                        "global",
                        "setuser match path",
                        "/var/lib/ceph/$type/$cluster-$id",
                    )?;
                }
                &CephType::Osd => {
                    config_set(
                        Path::new("/etc/ceph/ceph.conf"),
                        "global",
                        "setuser match path",
                        "/var/lib/ceph/$type/$cluster-$id",
                    )?;
                    debug!("Checking if osd owner needs updating");
                    //update_owner(
                    //   &Path::new(&format!("/var/lib/ceph/osd/ceph-{}", child)),
                    //  true,
                    //)?;
                }
                &CephType::Rgw => {}
            }
        } else if from == &CephVersion::Jewel && to == &CephVersion::Luminous {
            // ceph auth caps client.<ID> mon 'allow r, allow command "osd blacklist"'
            // osd '<existing OSD caps for user>'
            // Add or restart ceph-mgr daemons
            // ceph osd require-osd-release luminous

            debug!("Jewel -> Luminous specific");
            // For a given ceph daemon type is there anything specific that needs doing before
            // it get restarted?
            match ceph_type {
                &CephType::Mds => {}
                &CephType::Mgr => {}
                &CephType::Mon => {
                    // ceph osd set sortbitwise

                    // Startup a mgr on every mon
                    let hostname = super::get_hostname()?;
                    mgr_auth_add(handle, &hostname, false).map_err(|e| {
                        Error::new(ErrorKind::Other, e)
                    })?;
                    debug!("Getting mgr keyring");
                    let mgr_key = auth_get_key(handle, "mgr", &hostname).map_err(|e| {
                        Error::new(ErrorKind::Other, e)
                    })?;
                    create_dir(format!("/var/lib/ceph/mgr/ceph-{}", hostname))?;
                    save_keyring("mgr", &hostname, &mgr_key)?;
                    self.enable_mgr(&hostname)?;
                    self.start_mgr(&hostname)?;
                }
                &CephType::Osd => {}
                &CephType::Rgw => {}
            }
        }
        Ok(())
    }

    // Check with Ceph to see if the client has returned to healthy in the
    // cluster
    fn is_healthy(&self, handle: rados_t, c: &CephType, id: &str) -> IOResult<bool> {
        match c {
            &CephType::Mds => Ok(true),
            &CephType::Mgr => {
                // check for available
                let mgrs = mgr_dump(handle).map_err(
                    |e| Error::new(ErrorKind::Other, e),
                )?;
                Ok(true)
            }
            &CephType::Mon => {
                // run mon_status.  What do I check for?
                let status = mon_status(handle).map_err(
                    |e| Error::new(ErrorKind::Other, e),
                )?;
                let mon_rank: Option<&Mon> =
                    status.monmap.mons.iter().find(|mon| if mon.name == id {
                        true
                    } else {
                        false
                    });
                match mon_rank {
                    Some(ref mon) => {
                        if status.quorum.contains(&mon.rank) {
                            // This means the mon is back in quorum
                            Ok(true)
                        } else {
                            // This means the mon either connecting or having a problem
                            Ok(false)
                        }
                    }
                    None => {
                        Err(Error::new(
                            ErrorKind::Other,
                            format!("Unable to find mon {} in {:?}", id, status),
                        ))
                    }
                }
            }
            &CephType::Osd => {
                // run osd_tree and check for up/in?
                let tree = osd_tree(handle).map_err(
                    |e| Error::new(ErrorKind::Other, e),
                )?;
                Ok(true)
            }
            &CephType::Rgw => Ok(true),
        }
    }
}

// Search around for a ceph socket
fn find_socket(c: &CephType) -> IOResult<PathBuf> {
    debug!("Opening /var/run/ceph to find {} sockets to connect to", c);
    let p = Path::new("/var/run/ceph");
    for entry in p.read_dir()? {
        if let Ok(entry) = entry {
            trace!("dir entry: {:?}", entry);
            let file_str = entry.file_name().to_string_lossy().into_owned();
            match c {
                &CephType::Mon => {
                    if file_str.starts_with("ceph-mon") && file_str.ends_with("asok") {
                        return Ok(entry.path());
                    }
                }
                &CephType::Osd => {
                    if file_str.starts_with("ceph-osd") && file_str.ends_with("asok") {
                        return Ok(entry.path());
                    }
                }
                &CephType::Mds => {
                    if file_str.starts_with("ceph-mds") && file_str.ends_with("asok") {
                        return Ok(entry.path());
                    }
                }
                &CephType::Rgw => {
                    if file_str.starts_with("ceph-rgw") && file_str.ends_with("asok") {
                        return Ok(entry.path());
                    }
                }
                &CephType::Mgr => {
                    if file_str.starts_with("ceph-mgr") && file_str.ends_with("asok") {
                        return Ok(entry.path());
                    }
                }
            }
        }
    }
    Err(Error::new(ErrorKind::Other, "Unable to find ceph socket"))
}

// Get the running version of a ceph type ie Mon, Osd, etc
pub fn get_running_version(c: &CephType) -> IOResult<SemVer> {
    let socket = find_socket(c).map_err(|e| Error::new(ErrorKind::Other, e))?;
    debug!("Connecting to socket: {}", socket.display());
    let v = match ceph_version(&format!("{}", socket.display())) {
        Some(v) => v,
        None => {
            error!("Unable to discover ceph version.  Can't discern correct user");
            return Err(Error::new(ErrorKind::Other, "Unable to find ceph version"));
        }
    };
    let ceph_version = SemVer::parse(&v).map_err(
        |e| Error::new(ErrorKind::Other, e),
    )?;
    Ok(ceph_version)
}

fn config_set(p: &Path, section: &str, key: &str, value: &str) -> IOResult<()> {
    let mut conf = Ini::load_from_file(p).map_err(
        |e| Error::new(ErrorKind::Other, e),
    )?;
    conf.with_section(Some(section.to_string())).set(key, value);
    conf.write_to_file(p)?;
    Ok(())
}

fn ceph_release() -> IOResult<CephVersion> {
    debug!("Getting ceph release");
    let v = apt::get_installed_package_version("ceph").map_err(|e| {
        Error::new(ErrorKind::Other, e)
    })?;
    debug!("apt installed version: {}", v);
    let ceph_version = semver_from_debian_version(&v)?;
    debug!("ceph version: {}", ceph_version);
    match ceph_version.major {
        0 => {
            match ceph_version.minor {
                67 => Ok(CephVersion::Dumpling),
                72 => Ok(CephVersion::Emperor),
                80 => Ok(CephVersion::Firefly),
                87 => Ok(CephVersion::Giant),
                94 => Ok(CephVersion::Hammer),
                _ => Ok(CephVersion::Unknown),
            }
        }
        9 => Ok(CephVersion::Infernalis),
        10 => Ok(CephVersion::Jewel),
        11 => Ok(CephVersion::Kraken),
        12 => Ok(CephVersion::Luminous),
        _ => Ok(CephVersion::Unknown),
    }
}

fn ceph_user(c: &CephVersion) -> String {
    match c {
        &CephVersion::Dumpling => "root".into(),
        &CephVersion::Emperor => "root".into(),
        &CephVersion::Firefly => "root".into(),
        &CephVersion::Giant => "root".into(),
        &CephVersion::Hammer => "root".into(),
        &CephVersion::Infernalis => "ceph".into(),
        &CephVersion::Jewel => "ceph".into(),
        &CephVersion::Kraken => "ceph".into(),
        &CephVersion::Luminous => "ceph".into(),

        //TODO what should we return here?.  I can't figure out what the ceph version is
        &CephVersion::Unknown => "root".into(),
    }
}

// Save a keyring
fn save_keyring(client: &str, id: &str, key: &str) -> IOResult<()> {
    let base_dir = format!("/var/lib/ceph/{}/ceph-{}", client, id);
    if !Path::new(&base_dir).exists() {
        return Err(Error::new(
            ErrorKind::NotFound,
            format!("{} directory doesn't exist", base_dir),
        ));
    }
    debug!("Creating {}/keyring", base_dir);
    let mut f = File::create(format!("{}/keyring", base_dir))?;
    f.write_all(
        format!("[{}.{}]\n\tkey = {}\n", client, id, key)
            .as_bytes(),
    )?;
    Ok(())
}

// Any final things that need to happen that can be set from a ceph monitor
// This is optional as some of these settings can cause significant data movement
// or break older clients
pub fn final_checks(from: &CephVersion, to: &CephVersion) -> IOResult<()> {
    // Any final things that need to be done
    if from == &CephVersion::Hammer && to == &CephVersion::Jewel {
        // Hammer -> Jewel sortbitwise
        // ceph osd set required_jewel_osds
        // ceph osd crush tuanbles optimal
        debug!("Hammer -> Jewel final checks");
    } else if from == &CephVersion::Jewel && to == &CephVersion::Luminous {
        // Jewel -> Luminous
        debug!("Jewel -> Luminous final checks");
    }
    Ok(())
}


///Changes the ownership of the specified path.
///Changes the ownership of the specified path to the new ceph daemon user
///using the system's native chown functionality. This may take awhile,
///so this method will issue a set_status for any changes of ownership which
///recurses into directory structures.
fn update_owner(path: &Path, recurse_dirs: bool, release: &CephVersion) -> IOResult<()> {
    let user = ceph_user(release);
    let uid = match get_user_by_name(&user) {
        Some(user) => user,
        None => {
            return Err(Error::new(
                ErrorKind::Other,
                format!("uid for {} not found", user),
            ));
        }
    };
    debug!("Changing ownership of {:?} to {}", path, user);
    let start = SystemTime::now();
    if recurse_dirs {
        for entry in WalkDir::new(path) {
            chown(
                entry?.path(),
                Some(Uid::from_raw(uid.uid())),
                Some(Gid::from_raw(uid.primary_group_id())),
            ).map_err(|e| Error::new(ErrorKind::Other, e))?;
        }
    } else {
        chown(
            path,
            Some(Uid::from_raw(uid.uid())),
            Some(Gid::from_raw(uid.primary_group_id())),
        ).map_err(|e| Error::new(ErrorKind::Other, e))?;
    }
    let elapsed_time = start.duration_since(start).map_err(|e| {
        Error::new(ErrorKind::Other, e)
    })?;

    debug!(
        "Took {} seconds to change the ownership of path: {:?}",
        elapsed_time.as_secs(),
        path
    );
    Ok(())
}
