extern crate api;
extern crate ceph_rust;
extern crate chrono;
extern crate init_daemon;
extern crate nix;
extern crate rand;
extern crate reqwest;
extern crate regex;
extern crate semver;
extern crate users;
extern crate uuid;
extern crate walkdir;

use std::fmt;
use std::fs::{copy, File, read_dir, remove_file};
use std::io::{Error, ErrorKind, Read, Write};
use std::io::Result as IOResult;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::SystemTime;

use self::api::service::Version as ApiVersion;
use self::api::service::CephComponent;
use self::ceph_rust::ceph::{connect_to_ceph, ceph_version, disconnect_from_ceph};
use self::ceph_rust::cmd::{mon_dump, osd_unset, osd_set, osd_tree};
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
enum CephVersion {
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
    debug!("semver vers: {}", vers);
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
                //ApiVersion::from(new_version),
                &super::version_to_protobuf(new_version),
                CephComponent::from(c.clone()),
                http_proxy,
                https_proxy,
                gpg_key,
                apt_source,
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
    )?;
    // TODO: How can I do the final checks here??
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
    ) -> Result<(), String> {
        debug!(
            "Upgrading from {:?} to {:?}",
            apt::get_installed_package_version("ceph")?,
            version
        );
        let from_release = ceph_release().map_err(|e| e.to_string())?;
        debug!("from_release: {}", from_release);
        let handle = connect_to_ceph("admin", "/etc/ceph/ceph.conf").map_err(
            |e| {
                e.to_string()
            },
        )?;
        debug!("Connected to ceph: {:?}", handle);
        let backed_files = backup_conf_files().map_err(|e| e.to_string())?;
        debug!("Backed up conf files: {:?}", backed_files);

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
        self.upgrade(&CephType::from(c), version).map_err(
            |e| e.to_string(),
        )?;
        restore_conf_files(&backed_files).map_err(|e| e.to_string())?;
        // Record which release we upgraded to
        let to_release = ceph_release().map_err(|e| e.to_string())?;

        match c {
            CephComponent::Mgr => {}
            CephComponent::Mds => {
                debug!("Performing possible release specific mds changes");
                self.release_specific(&from_release, &to_release, &CephType::Mds)
                    .map_err(|e| e.to_string())?;
                debug!("Restarting mds");
                self.restart_mds().map_err(|e| e.to_string())?;
            }
            CephComponent::Mon => {
                debug!("Performing possible release specific mon changes");
                self.release_specific(&from_release, &to_release, &CephType::Mon)
                    .map_err(|e| e.to_string())?;
                debug!("Restarting mon");
                self.restart_mon().map_err(|e| e.to_string())?;
            }
            CephComponent::Osd => {
                debug!("Performing possible release specific osd changes");
                self.release_specific(&from_release, &to_release, &CephType::Osd)
                    .map_err(|e| e.to_string())?;
                debug!("Restarting osd");
                debug!("Setting noout, nodown to prevent cluster rebuilding");
                osd_set(handle, "noout", false, false).map_err(
                    |e| e.to_string(),
                )?;
                osd_set(handle, "nodown", false, false).map_err(
                    |e| e.to_string(),
                )?;
                self.restart_osd().map_err(|e| e.to_string())?;
                debug!("Unsetting noout, nodown");
                osd_unset(handle, "noout", false).map_err(|e| e.to_string())?;
                osd_unset(handle, "nodown", false).map_err(
                    |e| e.to_string(),
                )?;
            }
            CephComponent::Rgw => {
                debug!("Performing possible release specific rgw changes");
                self.release_specific(&from_release, &to_release, &CephType::Rgw)
                    .map_err(|e| e.to_string())?;
                debug!("Restarting rgw");
                self.restart_rgw().map_err(|e| e.to_string())?;
            }
        };

        return Ok(());
    }

    fn upgrade(&self, c: &CephType, from_version: &ApiVersion) -> IOResult<()> {
        info!("Beginning upgrade procedure");
        //basic upgrade things that all nodes need to do

        // debian version
        let candidate = apt::get_candidate_package_version("ceph").map_err(|e| {
            Error::new(ErrorKind::Other, e)
        })?;
        let candidate_version = super::version_to_protobuf(&candidate);

        if from_version != &candidate_version {
            error!(
                "Candidate version {:?} doesn't match requested version: {:?}",
                candidate_version,
                from_version
            );
            // TODO: Should we abort the upgrade if the requested version isn't matching?
        }
        let deb_installed_version = apt::get_installed_package_version("ceph").map_err(|e| {
            Error::new(ErrorKind::Other, e)
        })?;
        let installed_version = semver_from_debian_version(&deb_installed_version)?;
        debug!("installed_version: {}", installed_version);
        let running_version = get_running_version(c)?;
        debug!("running_version: {}", running_version);

        // Collocated ceph services.  IE Mon + OSD.  Mon was already upgraded.
        // Now we just need to restart the osd.
        if installed_version >= running_version {
            debug!("Installed version of ceph > than running version");
            // Return early
            return Ok(());
        }

        // All other cases require an upgrade of installed ceph packages
        // TODO: Put this behind a feature flag.  This is probably overkill
        // except on broken systems.
        apt::apt_remove(vec![
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
        ]).map_err(|e| Error::new(ErrorKind::Other, e))?;

        apt::apt_install(vec!["ceph"]).map_err(|e| {
            Error::new(ErrorKind::Other, e)
        })?;
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
                let output = c.output()?;
                if !output.status.success() {
                    return Err(Error::last_os_error());
                }
            }
            _ => {
                let mut c = Command::new("systemctl");
                c.arg("restart");
                match ceph_type {
                    CephType::Mon => c.arg("ceph-mon-all"),
                    CephType::Mds => c.arg("ceph-mds-all"),
                    CephType::Rgw => c.arg("ceph-rgw-all"),
                    _ => c.arg("ceph-mon-all"),
                };
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
        let hostname = {
            let mut f = File::open("/etc/hostname")?;
            let mut buff = String::new();
            f.read_to_string(&mut buff)?;
            // /etc/hostname seems to have a trailing \n
            buff.trim().to_string()
        };
        let h = connect_to_ceph("admin", "/etc/ceph/ceph.conf").map_err(
            |e| {
                Error::new(ErrorKind::Other, e)
            },
        )?;
        let osd_tree = ceph_rust::cmd::osd_tree(h).map_err(|e| {
            Error::new(ErrorKind::Other, e)
        })?;
        debug!("osd_tree: {:?}", osd_tree);
        // This should filter down to 1 node
        let hosts: Vec<&ceph_rust::cmd::CrushNode> = osd_tree
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
    ) -> IOResult<()> {
        if from == &CephVersion::Hammer && to == &CephVersion::Jewel {
            debug!("Hammer -> Jewel specific");
            // For a given ceph daemon type is there anything specific that needs doing before
            // it get restarted?
            match ceph_type {
                &CephType::Mds => {}
                &CephType::Mgr => {}
                &CephType::Mon => {}
                &CephType::Osd => {
                    //setuser match path = /var/lib/ceph/$type/$cluster-$id
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
                }
                &CephType::Osd => {}
                &CephType::Rgw => {}
            }
        }
        Ok(())
    }
}

// Search around for a ceph socket
fn find_socket(c: &CephType) -> IOResult<PathBuf> {
    debug!("Opening /var/run/ceph to find {} sockets to connect to", c);
    let p = Path::new("/var/run/ceph");
    for entry in p.read_dir()? {
        if let Ok(entry) = entry {
            trace!("dir entry: {:?}", entry);
            match c {
                &CephType::Mon => {
                    if entry.file_name().to_string_lossy().starts_with("ceph-mon") {
                        return Ok(entry.path());
                    }
                }
                &CephType::Osd => {
                    if entry.file_name().to_string_lossy().starts_with("ceph-osd") {
                        return Ok(entry.path());
                    }
                }
                &CephType::Mds => {
                    if entry.file_name().to_string_lossy().starts_with("ceph-mds") {
                        return Ok(entry.path());
                    }
                }
                &CephType::Rgw => {
                    if entry.file_name().to_string_lossy().starts_with("ceph-rgw") {
                        return Ok(entry.path());
                    }
                }
                &CephType::Mgr => {
                    if entry.file_name().to_string_lossy().starts_with("ceph-mgr") {
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
    debug!("ceph version from socket: {}", v);
    let ceph_version = SemVer::parse(&v).map_err(
        |e| Error::new(ErrorKind::Other, e),
    )?;
    debug!("ceph version from semver: {}", ceph_version);
    Ok(ceph_version)
}

fn ceph_release() -> IOResult<CephVersion> {
    debug!("Getting ceph release");
    let v = apt::get_installed_package_version("ceph").map_err(|e| {
        error!("get_installed_package_version failed: {:?}", e);
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
