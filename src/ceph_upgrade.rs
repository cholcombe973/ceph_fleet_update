extern crate ceph_rust;
extern crate chrono;
extern crate init_daemon;
extern crate nix;
extern crate rand;
extern crate reqwest;
extern crate regex;
extern crate semver;
extern crate uuid;

use std::fs::{copy, File, metadata, read_dir, remove_file};
use std::io::{Error, ErrorKind, BufRead, Write};
use std::io::Result as IOResult;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::str::FromStr;
use std::time::SystemTime;

use self::ceph_rust::ceph::{connect_to_ceph, ceph_version, disconnect_from_ceph};
use self::ceph_rust::cmd::{mon_dump, osd_tree};
use self::init_daemon::{detect_daemon, Daemon};
use self::nix::unistd::chown;
use self::semver::Version as SemVer;

use super::apt;
use super::debian::version::Version;
use super::os_type;

fn backup_conf_files() -> IOResult<Vec<PathBuf>> {
    debug!("Backing up /etc/ceph config files to /tmp");
    let mut backed_up = Vec::new();
    for entry in read_dir("/etc/ceph")? {
        let entry = entry?;
        let path = entry.path();
        // Should only be conf files in here
        if !path.is_dir() {
            // cp /etc/ceph/ceph.conf /tmp/ceph.conf
            copy(path, format!("/tmp/{}", path.display()))?;
        }
    }
    Ok(backed_up)
}

fn restore_conf_files(files: &Vec<PathBuf>) -> IOResult<()> {
    debug!("Restoring config files to /etc/ceph");
    for f in files {
        copy(f, format!("/etc/ceph/{}", f.display()))?;
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CephType {
    Mon,
    Osd,
    Mds,
    Rgw,
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
}

/// A server.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CephServer {
    pub ip_addr: IpAddr,
    pub id: String,
    pub rank: Option<i64>,
}

#[derive(Debug)]
pub struct CephNode {
    pub os_information: os_type::OSInformation,
}

pub fn discover_topology() -> Result<Vec<(CephServer, CephType)>, String> {
    let mut cluster: Vec<(CephServer, CephType)> = Vec::new();
    let handle = connect_to_ceph("admin", "/etc/ceph/ceph.conf").map_err(
        |e| {
            e.to_string()
        },
    )?;

    let mon_info = mon_dump(handle).map_err(|e| e.to_string())?;
    for mon in mon_info.mons {
        cluster.push((
            CephServer {
                ip_addr: IpAddr::from_str(mon.addr.split("").next().unwrap())
                    .map_err(|e| e.to_string())?,
                id: mon.name,
                rank: Some(mon.rank),
            },
            CephType::Mon,
        ));
    }
    let osd_info = osd_tree(handle).map_err(|e| e.to_string())?;
    for osd in osd_info.nodes {
        cluster.push((
            CephServer {
                ip_addr: IpAddr::from_str("").map_err(|e| e.to_string())?,
                id: "".to_string(),
                rank: None,
            },
            CephType::Osd,
        ))
    }
    disconnect_from_ceph(handle);
    Ok(cluster)
}

fn connect_and_upgrade(servers: Vec<CephServer>, port: u16) -> Result<(), String> {
    for s in servers {
        debug!("Connecting to {} to request upgrade", s.ip_addr);
        let req_socket = super::connect(&s.ip_addr.to_string(), port).map_err(|e| {
            e.to_string()
        })?;
        debug!("Requesting {} to upgrade", s.ip_addr);
        match super::upgrade_request(&mut req_socket, "") {
            Ok(_) => {
                info!("Upgrade succeeded.  Proceeding to next");
                continue;
            }
            Err(e) => {
                error!("Upgrade failed.  Recording error");
                continue;
            }
        };
    }
    Ok(())
}

///Main function to call which implements the upgrade logic
pub fn roll_cluster(
    new_version: &Version,
    hosts: Vec<(CephServer, CephType)>,
    port: u16,
) -> Result<(), String> {
    // Upgrade all mons in the cluster 1 by 1
    // Inspect the cluster health to make sure the mon upgrades were successful
    // Upgrade all osds in the cluster 1 by 1
    // Inspect the cluster health to make sure the osd upgrades were successful
    // Upgrade all mds in the cluster 1 by 1
    // Upgrade all rgw in the cluster 1 by 1
    let mons: Vec<CephServer> = hosts
        .iter()
        .filter(|c| c.1 == CephType::Mon)
        .map(|c| c.0)
        .collect();
    let osds: Vec<CephServer> = hosts
        .iter()
        .filter(|c| c.1 == CephType::Osd)
        .map(|c| c.0)
        .collect();
    let mds: Vec<CephServer> = hosts
        .iter()
        .filter(|c| c.1 == CephType::Mds)
        .map(|c| c.0)
        .collect();
    let rgws: Vec<CephServer> = hosts
        .iter()
        .filter(|c| c.1 == CephType::Rgw)
        .map(|c| c.0)
        .collect();
    connect_and_upgrade(mons, port)?;
    connect_and_upgrade(osds, port)?;
    connect_and_upgrade(mds, port)?;
    connect_and_upgrade(rgws, port)?;
    return Ok(());
}

// Edge cases:
// 1. Previous node dies on upgrade, can we retry?
impl CephNode {
    pub fn new() -> Self {
        CephNode { os_information: os_type::current_platform() }
    }
    pub fn upgrade_node(&self, version: &str) -> Result<(), String> {
        debug!(
            "Upgrading from {} to {}",
            ceph_version("/var/run/ceph/...").unwrap_or("".into()),
            version
        );
        return Ok(());
    }
    fn upgrade_osd(&self, version: String) -> Result<(), String> {
        /*
echo "Stopping osds"
systemctl stop ceph-osd.target
wget -q -O- 'https://download.ceph.com/keys/release.asc' | sudo apt-key add -

echo "Setting ceph upstream apt source"
echo "deb https://download.ceph.com/debian-jewel/ xenial main" >> /etc/apt/sources.list
echo "# deb-src https://download.ceph.com/debian-jewel/ xenial main" >> /etc/apt/sources.list

apt-get update
echo "Removing ceph"
apt-get install -y ceph
echo "Starting osds"
systemctl start ceph-osd.target
*/
        debug!(
            "Upgrading from {} to {}",
            ceph_version("/var/run/ceph/...").unwrap_or("".into()),
            version
        );
        // install apt proxy if needed
        // install ceph sources if needed
        // apt-get update
        // stop osd
        self.stop_osd(0);
        //backup ceph conf files
        // Check if these packages exist and remove all
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
        ])?;
        self.disable_osd(0);
        //
        apt::apt_install(vec!["ceph"])?;
        update_owner(&Path::new("/var/lib/ceph/osd-?"), true);
        self.enable_osd(0);
        self.start_osd(0);

        return Ok(());
    }

    ///Stops the specified OSD number.
    fn stop_osd(&self, osd_num: u64) -> ::std::io::Result<()> {
        let init_daemon = detect_daemon().map_err(|e| {
            ::std::io::Error::new(::std::io::ErrorKind::Other, e)
        })?;
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
    fn start_osd(&self, osd_num: u64) -> ::std::io::Result<()> {
        let init_daemon = detect_daemon().map_err(|e| {
            ::std::io::Error::new(::std::io::ErrorKind::Other, e)
        })?;
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
    fn disable_osd(&self, osd_num: u64) -> ::std::io::Result<()> {
        let init_daemon = detect_daemon().map_err(|e| {
            ::std::io::Error::new(::std::io::ErrorKind::Other, e)
        })?;
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
            }
            _ => {}
        };
        // Neither upstart nor the ceph-osd upstart script provides for
        // disabling the starting of an OSD automatically. The specific OSD
        // cannot be prevented from running manually, however it can be
        // prevented from running automatically on reboot by removing the
        // 'ready' file in the OSD's root directory. This is due to the
        // ceph-osd-all upstart script checking for the presence of this file
        // before starting the OSD.
        let ready_file: PathBuf = ["/var/lib/ceph", &format!("ceph-{}", osd_num), "ready"]
            .iter()
            .collect();
        if ready_file.exists() {
            remove_file(ready_file);
        }
        Ok(())
    }
    ///Enables the specified OSD number.
    ///Ensures that the specified osd_num will be enabled and ready to start
    ///automatically in the event of a reboot.
    ///osd_num: the osd id which should be enabled.
    fn enable_osd(&self, osd_num: u64) -> ::std::io::Result<()> {
        let init_daemon = detect_daemon().map_err(|e| {
            ::std::io::Error::new(::std::io::ErrorKind::Other, e)
        })?;
        match init_daemon {
            Daemon::Systemd => {
                let output = Command::new("systemctl")
                    .args(&["enable", &format!("ceph-osd@{}", osd_num)])
                    .output()?;
            }
            _ => {}
        };
        // When running on upstart, the OSDs are started via the ceph-osd-all
        // upstart script which will only start the osd if it has a 'ready'
        // file. Make sure that file exists.
        let ready_file_path: PathBuf = ["/var/lib/ceph", &format!("ceph-{}", osd_num), "ready"]
            .iter()
            .collect();
        let mut file = File::create(ready_file_path)?;
        file.write_all(b"ready")?;

        // Make sure the correct user owns the file. It shouldn't be necessary
        // as the upstart script should run with root privileges, but its better
        // to have all the files matching ownership.
        update_owner(&ready_file_path, true);
        Ok(())
    }

    // Examine a node and return a list of running ceph processes on it
    fn scan_node_for_ceph_processes(&self) -> Result<Vec<CephType>, String> {
        //TODO: Scan the crushmap to get this info?
        let mut ceph_processes: Vec<CephType> = Vec::new();
        for entry in try!(read_dir(Path::new("/var/run/ceph")).map_err(
            |e| e.to_string(),
        ))
        {
            let entry = try!(entry.map_err(|e| e.to_string()));
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
}

// Search around for a ceph socket
fn find_socket(c: CephType) -> IOResult<PathBuf> {
    debug!("Opening /var/run/ceph to find sockets to connect to");
    let p = Path::new("/var/run/ceph");
    for entry in p.read_dir()? {
        if let Ok(entry) = entry {
            match c {
                CephType::Mon => {
                    if entry.file_name().to_string_lossy().starts_with("ceph-mon") {
                        return Ok(entry.path());
                    }
                }
                CephType::Osd => {
                    if entry.file_name().to_string_lossy().starts_with("ceph-osd") {
                        return Ok(entry.path());
                    }
                }
                CephType::Mds => {
                    if entry.file_name().to_string_lossy().starts_with("ceph-mds") {
                        return Ok(entry.path());
                    }
                }
                CephType::Rgw => {
                    if entry.file_name().to_string_lossy().starts_with("ceph-rgw") {
                        return Ok(entry.path());
                    }
                }
            }
        }
    }
    return Err(Error::new(ErrorKind::Other, "Unable to find ceph socket"));
}

// Get the running version of a ceph type ie Mon, Osd, etc
pub fn get_running_version(c: CephType) -> IOResult<SemVer> {
    let socket = find_socket(c)?;
    let v = match ceph_version(&format!("/var/run/ceph/{}", socket.display())) {
        Some(v) => v,
        None => {
            error!("Unable to discover ceph version.  Can't discern correct user");
            return Err(Error::new(
                ErrorKind::Other,
                "Unable to find ceph version".into(),
            ));
        }
    };
    let ceph_version = SemVer::parse(&v).map_err(
        |e| Error::new(ErrorKind::Other, e),
    )?;
    Ok(ceph_version)
}

fn ceph_release(socket: &str) -> Option<CephVersion> {
    let v = match ceph_version(&format!("/var/run/ceph/{}", socket)) {
        Some(v) => v,
        None => {
            error!("Unable to discover ceph version.  Can't discern correct user");
            return None;
        }
    };
    let ceph_version = match SemVer::parse(&v) {
        Ok(v) => v,
        Err(e) => {
            error!("Semver failed to parse ceph version: {}", &v);
            return None;
        }
    };
    match ceph_version.major {
        0 => {
            match ceph_version.minor {
                67 => Some(CephVersion::Dumpling),
                72 => Some(CephVersion::Emperor),
                80 => Some(CephVersion::Firefly),
                87 => Some(CephVersion::Giant),
                94 => Some(CephVersion::Hammer),
                _ => None,
            }
        }
        9 => Some(CephVersion::Infernalis),
        10 => Some(CephVersion::Jewel),
        11 => Some(CephVersion::Kraken),
        12 => Some(CephVersion::Luminous),
        _ => None,
    }
}

fn ceph_user(c: CephType) -> Result<String, String> {
    let socket = match c {
        CephType::Mds => "",
        CephType::Mon => "",
        CephType::Osd => "",
        CephType::Rgw => "",
    };
    let release = ceph_release("");
    Ok("ceph".into())
}

///Changes the ownership of the specified path.
///Changes the ownership of the specified path to the new ceph daemon user
///using the system's native chown functionality. This may take awhile,
///so this method will issue a set_status for any changes of ownership which
///recurses into directory structures.
fn update_owner(path: &Path, recurse_dirs: bool) -> ::std::io::Result<()> {
    let user = ceph_user(CephType::Mon).unwrap();
    let user_group = format!("{ceph_user}:{ceph_user}", ceph_user = user);
    let mut cmd: Vec<String> = vec![
        "chown".into(),
        user_group,
        path.to_string_lossy().into_owned(),
    ];
    if metadata(path)?.is_dir() && recurse_dirs {
        cmd.insert(1, "-R".into());
    }
    debug!("Changing ownership of {:?} to {}", path, user_group);
    let start = SystemTime::now();
    Command::new("chown")
        .args(&[user_group, path.to_string_lossy().into_owned()])
        .output()?;
    let elapsed_time = start.duration_since(start).unwrap();

    debug!(
        "Took {} seconds to change the ownership of path: {:?}",
        elapsed_time.as_secs(),
        path
    );
    Ok(())
}
