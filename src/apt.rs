extern crate apt_pkg_native;
extern crate reqwest;

use std::fs::File;
use std::io::{Error, ErrorKind, Read, Write};
use std::io::Result as IOResult;
use std::path::Path;
use std::process::{Command, Stdio};

use self::apt_pkg_native::Cache;
use super::debian::version::Version;

// Add a ppa source to apt
pub fn add_source(source_string: &str) -> IOResult<()> {
    let sources = {
        let mut f = File::open("/etc/apt/sources.list")?;
        let mut buff = String::new();
        f.read_to_string(&mut buff)?;
        buff
    };
    if sources.contains(source_string) {
        debug!("sources.list already contains {}. Skipping", source_string);
        return Ok(());
    }
    let mut cmd = Command::new("add-apt-repository");
    cmd.arg("-y");
    cmd.arg(source_string);
    debug!("add-apt-repository cmd: {:?}", cmd);
    let output = cmd.output()?;
    if !output.status.success() {
        return Err(Error::new(
            ErrorKind::Other,
            String::from_utf8_lossy(&output.stderr).into_owned(),
        ));
    }
    return Ok(());
}

// Update the apt database to get the latest packages
pub fn apt_update() -> IOResult<()> {
    let mut cmd = Command::new("apt-get");
    cmd.arg("update");
    cmd.arg("-q");
    debug!("apt-get cmd {:?}", cmd);
    let output = cmd.output()?;
    if !output.status.success() {
        return Err(Error::new(
            ErrorKind::Other,
            String::from_utf8_lossy(&output.stderr).into_owned(),
        ));
    }
    return Ok(());
}

/// Install a list of packages
pub fn apt_install(packages: Vec<&str>) -> IOResult<()> {
    let mut cmd = Command::new("apt-get");
    cmd.arg("install");
    cmd.arg("-q");
    cmd.arg("-y");
    for package in packages {
        cmd.arg(package);
    }
    debug!("apt install cmd: {:?}", cmd);
    let output = cmd.output()?;
    if !output.status.success() {
        return Err(Error::new(
            ErrorKind::Other,
            String::from_utf8_lossy(&output.stderr).into_owned(),
        ));
    }
    return Ok(());
}

/// Remove a list of packages
pub fn apt_remove(packages: Vec<&str>) -> IOResult<()> {
    let mut cmd = Command::new("apt-get");
    cmd.arg("remove");
    cmd.arg("-q");
    cmd.arg("-y");
    for package in packages {
        cmd.arg(package);
    }
    cmd.arg("--purge");
    debug!("apt remove cmd: {:?}", cmd);
    let output = cmd.output()?;
    if !output.status.success() {
        return Err(Error::new(
            ErrorKind::Other,
            String::from_utf8_lossy(&output.stderr).into_owned(),
        ));
    }
    return Ok(());

}

/// Create the apt proxy file so that apt can reach the ceph upstream repo from behind
/// a firewall
fn create_apt_proxy(http_endpoint: &str, https_endpoint: &str) -> IOResult<usize> {
    debug!("Ensuring apt proxy exists");
    let mut bytes_written = 0;
    let mut f = File::create("/etc/apt/apt.conf.d/60proxy")?;
    bytes_written += f.write(
        format!("Acquire::http::Proxy \"{}\";", http_endpoint)
            .as_bytes(),
    )?;
    bytes_written += f.write(
        format!("Acquire::https::Proxy \"{}\";", https_endpoint)
            .as_bytes(),
    )?;

    Ok(bytes_written)
}

/// Ensure that the /etc/apt/apt.conf.d/60proxy is in place with the proper
/// endpoints
pub fn ensure_proxy(http_endpoint: &str, https_endpoint: &str) -> IOResult<()> {
    if !Path::new("/etc/apt/apt.conf.d/60proxy").exists() {
        create_apt_proxy(http_endpoint, https_endpoint)?;
    }
    Ok(())
}

// Get the GPG key for the ceph repo
pub fn get_gpg_key(l: &str) -> IOResult<()> {
    let mut resp = reqwest::get(l).map_err(|e| Error::new(ErrorKind::Other, e))?;
    if resp.status().is_success() {
        let mut content = String::new();
        resp.read_to_string(&mut content)?;
        let child = Command::new("apt-key")
            .stdin(Stdio::piped())
            .args(&["add", "-"])
            .spawn()?;
        if let Some(mut stdin) = child.stdin {
            stdin.write(&content.as_bytes())?;
            return Ok(());
        } else {
            return Err(Error::new(
                ErrorKind::Other,
                "Stdin to apt-key failed.  Unable to save gpg key"
                    .to_string(),
            ));
        }
    } else {
        return Err(Error::new(
            ErrorKind::Other,
            format!("Unable to download gpg key: {}", resp.status()),
        ));
    }
}

pub fn get_candidate_package_version(package_name: &str) -> Result<Version, String> {
    let mut cache = Cache::get_singleton();
    cache.reload();
    let mut found = cache.find_by_name(package_name);

    if let Some(view) = found.next() {
        match view.candidate_version() {
            Some(candidate) => return Ok(Version::parse(&candidate).map_err(|e| e.msg)?),
            None => return Err(format!("Unable to locate package {}", package_name)),
        }
    };
    Err(format!("Unable to locate package {}", package_name))
}

pub fn get_installed_package_version(package_name: &str) -> Result<Version, String> {
    let mut cache = Cache::get_singleton();
    cache.reload();
    let mut found = cache.find_by_name(package_name);

    if let Some(view) = found.next() {
        match view.current_version() {
            Some(installed) => return Ok(Version::parse(&installed).map_err(|e| e.msg)?),
            None => return Err(format!("Unable to locate package {}", package_name)),
        }
    }
    Err(format!("Unable to locate package {}", package_name))
}
