extern crate api;
#[macro_use]
extern crate clap;
extern crate debian;
#[macro_use]
extern crate log;
extern crate os_type;
extern crate protobuf;
extern crate semver;
extern crate simplelog;
extern crate slack_hook;
extern crate uuid;
extern crate zmq;

mod apt;
mod ceph_upgrade;

use std::fs::File;
use std::io::{Error, ErrorKind};
use std::io::Result as IOResult;
use std::process::Command;
use std::str::FromStr;
use std::thread;
use std::time::Duration;

use api::service::{CephComponent, Op, Operation, ResultType, OpResult, Version, VersionElement,
                   VersionPart, VersionResult};
use ceph_upgrade::{get_running_version, CephType, CephNode};
use clap::{Arg, App};
use debian::version::Version as DebianVersion;
use protobuf::Message as ProtobufMsg;
use protobuf::core::parse_from_bytes;
use protobuf::repeated::RepeatedField;
use semver::Version as SemVer;
use simplelog::{Config, CombinedLogger, TermLogger, WriteLogger};
use slack_hook::{Slack, PayloadBuilder};
use zmq::{Message, Socket};
use zmq::Result as ZmqResult;

/*
    TODO: 1. slack integration
          2. use as a library
          3. Email notifications
*/
fn listen(port: u16) -> ZmqResult<()> {
    debug!("Starting zmq listener with version({:?})", zmq::version());
    let context = zmq::Context::new();
    let mut responder = context.socket(zmq::REP)?;

    debug!("Listening on tcp://*:{}", port);
    assert!(responder.bind(&format!("tcp://*:{}", port)).is_ok());

    loop {
        let msg = responder.recv_bytes(0)?;
        debug!("Got msg len: {}", msg.len());
        trace!("Parsing msg {:?} as hex", msg);
        let operation = match parse_from_bytes::<api::service::Operation>(&msg) {
            Ok(bytes) => bytes,
            Err(e) => {
                error!("Failed parse_from_bytes {:?}.  Ignoring request", e);
                continue;
            }
        };

        debug!("Operation requested: {:?}", operation.get_Op_type());
        match operation.get_Op_type() {
            Op::InstalledVersion => {
                match handle_get_installed_version(&mut responder) {
                    Ok(_) => {
                        info!("Get installed version successful");
                    }
                    Err(e) => {
                        error!("Get installed version error: {:?}", e);
                    }
                };
            }
            Op::RunningVersion => {
                if !operation.has_service() {
                    error!("Running version request must include service field.  Ignoring request");
                    continue;
                }
                let service = operation.get_service();
                match handle_get_running_version(&mut responder, CephType::from(service)) {
                    Ok(_) => {
                        info!("Get running version successful");
                    }
                    Err(e) => {
                        error!("Get running version error: {:?}", e);
                    }
                };

            }
            Op::Upgrade => {
                if !operation.has_version() {
                    error!("Upgrade operation must include version field.  Ignoring request");
                    continue;
                }
                let version = operation.get_version();
                if !operation.has_service() {
                    error!("Upgrade request must include service field.  Ignoring request");
                    continue;
                }
                let service = operation.get_service();
                let http_proxy: Option<&str> = {
                    if operation.has_http_proxy() {
                        Some(operation.get_http_proxy())
                    } else {
                        None
                    }
                };
                let https_proxy: Option<&str> = {
                    if operation.has_https_proxy() {
                        Some(operation.get_https_proxy())
                    } else {
                        None
                    }
                };
                let gpg_key: Option<&str> = {
                    if operation.has_gpg_key() {
                        Some(operation.get_gpg_key())
                    } else {
                        None
                    }
                };
                let apt_source: Option<&str> = {
                    if operation.has_apt_source() {
                        Some(operation.get_apt_source())
                    } else {
                        None
                    }
                };
                match handle_upgrade(
                    &mut responder,
                    version,
                    service,
                    http_proxy,
                    https_proxy,
                    gpg_key,
                    apt_source,
                ) {
                    Ok(_) => {
                        info!("Upgrade successful");
                    }
                    Err(e) => {
                        error!("Upgrade error: {:?}", e);
                    }
                };
            }
            Op::Stop => {
                // Exit
                info!("Exit called");
                return Ok(());
            }
        };
        thread::sleep(Duration::from_millis(10));
    }
}

fn notify_slack(
    webhook: &str,
    channel: &str,
    bot_name: &str,
    msg: &str,
) -> Result<(), slack_hook::Error> {
    let slack = Slack::new(webhook)?;
    let p = PayloadBuilder::new()
        .text(msg)
        .channel(channel)
        .username(bot_name)
        .build()?;

    let res = slack.send(&p);
    match res {
        Ok(_) => debug!("Slack notified"),
        Err(e) => error!("Slack error: {:?}", e),
    };
    Ok(())
}

fn semver_to_protobuf(v: SemVer) -> Version {
    let mut version_msg = Version::new();
    let mut upstream = VersionPart::new();

    let mut major = VersionElement::new();
    major.set_alpha("".into());
    major.set_numeric(v.major);

    let mut minor = VersionElement::new();
    minor.set_alpha(".".into());
    minor.set_numeric(v.minor);

    let mut patch = VersionElement::new();
    patch.set_alpha(".".into());
    patch.set_numeric(v.patch);

    let elements: Vec<VersionElement> = vec![major, minor, patch];
    let upstream_elements: RepeatedField<VersionElement> = RepeatedField::from_vec(elements);

    upstream.set_elements(upstream_elements);
    version_msg.set_epoch(0);
    version_msg.set_upstream_version(upstream);

    version_msg
}

pub fn version_to_protobuf(v: &DebianVersion) -> Version {
    let mut version_msg = Version::new();
    let mut upstream = VersionPart::new();
    let mut debian = VersionPart::new();

    let upstream_elements: RepeatedField<VersionElement> = RepeatedField::from_vec(
        v.upstream_version
            .elements
            .iter()
            .map(|e| {
                let mut v = VersionElement::new();
                v.set_alpha(e.alpha.clone());
                v.set_numeric(e.numeric);
                v
            })
            .collect(),
    );
    let debian_elements: RepeatedField<VersionElement> = RepeatedField::from_vec(
        v.debian_revision
            .elements
            .iter()
            .map(|e| {
                let mut v = VersionElement::new();
                v.set_alpha(e.alpha.clone());
                v.set_numeric(e.numeric);
                v
            })
            .collect(),
    );
    upstream.set_elements(upstream_elements);
    debian.set_elements(debian_elements);

    version_msg.set_epoch(v.epoch);
    version_msg.set_upstream_version(upstream);
    version_msg.set_debian_revision(debian);

    version_msg
}

fn handle_get_installed_version(s: &mut Socket) -> IOResult<()> {
    let mut result = VersionResult::new();
    match apt::get_installed_package_version("ceph") {
        Ok(vers) => {
            result.set_result(ResultType::OK);
            let v = version_to_protobuf(&vers);
            result.set_version(v);
            let encoded = result.write_to_bytes().map_err(
                |e| Error::new(ErrorKind::Other, e),
            )?;
            let msg = Message::from_slice(&encoded)?;
            debug!("Responding to client with msg len: {}", msg.len());
            s.send_msg(msg, 0)?;
        }
        Err(e) => {
            result.set_result(ResultType::ERR);
            result.set_error_msg(e.to_string());
        }
    };
    Ok(())
}

fn handle_get_running_version(s: &mut Socket, c: CephType) -> IOResult<()> {
    let mut result = VersionResult::new();
    match get_running_version(&c) {
        Ok(vers) => {
            result.set_result(ResultType::OK);
            let v = semver_to_protobuf(vers);
            result.set_version(v);
            let encoded = result.write_to_bytes().map_err(
                |e| Error::new(ErrorKind::Other, e),
            )?;
            let msg = Message::from_slice(&encoded)?;
            debug!("Responding to client with msg len: {}", msg.len());
            s.send_msg(msg, 0)?;
        }
        Err(e) => {
            result.set_result(ResultType::ERR);
            result.set_error_msg(e.to_string());
        }
    };
    Ok(())
}

fn handle_upgrade(
    s: &mut Socket,
    version: &Version,
    service: CephComponent,
    http_proxy: Option<&str>,
    https_proxy: Option<&str>,
    gpg_key: Option<&str>,
    apt_source: Option<&str>,
) -> IOResult<()> {
    // Upgrade to a new release
    let mut result = OpResult::new();
    let c = CephNode::new();
    match c.upgrade_node(
        version,
        service,
        http_proxy,
        https_proxy,
        gpg_key,
        apt_source,
    ) {
        Ok(_) => {
            result.set_result(ResultType::OK);
            let encoded = result.write_to_bytes().map_err(
                |e| Error::new(ErrorKind::Other, e),
            )?;
            let msg = Message::from_slice(&encoded)?;
            debug!("Responding to client with msg len: {}", msg.len());
            s.send_msg(msg, 0)?;
        }
        Err(e) => {
            result.set_result(ResultType::ERR);
            result.set_error_msg(e.to_string());
        }
    };
    Ok(())
}

fn connect(host: &str, port: u16) -> ZmqResult<Socket> {
    debug!("Starting zmq sender with version({:?})", zmq::version());
    let context = zmq::Context::new();
    let requester = context.socket(zmq::REQ)?;

    debug!("Connecting to tcp://{}:{}", host, port);
    assert!(
        requester
            .connect(&format!("tcp://{}:{}", host, port))
            .is_ok()
    );
    Ok(requester)
}

fn running_version_request(s: &mut Socket) -> Result<Version, String> {
    let mut o = Operation::new();
    debug!("Creating running_version operation request");
    o.set_Op_type(Op::RunningVersion);

    let encoded = o.write_to_bytes().unwrap();
    let msg = Message::from_slice(&encoded).map_err(|e| e.to_string())?;
    debug!("Sending message");
    s.send_msg(msg, 0).map_err(|e| e.to_string())?;

    debug!("Waiting for response");
    let running_vers_response = s.recv_bytes(0).map_err(|e| e.to_string())?;
    debug!("Decoding msg len: {}", running_vers_response.len());
    let op_result = parse_from_bytes::<VersionResult>(&running_vers_response)
        .map_err(|e| e.to_string())?;
    match op_result.get_result() {
        ResultType::OK => {
            let v = op_result.get_version();
            debug!("Got running version: {:?}", v);
            Ok(v.clone())
        }
        ResultType::ERR => {
            if op_result.has_error_msg() {
                let msg = op_result.get_error_msg();
                error!("Running version request failed: {}", msg);
                Err(op_result.get_error_msg().into())
            } else {
                error!("Running version request but error_msg not set");
                Err(
                    "Running version request failed but error_msg not set".to_string(),
                )
            }
        }
    }
}

fn installed_version_request(s: &mut Socket) -> Result<Version, String> {
    let mut o = Operation::new();
    debug!("Creating installed_version operation request");
    o.set_Op_type(Op::InstalledVersion);

    let encoded = o.write_to_bytes().unwrap();
    let msg = Message::from_slice(&encoded).map_err(|e| e.to_string())?;
    debug!("Sending message");
    s.send_msg(msg, 0).map_err(|e| e.to_string())?;

    debug!("Waiting for response");
    let installed_vers_response = s.recv_bytes(0).map_err(|e| e.to_string())?;
    debug!("Decoding msg len: {}", installed_vers_response.len());
    let op_result = parse_from_bytes::<VersionResult>(&installed_vers_response)
        .map_err(|e| e.to_string())?;
    match op_result.get_result() {
        ResultType::OK => {
            let v = op_result.get_version();
            debug!("Got installed version: {:?}", v);
            Ok(v.clone())
        }
        ResultType::ERR => {
            if op_result.has_error_msg() {
                let msg = op_result.get_error_msg();
                error!("Installed version request failed: {}", msg);
                Err(op_result.get_error_msg().into())
            } else {
                error!("Installed version request but error_msg not set");
                Err(
                    "Installed version request failed but error_msg not set".to_string(),
                )
            }
        }
    }
}

fn upgrade_request(
    s: &mut Socket,
    version: Version,
    ceph_type: &CephType,
    http_proxy: Option<&str>,
    https_proxy: Option<&str>,
    gpg_key: Option<&str>,
    apt_source: Option<&str>,
) -> Result<(), String> {
    let mut o = Operation::new();
    debug!("Creating upgrade operation request");
    o.set_Op_type(Op::Upgrade);
    o.set_version(version);
    o.set_service(CephComponent::from(ceph_type.clone()));
    // Set proxies if needed
    if let Some(http_proxy) = http_proxy {
        o.set_http_proxy(http_proxy.into());
    }
    if let Some(https_proxy) = https_proxy {
        o.set_https_proxy(https_proxy.into());
    }
    if let Some(gpg_key) = gpg_key {
        o.set_gpg_key(gpg_key.into());
    }
    if let Some(apt_source) = apt_source {
        o.set_apt_source(apt_source.into());
    }

    let encoded = o.write_to_bytes().unwrap();
    let msg = Message::from_slice(&encoded).map_err(|e| e.to_string())?;
    debug!("Sending message");
    s.send_msg(msg, 0).map_err(|e| e.to_string())?;

    debug!("Waiting for response");
    let upgrade_response = s.recv_bytes(0).map_err(|e| e.to_string())?;
    debug!("Decoding msg len: {}", upgrade_response.len());
    let op_result = parse_from_bytes::<api::service::OpResult>(&upgrade_response)
        .map_err(|e| e.to_string())?;
    match op_result.get_result() {
        ResultType::OK => {
            debug!("Upgrade successful");
            Ok(())
        }
        ResultType::ERR => {
            if op_result.has_error_msg() {
                let msg = op_result.get_error_msg();
                error!("Upgrade failed: {}", msg);
                Err(op_result.get_error_msg().into())
            } else {
                error!("Upgrade failed but error_msg not set");
                Err("Upgrade failed but error_msg not set".to_string())
            }
        }
    }
}

// Upload this binary to all hosts on a host:port combo and launch it
fn upload_and_execute(hosts: Vec<(String, u16)>, listen_port: u16) -> IOResult<()> {
    // Create an ssh mastercontrol port.
    // Run an ssh command over the port to get to the first host
    // discover the topology of the cluster
    // copy this binary to all the other hosts in the cluster
    // startup the binary and have it listen on a port
    // shutdown the ssh mastercontrol port and use zmq to connect to all the machines
    let payload = std::env::current_exe()?;
    let binary_name = payload
        .file_name()
        .ok_or(
            "Unable to determine skynet's filename to send to remote hosts",
        )
        .map_err(|e| Error::new(ErrorKind::Other, e))?;
    for h in hosts {
        let c = Command::new("scp")
            .args(
                &[
                    "-P",
                    &h.1.to_string(),
                    &format!("{}:", payload.display()),
                    &h.0,
                ],
            )
            .output()?;
        if !c.status.success() {
            return Err(Error::last_os_error());
        }
        let run = Command::new("ssh")
            .args(
                &[
                    "nohup",
                    &binary_name.to_string_lossy(),
                    "--port",
                    &listen_port.to_string(),
                ],
            )
            .output()?;
        if !run.status.success() {
            return Err(Error::last_os_error());
        }
    }
    Ok(())
}

fn create_multiplex(host: &str, user: Option<&str>) -> IOResult<()> {
    let mut c = Command::new("ssh");
    c.args(&["-A", "-M", "-S", "mastercontrol"]);
    if let Some(user) = user {
        c.arg(&format!("{}@{}", user, host));
    } else {
        c.arg(host);
    }
    Ok(())
}

fn main() {
    let matches = App::new("Skynet")
        .version(crate_version!())
        .author(crate_authors!())
        .about("Upgrade ceph automatically")
        .arg(
            Arg::with_name("leader")
                .help("Is this a leader or follower node.")
                .long("leader")
                .short("l")
                .required(false),
        )
        .arg(
            Arg::with_name("log")
                .default_value("/var/log/skynet.log")
                .help("Default log file location")
                .long("logfile")
                .takes_value(true)
                .required(false),
        )
        .arg(
            Arg::with_name("port")
                .default_value("5556")
                .help("Port to listen on")
                .long("port")
                .takes_value(true)
                .required(false),
        )
        .arg(
            Arg::with_name("http_proxy")
                .help("HTTP proxy if needed to use")
                .long("httpproxy")
                .takes_value(true)
                .required(false),
        )
        .arg(
            Arg::with_name("https_proxy")
                .help("HTTPS proxy if needed to use")
                .long("httpsproxy")
                .takes_value(true)
                .required(false),
        )
        .arg(
            Arg::with_name("version")
                .help("Version to upgrade to.  Must match package manager version")
                .long("version")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("repo_source")
                .help("Apt source for new repository if needed")
                .long("reposource")
                .takes_value(true)
                .required(false),
        )
        .arg(
            Arg::with_name("repo_key")
                .help("GPG key for the repository")
                .long("repokey")
                .takes_value(true)
                .required(false),
        )
        .arg(Arg::with_name("v").short("v").multiple(true).help(
            "Sets the level of verbosity",
        ))
        .get_matches();
    let level = match matches.occurrences_of("v") {
        0 => log::LogLevelFilter::Info, //default
        1 => log::LogLevelFilter::Debug,
        _ => log::LogLevelFilter::Trace,
    };
    let _ = CombinedLogger::init(vec![
        TermLogger::new(level, Config::default()).unwrap(),
        WriteLogger::new(
            level,
            Config::default(),
            File::create(matches.value_of("log").unwrap()).unwrap()
        ),
    ]);
    let http_proxy: Option<&str> = matches.value_of("http_proxy");
    let https_proxy: Option<&str> = matches.value_of("https_proxy");
    let gpg_key: Option<&str> = matches.value_of("repo_key");
    let apt_source: Option<&str> = matches.value_of("repo_source");
    let port = u16::from_str(matches.value_of("port").unwrap()).unwrap();
    let v = DebianVersion::parse(matches.value_of("version").unwrap()).unwrap();
    if matches.is_present("leader") {
        //Initiate the discovery leader
        let cluster_hosts = match ceph_upgrade::discover_topology() {
            Ok(hosts) => hosts,
            Err(e) => {
                error!("Discovering the cluster topology failed: {:?}.  Exiting", e);
                return;
            }
        };
        debug!("Cluster hosts: {:?}", cluster_hosts);
        if let Err(e) = upload_and_execute(vec![], port) {
            error!("Uploading and starting binaries failed: {:?}. exiting", e);
            return;
        }
        match ceph_upgrade::roll_cluster(
            &v,
            cluster_hosts,
            port,
            http_proxy,
            https_proxy,
            gpg_key,
            apt_source,
        ) {
            Ok(_) => {
                info!("Cluster rolling upgrade completed");
            }
            Err(e) => {
                error!("Cluster rolling upgrade failed: {:?}", e);
                return;
            }
        };
    } else {
        //follower
        //start up a listener
        match listen(port) {
            Ok(_) => {
                info!("Listen finished");
                return;
            }
            Err(e) => {
                error!("Listen failed: {:?}", e);
                return;
            }
        };
    }
}
