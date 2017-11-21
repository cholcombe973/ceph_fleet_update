extern crate api;
#[macro_use]
extern crate clap;
extern crate debian;
extern crate dns_lookup;
extern crate ifaces;
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
use std::ffi::OsStr;
use std::io::{Error, ErrorKind, Read, Write};
use std::io::Result as IOResult;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::str::FromStr;
use std::thread;
use std::time;
use std::time::Duration;

use api::service::{CephComponent, CephServer, HostResult, Op, Operation, ResultType, OpResult,
                   Version, VersionElement, VersionPart, VersionResult};
use ceph_upgrade::{get_running_version, CephType, CephNode};
use clap::{Arg, App};
use debian::version::Version as DebianVersion;
use dns_lookup::lookup_host;
use ifaces::Interface;
use protobuf::Message as ProtobufMsg;
use protobuf::core::parse_from_bytes;
use protobuf::repeated::RepeatedField;
use semver::Version as SemVer;
use simplelog::{Config, SimpleLogger};
use slack_hook::{Slack, PayloadBuilder};
use zmq::{Message, Socket};
use zmq::Result as ZmqResult;

/*
    TODO: 1. slack integration
          2. use as a library
          3. Email notifications
*/
fn listen(
    port: u16,
    version: &DebianVersion,
    http_proxy: Option<&str>,
    https_proxy: Option<&str>,
    gpg_key: Option<&str>,
    apt_source: Option<&str>,
) -> ZmqResult<()> {
    debug!(
        "Starting zmq listener thread with version({:?})",
        zmq::version()
    );
    let context = zmq::Context::new();
    let mut responder: Socket = context.socket(zmq::REP)?;
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
            Op::BecomeLeader => {
                if let Err(e) = handle_become_leader(
                    &mut responder,
                    version,
                    port,
                    http_proxy,
                    https_proxy,
                    gpg_key,
                    apt_source,
                )
                {
                    error!("BecomeLeader error: {:?}", e);
                }
                info!("BecomeLeader finished");
                // Exit after become leader is finished
                return Ok(());
            }
            Op::ListHosts => {
                if let Err(e) = handle_list_hosts(&mut responder) {
                    error!("ListHosts error: {:?}", e);
                }
            }
            Op::InstalledVersion => {
                if let Err(e) = handle_get_installed_version(&mut responder) {
                    error!("Get installed version error: {:?}", e);
                }
            }
            Op::RunningVersion => {
                if !operation.has_service() {
                    error!("Running version request must include service field.  Ignoring request");
                    continue;
                }
                let service = operation.get_service();
                if let Err(e) = handle_get_running_version(
                    &mut responder,
                    CephType::from(service),
                )
                {
                    error!("Get running version error: {:?}", e);
                }
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
                if let Err(e) = handle_upgrade(
                    &mut responder,
                    version,
                    service,
                    http_proxy,
                    https_proxy,
                    gpg_key,
                    apt_source,
                )
                {
                    error!("Upgrade error: {:?}", e);
                }
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

fn semver_to_protobuf(v: &SemVer) -> Version {
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

fn handle_become_leader(
    s: &mut Socket,
    version: &DebianVersion,
    port: u16,
    http_proxy: Option<&str>,
    https_proxy: Option<&str>,
    gpg_key: Option<&str>,
    apt_source: Option<&str>,
) -> IOResult<()> {
    let mut result = OpResult::new();
    result.set_result(ResultType::OK);
    let encoded = result.write_to_bytes().map_err(
        |e| Error::new(ErrorKind::Other, e),
    )?;
    let msg = Message::from_slice(&encoded)?;
    debug!("Responding to client with msg len: {}", msg.len());
    s.send_msg(msg, 0)?;

    info!("I am the leader");
    let cluster_hosts = match ceph_upgrade::discover_topology() {
        Ok(hosts) => hosts,
        Err(e) => {
            error!("Discovering the cluster topology failed: {:?}", e);
            return Err(Error::new(
                ErrorKind::Other,
                "Discover cluster topology failed",
            ));
        }
    };
    debug!("apt_source: {:?}", apt_source);
    debug!("Cluster hosts: {:?}", cluster_hosts);
    match ceph_upgrade::roll_cluster(
        &version,
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
            return Err(Error::new(ErrorKind::Other, "Rolling upgrade failed"));
        }
    }
    Ok(())
}

fn handle_list_hosts(s: &mut Socket) -> IOResult<()> {
    let mut result = HostResult::new();
    match ceph_upgrade::discover_topology() {
        Ok(hosts) => {
            result.set_result(ResultType::OK);
            let host_list: RepeatedField<CephServer> = RepeatedField::from_vec(
                hosts
                    .iter()
                    .map(|h| {
                        let mut c = CephServer::new();
                        c.set_addr(h.0.addr.clone());
                        c.set_id(h.0.id.clone());
                        if let Some(rank) = h.0.rank {
                            c.set_rank(rank.clone());
                        }
                        c.set_component(CephComponent::from(h.1.clone()));
                        c
                    })
                    .collect(),
            );
            result.set_hosts(host_list);
        }
        Err(e) => {
            result.set_result(ResultType::ERR);
            result.set_error_msg(e.to_string());
        }
    };
    let encoded = result.write_to_bytes().map_err(
        |e| Error::new(ErrorKind::Other, e),
    )?;
    let msg = Message::from_slice(&encoded)?;
    debug!("Responding to client with msg len: {}", msg.len());
    s.send_msg(msg, 0)?;
    Ok(())
}

fn handle_get_installed_version(s: &mut Socket) -> IOResult<()> {
    let mut result = VersionResult::new();
    match apt::get_installed_package_version("ceph") {
        Ok(vers) => {
            result.set_result(ResultType::OK);
            let v = version_to_protobuf(&vers);
            result.set_version(v);
        }
        Err(e) => {
            result.set_result(ResultType::ERR);
            result.set_error_msg(e.to_string());
        }
    };
    let encoded = result.write_to_bytes().map_err(
        |e| Error::new(ErrorKind::Other, e),
    )?;
    let msg = Message::from_slice(&encoded)?;
    debug!("Responding to client with msg len: {}", msg.len());
    s.send_msg(msg, 0)?;
    Ok(())
}

fn handle_get_running_version(s: &mut Socket, c: CephType) -> IOResult<()> {
    let mut result = VersionResult::new();
    match get_running_version(&c) {
        Ok(vers) => {
            result.set_result(ResultType::OK);
            let v = semver_to_protobuf(&vers);
            result.set_version(v);

        }
        Err(e) => {
            result.set_result(ResultType::ERR);
            result.set_error_msg(e.to_string());
        }
    };
    let encoded = result.write_to_bytes().map_err(
        |e| Error::new(ErrorKind::Other, e),
    )?;
    let msg = Message::from_slice(&encoded)?;
    debug!("Responding to client with msg len: {}", msg.len());
    s.send_msg(msg, 0)?;
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
            debug!("Upgrading went OK");
            result.set_result(ResultType::OK);
        }
        Err(e) => {
            error!("Upgrading failed: {}", e.to_string());
            result.set_result(ResultType::ERR);
            result.set_error_msg(e.to_string());
        }
    };
    let encoded = result.write_to_bytes().map_err(
        |e| Error::new(ErrorKind::Other, e),
    )?;
    let msg = Message::from_slice(&encoded)?;
    debug!("Responding to client with msg len: {}", msg.len());
    s.send_msg(msg, 0)?;
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

fn become_leader_request(s: &mut Socket) -> Result<(), String> {
    let mut o = Operation::new();
    debug!("Creating become_leader operation request");
    o.set_Op_type(Op::BecomeLeader);

    let encoded = o.write_to_bytes().unwrap();
    let msg = Message::from_slice(&encoded).map_err(|e| e.to_string())?;
    debug!("Sending message");
    s.send_msg(msg, 0).map_err(|e| e.to_string())?;

    debug!("Waiting for response");
    let leader_resp = s.recv_bytes(0).map_err(|e| e.to_string())?;
    let op_result = parse_from_bytes::<OpResult>(&leader_resp).map_err(
        |e| e.to_string(),
    )?;
    match op_result.get_result() {
        ResultType::OK => {
            debug!("Become leader succeeded ");
            Ok(())
        }
        ResultType::ERR => {
            if op_result.has_error_msg() {
                let msg = op_result.get_error_msg();
                error!("Become leader request failed: {}", msg);
                Err(op_result.get_error_msg().into())
            } else {
                error!("Become leader but error_msg not set");
                Err("Become leader failed but error_msg not set".to_string())
            }
        }
    }
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

fn stop_request(s: &mut Socket) -> Result<(), String> {
    let mut o = Operation::new();
    debug!("Creating stop operation request");
    o.set_Op_type(Op::Stop);

    let encoded = o.write_to_bytes().unwrap();
    let msg = Message::from_slice(&encoded).map_err(|e| e.to_string())?;
    debug!("Sending message");
    s.send_msg(msg, 0).map_err(|e| e.to_string())?;

    Ok(())
}

fn list_hosts_request(s: &mut Socket) -> Result<Vec<CephServer>, String> {
    let mut o = Operation::new();
    debug!("Creating list_hosts operation request");
    o.set_Op_type(Op::ListHosts);

    let encoded = o.write_to_bytes().unwrap();
    let msg = Message::from_slice(&encoded).map_err(|e| e.to_string())?;
    debug!("Sending message");
    s.send_msg(msg, 0).map_err(|e| e.to_string())?;

    debug!("Waiting for response");
    let list_hosts_response = s.recv_bytes(0).map_err(|e| e.to_string())?;
    debug!("Decoding msg len: {}", list_hosts_response.len());
    let op_result = parse_from_bytes::<HostResult>(&list_hosts_response)
        .map_err(|e| e.to_string())?;
    match op_result.get_result() {
        ResultType::OK => {
            let hosts = op_result.get_hosts();
            debug!("Got hosts: {:?}", hosts);
            Ok(hosts.to_vec())
        }
        ResultType::ERR => {
            if op_result.has_error_msg() {
                let msg = op_result.get_error_msg();
                error!("List hosts request failed: {}", msg);
                Err(op_result.get_error_msg().into())
            } else {
                error!("List hosts request but error_msg not set");
                Err(
                    "List hosts request failed but error_msg not set".to_string(),
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
    debug!("op_result: {:?}", op_result);
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

fn owned_hostname(s: &str) -> Result<bool, String> {
    let my_hostname = {
        let mut buff = String::new();
        let mut f = File::open("/etc/hostname").map_err(|e| e.to_string())?;
        f.read_to_string(&mut buff).map_err(|e| e.to_string())?;
        buff.trim().to_string()
    };
    if my_hostname == s || my_hostname.contains(s) {
        Ok(true)
    } else {
        Ok(false)
    }
}

fn owned_ip(s: &str) -> Result<bool, String> {
    debug!("owned_ip: {}", s);
    let my_netifaces = Interface::get_all().map_err(|e| e.to_string())?;
    let s_sockaddr = if s.contains(":") {
        SocketAddr::from_str(s).map_err(|e| e.to_string())?
    } else {
        SocketAddr::from_str(&format!("{}:0", s)).map_err(
            |e| e.to_string(),
        )?
    };
    for iface in my_netifaces {
        if let Some(addr) = iface.addr {
            if addr == s_sockaddr {
                //
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn scp_binary(
    binary_name: &OsStr,
    payload: &PathBuf,
    ssh_user: &Option<&str>,
    ssh_identity: &Option<&str>,
    host: (&str, u16),
) -> IOResult<()> {
    info!("Binary: {:?}", binary_name);
    info!("Uploading {} to {}:{}", payload.display(), host.0, host.1);
    let mut c = Command::new("scp");
    if let &Some(ssh_identity) = ssh_identity {
        c.arg("-i");
        c.arg(ssh_identity);
    }
    c.arg("-P");
    c.arg(&host.1.to_string());
    c.arg(&format!("{}", payload.display()));
    if let &Some(ssh_user) = ssh_user {
        c.arg(&format!("{}@{}:", ssh_user, host.0));
    } else {
        c.arg(&format!("{}:", host.0));
    }
    c.output()?;
    debug!("scp: {:?}", c);
    if !c.status()?.success() {
        let e = Error::last_os_error();
        match e.kind() {
            //The binary is already uploaded and running
            ErrorKind::WouldBlock => return Ok(()),
            _ => return Err(e),
        }
    }
    Ok(())
}

fn start_binary(
    binary_name: &OsStr,
    ssh_user: &Option<&str>,
    ssh_identity: &Option<&str>,
    sudo_password: &Option<&str>,
    host: (&str, u16),
    listen_port: u16,
    version: &DebianVersion,
    http_proxy: Option<&str>,
    https_proxy: Option<&str>,
    repo_key: Option<&str>,
    repo_source: Option<&str>,
) -> IOResult<()> {
    let mut run = Command::new("ssh");
    if let &Some(ssh_identity) = ssh_identity {
        run.arg("-i");
        run.arg(ssh_identity);
    }
    run.arg("-p");
    run.arg(&host.1.to_string());
    if let &Some(ssh_user) = ssh_user {
        run.arg(&format!("{}@{}", ssh_user, host.0));
    } else {
        run.arg(host.0);
    }
    if sudo_password.is_some() {
        debug!("Setting stdin and stdout to piped");
        run.stdin(Stdio::piped());
        run.stdout(Stdio::piped());
        run.arg("nohup");
        run.arg("sudo");
        run.arg("-S");
    } else {
        run.arg("nohup");
        run.arg("sudo");
    }
    run.args(
        &[
            &format!("./{}", binary_name.to_string_lossy()),
            "--port",
            &listen_port.to_string(),
            "--version",
            &format!("{}", version),
            "-vv",
        ],
    );
    if let Some(http_proxy) = http_proxy {
        run.arg("--httpproxy");
        run.arg(http_proxy);
    }
    if let Some(https_proxy) = https_proxy {
        run.arg("--httpsproxy");
        run.arg(https_proxy);
    }
    if let Some(repo_key) = repo_key {
        run.arg("--repokey");
        run.arg(repo_key);
    }
    if let Some(repo_source) = repo_source {
        run.arg("--reposource");
        run.arg(repo_source);
    }
    run.args(&["2>&1 > skynet.log"]);
    debug!("ssh: {:?}", run);

    let child = run.spawn()?;
    if let &Some(sudo_password) = sudo_password {
        debug!("Handling sudo password");
        if let Some(mut stdin) = child.stdin {
            debug!("Writing sudo password to stdin");
            stdin.write_all(&format!("{}\n", sudo_password).as_bytes())?;
        }
    }
    thread::sleep(time::Duration::from_secs(1));
    Ok(())
}

// host: (address, ssh port), skynet listen port
fn upload_and_execute(
    ssh_user: Option<&str>,
    ssh_identity: Option<&str>,
    sudo_password: Option<&str>,
    host: (String, u16),
    listen_port: u16,
    version: &DebianVersion,
    http_proxy: Option<&str>,
    https_proxy: Option<&str>,
    repo_key: Option<&str>,
    repo_source: Option<&str>,
) -> IOResult<Socket> {
    // 1. Upload this binary to the first host
    // 2. Ask the first host to list the other hosts.
    // 3. Upload to the other hosts, start and tell first host to kick off the upgrade
    let mut finished_hosts: Vec<String> = Vec::new();
    let payload = std::env::current_exe()?;
    let binary_name = payload
        .file_name()
        .ok_or(
            "Unable to determine skynet's filename to send to remote hosts",
        )
        .map_err(|e| Error::new(ErrorKind::Other, e))?;
    info!("Uploading to {}", host.0);
    scp_binary(
        &binary_name,
        &payload,
        &ssh_user,
        &ssh_identity,
        (&host.0, host.1),
    )?;
    info!("Starting skynet on {}", host.0);
    start_binary(
        &binary_name,
        &ssh_user,
        &ssh_identity,
        &sudo_password,
        (&host.0, 22),
        listen_port,
        version,
        http_proxy,
        https_proxy,
        repo_key,
        repo_source,
    )?;
    finished_hosts.push(host.0.clone());
    thread::sleep(Duration::from_secs(1));
    debug!("connecting to {}:{}", host.0, listen_port);
    let mut conn = connect(&host.0, listen_port).map_err(|e| {
        Error::new(ErrorKind::Other, e)
    })?;
    info!("Requesting ceph cluster host list");
    let cluster_hosts = list_hosts_request(&mut conn).map_err(|e| {
        Error::new(ErrorKind::Other, e)
    })?;
    debug!("Cluster hosts: {:?}", cluster_hosts);
    'hosts: for h in cluster_hosts {
        info!("finished_hosts: {:?}", finished_hosts);
        let ips: Vec<IpAddr> = lookup_host(h.get_addr())?;
        for ip in ips {
            match ip {
                IpAddr::V4(v4_addr) => {
                    if finished_hosts.contains(&v4_addr.to_string()) {
                        info!("Host already finished.  Skipping");
                        continue 'hosts;
                    }
                }
                IpAddr::V6(v6_addr) => {
                    if finished_hosts.contains(&v6_addr.to_string()) {
                        info!("Host already finished.  Skipping");
                        continue 'hosts;
                    }
                }
            }
        }

        if finished_hosts.contains(&h.get_addr().to_string()) {
            info!("Host already finished.  Skipping");
            continue;
        }
        info!("Uploading to {}", h.get_addr());
        let res = scp_binary(
            &binary_name,
            &payload,
            &ssh_user,
            &ssh_identity,
            (h.get_addr(), 22),
        );
        info!("scp: {:?}", res);
        start_binary(
            &binary_name,
            &ssh_user,
            &ssh_identity,
            &sudo_password,
            (h.get_addr(), 22),
            listen_port,
            version,
            http_proxy,
            https_proxy,
            repo_key,
            repo_source,
        )?;
        finished_hosts.push(h.get_addr().to_string());
    }
    Ok(conn)
}

fn main() {
    let matches = App::new("Skynet")
        .version(crate_version!())
        .author(crate_authors!())
        .about("Upgrade ceph automatically")
        .arg(
            Arg::with_name("host")
                .help("Host to gather cluster info and coordinate the upgrade")
                .long("host")
                .takes_value(true)
                .required(false),
        )
        .arg(
            Arg::with_name("port")
                .default_value("5556")
                .help("Port for upgrade servers listen on")
                .short("p")
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
        .arg(
            Arg::with_name("ssh_user")
                .help("User to ssh with")
                .long("sshuser")
                .takes_value(true)
                .required(false),
        )
        .arg(
            Arg::with_name("ssh_identity")
                .help("SSH identity file to use")
                .long("sshidentity")
                .takes_value(true)
                .required(false),
        )
        .arg(
            Arg::with_name("sudo_password")
                .help("Sudo password to use")
                .long("sudopassword")
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
    println!("{:?}", level);
    let _ = SimpleLogger::init(level, Config::default()).unwrap();
    info!("Skynet starting up");
    let host = matches.value_of("host");
    let http_proxy = matches.value_of("http_proxy");
    let https_proxy = matches.value_of("https_proxy");
    let gpg_key = matches.value_of("repo_key");
    let apt_source = matches.value_of("repo_source");
    let port = u16::from_str(matches.value_of("port").expect("port option missing")).unwrap();
    let ssh_user = matches.value_of("ssh_user");
    let ssh_identity = matches.value_of("ssh_identity");
    let sudo_password = matches.value_of("sudo_password");
    let v = DebianVersion::parse(matches.value_of("version").expect("version option missing"))
        .unwrap();
    debug!("version requested: {:?}", v);

    // Uploading to first host and setting up
    if let Some(host) = host {
        if let Ok(mut s) = upload_and_execute(
            ssh_user,
            ssh_identity,
            sudo_password,
            (host.to_string(), 22),
            port,
            &v,
            http_proxy,
            https_proxy,
            gpg_key,
            apt_source,
        )
        {
            match become_leader_request(&mut s) {
                Ok(_) => {
                    info!("become leader request successful");
                }
                Err(e) => {
                    error!("become leader failed: {}", e);
                    return;
                }
            };
        }
        info!("Finished");
    } else {
        info!("I am a follower");
        //start up a listener
        match listen(port, &v, http_proxy, https_proxy, gpg_key, apt_source) {
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
