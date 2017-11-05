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

use api::service::{Op, Operation, ResultType, OpResult, Version, VersionElement, VersionPart,
                   VersionResult};
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
fn listen(port: &str) -> ZmqResult<()> {
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
                match handle_get_running_version(&mut responder) {
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
                match handle_upgrade(&mut responder, version) {
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

fn version_to_protobuf(v: DebianVersion) -> Version {
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
            let v = version_to_protobuf(vers);
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

fn handle_get_running_version(s: &mut Socket) -> IOResult<()> {
    let mut result = VersionResult::new();
    match get_running_version(CephType::Osd) {
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

fn handle_upgrade(s: &mut Socket, version: &str) -> IOResult<()> {
    // Upgrade to a new release
    let mut result = OpResult::new();
    let c = CephNode::new();
    match c.upgrade_node(version) {
        Ok(vers) => {
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

fn upgrade_request(s: &mut Socket, version: &str) -> Result<(), String> {
    let mut o = Operation::new();
    debug!("Creating upgrade operation request");
    o.set_Op_type(Op::Upgrade);
    o.set_version(version.into());

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
fn upload_and_execute(hosts: Vec<(String, u16)>, listen_port: u16) -> Result<(), String> {
    // Create an ssh mastercontrol port.
    // Run an ssh command over the port to get to the first host
    // discover the topology of the cluster
    // copy this binary to all the other hosts in the cluster
    // startup the binary and have it listen on a port
    // shutdown the ssh mastercontrol port and use zmq to connect to all the machines
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
    let port = u16::from_str(matches.value_of("port").unwrap()).unwrap();
    let v = DebianVersion::parse("1:21.7-3").unwrap();
    if matches.is_present("leader") {
        //Initiate the discovery leader
        let cluster_hosts = ceph_upgrade::discover_topology();
        debug!("Cluster hosts: {:?}", cluster_hosts);
        if let Err(e) = upload_and_execute(vec![], port) {
            error!("Uploading and starting binaries failed. exiting");
            return;
        }
    //ceph_upgrade::roll_cluster(&v, cluster_hosts);
    } else {
        //follower
        //start up a listener
    }
}

// Generic update to handle rhel/centos or ubuntu/debian
pub trait UpdatePackage {
    /// Upgrade a package.  Centos/Debian should each impl this
    fn upgrade(&self) -> Result<(), String>;
}

pub struct CentosPackage {
    name: String,
    version: String,
}

pub struct UbuntuPackage {
    name: String,
    version: String,
}

impl UpdatePackage for CentosPackage {
    fn upgrade(&self) -> Result<(), String> {
        let c = Command::new("yum")
            .args(&["update", &self.name])
            .output()
            .map_err(|e| e.to_string())?;
        if !c.status.success() {
            return Err(String::from_utf8_lossy(&c.stderr).into_owned());
        }
        Ok(())
    }
}

/*
impl UpdatePackage for UbuntuPackage {
    /// Add keys for the repo
    fn install_keys() {}
    /// Add the upgrade repo
    fn add_repo() {}
    fn upgrade(&self) -> Result<(), String> {
        let c = Command::new("apt")
            .args(&["install", &self.name])
            .output()
            .map_err(|e| e.to_string())?;
        if !c.status.success() {
            return Err(String::from_utf8_lossy(&c.stderr).into_owned());
        }
        Ok(())
    }
}
*/

/*
# Edge cases:
# 1. Previous node dies on upgrade, can we retry?
def roll_monitor_cluster(new_version, upgrade_key):
    """
    This is tricky to get right so here's what we're going to do.
    :param new_version: str of the version to upgrade to
    :param upgrade_key: the cephx key name to use when upgrading
    There's 2 possible cases: Either I'm first in line or not.
    If I'm not first in line I'll wait a random time between 5-30 seconds
    and test to see if the previous monitor is upgraded yet.
    """
    log('roll_monitor_cluster called with {}'.format(new_version))
    my_name = socket.gethostname()
    monitor_list = []
    mon_map = get_mon_map('admin')
    if mon_map['monmap']['mons']:
        for mon in mon_map['monmap']['mons']:
            monitor_list.append(mon['name'])
    else:
        status_set('blocked', 'Unable to get monitor cluster information')
        sys.exit(1)
    log('monitor_list: {}'.format(monitor_list))

    # A sorted list of osd unit names
    mon_sorted_list = sorted(monitor_list)

    try:
        position = mon_sorted_list.index(my_name)
        log("upgrade position: {}".format(position))
        if position == 0:
            # I'm first!  Roll
            # First set a key to inform others I'm about to roll
            lock_and_roll(upgrade_key=upgrade_key,
                          service='mon',
                          my_name=my_name,
                          version=new_version)
        else:
            # Check if the previous node has finished
            status_set('waiting',
                       'Waiting on {} to finish upgrading'.format(
                           mon_sorted_list[position - 1]))
            wait_on_previous_node(upgrade_key=upgrade_key,
                                  service='mon',
                                  previous_node=mon_sorted_list[position - 1],
                                  version=new_version)
            lock_and_roll(upgrade_key=upgrade_key,
                          service='mon',
                          my_name=my_name,
                          version=new_version)
    except ValueError:
        log("Failed to find {} in list {}.".format(
            my_name, mon_sorted_list))
        status_set('blocked', 'failed to upgrade monitor')


def upgrade_monitor(new_version):
    current_version = get_version()
    status_set("maintenance", "Upgrading monitor")
    log("Current ceph version is {}".format(current_version))
    log("Upgrading to: {}".format(new_version))

    try:
        add_source(config('source'), config('key'))
        apt_update(fatal=True)
    except subprocess.CalledProcessError as err:
        log("Adding the ceph source failed with message: {}".format(
            err.message))
        status_set("blocked", "Upgrade to {} failed".format(new_version))
        sys.exit(1)
    try:
        if systemd():
            for mon_id in get_local_mon_ids():
                service_stop('ceph-mon@{}'.format(mon_id))
        else:
            service_stop('ceph-mon-all')
        apt_install(packages=determine_packages(), fatal=True)

        # Ensure the files and directories under /var/lib/ceph is chowned
        # properly as part of the move to the Jewel release, which moved the
        # ceph daemons to running as ceph:ceph instead of root:root.
        if new_version == 'jewel':
            # Ensure the ownership of Ceph's directories is correct
            owner = ceph_user()
            chownr(path=os.path.join(os.sep, "var", "lib", "ceph"),
                   owner=owner,
                   group=owner,
                   follow_links=True)

        if systemd():
            for mon_id in get_local_mon_ids():
                service_start('ceph-mon@{}'.format(mon_id))
        else:
            service_start('ceph-mon-all')
    except subprocess.CalledProcessError as err:
        log("Stopping ceph and upgrading packages failed "
            "with message: {}".format(err.message))
        status_set("blocked", "Upgrade to {} failed".format(new_version))
        sys.exit(1)


def lock_and_roll(upgrade_key, service, my_name, version):
    start_timestamp = time.time()

    log('monitor_key_set {}_{}_{}_start {}'.format(
        service,
        my_name,
        version,
        start_timestamp))
    monitor_key_set(upgrade_key, "{}_{}_{}_start".format(
        service, my_name, version), start_timestamp)
    log("Rolling")

    # This should be quick
    if service == 'osd':
        upgrade_osd(version)
    elif service == 'mon':
        upgrade_monitor(version)
    else:
        log("Unknown service {}.  Unable to upgrade".format(service),
            level=ERROR)
    log("Done")

    stop_timestamp = time.time()
    # Set a key to inform others I am finished
    log('monitor_key_set {}_{}_{}_done {}'.format(service,
                                                  my_name,
                                                  version,
                                                  stop_timestamp))
    status_set('maintenance', 'Finishing upgrade')
    monitor_key_set(upgrade_key, "{}_{}_{}_done".format(service,
                                                        my_name,
                                                        version),
                    stop_timestamp)


def wait_on_previous_node(upgrade_key, service, previous_node, version):
    log("Previous node is: {}".format(previous_node))

    previous_node_finished = monitor_key_exists(
        upgrade_key,
        "{}_{}_{}_done".format(service, previous_node, version))

    while previous_node_finished is False:
        log("{} is not finished. Waiting".format(previous_node))
        # Has this node been trying to upgrade for longer than
        # 10 minutes?
        # If so then move on and consider that node dead.

        # NOTE: This assumes the clusters clocks are somewhat accurate
        # If the hosts clock is really far off it may cause it to skip
        # the previous node even though it shouldn't.
        current_timestamp = time.time()
        previous_node_start_time = monitor_key_get(
            upgrade_key,
            "{}_{}_{}_start".format(service, previous_node, version))
        if (current_timestamp - (10 * 60)) > previous_node_start_time:
            # Previous node is probably dead.  Lets move on
            if previous_node_start_time is not None:
                log(
                    "Waited 10 mins on node {}. current time: {} > "
                    "previous node start time: {} Moving on".format(
                        previous_node,
                        (current_timestamp - (10 * 60)),
                        previous_node_start_time))
                return
        else:
            # I have to wait.  Sleep a random amount of time and then
            # check if I can lock,upgrade and roll.
            wait_time = random.randrange(5, 30)
            log('waiting for {} seconds'.format(wait_time))
            time.sleep(wait_time)
            previous_node_finished = monitor_key_exists(
                upgrade_key,
                "{}_{}_{}_done".format(service, previous_node, version))


def get_upgrade_position(osd_sorted_list, match_name):
    for index, item in enumerate(osd_sorted_list):
        if item.name == match_name:
            return index
    return None


# Edge cases:
# 1. Previous node dies on upgrade, can we retry?
# 2. This assumes that the osd failure domain is not set to osd.
#    It rolls an entire server at a time.
def roll_osd_cluster(new_version, upgrade_key):
    """
    This is tricky to get right so here's what we're going to do.
    :param new_version: str of the version to upgrade to
    :param upgrade_key: the cephx key name to use when upgrading
    There's 2 possible cases: Either I'm first in line or not.
    If I'm not first in line I'll wait a random time between 5-30 seconds
    and test to see if the previous osd is upgraded yet.

    TODO: If you're not in the same failure domain it's safe to upgrade
     1. Examine all pools and adopt the most strict failure domain policy
        Example: Pool 1: Failure domain = rack
        Pool 2: Failure domain = host
        Pool 3: Failure domain = row

        outcome: Failure domain = host
    """
    log('roll_osd_cluster called with {}'.format(new_version))
    my_name = socket.gethostname()
    osd_tree = get_osd_tree(service=upgrade_key)
    # A sorted list of osd unit names
    osd_sorted_list = sorted(osd_tree)
    log("osd_sorted_list: {}".format(osd_sorted_list))

    try:
        position = get_upgrade_position(osd_sorted_list, my_name)
        log("upgrade position: {}".format(position))
        if position == 0:
            # I'm first!  Roll
            # First set a key to inform others I'm about to roll
            lock_and_roll(upgrade_key=upgrade_key,
                          service='osd',
                          my_name=my_name,
                          version=new_version)
        else:
            # Check if the previous node has finished
            status_set('blocked',
                       'Waiting on {} to finish upgrading'.format(
                           osd_sorted_list[position - 1].name))
            wait_on_previous_node(
                upgrade_key=upgrade_key,
                service='osd',
                previous_node=osd_sorted_list[position - 1].name,
                version=new_version)
            lock_and_roll(upgrade_key=upgrade_key,
                          service='osd',
                          my_name=my_name,
                          version=new_version)
    except ValueError:
        log("Failed to find name {} in list {}".format(
            my_name, osd_sorted_list))
        status_set('blocked', 'failed to upgrade osd')


def upgrade_osd(new_version):
    current_version = get_version()
    status_set("maintenance", "Upgrading osd")
    log("Current ceph version is {}".format(current_version))
    log("Upgrading to: {}".format(new_version))

    try:
        add_source(config('source'), config('key'))
        apt_update(fatal=True)
    except subprocess.CalledProcessError as err:
        log("Adding the ceph sources failed with message: {}".format(
            err.message))
        status_set("blocked", "Upgrade to {} failed".format(new_version))
        sys.exit(1)

    try:
        # Upgrade the packages before restarting the daemons.
        status_set('maintenance', 'Upgrading packages to %s' % new_version)
        apt_install(packages=determine_packages(), fatal=True)

        # If the upgrade does not need an ownership update of any of the
        # directories in the osd service directory, then simply restart
        # all of the OSDs at the same time as this will be the fastest
        # way to update the code on the node.
        if not dirs_need_ownership_update('osd'):
            log('Restarting all OSDs to load new binaries', DEBUG)
            service_restart('ceph-osd-all')
            return

        # Need to change the ownership of all directories which are not OSD
        # directories as well.
        # TODO - this should probably be moved to the general upgrade function
        #        and done before mon/osd.
        update_owner(CEPH_BASE_DIR, recurse_dirs=False)
        non_osd_dirs = filter(lambda x: not x == 'osd',
                              os.listdir(CEPH_BASE_DIR))
        non_osd_dirs = map(lambda x: os.path.join(CEPH_BASE_DIR, x),
                           non_osd_dirs)
        for path in non_osd_dirs:
            update_owner(path)

        # Fast service restart wasn't an option because each of the OSD
        # directories need the ownership updated for all the files on
        # the OSD. Walk through the OSDs one-by-one upgrading the OSD.
        for osd_dir in _get_child_dirs(OSD_BASE_DIR):
            try:
                osd_num = _get_osd_num_from_dirname(osd_dir)
                _upgrade_single_osd(osd_num, osd_dir)
            except ValueError as ex:
                # Directory could not be parsed - junk directory?
                log('Could not parse osd directory %s: %s' % (osd_dir, ex),
                    WARNING)
                continue

    except (subprocess.CalledProcessError, IOError) as err:
        log("Stopping ceph and upgrading packages failed "
            "with message: {}".format(err.message))
        status_set("blocked", "Upgrade to {} failed".format(new_version))
        sys.exit(1)


def _upgrade_single_osd(osd_num, osd_dir):
    """Upgrades the single OSD directory.

    :param osd_num: the num of the OSD
    :param osd_dir: the directory of the OSD to upgrade
    :raises CalledProcessError: if an error occurs in a command issued as part
                                of the upgrade process
    :raises IOError: if an error occurs reading/writing to a file as part
                     of the upgrade process
    """
    stop_osd(osd_num)
    disable_osd(osd_num)
    update_owner(osd_dir)
    enable_osd(osd_num)
    start_osd(osd_num)

*/
