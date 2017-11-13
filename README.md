# Skynet
Pretty damn smart.  Not quite self aware ;).  Rolling upgrades of Ceph clusters while you sit back.  Do you hate describing your Ceph cluster to your tools?  Have I got a tool for you!
How does this tool differ from all the other ceph upgrade tools?  
1. Drop the binary on any node in the cluster and it will ask ceph for the topology.
2. It will then copy itself over ssh to all in the cluster and start itself listening on a port
3. It will upgrade the nodes in the preferred order ( mons -> osds -> mds -> rgw ) and then shutdown.

Kick this off, sit back and enjoy

#### Small print
1. The host you are launching this from should have ssh access to all hosts in the ceph cluster.
If that is true than you can sit back and let it run.  If that's not possible
consider submitting a PR to make Skynet smarter!
2. If you have ingress/egress firewalls on your hosts please ensure that the port you're using
is open.
3. Make sure libzmq is installed on the hosts.
