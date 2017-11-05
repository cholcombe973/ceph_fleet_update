# Skynet
Pretty damn smart.  Not quite self aware ;).  Rolling upgrades of Ceph clusters while you sit back.  Do you hate describing your Ceph cluster to your tools?  Have I got a tool for you!
How does this tool differ from all the other ceph upgrade tools?  
1. Drop the binary on any node in the cluster and it will ask ceph for the topology.
2. It will then copy itself over ssh to all in the cluster and start itself listening on a port
3. It will upgrade the nodes in the preferred order ( mons -> osds -> mds -> rgw ) and then shutdown.

Kick this off, sit back and enjoy
