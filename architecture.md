The upgrade process is divided into a few discrete steps:
1. Discover the topology of your cluster
2. Deploy copies of itself to every node on the cluster 
3. Start it up on a port
4. Ask all the nodes in the cluster for their version information.
5. Upgrade the mon cluster
6. Upgrade the osd cluster
7. Upgrade the metadata cluster
8. Upgrade radosgateways

Your job stops at step 1.  Kick off the program from any node that has a ceph admin key, sit
back and watch the magic.
```
Drop on any node that has a ceph-admin key
+--------+
|  Mon 1 |                     +--------+
+--------+                  +-^+  Mon 2 |3) start()
1)discover_topology()       |  +--------+
2)deploy +------------------+
           ssh copy myself  |  +--------+
           using ssh-agent  +-^+  Mon 3 |3) start()
           creds            |  +--------+
                            |
                            +------------+------------+
                            |            |            |
                        +---v----+  +----v---+   +----v---+
                        |  Osd 1 |  | Osd 2  |   |  Osd 3 |
                        +--------+  +--------+   +--------+
                         3)start()   3)start()    3)start()
```
