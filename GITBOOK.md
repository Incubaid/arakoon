# Arakoon

[Arakoon](http://arakoon.org/) is a distributed key-value store that guarantees consistency above anything else. It was created by the Open vStorage team due to a lack of existing solutions fitting our requirements, and is available as Open Source software.

Arakoon aims to be easy to understand and use, whilst at the same time taking the following features into consideration:
* Consistency: The system as a whole needs to provide a consistent view on the distributed state. This stems from the experience that eventual consistency is too heavy a burden for a user application to manage. A simple example is the retrieval of the value for a key where you might receive none, one or multiple values depending on the weather conditions. The next question is always: Why don’t a get a result? Is it because there is no value, or merely because I currently cannot retrieve it?
* Conditional and Atomic Updates: We don’t need full blown transactions (would be nice to have though), but we do need updates that abort if the state is not what we expect it to be. So at least an atomic conditional update and an atomic multi-update are needed.
* Robustness: The system must be able to cope with failure of individual components, without concessions to consistency. However, whenever consistency can no longer be guaranteed, updates must simply fail.
* Locality Control: When we deploy a system over 2 datacenters, we want guarantees that the entire state is indeed present in both datacenters. This is something we could not get from distributed hash tables using consistent hashing.
* Healing & Recovery: Whenever a component dies and is subsequently revived or replaced, the system must be able to guide that component towards a situation where that node again fully participates. If this cannot be done fully automatically, then human intervention should be trivial.
* Explicit Failure: Whenever there is something wrong, failure should propagate quite quickly.

## An Arakoon deployment
An Arakoon cluster consist of a small collection of nodes (typically 1,2,3 or 5 nodes) that contains the full range of key-value pairs, and clients that manipulate the key/value space. In principle, all nodes have the entire key/value space. There is one distinguished node called the master with which all clients communicate to perform updates. A client contacts any node to find out the master, and then just conversates with the master. If a master dies, a new one is elected automatically, and clients fail over to that master. A slave node is a node that is not master. A node that is not up-to-date cannot become master.

The diagram below shows 3 clients connected to an Arakoon cluster that consists of 3 Arakoon nodes.
![](docs/Images/ArakoonDeployment.png)

Updates to the Arakoon database are consistent. An Arakoon client always looks up the master of a cluster and then sends a request to the master. The master node of a cluster has a queue of all client requests. The moment that a request is queued, the master node sends the request to all his slaves and writes the request in the Transaction Log (TLog). When the slaves receive a request, they store this also in their proper TLog and send an acknowledgement to the master.

A master awaits for the acknowledgements of the slaves. When he receives an acknowledgement of half the nodes plus one, the master pushes the key/value pair in its database. In a five node setup (one master and four slaves), the master must receive an acknowledgement of two slaves before he writes his data to the database, since he is also taken into account as node.

After having written his data in his database, the master starts the following request in his queue. When a slave receives this new request, the slaves first write the previous request in their proper database before handling the new request. This way a slave is always certain that the master has successfully written the data in his proper database.

If the master node dies, a new master election happens using [paxos](https://en.wikipedia.org/wiki/Paxos_(computer_science)). If a slave dies, the master is not affected. When a slave comes up, it tries to catchup. The slave first downloads the missing parts of the log. Then it iterates over the tlog while adding the missing updates to its local store. When finished the client again compares its log state with that of the other nodes to make sure it is up to date.

The directory structure of a node typically looks like this:
```
root@cmp02:/opt/OpenvStorage/db/arakoon/ovsdb# ls -als
total 0
0 drwxr-xr-x 1 ovs ovs 14 Dec  1 17:29 .
0 drwxr-xr-x 1 ovs ovs 10 Dec  1 17:29 ..
0 drwxr-xr-x 1 ovs ovs 84 Dec  2 13:22 db
0 drwxr-xr-x 1 ovs ovs 58 Dec  3 12:07 tlogs
root@cmp02:/opt/OpenvStorage/db/arakoon/ovsdb/db# ls -als
total 680
  0 drwxr-xr-x 1 ovs ovs     84 Dec  2 13:22 .
  0 drwxr-xr-x 1 ovs ovs     14 Dec  1 17:29 ..
676 -rwxr-xr-x 1 ovs ovs 690176 Dec  3 14:03 tGLpPNHEDxNOOqOZ.db
  4 -rw-r--r-- 1 ovs ovs    276 Dec  3 14:03 tGLpPNHEDxNOOqOZ.db.wal
root@cmp02:/opt/OpenvStorage/db/arakoon/ovsdb/tlogs# ls -als
total 13744
   0 drwxr-xr-x 1 ovs ovs      58 Dec  3 12:07 .
   0 drwxr-xr-x 1 ovs ovs      14 Dec  1 17:29 ..
3732 -rw-r--r-- 1 ovs ovs 3819824 Dec  2 08:32 000.tlx
3776 -rw-r--r-- 1 ovs ovs 3863492 Dec  2 21:09 001.tlx
3920 -rw-r--r-- 1 ovs ovs 4013559 Dec  3 12:07 002.tlx
2316 -rw-r--r-- 1 ovs ovs 2368293 Dec  3 14:03 003.tlog
```

nodename.db: the actual key/value store.
nodename.db.wal: security database to keep the key/value store consistent, for example in case of a failure.
XXX.tlog: The transaction log files, contains the logs for all transactions executed on the node.
XXX.tlx: A compressed transaction log file.

## Arakoon Log files
The log files for the different Arakoon databases can be found under `/var/log/syslog` or through `journalctl -u <DB name>....service -n 10000 --no-pager | less`.

