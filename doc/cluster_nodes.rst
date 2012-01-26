=============
Cluster Nodes
=============
A cluster consists of two or three types of nodes. There is one master node.
The other nodes in the cluster can be slave nodes or learner nodes.

Master Node
===========
An Arakoon cluster always has *one* master node. Arakoon uses Multi-Paxos_ to
decide which node becomes the master node. However, you have the possibility to
set a fixed master node.

When the nodes are started, they immediately negotiate who becomes the master.
Once the roles are assigned, they get a lease time. The master node has a lease
time of five seconds, the slave nodes have a lease time of ten seconds.

When the lease time is expired, the master node requests again to become the
master and in normal circumstances the slaves accept this request.

However, in some situations it is possible that the master is not able to
negotiate to be reelected as master, for example when the node is down or when
there is too heavy load on the master node. In that case there is a
re-negotiation between the remaining nodes to elect the new master node.

.. _Multi-Paxos: http://en.wikipedia.org/wiki/Paxos_(computer_science)#Multi-Paxos

Slave Node
==========
A slave node of an Arakoon cluster only follows the master and is also eligible
to select a master. He waits for requests coming from the master and sends an
acknowledgement when the request has been written to his own transaction log.
When a following request arrives, the slave is certain that the master has
successfully written the data of the previous transaction into his own
database. This is the moment for the slave to write the data of the previous
request in his database.

Learner Node
============
A learner node is a passive node that follows the cluster's updates but impacts
neither progress nor majority. You sometimes want to have a node like this for
fast but sloppy read access. Adding a learner is similar to adding a regular
node.
