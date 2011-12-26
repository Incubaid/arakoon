================
Managing Arakoon
================
In this section you can find the basic commands to manage Arakoon.

Creating a Cluster
==================
After the installation of Arakoon via its `debian package`_, you can create an
Arakoon configuration file anywhere on your harddisk. By default Arakoon looks
for the configuration file cfg/arakoon.ini, relative to the current working
directory. If you want to use another location and name for the configuration
file, you must use the -config argument with the arakoon command.

To create a cluster, you have to define the whole cluster and its nodes in a
`configuration file`_. For an example, see the `Arakoon Deployment page`_.

The defined directories in the configuration file must be created before you
can continue with Arakoon. If directories are missing, you get errors when
starting an Arakoon node.

.. _debian package: installing_arakoon.html
.. _configuration file: arakoon_configuration.html
.. _Arakoon Deployment page: arakoon_deployment.html

Starting a Node
===============
Once you have created the configuration file, you can start one or more nodes.
In the further documentation on this page, the Arakoon configuration file is
stored in /root/cfg/my_cluster.cfg and the working directory is /root.

To start a node 'Node_0' on Machine 1::

    arakoon -daemonize -config /root/cfg/my_cluster.cfg --node Node_0

If you don't want to use the -config parameter, the Arakoon configuration file
must be arakoon.ini and must be located in the cfg directory in your current
working directory. In this example this would mean that you only need to rename
my_cluster.cfg to arakoon.ini.

To make your cluster work, you have to copy the configuration file to your
other machines, which also have Arakoon installed. Then start another node of
the cluster. Once there are two nodes up, you can start using the Arakoon
cluster.

.. important::
   When you run Arakoon as daemon, you must use an absolute path to the
   configuration file.

Adding a Learner Node
=====================
If you want to add a Learner Node (see also `Configuring Arakoon`_), you have
to modify the Arakoon configuration file. Do not add the learner node to the
list of nodes in the cluster, just define the learner.

In the definition of the Learner node you must indicate that the node is a
learner and from which targets it must learn. The other parameters are similar
to a normal node.

.. important::
   Do not forget to update all the configuration files of your cluster.

::

    [Learner_0]
    ip = 192.168.1.100
    client_port = 7180
    messaging_port = 10100
    home = /tmp/learner_0
    log_dir = /tmp/learner_0
    learner = true
    targets = Node_0, Node_1, Node_2
    log_level = debug

Start a learner node the same way as a `normal node`_.

::

    arakoon -daemonize -config /root/cfg/my_cluster.cfg --node Learner_0

.. _Configuring Arakoon: pylabs/configuring_arakoon.html
.. _normal node: `Starting a Node`_

Master Node
===========
In some cases it is useful to select an Arakoon node to be the master node,
see also `Cluster Nodes`. To do so, add a master argument in the [global]
section of the configuration file. To have a backup in case the defined master
node is down, add an extra parameter preferred_master. The other nodes start
negotiating to become master in case the defined master is down.

::

    [global]
    cluster=Node_0, Node_1, Node_2
    master=Node_0
    preferred_master=true

.. _Cluster Nodes: cluster_nodes.html

Cloning a Node
==============
When you intend to grow your cluster, it might be interesting to clone the
existing node prior to changing the cluster configuration. Once the cluster
configuration is changed to two nodes, both nodes need to accept an update
before it will be accepted. If the first node contains a lot of data, the
second node must perform a large catchup operation (to make it in sync with the
original node). To minimize the catchup window, you can clone the first node in
the cluster before the configuration is changed. This way the catchup procedure
is limited to the updates that were added since the moment of cloning.

To add a clone, add a new node to the configuration file and start it this
way::

    arakoon -config /root/cfg/my_cluster.cfg --node Node_4 -catchup-only

.. important::
   Make sure that the node is added to the cluster parameter in the
   configuration file.

When the cloning is finished, you can start the new node as a normal node. To
clone a node via the Arakoon PyLabs client see `Cloning a Node`_.

.. _Cloning a Node: pylabs/cloning_a_node.html

Collapsing TLogs
================
When the number of updates is higher than the number of additions, the
collection of TLogs keeps on growing. You can however reduce the space needed
on disk if you collapse old TLogs into a head database.

To collapse the TLogs::

    arakoon --collapse <tlog_dir> <n>

Where the <tlog_dir> is defined in the Arakoon configuration file and <n> is
the number of TLogs that must remain. For example when you have 40 TLog files
in /tmp/Node_0 and you want to keep the five most recent TLogs, run this
command::

    arakoon --collapse /tmp/Node_0 5

Investigating TLogs
===================
It is not possible to read TLogs as stored on the file system. If you want
human-readable output, you need a dump of the TLog.

    arakoon --dump-tlog <path_to_tlog_file>

For example::

    arakoon --dump-tlog /opt/qbase5/var/db/mycluster/mycluster_0/000.tlog 
    0:MasterSet ;"mycluster_0";0
    1:MasterSet ;"mycluster_0";0
    2:MasterSet ;"mycluster_0";0
    3:MasterSet ;"mycluster_0";0
    4:MasterSet ;"mycluster_0";0
    5:MasterSet ;"mycluster_0";0
    6:MasterSet ;"mycluster_0";0
    7:MasterSet ;"mycluster_0";0
    8:MasterSet ;"mycluster_0";0
    9:MasterSet ;"mycluster_0";0
    10:MasterSet ;"mycluster_0";0
    11:MasterSet ;"mycluster_0";0
    12:MasterSet ;"mycluster_0";0
    13:MasterSet ;"mycluster_0";0
    14:MasterSet ;"mycluster_0";0
    15:MasterSet ;"mycluster_0";0
    16:MasterSet ;"mycluster_0";0
    17:MasterSet ;"mycluster_0";0
    18:MasterSet ;"mycluster_0";0
    19:MasterSet ;"mycluster_0";0
    20:MasterSet ;"mycluster_0";0
    21:MasterSet ;"mycluster_0";0
    22:MasterSet ;"mycluster_0";0
    23:Set       ;"key1";6
    24:MasterSet ;"mycluster_0";0
    ...

Backing up the database
=======================
You can perform a hot backup from a live slave node from the command line. If
you also backup the tlogs and conifguration file of the node, you have all the
required data to be able to rebuild a node. This type of backup allows you to
be able to recover from multi-node failure.

The backup of the database can be done by running the following command::

    # arakoon --backup-db <cluster_id> <ip> <port> <location>
    # e.g.
    arakoon --backup-db ricky 127.0.0.1 7080 /mnt/drv/2011-19-07/mybackup.db
