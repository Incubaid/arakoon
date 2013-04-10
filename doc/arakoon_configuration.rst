=====================
Arakoon Configuration
=====================
To configure an Arakoon setup you only have to create a plain-text
configuration file.

Below you can find an example of a configuration file, based on the three node
setup shown on the :doc:`Arakoon Deployment page <arakoon_deployment>`. This
configuration file must be stored somewhere on your file system. The file name
can be freely chosen.

::

    [global]
    # A setup with 3 nodes

    cluster = Node_0, Node_1, Node_2
    cluster_id = Demo_Cluster

    #Optional parameters:

    #master = Node_0
    #preferred_master = true
    #lease_period = 60
    #readonly = true

    #quorum = 1

    [default_log_config]
    # available levels are: debug info notice warning error fatal
    client_protocol = debug
    paxos = debug
    tcp_messaging = debug

    [Node_0]
    ip = 192.168.1.1
    client_port = 7080
    messaging_port = 10000
    home = /tmp/Node_0
    log_dir = /tmp/Node_0
    log_level = debug
    log_config = default_log_config
    #disable_tlog_compression = true

    [Node_1]
    ip = 192.168.1.2
    client_port = 7081
    messaging_port = 10001
    home = /tmp/arakoon_1
    #optional different directory for .tlog, .tlc and friends
    #tlog_dir = /tmp/Node_1_tlog
    log_dir = /tmp/Node_1
    log_level = debug
    log_config = default_log_config
    #disable_tlog_compression = true

    [Node_2]
    ip = 192.168.1.3
    client_port = 7082
    messaging_port = 10002
    home = /tmp/Node_2
    #optional different directory for .tlog, .tlc and friends
    #tlog_dir = /tmp/Node_2_tlog
    log_dir = /tmp/Node_2
    log_level = debug
    log_config = default_log_config
    #disable_tlog_compression = true

Content of a Configuration File
===============================
An Arakoon configuration file consists of at least two sections:

- global
- node

Optionally one or more log config sections can be added.

Global Section
--------------
The global section of the configuration file defines the Arakoon setup.
These are the possible parameters:

cluster
  A comma-separated list with the names of the nodes in the setup

cluster_id
  The name of the cluster

master *(optional)*
  The name of a node which must be the master of the cluster, preferred to use
  in combination with the preferred_master parameter.

preferred_master *(optional)*
  True/false value. When the parameter is set to true, Arakoon uses the node,
  defined in the master parameter, as master when available. When it is set to
  false, the cluster goes down if the master node is not available.

lease_period *(optional)*
  Set another lease period than the default value of five seconds. See also
  :doc:`Cluster Nodes <cluster_nodes>`.

readonly *(optional)*
  Allows you to set a single node Arakoon cluster in read only mode. Make sure
  that you restart your Arakoon cluster after changing the read-only flag!

quorum *(optional)*
  In normal situations, the master needs half plus one acknowledgments of the
  nodes (see also :doc:`Arakoon Deployment <arakoon_deployment>`) before
  writing data in the database. By using this parameter you can set the number
  of acknowledgments.

.. warning::
   Only set this if you know what you’re doing and understand the risk.

   In a two node setup, you might want to be able to continue when 1 node is
   down, so you lower the quorum to 1 ISO, the default.

   The downside is that if you do this on both sides of a split network you
   will end up having 2 realities.

Node Section
------------
The node section of the configuration file defines each node in the Arakoon
setup. These are the possible parameters:

ip
  IP address of the node

client_port
  Port via which a client connects to the node

messaging_port
  Port via which the nodes of the cluster communicate with each other. If the
  nodes run on different IP addresses, they can all use the same port; if for
  example all nodes run on localhost, then you need a port per node

home
  Location home directory of the node on the file system. The home directory
  must exist on the file system. It is recommended to locate the home directory
  on fast disks because it has a lot of random access operations

tlog_dir *(optional)*
  Location of the node's transaction logs on the file system. By default this
  is the same directory as the home location. If you locate your home directory
  on fast disks, you may want to reserve the fast and expensive disks to only
  the node database and not the transaction log file.

log_dir *(optional)*
  Location of the node's log files on the file system. By default this is the
  same directory as the home location but you may choose another location.

log_config *(optional)*
  The log config to be used for this node.

log_level
  The level of logging on the node, possible options are:

  - fatal: contains only errors with a fatal result for the setup
  - error: contains only errors and errors with a fatal result for the setup
  - warning: contains only warnings, errors, and errors with a fatal result
    for the setup
  - notice: normal but significant condition
  - info: the recommended level, informational messages, for example
    connect/disconnect of a client
  - debug: includes all logging, only used for debugging purposes

disable_tlog_compression *(optional)*
  A transaction log is by default compressed when it has reached its maximum
  of entries (100.000). If you don't want to compress these log, set the
  parameter to true.

Log Config Section
-----------

The log config section specifies the log level for the different 'log sections'
Arakoon logs to. In combination with the specified log_level of the node this
allows controlling which messages get logged.

client_protocol *(optional)*
  The level of logging associated with this section. Same options as log_level.
  The default value is debug.

paxos *(optional)*
  The level of logging associated with this section. Same options as log_level.
  The default value is debug.

tcp_messaging *(optional)*
  The level of logging associated with this section. Same options as log_level.
  The default value is debug.

Forced vs Preferred Master
==========================
When you only use the master parameter, this means that you force the selected
node to be the master of the cluster. This has the disadvantage that when the
master node goes down, the whole cluster is no longer available.

To avoid that an Arakoon cluster is inactive the moment that the master node
goes down, add the parameter preferred_master. This option assures that the
selected node of the master parameter is set as master but when that node goes
down, the slaves automatically start electing a new master.
