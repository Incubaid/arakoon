==================
Working with TLogs
==================
TLogs are transactions logs which keep track of all client requests to Arakoon.

Collapsing TLogs
================
When the number of updates is higher than the number of additions, the
collection of TLogs keeps on growing. You can however reduce the space needed
on disk if you collapse old TLogs into a head database or token cabinet (TC).

Collapse the TLogs with the --collapse or --collapse-remote commands of
Arakoon.

::

    arakoon --help

    ...
      --collapse <tlog_dir> <n> collapses n tlogs from <tlog_dir> into head database
      --collapse-remote <cluster_id> <ip> <port> <n> tells node to collapse <n> tlogs into its head database
    ...

For example collapse all but the four most recent TLogs of Node_2::

    arakoon --collapse /tmp/Node_2 4

    arakoon --collapse-remote Demo_Cluster 192.168.1.3 7082 4

This adds all but the four most recent TLogs into the head database. When the
TLogs are added to the main database, they are removed.

Difference between Collapse and Remote Collapse
-----------------------------------------------
In a live cluster it is possible that a slave is catching up from the master.
Suppose that the master is collapsing the TLogs and removes a TLog while the
slave is at that moment catching up from that TLog. The slave then thinks that
the catchup is complete. When the slave tries to go back into normal slave
mode, he notices that he is still not in sync with the master so he goes back
into catchup mode.

To avoid this procedure, run the collapse as an in-process action. This way the
master detects that a catchup is running and won't delete the TLog until the
catchup has finished.

Use the remote collapsing to run the collapse as an in-process action. If you
run the normal collapse function, the master does not detect possible catchup
actions.

Disabling TLog Compression
==========================
When you have very large transaction logs, the compression of the tlog file
with 100.000 updates can consume too much CPU power so that it slows down your
whole arakoon cluster. To avoid the compression to be executed, you must add a
parameter to the Arakoon configuration file, disable_tlog_compression.

Add the parameter to the node section of the Arakoon configuration file on each
machine of the cluster.

::

    ...

    [Node_0]
    ip = 192.168.1.1
    client_port = 7080
    messaging_port = 10000
    home = /tmp/Node_0
    log_dir = /tmp/Node_0
    log_level = debug
    disable_tlog_compression = false
     
    [Node_1]
    ip = 192.168.1.2
    client_port = 7081
    messaging_port = 10001
    home = /tmp/arakoon_1
    #optional different directory for .tlog, .tlc and friends
    #tlog_dir = /tmp/Node_1_tlog
    log_dir = /tmp/Node_1
    log_level = debug
    disable_tlog_compression = true
     
    ...

.. important::
   Make sure that you apply this change on all nodes of your cluster.

