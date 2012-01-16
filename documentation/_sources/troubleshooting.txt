===============
Troubleshooting
===============
.. contents::

Restarting a Node
=================
If for whatever reason a node dies, it can just be restarted. In normal
situations, it will just catch up from the other nodes and resume its
responsibilities.

Corrupt Database
================
If a database is corrupt, one can delete the .db and .db.wal files from the
nodes home directory. Afterwards, the node must be restarted. It will fill the
database from the TLogs, and resume its normal tasks.

If the database is corrupt, the arakoon node refuses to start. In the log file
you see the following log message::

    Dec 29 08:09:04: fatal: going down: Failure("invalid meta data")

A second log message that also indicates that the database has been corrupted,
is the following.

::

    Dec 29 08:09:04:  Failure("miscellaneous error")

A third log message that also indicates that the database has been corrupted,
is the following.

::

    Jan 24 08:59:53: fatal: going down: Failure("invalid record header")

If you encounter any of these messages, proceed with the deleting the database
files (.db and .db.wal) and restarting the node.

Store ahead of TLogs
====================
If the store is ahead of the TLogs, the Arakoon daemon refuses to start and you
see the following error message in the Arakoon log file.

::

    Feb 16 09:17:35: error: Store counter (53107) ahead of tlogs (53105). Aborting
    Feb 16 09:17:35: fatal: after pick
    Feb 16 09:17:35: fatal: going down: Tlc2.TLCCorrupt(_)

You can resolve the issue by deleting the database files of the Arakoon node.
The database files are located in the node home directory and have extensions
.db and .db.wal. After deleting the database files you must restart the Arakoon
process. After startup the Arakoon process will rebuild the database from its
TLogs.

Corrupt Transaction Log (TLOG)
==============================
Arakoon has a built-in mechanism that truncates the last incomplete (or
corrupt) TLog entries. However this mechanism will not work in all corruption
cases. Manual intervention is required when, after truncation the tlog counter
is behind on the store counter. If this is the case you will need to truncate
the tlog file from the command line. To do this you first need to figure which
tlog is the last one. This can be done by listing the files in the node home
directory. Each tlog file has an incrementing sequence number, so we are
looking for the highest sequence number. The tlog in question should also be
the only file with a '.tlog' extension. After you locate the file, issue the
following command

::

    arakoon --truncate-tlog <FULL_PATH_TO_THE_TLOG>

After this you need to delete the database files as there will be entries in
the db that don't have a corresponding tlog entry. Follow the same steps as
described in the section "Store ahead of tlogs" to delete the database files.

Log Rotation
============
Sending SIGUSR1 to an Arakoon process causes it to close and reopen its node
log file. This can be used in combination with log rotate to do whatever you
want with the logs.

For example::

    # ps -ef | grep arakoon
    root      5422     1  0 Jul05 ?        00:00:35 arakoon --node mycluster_0 -config /opt/qbase5/cfg/qconfig/arakoon/mycluster/mycluster.cfg -daemonize

On this machine, there is a pid 5422 of the Arakoon process that runs the node
'mycluster_0' of the cluster. Now you can do a log rotation by moving this
node's log file (in this example:
/opt/qbase5/var/log/mycluster/mycluster_0/mycluster_0.log) and then send a
SIGUSR1 to the Arakoon process. By doing so, a new mycluster_0.log file is
created, while the original one is stored in the other location.

::

    /opt/qbase5/var/log/mycluster/mycluster_0# mv mycluster_0.log /tmp
    /opt/qbase5/var/log/mycluster/mycluster_0# kill -USR1 5422
    /opt/qbase5/var/log/mycluster/mycluster_0# cat mycluster_0.log 
    Jul  6 10:40:27: info: XXX injecting event into inject
    /opt/qbase5/var/log/mycluster/mycluster_0#

