=================
Rolling Checksums
=================

Problem
=======
If a node crashed, and failed to write some tlog entries to disk, this is not detected by Arakoon. The node announces it's in sync up to the last entry in the tlogs, even if other nodes diverged while the node was offline.

Example
-------
Consider this situation:

+----------------------------------+------------------------------------+------------------------------------+
| node0                            | node1                              | node 2                             |
+==================================+====================================+====================================+
| 0:(Vm (node0,0.000000))          | 0:(Vm (node0,0.000000))            | 0:(Vm (node0,0.000000))            |
+----------------------------------+------------------------------------+------------------------------------+
| 1:(Vc ([Set;"a";1;"...";],false) | 1:(Vc ([Set;"a";1;"...";],false)   | 1:(Vc ([Set;"a";1;"...";],false)   |
+----------------------------------+------------------------------------+------------------------------------+
| 2:(Vc ([Set;"b";1;"...";],false) | *2:(Vc ([Set;"b";1;"...";],false)* | *2:(Vc ([Set;"b";1;"...";],false)* |
+----------------------------------+------------------------------------+------------------------------------+
| 3:(Vc ([Set;"c";1;"...";],false) | *3:(Vc ([Set;"c";1;"...";],false)* |                                    |
+----------------------------------+------------------------------------+------------------------------------+

Node1 and node2 crashed, and the last tlog entries were lost. They are restarted, while node0 is still offline. When node0 comes back, this will result in the following situation:

+--------------------------------------+----------------------------------+----------------------------------+
| node0                                | node1                            | node 2                           |
+======================================+==================================+==================================+
| 0:(Vm (node0,0.000000))              | 0:(Vm (node0,0.000000))          | 0:(Vm (node0,0.000000))          |
+--------------------------------------+----------------------------------+----------------------------------+
| 1:(Vc ([Set;"a";1;"...";],false)     | 1:(Vc ([Set;"a";1;"...";],false) | 1:(Vc ([Set;"a";1;"...";],false) |
+--------------------------------------+----------------------------------+----------------------------------+
| **2:(Vc ([Set;"b";1;"...";],false)** | **2:(Vm (node1,0.000000))**      | **2:(Vm (node1,0.000000))**      |
+--------------------------------------+----------------------------------+----------------------------------+
| 3:(Vc ([Set;"d";1;"...";],false)     | 3:(Vc ([Set;"d";1;"...";],false) | 3:(Vc ([Set;"d";1;"...";],false) |
+--------------------------------------+----------------------------------+----------------------------------+

Checksums
=========
This problem can be solved by using a rolling checksum, computed over all the entries in the tlogs. This checksum should be the same for all nodes. The checksum is part of the value that is synced with multi-paxos.

1. The client sends a request to the master node.
2. The master computes the rolling checksum, and makes a value of this checksum and the update commands.
3. This value is sent to the slaves in an accept request.
4. The slaves compute the rolling checksum, and compare it with the checksum in the value.
5. If the checksums are equal, the tlogs are in sync, the value is written to the tlogs, and the algorithm proceeds as usual.
6. If the checksums are different, something bad happened. The node halts, and the tlogs need to be inspected manually.

Remark
------
When several consecutive entries in the tlogs have the same number, it is only the last one that is agreed upon by multi-paxos, and thus only this entry is used in the computation of the checksum.

Tlog Specification
==================
* Serial number (int64)
* Crc-32 checksum of Cmd (int32)
* Cmd
  - Value
  - Marker, optional (string option)

Older value format
------------------
* Update
  - Update type (int32 between 1 and 16)
  - Update details (depends on type)
* Synced (bool)

Old value format
----------------
* 0xff (int32)
* Value type (char 'c' or 'm')
* Value details (depends on type)

New value format
----------------
* 0x100 (int32)
* Checksum (int32 if crc-32 is used)
* Value type (char 'c' or 'm')
* Value details (depends on type)

Checksums in store
==================
The current tlog index and checksum are stored in the local store. When a node starts, they are compared with the values in the tlog.
Thus, after a collapse, the checksum of the last collapsed value is saved in the head database. Therefore the rolling tlog checksum is a continuation of the checksum in the head database.

Upgrade Path
============
To upgrade Arakoon to the new version, with a new tlog format, the nodes need to be restarted. A node that restarts after the upgrade can still read the old tlogs, and the checksums of these values will be set to None. Values with checksum None will never be valuated.

Nodes that need to do a catchup will do this as usual. The first received values will have checksum None, and are written to the tlogs in the old format, until all values from before the upgrade are synced. All values that are created after the upgrade will get a checksum. The checksum of the first value is a normal checksum (not depending on previous values), and all following checksums are rolling.

New and old nodes can not and will not communicate. If the nodes are restarted one by one, the old nodes will keep going as long as possible, while the new nodes can't make progress because they don't have a majority. When the critical point is reached, the new nodes will do a catchup and take over.
