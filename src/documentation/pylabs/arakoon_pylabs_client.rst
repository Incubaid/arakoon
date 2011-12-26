=====================
Arakoon PyLabs Client
=====================
When you have set up your `Arakoon setup`_, you can use a PyLabs client to
interact with Arakoon.

.. _Arakoon setup: configuring_arakoon.html

Connecting to an Arakoon Cluster
================================
To get connect to an Arakoon cluster, you have to create the cluster
configuration and add the nodes on each machine that will run an Arakoon
client.

::

    cli_cfg = q.clients.arakoon.getClientConfig('Demo_Cluster')
    cli_cfg.addNode( 'Node_0', ip='192.168.1.1', clientPort=7080)
    cli_cfg.addNode( 'Node_1', ip='192.168.1.2', clientPort=7081)
    cli_cfg.addNode( 'Node_2', ip='192.168.1.3', clientPort=7082)

When you have created this configuration on the client machine, you can create
a connection as follows::

    myClient = q.clients.arakoon.getClient('Demo_Cluster')

    myClient.<TAB>
    myClient.
    myClient.allowDirtyReads(         myClient.prefix(
    myClient.delete(                  myClient.range(
    myClient.disallowDirtyReads(      myClient.range_entries(
    myClient.exists(                  myClient.sequence(
    myClient.expectProgressPossible(  myClient.set(
    myClient.get(                     myClient.setDirtyReadNode(
    myClient.getDirtyReadNode(        myClient.statistics(
    myClient.hello(                   myClient.testAndSet(
    myClient.multiGet(                myClient.whoMaster(

.. tip::
   If the client is running on one of the Arakoon machines there is a shortcut
   method for generating the client configuration as follows::

       cli_cfg = q.clients.arakoon.getClientConfig('Demo_Cluster')
       cli_cfg.generateFromServerConfig()
       myClient = q.clients.arakoon.getClient('Demo_Cluster')

Doing dirty reads
=================
Starting from the 0.10.0 release of arakoon it is possible to do dirty reads
from the server node. This means that you will be reading out values which
might not be 100% up to date, i.e. the value might actually be updated on
another node (and the update will eventually be pushed to the node you are
querying). Allowing dirty reads allows clients to read data from slaves and
learner nodes.

To enable dirty reads issue the following command::

    myClient.allowDirtyReads()

By default the extension will pick a random node from the nodes in the client
configuration taht will be used for dirty reads. If you want to read from a
specific node, you can override by issuing the following command::

    myClient.setDirtyReader('Node_0')

An alternative way of doing things is to create a named client configuration
which only holds the node you want to perform dirty reads against. This can be
done like this::

    cli_cfg = q.client.arakoon.getConfig('Demo_Cluster', 'dirty_read_cfg')
    cli_cfg.addNode( 'Node_0', ip='192.168.1.1', clientPort=7080 )
    dirtyReader = q.clients.arakoon.getClient( 'mindy', 'dirty_read_cfg' )
    dirtyReader.allowDirtyReads()

Setting Key-Value Pairs
=======================
Normal set operations
---------------------
If we would like to add the following keys and their values:

+-----+---------+
| Key | Value   |
+=====+=========+
| 1   | Value A |
+-----+---------+
| 2   | Value B |
+-----+---------+
| 3   | Value C |
+-----+---------+

We use the set() method as follows::

    myClient.set('1', 'Value A')
    myClient.set('2', 'Value B')
    myClient.set('3', 'Value C')

Confirm
-------
Confirm allows you to only update key-value pairs when the new value differs
from the current value.

We use the confirm method as follows::

    myClient.confirm('1', 'Value A')
    myClient.confirm('2', 'Value B')
    myClient.confirm('1', 'Value A')

Getting Keys and Key-Value Pairs
================================
This section explains how to get the value of a key, how to get the keys
between two known keys, and how to get the key-value pairs between two known
keys.

Getting the Value of a Key
--------------------------
To continue with the previous example, let's assume we want to retrieve that
value of key '2', in this case we would use::

    myClient.get('2')

This returns the value for key '2'.

Getting a Range of Keys
-----------------------
To get a range of the available keys present between two keys, we need to
specify 4 things:

1. Starting key.
2. If we shall include the starting key in the result.
3. Ending key.
4. If we shall include the ending key in the result.

So in our case, if we want know the keys that lie in the range between key '1'
and key '3', including both the starting and ending keys. In this case, we
use::

    myClient.range('1', True, '3', True)

This returns::

    ['1', '2', '3']

Getting a Range of Key-Value Pairs
----------------------------------
Similar to getting a range of keys alone, we need to specify the same four
parameters mentioned above.

For example, if we want to display the key-value pairs between key '1' and key
'3', excluding key '3', we use::

    myClient.range_entries('1', True, '3', False)

This returns::

    [('2', 'Value B'), ('1', 'Value A')]

Statistics
==========
Arakoon allows you to request a set of basic statistics from the master node.
To display these statistics we use::

    myClient.statistics()

This returns::

    {'avg_get_size': 0.0,
     'avg_set_size': 0.0,
     'del_timing': {'avg': 0.0,
                    'max': 0.0,
                    'min': 1.7976931348623157e+308,
                    'var': 0.0},
     'get_timing': {'avg': 0.0,
                    'max': 0.0,
                    'min': 1.7976931348623157e+308,
                    'var': 0.0},
     'last': 1307801398.8910699,
     'mget_timing': {'avg': 0.0,
                     'max': 0.0,
                     'min': 1.7976931348623157e+308,
                     'var': 0.0},
     'n_deletes': 0,
     'n_gets': 0,
     'n_multigets': 0,
     'n_ops': 0,
     'n_sequences': 0,
     'n_sets': 0,
     'n_testandsets': 0,
     'node_is': {'Node_0': 302439, 'Node_1': 302432, Node_2': 302438},
     'op_timing': {'avg': 0.0,
                   'max': 0.0,
                   'min': 1.7976931348623157e+308,
                   'var': 0.0},
     'seq_timing': {'avg': 0.0,
                    'max': 0.0,
                    'min': 1.7976931348623157e+308,
                    'var': 0.0},
     'set_timing': {'avg': 0.0,
                    'max': 0.0,
                    'min': 1.7976931348623157e+308,
                    'var': 0.0},
     'start': 1307801398.8910699,
     'tas_timing': {'avg': 0.0,
                    'max': 0.0,
                    'min': 1.7976931348623157e+308,
                    'var': 0.0}}

With these statistics, we can for example calculate the average sets per second
(since the starting of the node) using a simple formula::

    n_sets / (last - start)

.. note:: Keep in mind that the statistics are managed by the master node; this
   means that with every master switch, the statistics will be reset.

Collapsing TLogs
================
See also `Working with TLogs`_ for more information.

::

    Definition: cluster.remoteCollapse(self, nodeName, n)
    Documentation:
        Tell the targetted node to collapse n tlog files

To keep four TLog files on Node_2::

    cluster.remoteCollapse(Node_2, 4)

.. _Working with TLogs: ../working_with_tlogs.html

Disabling/Enabling TLOG Compression
===================================
If you want to disable TLOG compression in your Arakoon cluster, you have to
execute the following steps on *each* node of your cluster::

    cluster = q.manage.arakoon.getCluster('ricky')

    cluster.disableTlogCompression() 

By default the compression is enabled. If you have turned off the compression
and want to enable it again, use enableTlogCompression on the cluster object.

.. note:: Instead of using the Q-Shell, you can directly manipulate the
   [Arakoon configuration file].

