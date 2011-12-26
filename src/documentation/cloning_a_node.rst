==============
Cloning a Node
==============
When you intend to grow your cluster, it might be interesting to clone the
existing node prior to changing the cluster configuration. Once the cluster
configuration is changed to two nodes, both nodes need to accept an update
before it will be accepted. If the first node contains a lot of data, the
second node must perform a large catchup operation (to make it in sync with the
original node) before it accepts new updates. During the catchup period no new
values can be added nor updated.

To minimize the catchup window, you can clone a node in the cluster before the
configuration is changed. This way the catchup procedure is limited to the
updates that were added since the moment of cloning.

To add a clone, you first need to add a node to the cluster as shown in the
`Configuring Arakoon section`_::

    newcluster = q.manage.arakoon.getCluster('Demo_Cluster')
    newcluster.addNode(name='Node_4', ip='192.168.1.4', clientPort=7083, messagingPort=10003)
    newcluster.addLocalNode('Node_4')
    newcluster.createDirs('Node_4')

After creating the new node, simply call the catchupOnly function on the
cluster object to clone a node::

    newcluster.catchupOnly('Node_4')

To clone a node via the Arakoon CLI, see `Arakoon CLI`_.

.. _Configuring Arakoon section: configuring_arakoon.html
.. _Arakoon CLI: managing_arakoon.html#cloning-a-node
