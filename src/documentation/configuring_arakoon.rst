===================
Configuring Arakoon
===================
Arakoon is a key-value store, consisting of a couple of nodes, that form a
cluster. 

The setup below shows a three server setup, each running one Arakoon node.
Since Arakoon is completely integrated in PyLabs, it's the easiest way to
configure Arakoon with the Q-Shell.

.. image:: {{ base }}img/ArakoonSetup.png

To start working with an Arakoon setup you have to follow phases:

1. `Creating a physical node setup`_ or `Create a demo setup`_
2. `Generating the Client Configuration`_

.. _Creating a physical node setup: `Creating the cluster`_
.. _Create a demo setup: `Demo Setup`_

Creating the Cluster
====================
A first step in configuring Arakoon, is creating a cluster. You can create the
cluster on any of the machines that you will use. In this example we start with
Machine 1.

Start the Q-Shell on this machine and create the cluster **Demo_Cluster**::

    newcluster = q.manage.arakoon.getCluster('Demo_Cluster')

Configuring the First Node
==========================
When you have your cluster, you can start adding the Arakoon nodes by using the
addNode method on your cluster object.

::

    newcluster.addNode(name='Node_0', ip='192.168.1.1', clientPort=7080, messagingPort=10000)

This method only adds the node to the configuration of Machine 1. You still
have to add the indicate that the node must start locally, i.e. on Machine 1,
and create the `directories`_ of the node.

::

    newcluster.addLocalNode('Node_0')
    newcluster.createDirs('Node_0')

You have now one machine running one cluster with one node.

The following step is to add the other nodes.

.. _directories: arakoon_deployment.html#directory-structure

Adding Nodes to a Cluster
=========================
If you want to add a node to the same cluster as the one created in the
previous step, you have to follow the same steps but with the according values.

Execute these steps on the proper machines, i.e. configure **Node_1** on
**Machine 2** and **Node_2** on **Machine 3**.

.. important::
   Make sure that you use the same cluster name! In our example this is
   **Demo_Cluster**.

Node_1 on Machine 2
-------------------
Configure Node_1::

    newcluster = q.manage.arakoon.getCluster('Demo_Cluster')
    newcluster.addNode(name='Node_1', ip='192.168.1.2', clientPort=7081, messagingPort=10001)
    newcluster.addLocalNode('Node_1')
    newcluster.createDirs('Node_1')

Node_2 on Machine 3:
--------------------
Configure Node_2::

    newcluster = q.manage.arakoon.getCluster('Demo_Cluster')
    newcluster.addNode(name='Node_2', ip='192.168.1.3', clientPort=7082, messagingPort=10002)
    newcluster.addLocalNode('Node_2')
    newcluster.createDirs('Node_2')

Linking the Nodes
=================
When you have created and configured the different nodes, you have to make the
each node aware that there are other nodes in the cluster. On each machine you
have to add the other nodes in the cluster.

For example on Machine 1 you have to add Node_1 and Node_2 to the cluster, on
Machine 2 add Node_0 and Node_2, and on Machine 3 add Node_0 and Node_1.

Machine 1
---------
Link Node_1 and Node_2 to Node_0::

    newcluster.addNode(name='Node_1', ip='192.168.1.2', clientPort=7081, messagingPort=10001)
    newcluster.addNode(name='Node_2', ip='192.168.1.3', clientPort=7082, messagingPort=10002)

Machine 2
---------
Link Node_0 and Node_2 to Node_1::

    newcluster.addNode(name='Node_0', ip='192.168.1.1', clientPort=7080, messagingPort=10000)
    newcluster.addNode(name='Node_2', ip='192.168.1.3', clientPort=7082, messagingPort=10002)

Machine 3
---------
Link Node_0 and Node_1 to Node_2::

    newcluster.addNode(name='Node_0', ip='192.168.1.1', clientPort=7080, messagingPort=10000)
    newcluster.addNode(name='Node_1', ip='192.168.1.2', clientPort=7081, messagingPort=10001)

Starting Your Setup
===================
A last phase to make your Arakoon setup active is to start the cluster on each
node. This will only start the local Arakoon node, which means that you have to
start the cluster on each node.

::

    newcluster.start()

Congratulations, you have now a three node Arakoon setup! See also the
`Managing Arakoon`_ page to see how you can start nodes with the command line.

.. _Managing Arakoon: managing_arakoon.html

Adding a Learner Node
=====================
A `learner node`_ is a passive node that follows the cluster's updates but
impacts neither progress nor majority. You sometimes want to have a node like
this for fast but sloppy read access. Adding a learner is similar to adding a
regular node, but with two extra arguments. Suppose that we add a fourth
machine, hosting Node_3 of the cluster.

::

    newcluster = q.manage.arakoon.getCluster('Demo_Cluster')
    newcluster.addNode(name='Node_3', ip='192.168.1.3', clientPort=7083, messagingPort=10003, isLearner=True, targets=('Node_0', 'Node_1', 'Node_2')
    newcluster.addLocalNode('Node_3')
    newcluster.createDirs('Node_3')

.. _learner node: cluster_nodes.html#learner-node

Demo Setup
==========
If you want to quickly set up a demo Arakoon environment, you can use the setUp
method on a cluster object. This method expects a number of nodes that you want
to add in the cluster.

With this function you create a cluster on your server, including the defined
number of nodes.

::

    testcluster = q.manage.arakoon.getCluster('TestCluster')
    testcluster.setUp(3)
    testcluster.start()

The result is a cluster ('TestCluster') with three nodes (TestCluster_0,
TestCluster_1, and TestCluster_2), all running on your local server.

Read-only mode
==============
Single node Arakoon clusters can be put in read-only mode.

::

    testcluster = q.manage.arakoon.getCluster('TestCluster')
    testcluster.setReadOnly()
    testcluster.restart()

.. important::
   Make sure that you restart your Arakoon cluster after changing the
   read-only flag!

Generating the Client Configuration
===================================
After having set up your Arakoon environment (physical setup as well as demo
setup), you have to generate the client configuration of your Arakoon
installation via the Q-Shell.

::

    clientconfig = q.clients.arakoon.getClientConfig('<arakoon cluster name>')
    clientconfig.generateFromServerConfig()

