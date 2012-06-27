===============
Arakoon Nursery
===============

Introducing Arakoon Nursery
===========================

A normal Arakoon cluster is limited to the size of a physical disk in your cluster, because the Arakoon database can never grow beyond the size of the disk that it hosts.

If the Arakoon database risks to grow beyond the host disk size, you can use the **nursery** functionality. The nursery function allows you to create a cluster-like setup of multiple Arakoon clusters.

As opposed to a real cluster, each Arakoon cluster in the nursery setup contains only a part of the data. 

You can compare a nursery setup with an old encyclopedia in book form. Those encyclopedias were divided in a couple of volumes, divided in an alphabetical way. One volume can then be considered as one Arakoon cluster in a nursery setup. For example volume one contains the information in a range from A to C, volume two from D to F, and so on. 
With Arakoon nursery we do exactly the same. We define the range of keys that must be stored in an Arakoon cluster. To define this range, the clusters are separated by strings. In the example below you can see a three-cluster nursery setup.

.. image:: /img/ArakoonNursery.png
    :width: 500pt

A separator divides two clusters, where in the left cluster the separator is excluded, in the right cluster it is included. In the example 'J' is excluded from Arakoon Cluster 1, but included in Arakoon Cluster 2, similar for 'R', which is excluded from cluster 2, but included in cluster 3.

Cluster one and three are also referred to as the *Boundary* clusters.


Nursery: Functional
===================

A nursery setup consists of at least two Arakoon clusters. One cluster in the nursery setup will have the role of *Nursery Keeper* (NK). The NK is responsible for the segmentation of the nursery setup. It has the knowledge which information belongs to which Arakoon cluster.

Instead of using the Arakoon client directly, you can use the nursery client to execute transactions. Since the nursery client is aware of the complete Nursery setup, he directs the transactions to the proper Arakoon client as shown below. The nursery client has the same functions as the regular Arakoon client, but be aware that this client is only available through the :doc: `Nursery Pylabs Client <pylabs/nursery_pylabs_client>`.

.. image:: /img/ArakoonNurseryTransaction.png
    :width: 500pt

The transaction itself remains identical to a transaction on a normal Arakoon cluster.


General Steps to Set Up Arakoon Nursery
=======================================

1. Create Arakoon clusters
2. Generate client configuration for the clusters.
3. Define the Nursery Keeper on one of the Arakoon clusters.
4. Initialize the Nursery Keeper.
5. Add the other clusters to the Nursery Keeper
6. Define the ranges of the clusters


Commandline
===========
Setting Up One Cluster Nursery
------------------------------

1. Create an Arakoon cluster, the example below shows a setup with three nodes.
For more information about the configuration file, see http://arakoon.org/documentation/arakoon_configuration.html

    ::

        [global]
        cluster = Cluster_Left_0, Cluster_Left_1, Cluster_Left_2
        cluster_id = arakoonleft
    
        [Cluster_Left_0]
        ip = 127.0.0.1
        client_port = 7080
        messaging_port = 10000
        home = /tmp/arakoonleft/Cluster_Left_0
        log_dir = /tmp/arakoonleft/Cluster_Left_0
        log_level = debug
    
        [Cluster_Left_1]
        ip = 127.0.0.1
        client_port = 7081
        messaging_port = 10001
        home = /tmp/arakoonleft/Cluster_Left_1
        log_dir = /tmp/arakoonleft/Cluster_Left_1
        log_level = debug
    
        [Cluster_Left_2]
        ip = 127.0.0.1
        client_port = 7082
        messaging_port = 10002
        home = /tmp/arakoonleft/Cluster_Left_2
        log_dir = /tmp/arakoonleft/Cluster_Left_2
        log_level = debug

2. Create the directories that are defined in the configuration file. 

3. Start Arakoon nodes: ``for i in 0 1 2; do arakoon -daemonize -config /path/to/arakoonconfig/arakoonleft.ini --node Cluster_Left_$i; done``

4. Check if everything works by adding and retrieving a key-pair in your Arakoon setup. In this example we assume that your working directory is the one where you have stored your Arakoon configuration file.

::

    arakoon -config arakoonleft.ini --set a foo

    arakoon -config arakoonleft.ini --get a
    This should return "foo", if not, your Arakoon setup is not configured correctly.

5. To make this cluster the Nursery Keeper (NK), add the following lines to your Arakoon configuration file.

    i. The section is always called 'nursery'.
    ii. *cluster_id*: the cluster_id as defined in the section [global].
    iii. *cluster*: the nodes that are used for the NK.

    ::

        [nursery]
        cluster_id = arakoonleft
        cluster = Cluster_Left_0, Cluster_Left_1, Cluster_Left_2

6. Initialize the nursery setup: ``arakoon -config arakoonleft.ini --nursery-init arakoonleft``
The nursery setup now contains one cluster, which is identical to just one cluster. 

Adding a Cluster to a Nursery Setup
-----------------------------------

1. Create another Arakoon cluster. The example below shows an identical setup as cluster one.
Again a nursery section is created.

    i. cluster_id: the id of the Arakoon cluster that has the role of NK
    ii. cluster: the nodes of the Arakoon cluster that are included for the NK

For each node, create a new section with its IP and client port.

::

    [global]
    cluster = Cluster_Right_0, Cluster_Right_1, Cluster_Right_2
    cluster_id = arakoonright

    [Cluster_Right_0]
    ip = 127.0.0.1
    client_port = 7180
    messaging_port = 10100
    home = /tmp/arakoonright/Cluster_Right_0
    log_dir = /tmp/arakoonright/Cluster_Right_0
    log_level = debug

    [Cluster_Right_1]
    ip = 127.0.0.1
    client_port = 7181
    messaging_port = 10101
    home = /tmp/arakoonright/Cluster_Right_1
    log_dir = /tmp/arakoonright/Cluster_Right_1
    log_level = debug

    [Cluster_Right_2]
    ip = 127.0.0.1
    client_port = 7182
    messaging_port = 10102
    home = /tmp/arakoonright/Cluster_Right_2
    log_dir = /tmp/arakoonright/Cluster_Right_2
    log_level = debug

    [nursery]
    cluster_id = arakoonleft
    cluster = Cluster_Left_0, Cluster_Left_1, Cluster_Left_2

    [Cluster_Left_0]
    ip = 127.0.0.1
    client_port = 7080

    [Cluster_Left_1]
    ip = 127.0.0.1
    client_port = 7081

    [Cluster_Left_2]
    ip = 127.0.0.1
    client_port = 7082


2. Start all nodes of your new cluster: ``for i in 0 1 2; do arakoon -daemonize -config /opt/arakoon/cfg/arakoonright.ini  --node Cluster_Right_$i; done``

3. Set a limiter between the two clusters. For example, all keys that are greater than 'n' (included), must be stored in this new cluster: ``arakoon -config /opt/arakoon/cfg/arakoonleft.ini --nursery-migrate arakoonleft n arakoonright``

The separator separates two clusters where the used separator is always included in the right cluster, in this example 'arakoonright'.


Adding An Arakoon Cluster to Existing Nursery Setup
===================================================
When you want to add an Arakoon cluster, you have three different situations:

1. the new Arakoon cluster is added in front of the nursery setup (before arakoonleft)
2. the new Arakoon cluster is added somewhere in the middle (f.e. between arakoonleft and arakoonright)
3. the new Arakoon cluster is added at the end of the nursery setup (after arakoonright)

In all situations, you only need to migrate once. 

When you add your new cluster in between two existing clusters, you also need to migrate only once. This is because the starting point of the cluster on the right automatically becomes the end-point of the new cluster. In this case you set a new end-point for your cluster on the left, which is then the new starting point of the new cluster.

Examples:

::

    #at the beginning:
    arakoon -config /opt/arakoon/cfg/arakoonleft.ini --nursery-migrate arakoonfront f arakoonleft

    #at the end:
    arakoon -config /opt/arakoon/cfg/arakoonleft.ini --nursery-migrate arakoonright v arakoonend

    #between arakoonleft and arakoonright
    #set endpoint 'n' from arakoonleft to 'j'
    arakoon -config /opt/arakoon/cfg/arakoonleft.ini --nursery-migrate arakoonleft j arakooninsert



Deleting a Cluster from a Nursery Setup
=======================================
Besides adding a new Arakoon cluster to a nursery setup, you can also delete a cluster from it. Via the nursery keeper, you can perform this action.

There is a difference between removing a boundary cluster and a cluster in between clusters. In case of a non-boundary cluster you have to provide a separator to the function.

Remove a boundary cluster:

.. sourcecode:: python

    arakoon -config /opt/arakoon/cfg/arakoonleft.ini --nursery-delete arakoonfront
    arakoon -config /opt/arakoon/cfg/arakoonleft.ini --nursery-delete arakoonend

The adjacent cluster automatically adjusts its new start or end key.

Remove a cluster from in between other clusters:

.. sourcecode:: python

    arakoon -config /opt/arakoon/cfg/arakoonleft.ini --nursery-delete arakooninsert j
    # or
    arakoon -config /opt/arakoon/cfg/arakoonleft.ini --nursery-delete arakooninsert n

Seen from a functional point of view, it is not important which separator you use for deleting an in-the-middle cluster. Seen from a load-balancing point of view, there is a big difference.

When using the starting point, the cluster on the left will be loaded with the data of the deleted cluster.
When using the end point, the cluster on the right will take the data of the deleted cluster.

