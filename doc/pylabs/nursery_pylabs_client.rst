=====================
Nursery Pylabs Client
=====================

Besides using Arakoon nursery via command-line, you can also use a Pylabs extension.

As seen in the :doc:`Nursery chapter <../nursery>` earlier, the Arakoon nursery creates a cluster-like setup of multiple Arakoon clusters.

In this section you find more information about the usage of Arakoon nursery via Pylabs. 


General Steps to Set Up Arakoon Nursery
=======================================

1. Create Arakoon clusters
2. Generate client configuration for the clusters.
3. Define the Nursery Keeper on one of the Arakoon clusters.
4. Initialize the Nursery Keeper.
5. Add the other clusters to the Nursery Keeper
6. Define the ranges of the clusters
7. Create the nursery client


Setting Up Arakoon Nursery
==========================
In this example you will see a demo nursery setup with three Arakoon clusters (leftCluster, middleCluster, and rightCluster) each containing one node. All nodes run on the same machine

1. Create the three Arakoon clusters

.. sourcecode:: python

    left = q.manage.arakoon.getCluster('leftCluster')
    middle = q.manage.arakoon.getCluster('middleCluster')
    right = q.manage.arakoon.getCluster('rightCluster')

2. Set up the three clusters, each with one node

.. sourcecode:: python

    left.setUp(1)
    middle.setUp(1)
    right.setUp(1)

3. Update the ports in the configuration files of the middle and right clusters (``/opt/qbase5/cfg/qconfig/arakoon/<cluster name>/<cluster name>.cfg``). Below you see the default values, update the two parameters in both file, make sure that no port is used twice.

::

    client_port = 7080
    messaging_port = 7081

4. Generate the Arakoon client for each cluster:

.. sourcecode:: python

    client_left = q.clients.arakoon.getClientConfig('leftCluster')
    client_left.generateFromServerConfig()
    client_middle = q.clients.arakoon.getClientConfig('middleCluster')
    client_middle.generateFromServerConfig()
    client_right = q.clients.arakoon.getClientConfig('rightCluster')
    client_right.generateFromServerConfig()

5. Assign the nursery keeper function to one of the Arakoon clusters. In this example we take the *leftCluster* as nursery keeper.

.. sourcecode:: python

    left.setNurseryKeeper('leftCluster')
    middle.setNurseryKeeper('leftCluster')
    right.setNurseryKeeper('leftCluster')

6. Start your three Arakoon clusters:

.. sourcecode:: python

    left.start()
    middle.start()
    right.start()

7. Get the nursery keeper to start working with the created nursery setup:

.. sourcecode:: python

    q.manage.nursery.getNursery(?
    Definition: q.manage.nursery.getNursery(self, clusterId)
    Documentation:
        Retrieve the nursery manager for the nursery with the provided keeper
    
    
        Parameters:
    
        - clusterId:  The keeper of the nursery
    
    nursekeeper = q.manage.nursery.getKeeper('leftCluster') 

8. Initialize the nursery setup:

.. sourcecode:: python

    nursekeeper.initialize(?
    Definition: nk.initialize(self, firstClusterId)
    Documentation:
        Initialize the routing of the nursery so it only contains the provided cluster
        
        A nursery can only be initialized once
        
        
        Parameters:
        
        - firstClusterId:  The first cluster of the nursery
    
    nursekeeper.initialize('leftCluster') 

9. The last step before using the nursery setup is to divide it into different segments. In this example you will see that 'j' and 'r' are the separators between the clusters.

.. sourcecode:: python

    nursekeeper.migrate(?
    Definition: nk.migrate(self, leftClusterId, separator, rightClusterId)
    Documentation:
        Trigger a migration of key range in the nursery.
        
        Either leftClusterId or rightClusterId must already be part of the nursery. 
        So it is not possible to add two clusters at the same time.
        
        The separator will serve as the boundary of the key range that is migrated.
        
        For more documentation see the docs on our portal (www.arakoon.org)
        
        
        Parameters:
        
        - leftClusterId:   The cluster that will be responsible for the key range up to the separator
        - separator:       The separator separating the key ranges between the two clusters
        - rightClusterId:  The cluster that will be responsible for the key range starting with the separator

    nursekeeper.migrate('leftCluster', 'j', 'middleCluster')
    nursekeeper.migrate('middleCluster', 'r', 'rightCluster')

The nursery setup is now fully configured.


Working with the Nursery Client
===============================
Working with the nursery client is similar to working with the normal :doc:`Arakoon client <arakoon_pylabs_client>`. You can do the three base Arakoon functions, set, get, and delete. The nursery client will act as a proxy for the Arakoon clients.

To get the nursery client, you must use the cluster name that acts as the nursery keeper:

.. sourcecode:: python

    q.clients.nursery.getClient(?
    Definition:  nclient = q.clients.nursery.getClient(self, cluster_id)

    nurse_client = q.clients.nursery.getClient('leftCluster')

From that moment on you can start transactions.

::

    nurse_client.set('x', 'foo')

    nurse_client.get('x')
    "foo"

    nurse_client.delete('x')


Adding An Arakoon Cluster
=========================
When you want to add Arakoon cluster, you have three different situations:

1. the new Arakoon cluster is added in front of the nursery setup (before leftCluster)
2. the new Arakoon cluster is added somewhere in the middle (f.e. between middleCluster and rightCluster)
3. the new Arakoon cluster is added at the end of the nursery setup (after rightCluster)

In the first and last situation, you only need to migrate once. When you place your new cluster between two existing clusters, you need to migrate twice.

Example for cases 1 and 3:

.. sourcecode:: python

    #at the beginning:
    nursekeeper.migrate('frontCluster', 'f', 'leftCluster')

    #at the end:
    nursekeeper.migrate('rightCluster', 'v', 'endCluster')

Example for case 2:
Here you need to do twice a migration. One to indicate that the left cluster has a new end point, another one to set the endpoint of the inserted Arakoon cluster.

.. sourcecode:: python

    #between middleCluster and rightCluster:
    nursekeeper.migrate('middleCluster', 'n', 'insertCluster')
    nursekeeper.migrate('insertCluster', 'r', 'rightCluster')


Deleting a Cluster from a Nursery Setup
=======================================
Besides adding a new Arakoon cluster to a nursery setup, you can also delete a cluster from it. Via the nursery keeper, you can perform this action.

.. sourcecode:: python

    #help
    nursekeeper.delete(?
    Definition: nk.delete(self, clusterId, separator=None)
    Documentation:
        Remove a cluster from the nursery. If the cluster is a boundary cluster, no separator can be provided.
        
        
        Parameters:
        
        - clusterId:  The cluster to be removed
        - separator:  Separator separating the clusters neighbouring the cluster that will be removed (not valid when deleting boundary clusters

In the help-function, you can see that there is a difference between removing a boundary cluster and a cluster in between clusters.

Remove a boundary cluster:

.. sourcecode:: python

    nursekeeper.delete('frontCluster')
    nursekeeper.delete('endCluster')

The adjacent cluster automatically adjusts its new start or end key.

Remove a cluster in between other clusters:

.. sourcecode:: python

    nursekeeper.delete('insertCluster', 'r')
    # or
    nursekeeper.delete('insertCluster', 'n')

Seen from a functional point of view, it is not important which separator you use for deleting an in-the-middle cluster. Seen from a load-balancing point of view, there is a big difference.

When using the starting point, the cluster on the left will be loaded with the data of the deleted cluster.
When using the end point, the cluster on the right will take the data of the deleted cluster.
