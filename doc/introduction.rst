============
Introduction
============
Arakoon is a key-value store that guarantees consistency above anything else.
It was created by the Open vStorage team due to a lack of existing solutions
fitting our requirements, and is available as Open Source software.

Goals
=====
Arakoon aims to be easy to understand and use, whilst at the same time taking
the following features into consideration.

Consistency
-----------
The system as a whole needs to provide a consistent view on the distributed
state. This stems from the experience that eventual consistency is too heavy a
burden for a user application to manage.

A simple example is the retrieval of the value for a key where you might
receive none, one or multiple values depending on the weather conditions. The
next question is always: Why don’t a get a result? Is it because there is no
value, or merely because I currently cannot retrieve it?

Conditional and Atomic Updates
------------------------------
We don't need full blown transactions (would be nice to have though), but we
do need updates that abort if the state is not what we expect it to be. So at
least an atomic conditional update and an atomic multi-update are needed.

Robustness
----------
The system must be able to cope with failure of individual components, without
concessions to consistency.

However, whenever consistency can no longer be guaranteed, updates must simply
fail.

Locality Control
----------------
When we deploy a system over 2 datacenters, we want guarantees that the entire
state is indeed present in both datacenters. This is something we could not get
from distributed hash tables using consistent hashing.

Healing & Recovery
------------------
Whenever a component dies and is subsequently revived or replaced, the system
must be able to guide that component towards a situation where that node again
fully participates. If this cannot be done fully automatically, then human
intervention should be trivial.

Explicit Failure
----------------
Whenever there is something wrong, failure should propagate quite quickly.

This in contrast to systems that keep on trying to remedy the situation
themselves all the time.

When to use Arakoon
===================
Arakoon may not be for everyone. To get a clearer picture of what Arakoon is
capable of, and to know whether or not it suits you, take a look at the table
below.

.. table::
   :class: when-to-use

   +-------------------------------------------------------------------------------+-----+
   | Use Case                                                                      |     |
   +===============================================================================+=====+
   | You need to manage a limited set of key-value pairs, fitting on a single disk | |y| |
   +-------------------------------------------------------------------------------+-----+
   | You need the ability to replicate on multiple locations                       | |y| |
   +-------------------------------------------------------------------------------+-----+
   | You need immediate consistency over replicas                                  | |y| |
   +-------------------------------------------------------------------------------+-----+
   | You need atomic (multi-)updates                                               | |y| |
   +-------------------------------------------------------------------------------+-----+
   | You need range queries                                                        | |y| |
   +-------------------------------------------------------------------------------+-----+
   | Your applications do not handle eventual consistency                          | |y| |
   +-------------------------------------------------------------------------------+-----+
   | You need infinite scalability                                                 | |n| |
   +-------------------------------------------------------------------------------+-----+
   | You need built in security                                                    | |n| |
   +-------------------------------------------------------------------------------+-----+
   | You need windows on the server side                                           | |n| |
   +-------------------------------------------------------------------------------+-----+
   | You need to store key/values sizes >=16MB                                     | |n| |
   +-------------------------------------------------------------------------------+-----+

.. |y| image:: /img/check.png
.. |n| image:: /img/error.png

If the Arakoon features match your requirements, then continue reading about
the deployment of an Arakoon cluster.

Arakoon Deployment
==================
An Arakoon cluster consists of a small set of nodes that contain the full range
of key-value pairs. Each node carries all the data, yet one node is assigned to
be the master node.

The master node manages all the updates for all the clients. Clients only talk
to the master node, but may contact other nodes to discover who the master node
is; they do so because nodes can assume different roles.

The diagram below shows 3 clients connected to an Arakoon cluster that consists
of 3 Arakoon nodes.

.. image:: /img/ArakoonDeployment.png
   :alt: Arakoon cluster deployment scheme
