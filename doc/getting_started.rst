===============
Getting Started
===============

Introduction
============
This page will guide you through the setup of a 3 node Arakoon cluster.
For simplicity, all of the nodes run on 1 machine.

Getting it
==========
The simplest is to download the archive. Other options (from source,
using Debian packages,...) exist too, but this is the easiest.

::

    $> wget http://www.arakoon.org/downloads/arakoon-1.0.0.tgz

    $> tar -zxvf arakoon-1.0.0.tgz
    arakoon-1.0.0/arakoon
    arakoon-1.0.0/cfg/arakoon.ini

    $> cd arakoon-1.0.0
    arakoon-1.0.0$>

Configuration
=============
An Arakoon cluster needs a config file for every node. This config file
needs to be exactly the same for every node, and for a cluster on a
single machine, all nodes can use the same file location. Here is the one
we'll be using today::

    arakoon-1.0.0$> cat cfg/arakoon.ini

    [global]
    cluster_id = ricky
    cluster = arakoon_0,arakoon_1,arakoon_2

    [arakoon_0]
    ip = 127.0.0.1
    client_port = 4000
    messaging_port = 4010
    home = /tmp/arakoon/arakoon_0

    [arakoon_1]
    ip = 127.0.0.1
    client_port = 4001
    messaging_port = 4011

    [arakoon_2]
    ip = 127.0.0.1
    client_port = 4002
    messaging_port = 4012

Starting the nodes
==================
An arakoon node will not start unless the configured directories exist.
Other things you need to provide are the name of the node, and the
location of its configuration file.

::

    arakoon-1.0.0$> mkdir -p /tmp/arakoon/arakoon_0
    arakoon-1.0.0$> ./arakoon -config cfg/arakoon.ini --node arakoon_0

In another terminal tab, start the second node. Fortunately, Arakoon
considers its default configuration location to be **./cfg/arakoon.ini**.

::

    arakoon-1.0.0$> mkdir -p /tmp/arakoon/arakoon_1
    arakoon-1.0.0$> ./arakoon --node arakoon_1

In yet another terminal tab, start the third node::

    arakoon-1.0.0$> mkdir -p /tmp/arakoon/arakoon_2
    arakoon-1.0.0$> ./arakoon --node arakoon_2

First steps
===========
The arakoon binary is also an ad-hoc client, administrative too,...
We'll show some examples below.

Who's the master
----------------
::

    arakoon-1.0.0$> ./arakoon --who-master
    arakoon_1

Basic Set/Get/Delete
--------------------
::

    arakoon-1.0.0$> ./arakoon --set some_key some_value
    arakoon-1.0.0$> ./arakoon --get some_key
    "some_value"
    arakoon-1.0.0$> ./arakoon --delete some_key
    arakoon-1.0.0$> ./arakoon --get some_key
    Fatal error: exception Arakoon_exc.Exception(5, "some_key")
    Raised at file "src/core/lwt.ml", line 557, characters 22-23
    Called from file "src/unix/lwt_main.ml", line 38, characters 8-18
    Called from file "src/client/client_main.ml", line 68, characters 12-31
    Called from file "src/main/main.ml", line 395, characters 26-37
    Called from file "src/main/arakoon.ml", line 1, characters 0-12

One node goes down
------------------
Go to the terminal tab for arakoon_0 and kill the node (*ctrl-c*).

::

    arakoon-1.0.0$> ./arakoon -config cfg/arakoon.ini --node arakoon_0
    ^C
    arakoon-1.0.0$>./arakoon --expect-progress-possible
    true
    arakoon-1.0.0$>./arakoon --set still_alive yes
    arakoon-1.0.0$>./arakoon --get still_alive 
    "yes"

You can verify the cluster still behaves properly. This is because the
**majority** of the nodes is just fine.

Inspect transaction logs
------------------------
Arakoon keeps record of the everything you do so it can replay it to
nodes that could not follow the cluster (because they were down,
disconnected,...). This is what it looks like::

    arakoon-1.0.0$>./arakoon --dump-tlog /tmp/arakoon/arakoon_0/000.tlog 
    0:MasterSet ;"arakoon_1";0
    ...
    5:Set       ;"some_key";10
    ...
    12:Delete    ;"some_key"
    13:MasterSet ;"arakoon_1";0
    ...


Kill another Node and Whipe it
------------------------------
An Arakoon cluster needs to have a majority of nodes in sync to be able to have progress.
So go to the terminal tab for arakoon_1 and kill it.::

   arakoon-1.0.0$>./arakoon --node arakoon_1
   ^C
   arakoon-1.0.0$>./arakoon --get some_key
   Fatal error: exception Failure("No Master")
   Raised at file "src/core/lwt.ml", line 557, characters 22-23
   Called from file "src/unix/lwt_main.ml", line 38, characters 8-18
   Called from file "src/client/client_main.ml", line 68, characters 12-31
   Called from file "src/main/main.ml", line 395, characters 26-37
   Called from file "src/main/arakoon.ml", line 1, characters 0-12
   arakoon-1.0.0$>rm -rf /tmp/arakoon/arakoon_1/*
   arakoon-1.0.0$>./arakoon --node arakoon_1

The node has been restarted.
Now go to a free tab, and try to get the value::
    
    arakoon-1.0.0$>./arakoon --get some_key
    "some_value"

Arakoon nodes repair themselves using their siblings. Most of the time it's automatic, 
but sometimes they need assistence (if a database is corrupt fe)
