===============
Getting Started
===============

Introduction
============
This page will guide you through the setup of a 3 node Arakoon cluster.
For simplicity, all of the nodes run on 1 machine.

Getting it
==========
The simplest is to download and install the debian archive. It's not difficult to build from source neither, but it takes longer.

::

    $> wget https://github.com/openvstorage/arakoon/releases/download/1.7.1/arakoon_1.7.1_amd64.deb
    ...
    $> sudo dpkg -i ./arakoon_1.7.1_amd64.deb
    ...
    $> arakoon --version
    version: 1.7.1
    git_revision: "tags/1.7.1-0-g68fa200"
    compiled: "19/03/2014 12:56:43 UTC"
    machine: "romain-ThinkPad 3.11.0-18-generic x86_64 x86_64 x86_64 GNU/Linux"
    compiler_version : "4.01.0"
    tlogEntriesPerFile: 100000
    dependencies:
    camlbz2        0.6.0  Bindings for bzip2
    camltc         0.9.2  Tokyo Cabinet bindings for OCaml.
    lwt            2.4.4  A cooperative threads library for OCaml
    ounit          2.0.0  Unit testing framework loosely based on HUnit. It is similar to JUnit, and other XUnit testing frameworks
    snappy         0.1.0  Bindings to snappy - fast compression/decompression library
    ssl            0.4.6  Bindings for the libssl


    $> mkdir -p cluster/cfg
    $> cd cluster
    cluster$>

Configuration
=============
An Arakoon cluster needs a config file for every node. This config file
needs to be exactly the same for every node, and for a cluster on a
single machine, all nodes can use the same file location. Here is the one
we'll be using today::

    cluster$> cat cfg/arakoon.ini

    [global]
    cluster_id = ricky
    cluster = arakoon_0,arakoon_1,arakoon_2

    [arakoon_0]
    ip = 127.0.0.1
    client_port = 4000
    messaging_port = 4010
    home = /tmp/arakoon/arakoon_0
    log_level = info

    [arakoon_1]
    ip = 127.0.0.1
    client_port = 4001
    messaging_port = 4011
    home = /tmp/arakoon/arakoon_1
    log_level = info

    [arakoon_2]
    ip = 127.0.0.1
    client_port = 4002
    messaging_port = 4012
    home = /tmp/arakoon/arakoon_2
    log_level = info

It's exactly this config that allows nodes to find each other.

Starting the nodes
==================
An arakoon node will not start unless the configured directories exist.
Other things you need to provide are the name of the node, and the
location of its configuration file.

::

    cluster$> mkdir -p /tmp/arakoon/arakoon_0
    cluster$> arakoon -config cfg/arakoon.ini --node arakoon_0

In another terminal tab, start the second node. Fortunately, Arakoon
considers its default configuration location to be **./cfg/arakoon.ini**.

::

    cluster$> mkdir -p /tmp/arakoon/arakoon_1
    cluster$> arakoon --node arakoon_1

In yet another terminal tab, start the third node::

    cluster$> mkdir -p /tmp/arakoon/arakoon_2
    cluster$> arakoon --node arakoon_2

First steps
===========
The arakoon binary is also an ad-hoc client, administrative tool,...
We'll show some examples below.

Who's the master
----------------
::

    cluster$> arakoon --who-master
    arakoon_0

The actual output might vary as any of the 3 nodes can become master.
It depends on which node wins the elections.
Normally, a client finds out who the master is, and then talks to that node for requests

Basic Set/Get/Delete
--------------------
::

    cluster$> arakoon --set some_key some_value
    cluster$> arakoon --get some_key
    "some_value"


    cluster$> arakoon --delete some_key
    cluster$> arakoon --get some_key
    Fatal error: exception Arakoon_exc.Exception(5, "some_key")
    $>

One node goes down
------------------
Go to the terminal tab for arakoon_0 and kill the node (*ctrl-c*).

::

    cluster$> arakoon -config cfg/arakoon.ini --node arakoon_0
    ^C
    cluster$> arakoon --who-master
    arakoon_2
    cluster$> arakoon --set still_alive yes
    cluster$> arakoon --get still_alive
    "yes"

You can verify the cluster still behaves properly. This is because the
**majority** of the nodes is just fine.

Inspect transaction logs
------------------------
Arakoon keeps record of the everything you do so it can replay it to
nodes that could not follow the cluster (because they were down,
disconnected,...). This is what it looks like::

    cluster$> arakoon --dump-tlog /tmp/arakoon/arakoon_0/000.tlog
    0:(Vm (arakoon_0,0.000000))
    1:(Vc ([NOP;],false)
    ...
    80:(Vc ([Set            ;"some_key";10;"...";],false)
    ...
    137:(Vc ([NOP;],false):"closed:arakoon_0"


Whenever a node goes down, the very last thing it will do is write a marker on the tlog.
This ensures the node can safely know everything is ok the next time it starts up.

Kill another Node and Wipe it
------------------------------
An Arakoon cluster needs to have a majority of nodes in sync to be able to have progress.
So go to the terminal tab for arakoon_1 and kill it.::

   cluster$> arakoon --node arakoon_1
   ^C
   cluster$> arakoon --get some_key
   Fatal error: exception Failure("No Master")
   cluster$> rm -rf /tmp/arakoon/arakoon_1/*
   cluster$> arakoon --node arakoon_1

The node has been restarted.
Now go to a free tab, and try to get the value::

    cluster$> arakoon --get some_key
    "some_value"

Arakoon nodes repair themselves using their siblings. Most of the time it's automatic,
but sometimes they need assistence (if a database is corrupt fe)
