=================
Arakoon manifesto
=================
:Author: Romain Slootmaekers
:Date: June 2012
:Abstract: Arakoon is a simple distributed key value store that
    prefers consistency above everything else.

.. contents:: Table of Contents
   :depth: 3

introduction
============
What kind of document is this?
------------------------------
we focus being on *getting the information in there first*. 

why Arakoon?
------------
We have been using several distributed non relational data stores for a long time now, and it has not been a satisfying experience.

what we aim for
---------------
We want a simple distributed key/value store that is easy to understand and use.
We don't need infinite scalability (and in fact we have several limitations), but we do have some requests regarding

consistency
~~~~~~~~~~~
The system as a whole needs to provide a consistent view on the distributed state.
This stems from the experience that eventual consistency is too heavy a burden for a user application to manage. 
A simple example is a retrieval of the value for a key where you might receive none, one or multiple values depending on the weather conditions. The next question is always: Why don't a get a result? Is it because there is no value, or merely because I currently cannot retreive it?

conditional atomic updates
~~~~~~~~~~~~~~~~~~~~~~~~~~
We don't need full blown transactions (would be nice to have though), 
but we do need updates that abort if the state is not what we expect it to be.
So at least an atomic conditional update and an atomic multi-update are needed.

robustness
~~~~~~~~~~
The system must be able to cope with failure of individual components, without concessions to consistency.
However, whenever consistency can no longer be guaranteed, updates must simply fail.

locality control
~~~~~~~~~~~~~~~~
When we deploy a system over 2 datacenters, we want guarantees that the entire state is indeed present in both datacenters. (This is something we could not get from distributed hash tables using consistent hashing)

healing/recovery
~~~~~~~~~~~~~~~~
Whenever a component dies and is subsequently revived, or replaced the system must be able to guide that component towards a situation where that node again fully participates. 
If this cannot be done fully automatically, then human intervention should be trivial.

range queries
~~~~~~~~~~~~~
We want to be able to retrieve information about the ranges of keys. 
For example what are the key/value pairs in the range *[x,y]*


explicit failure
~~~~~~~~~~~~~~~~
Whenever there is something wrong, failure should propagate quite quickly.
This in contrast to systems that keep on trying to remedy the situation themselves al the time.

atomic multi updates
~~~~~~~~~~~~~~~~~~~~
Arakoon supports a sequence update which is an all or nothing thing. This simplifies reasoning about failures.

test_and_set
~~~~~~~~~~~~
This is a conditional update, that only changes a value for a key when the store has the expected state.
An Arakoon *test_and_set* can be used to set a new value, 
update an existing value, or remove an existing value.
It's also important to notice Arakoon returns the *old* value after a
*test_and_set*, allowing one to determine whether an update took place.

high level overview
-------------------
Arakoon deployments consist of a small collection of *nodes* (typically 1,2,3 or 5 nodes) that keep replicas of key/values, and *clients* that manipulate the key/value space.
In principle, all nodes have the entire key/value space.
There is one distinguished node called the master with which all clients communicate to perform updates.
A client contacts any node to find out the master, and then just conversates with the master.
If a master dies, a new one is elected automatically, and clients fail over to that master.
A slave node is a node that is not master.
A node that is not up-to-date cannot become master.

limitations
-----------
capacity
~~~~~~~~
Since all nodes store the entire space, the capacity of the smallest node limits the system.

number of clients
~~~~~~~~~~~~~~~~~
Since all updates go through the master, the system is not suited for large amounts of concurrent clients.

opaque values
~~~~~~~~~~~~~
The system does not really understand the values, and hence cannot do validation, or transformations...

hidden master failure
~~~~~~~~~~~~~~~~~~~~~
If the key value store on the master silently corrupts, gets will be affected.


basic client interface
======================
some notation
-------------
Before we can descibe the client's interface, 
we need to introduce some notational tools to make it easier to convey things in a concise manner.

+--------------+--------------------------------------------------------+
| notation     | how to read it                                         |
+==============+========================================================+
| ``:``        | has type                                               |
+--------------+--------------------------------------------------------+
| ``unit``     | aka void                                               |
+--------------+--------------------------------------------------------+
| ``x list``   | a list of items of type x                              |
+--------------+--------------------------------------------------------+
| ``x array``  | a fixed size sequence of items of type x               |
+--------------+--------------------------------------------------------+
| ``x option`` | either (Some x) or None                                |
+--------------+--------------------------------------------------------+
| ``string``   | char array                                             |
+--------------+--------------------------------------------------------+
| ``x -> y``   | a function from x to y                                 |
+--------------+--------------------------------------------------------+
| ``x C.t``    | something that *eventually yields* something of type x |
+--------------+--------------------------------------------------------+

For example ``a : string`` just means that ``a`` is a ``string``;
``fibonacci : int -> int`` just means that fibonnacci is a function that takes an integer, and returns an integer as result;
``cat : string -> string -> string`` reads as 'cat is a function that takes 2 strings and returns a string as result'

``write: channel -> string -> unit C.t`` reads as 'write takes a channel and a string and eventually yields unit.


A client has a dictionary interface with some adjustments for latency: the functions have a return value of type ``a C.t``.

type key = string
~~~~~~~~~~~~~~~~~
Keys are strings.
There are no strict size limitations, which means that they such be small enough to be handled in their entirety

type value = string
~~~~~~~~~~~~~~~~~~~
Values are strings too.

type update
~~~~~~~~~~~
An update is either ``Set(key,value)`` or ``Delete(key)``.

exists : key -> bool C.t
~~~~~~~~~~~~~~~~~~~~~~~~
See if a value exists for a specific key, without retrieving it.

get : key -> value C.t
~~~~~~~~~~~~~~~~~~~~~~
You can look up a value if you have the key. It will eventually yield either a value, or raise an exception.

set : key -> value -> unit C.t
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
You can update a value for a key, regardless of current value (if any).

confirm : key -> value -> unit C.t
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
If *get key* would yield this *value* then this does nothing, 
else, it behaves as a set.

delete : key -> unit C.t
~~~~~~~~~~~~~~~~~~~~~~~~
You can remove a key/value pair. 
There was a suggestion to open this op to allow a regular expression.

test_and_set : key -> value option -> value option -> (value option) C.t
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
This is a careful update.

*test_and_set k expected new* only modifies the value to *new* if the old value is *expected*.
The originaly stored value will be returned.
Using a *value option* instead of a value allows you to only set a value only if there was none for that key.
Using *None* as new allows you to do a careful delete as well.

range : key option -> bool -> key option -> bool -> int -> key list C.t
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*range bkey binc ekey einc max* will yield a list of keys where
``max`` is the maximum number of keys (if ``max < 0`` then you want them all).
The keys fall in the range *kbey..ekey*.
*binc* and *einc* specify if the borders are included (*true*) or not.

range_entries: key option -> bool -> key option -> bool -> int -> bool -> (key * value) list C.t
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
will yield a list of key value pairs.
The parameters have the same semantics as for the range method.

rev_range_entries : key option -> bool -> key option -> bool -> int -> (key * value ) list C.t
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*rev_range_entries bkey binc ekey einc max* will yield a list of key value pairs, 
just like *range_entries*, but with reverse ordering: *bkey* is the higher boundary, *ekey* the lower.
This can be used to support backwards paging.

sequence: update list -> unit C.t
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Provides an atomic multi-update. Either all updates are performed or none. 
While this is not a full transaction, it provides enough functionality to safeguard consistency.

synced_sequence: update_list -> unit C.t
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Provides an atomic multi-update, just like *sequence*, but with the added action of a file system synchronisation (fsync), 
before the call returns. Some people feel safer that way.

assert : key -> value option -> unit C.t
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*assert key vo* throws an exception if the value associated with the key is not what was expected. 
This can be used to interrupt sequences.

..
    %\paragraph{hello : string $\rightarrow$ string $\rightarrow$ string C.t}
    %The client identifies itself to the server and tells the server the cluster id, and the server replies with its version string.

who_master: unit -> string option C.t
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Allows the client to know which node currently acts as master.
If there is no master, or it is not known to this node, the result is None.

..
    %\paragraph{last\_entries: int $\rightarrow$ out\_channel $\rightarrow$ unit C.t}
    %allows the client to stream the transaction log into a channel, starting from entry $i$

multi_get: key list -> (value list) C.t
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Allows you to fetch multiple values in one roundtrip to the server.

expect_progress_possible: unit -> bool C.t
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Asks the master node if it thinks progress is possible. 
This means that the master has seen enough that indicates enough slaves are still following its lead. 
False positives are possible.

user_function: string -> string option -> string option C.t
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Allows user registered manipulation of the store. 
More about this in :ref:`User Functions <user-functions>`

get_key_count: unit -> int64 C.t
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Yields the number of items stored in arakoon.


Consistent updates
==================
An update is sent to the master.
The master adds it to its log, and tries to get consensus about the update with the slaves.
Once consensus has been reached about the first log entry,
the master adds the entry to the persistent local key-value store.
Slaves can move the updates from their log
into their local key-value store asynchronously.

::

    (nicked from wikipedia)

    C      M            S0  S1
    |      |            |    |  --- first request ---
    X----->|            |    |  Request
    |      X----------->|--->|  Prepare(N)
    |      |<-----------X----X  Promise(N,  I, {Va,Vb,Vc})
    |      X----------->|--->|  Accept!(N,  I, Vn)
    |      |<-----------X----X  Accepted(N, I)
    |<-----X                    Response
                                --- other requests ---
    X----->|            |    |  Request
    |      X----------->|--->|  Accept(N,   I+1, W)
    |      |<-----------X----X  Accepted(N, I+1)
    |<-----X            |    |  Response

    X----->|            |    |
    |      X----------->|--->|  Accept(N, I+2,  X)
    |      |<-----------X----X  Accepted(N,I+2)
    ...

The first request with M as Master (= leader) needs a full paxos round,
while subsequent updates with the same leader skip the first phase.
This boils down to a single roundtrip from master to slaves per update.
If the different nodes have failure modes independent of each other (independent power supplies, different disks, ...),
one needs not await the push-through to disk and the message can be pushed asynchronously to the local key-value store.
This optimistic behaviour needs to be a configuration option, since the application cannot assess this by itself.
One can also go below 1 roundtrip per update by stuffing multiple updates together.
This increases throughput.


Individual Slave failure
------------------------
If a slave dies, the master is not affected.
When a slave comes up, there are several possibilities. ...


Electing a master
=================
...


Restart and Failure Scenarios
=============================


Implementation choices
======================
Inter-node communication
------------------------
The nodes are fully connected with each other over tcp sockets. We're playing with the idea of using IB (not IPoIB).

Client-Node communication
-------------------------
The client node communication has different needs, and hence a rpc like approach will be used. Following table describes what we do with primitives.
Just note that a list should be written out head first, so that naieve de-serialization will return the original.

+----------+------------------------------------------------------------------------------+--------------+
| type     | marshalled form                                                              | size (bytes) |
+==========+==============================================================================+==============+
| char     | ascii                                                                        | 1            |
+----------+------------------------------------------------------------------------------+--------------+
| bool     | false -> '0' | true -> '1'                                                   | 1            |
+----------+------------------------------------------------------------------------------+--------------+
| size     | little endian                                                                | 4            |
+----------+------------------------------------------------------------------------------+--------------+
| varint   | continuation bit announces next byte                                         |   ..         |
+----------+------------------------------------------------------------------------------+--------------+
| string   | [length:varint ][bytes]                                                      | ... + n      |
+----------+------------------------------------------------------------------------------+--------------+
| float    | IEE754 double                                                                | 8            |
+----------+------------------------------------------------------------------------------+--------------+
| x option | '0' (None) or '1' [x]                                                        | *|x| + 1*    |
+----------+------------------------------------------------------------------------------+--------------+
| x list   | [size:varint][x\ :subscript:`n-1`\ ]...[x\ :subscript:`0`\ ]                 |              |
+----------+------------------------------------------------------------------------------+--------------+
| x array  | [size:varint][x\ :subscript:`0`\ ]...[x\ :subscript:`n-1`\ ]                 |              |
+----------+------------------------------------------------------------------------------+--------------+


+-------------+---------------------+
| 1:size      | 0x01 00 00 00       |
+-------------+---------------------+
| 70000:size  | 0x70 0x17 0x01 0x00 |
+-------------+---------------------+
| 1:int       | 0x01                |
+-------------+---------------------+
| 40:int      | 0x40                |
+-------------+---------------------+
| 70000:int   | 0xf0a204            |
+-------------+---------------------+
| 3.14:float  | 0x1f85eb51b81e0940  |
+-------------+---------------------+

+----------------+-----------------------------------------------------------+
| Set(key,value) | [1:int\ :subscript:`32`\ ][key:string][value:string]      |
+----------------+-----------------------------------------------------------+
| Delete(key)    | [2:int\ :subscript:`32`\ ][key:string]                    |
+----------------+-----------------------------------------------------------+
| Assert(k,vo)   | [8:int\ :subscript:`32`\ ][key:string][vo: string option] |
+----------------+-----------------------------------------------------------+
| sequence       | [size(data):int\ :subscript:`32`\ ][bytes(data)]          |
+----------------+-----------------------------------------------------------+

protocol definition
-------------------
The protocol is a very simple request/response based binary protocol.
The client is the active party, and sends a command

successful client-server conversation
~~~~~~~~~~~~~~~~~~~

::

    client: [command : size][request_size:size][parameter_0][parameter_1]... (&flush)
    server: [response_size:size][0x00][result_0][result_1]... (&flush)

::

    client: [command : size][parameter_0][parameter_1].. (&flush)
    server: [response_size:size][rc:varint][string]      (&flush)

Each command is masked with the magic sequence ``0xb1ff0000``.
The node checks the magic, proceeds with reading the parameters, and processes the request.
Then the response code and response are written.
If a failure happens, the server writes out a return code different from zero, and a string with a message after which he may close the connection.

The command codes and return codes are listed below.

======================== ===========
command                  code
======================== ===========
ping                     0x0000 0001
who_master               0x0000 0002
exists                   0x0000 0007
get                      0x0000 0008
set                      0x0000 0009
delete                   0x0000 000a
range                    0x0000 000b
prefix_keys              0x0000 000c
test_and_set             0x0000 000d
last_entries             0x0000 000e
range_enties             0x0000 000f
sequence                 0x0000 0010
multi_get                0x0000 0011
expect_progress_possible 0x0000 0012
user_function            0x0000 0015
assert                   0x0000 0016
get_key_count            0x0000 001a
confirm                  0x0000 001b
rev_range_entries        0x0000 0023
synced_sequence          0x0000 0024
======================== ===========

=========== ====================
return code condition
=========== ====================
0x0000 0000 success!
0x0000 0001 command has no magic
0x0000 0002 too many dead nodes
0x0000 0003 no hello
0x0000 0004 not master
0x0000 0005 not found
0x0000 0006 wrong cluster
0x0000 0007 assertion failed
...         ...
0x0000 00ff unknown failure
=========== ====================



nodes in OCaml
--------------
Implementing the nodes in OCaml using Ocsigen's LWT library gives ample control over the fine grained concurrency we need through monadic coroutines.

Other clients
-------------
Besides the obvious OCaml client, Arakoon provides some clients written in other languages:

* python
* php
* C

These are pure clients, in the sense that for example the Python client is 100% Python and not a wrapped C client. 
We might have to reconsider this strategy as the number of different clients grows. ... meanwhile, there are unconfirmed rumours about an Erlang client in the wilde.

local key/value store
---------------------
Stuff about baardskeerder


forced master and quorum
------------------------
We needed to solve the case where you have only 2 nodes.
The simplest solution is to allow the master to be chosen by configuration and the quorum to be fixed. 
This way, you can chose for the 2 node case where you want the master, 
and if you're willing to take the risk to keep on writing in a case of a slave node not responding, you just set the quorum to 1.

Other strategical decisions
===========================
REST interface
--------------
We decided not to offer a REST interface.

dynamically adding nodes
------------------------
All the nodes know one another from their configuration files,
but adding a node is not trivial. 
For example, one wants to add a third node in a two node setup and starts a node with a config referring to the two existing nodes. 
But these older nodes are clueless 
and have another opinion on how many nodes need to concur. 
A cluster protects itself by not answering to nodes it does not know; 
but this means adding a node means you need to restart the existing nodes.

It might be better to make the *known_nodes* a paxos value 
over which consensus must be reached, but this has additional risks.
The population can only change with reasonable increments.


State machine
=============

...


.. _user-functions:

User functions
==============
User functions are a flexible way to add functionality at server side.
For example, if one would like to atomically increment a counter, 
without user functions one first has to do a *get*
and then a *test_and_set*, and if there was a race, retry.
With user functions this is quite simple. 
User functions are compiled separately, and dynamically loaded at node startup time.

There are three steps in installing a user function.

* implement the function
* register the function in the registry
* configure the node to load the module at startup time

Implementing a user function
----------------------------
When called, a user function gets passed a parameter of class type *user_db*.
The *user_db* class type provides the interface for manipulating the store. 
Each call to a user function is executed *inside a transaction*

.. code-block:: ocaml

module UserDB : 
sig
  type tx = Core.BS.tx
  type k = string
  type v = string
  val set : tx -> k -> v -> unit Lwt.t
  val get : tx -> k -> Baardskeerder.v Baardskeerder.result Lwt.t
end

User functions have the following type:

::

    type f = UserDB.tx -> string option -> (string option) Lwt.t

Within the body of the user function, 
one can make calls using the transaction value that is passed in.
A sample implementation, for incrementing a counter is provided below.

.. code-block:: ocaml

    (* file : plugin_update_max.ml *)
    let update_max tx po = 
      let _k = "max" in
      let vo2i = function
          | None -> 0
          | Some v -> s2i v
      in
      UserDB.get tx _k >>= fun vr ->
      let i2 = match vr with
          | Baardskeerder.OK v -> s2i v
          | Baardskeerder.NOK k -> 0
      in
      let i = vo2i po in
      let m  = max i i2 in
      let ms = i2s m in
      UserDB.set tx _k ms >>= fun () ->
      Lwt.return (Some ms)


    let () = Userdb.Registry.register "update_max" update_max



The last line of the module takes care of the registration of the function.

registration
------------
Registration is very simple: It's done by calling *Registry.register*, from inside the module that implements the function.

extra Node configuration
------------------------
The arakoon configuration file needs to have an extra line 

::

    # file arakoon.ini

    ...

    #plugin module needs to be in home
    plugins = plugin_plugin_update_max

    [arakoon_0]
    home = /tmp/arakoon_0

    ...

This will cause the node to load *plugin_update_max.cmxs* when it starts. 
This file needs to be available in the home directory of *all* the nodes of the cluster. After the nodes are started, clients can make use of this.

Important remarks
-----------------
Once a user function is installed, it needs to remain available, with the same functionality for as long as user function calles are stored inside the transaction logs, as they need to be re-evaluated when one replays a transaction log to a store (for example when a node crashed, leaving a corrupt database behind).

The input and output are of type *String option*, which means that if you want to pass in a string list, you need to device some kind of (de)marshalling. 
Furtunately, the *Llio* module is available both on client and server side, and has most things you need.



Scaling Arakoon
===============
We want to be able to use arakoon for increasingly large key-value spaces. 
For a single arakoon cluster the capacity is limited by the size of a single disk. 
So it is only natural to allow different arakoon clusters to team up. 
A *nursery*\ [#f3]_ provides a semi-unified view on a set of arakoon clusters. 
Each cluster is uniquely responsible for a key prefix interval. 

Limitations
-----------
The simple strategy of mapping a cluster to an interval of keys already implies some limitations compared to the single cluster setup. 
As a result, applications willing to scale from a single cluster to a nursery need to do some planning.

impact on sequences
~~~~~~~~~~~~~~~~~~~
Sequences are multiple updates that are done atomically. 
Since atomicity can only be achieved inside 1 cluster [#f4]_, this means that all keys for a sequence need to share the same prefix. 

impact on ranges
~~~~~~~~~~~~~~~~
Every cluster is responsible for a specific interval. 
As client range query will only be served by a single cluster, it means that only ranges that fit within a cluster interval can be served.

Migrations
----------
Once a cluster is filled, one needs to be able to split it, or move part of its interval elsewhere. 
This process is called migration.
Each cluster has a *public* interval [k\ :subscript:`b`\ ,k\ :subscript:`e`\ ) it serves to clients, 
as well as a *private* interval it contains. 
As such, migrating a part of a cluster's interval to another cluster becomes feasible. 
If we're moving keys away from a *source* cluster [k\ :subscript:`b`\ , k\ :subscript:`e`\ )

* shrink the public interval of the source cluster from 
  [k\ :subscript:`b`\ , k\ :subscript:`e`\ ) to [k\ :subscript:`b`\ , k\ :subscript:`e` - a). 
  The private interval of the source remains [k\ :subscript:`b`\ , k\ :subscript:`e`\ )
* add the key/value pairs in [k\ :subscript:`e` - a,k\ :subscript:`e`\ ) to the target cluster
* extend the public range of the target cluster from
  [k\ :subscript:`e`\ ,k\ :subscript:`z`\ ) to [k\ :subscript:`e` - a, k\ :subscript:`z`\ )
* delete the key/value pairs on the source in [k\ :subscript:`e` -a,k\ :subscript:`e`\ ).
  update the private range on the source to [k\ :subscript:`b`\ , k\ :subscript:`e`\ -a).

This work can be done by a privileged client responsible for the migration. 
That client can die at any point, and figure out, at resumption, what it needs to do to complete the task. 
The problem with the migration strategy is that there is a point in time where none of the clusters is serving [k\ :subscript:`e` -a, k\ :subscript:`e`\ ), so any request for anything in that interval is refused.

Large migrations (and most of them will be), need to be done in multiple smaller steps.
This means we repeatedly have to execute the above procedure, 
each time choosing an *a* that chips off a set of key value pairs that can be migrated using a multiget and a sequence.

Client side support
-------------------
Each client needs to know which cluster is responsible for a certain key(-range). 
This information is kept in a routing table. At construction time, a client fetches this from a designated Arakoon that knows all the clusters in a nursery. 
The privileged clients performing migrations also must update this designated arakoon. 
As far as clients are concerned, it's not really important that the nursery clients have correct routing tables: 
If a client asks something from a cluster that's not able to comply, it will simply refuse. 
This means that either, there is some migration, or that the client has outdated routing information. 
In that case, it can simply refetch the ranges from the clusters it knows, or refetch it from the designated arakoon that keeps this information.

Problems
--------
We depend on having a designated arakoon that knows all the clusters in a nursery, and their routing tables. 
So conceptually, we introduced a single point of failure. 
Since this point is in reality an arakoon cluster which is synchronuously replicated, that should not pose big practical problems.

Having to maintain configuration of (multiple/many?) arakoon clusters on lots of machines will become a significant problem. 
As this information is both crucial, and maintained by humans, which is a recipe for disaster. In time, we should move to service discovery. 
Possible options are:

* XMPP disco
* DNS-Based Service Discovery
* openSLP
* Salutation
* UPnP
* svrloc
* ...

.. rubric:: Footnotes

.. [#f3] after a *a nursery of raccoons*
.. [#f4] you could build transactionality across clusters, but it's a can of worms
