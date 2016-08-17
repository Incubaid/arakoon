Arakoon
=======
[![](https://travis-ci.org/openvstorage/arakoon.svg?branch=1.9)](https://travis-ci.org/openvstorage/arakoon)

Arakoon is a distributed key/value store with preference for consistency. More technically, it's a [Multi-Paxos](http://en.wikipedia.org/wiki/Paxos_%28computer_science%29#Multi-Paxos) implementation (written in [OCaml](http://ocaml.org/)) running on top of [TokyoCabinet](http://fallabs.com/tokyocabinet/).

Arakoon is licensed under the Apache License, version 2. For a full license text, see `LICENSE`.

For more information, see the project homepage at http://arakoon.org.

Building Arakoon
----------------
For compilation instructions, see the `COMPILING` document.
Information about the libraries we use is available in `LIBRARIES`.

Quickstart
----------
Start by building Arakoon. To set up a single-node demo server, a basic configuration file is required, e.g.

```
$> cat single.ini
[global]
cluster = arakoon_0
cluster_id = ricky

[arakoon_0]
ip = 127.0.0.1
client_port = 4000
messaging_port = 4010
home = /tmp
log_level = info
```

Now, start the node using

```
$> ./arakoon.native -config single.ini --node arakoon_0
```

This starts a single node cluster for you to play with.

You can set and retrieve values from the command line using

```
$> ./arakoon.native -config examples/single.ini --set hello world
$> ./arakoon.native -config examples/single.ini --get hello
"world"
```

An overview of all command-line options is available by running `arakoon.native --help`.

Of course you want to use Arakoon from within a program. We provide OCaml, C and Python clients.

Documentation
--------------
For additional information please visit the [Arakoon GitBook](https://openvstorage.gitbooks.io/arakoon).


Releases
--------

Arakoon releases are available from the [releases](https://github.com/openvstorage/arakoon/releases) page. Any `1.X` branch corresponds to the development of releases in the `1.X` series.

Have fun,

The Arakoon team
