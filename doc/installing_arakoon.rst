==================
Installing Arakoon
==================
This section explains you how you can install Arakoon, Open vStorage's distributed
key-value store. You can also install a Python or OCaml client to interact with
Arakoon.

Prerequisites
=============
- Ubuntu 64-bit
- Root privileges on Ubuntu
- `Arakoon Debian Package <http://arakoon.org/download.html>`_

Optional

- Arakoon Python Client, requires python-setuptools package
- Arakoon OCaml Client

Installing Arakoon
==================
To install Arakoon:

1. Download `Arakoon 0.11.0 <http://arakoon.org/download.html>`_.
2. Install the Debian::

    ~# dpkg -i arakoon_x.x.x-y_amd64.deb
    Selecting previously deselected package arakoon.
    (Reading database ... 144088 files and directories currently installed.)
    Unpacking arakoon (from arakoon_x.x.x-y_amd64.deb) ...
    Setting up arakoon (x.x.x-y) ...
    Processing triggers for man-db ...
    ~#

Installing the Arakoon Python Client
====================================
To install the Arakoon Python Client, you only have to install the
`.egg <http://arakoon.org/download.html>`_ file.

::

    ~# easy_install arakoon-x.x.x-py2.6.egg
    install_dir /usr/local/lib/python2.6/dist-packages/
    Processing arakoon-x.x.x-py2.6.egg
    creating /usr/local/lib/python2.6/dist-packages/arakoon-x.x.x-py2.6.egg
    Extracting arakoon-x.x.x-py2.6.egg to /usr/local/lib/python2.6/dist-packages
    Adding arakoon x.x.x to easy-install.pth file

    Installed /usr/local/lib/python2.6/dist-packages/arakoon-x.x.x-py2.6.egg
    Processing dependencies for arakoon==x.x.x
    Finished processing dependencies for arakoon==x.x.x
    ~#

Installing the Arakoon OCaml Client
===================================
To install the Arakoon OCaml Client, you only have to install the
`.deb <http://arakoon.org/download.html>`_ file.

::

    ~# dpkg -i libarakoon-ocaml-dev_x.x.x-y_amd64.deb 
    Selecting previously deselected package libarakoon-ocaml-dev.
    (Reading database ... 144273 files and directories currently installed.)
    Unpacking libarakoon-ocaml-dev (from libarakoon-ocaml-dev_x.x.x-y_amd64.deb) ...
    Setting up libarakoon-ocaml-dev (x.x.x-y) ...
    ~#

Other Clients
=============
Arakoon has also a python available on github.com_.

.. _github.com: https://github.com/openvstorage/arakoon/tree/1.8/src/client
