import string
import urllib
import md5
import argparse
import sys

template = """
=========
Downloads
=========
On this page you can find all the required packages to install Arakoon Release {version}

Arakoon {version} can only be installed on Ubuntu 64-bit.

If you want support for other distros, please contact us and we'll see what we can do.

Licensing
=========
Arakoon is released under a dual license model as specified on the `licensing page`_.

.. _licensing page: licensing.html

Arakoon version {version}
======================
`Release notes`_

.. _Release notes: releases/${version}.html

Server
------
+-------------------------+------------------+----------------------+------------------------------------------+
| Debian Package x86_64   | {version:>13}__  | {deb_size:>17.0f} KB | MD5: {deb_md5:>32}    |
+-------------------------+------------------+----------------------+------------------------------------------+
| Ubuntu x86_64 Q-Package | {version:>13}    | name: arakoon        | domain: pylabs.org                       |
+-------------------------+------------------+----------------------+------------------------------------------+

.. __: {deb_url}

Python Client
-------------
+-------------------------+------------------+----------------------+------------------------------------------+
| Python 2.7 egg          | {version:>13}__  | {egg_size:>17.1f} KB | MD5: {egg_md5:>32}    |
+-------------------------+------------------+----------------------+------------------------------------------+
| Ubuntu x86_64 Q-Package | {version:>13}    | name: arakoon_client | domain: pylabs.org                       |
+-------------------------+------------------+----------------------+------------------------------------------+

.. __: {egg_url}

OCaml client
------------
+-------------------------+------------------+----------------------+------------------------------------------+
| Debian Package x86_64   | {version:>13}__  | {lib_size:>17.1f} KB | MD5: {lib_md5:>32}    |
+-------------------------+------------------+----------------------+------------------------------------------+

.. __: {lib_url}

Sources
-------
+---------+-----------------+-------------------------+---------------------------------------+
| Archive | {version:>13}__ | {source_size:>20.2f} KB | MD5: {source_md5:>32} |
+---------+-----------------+-------------------------+---------------------------------------+

.. __: {source_url}

Download Archives
=================
Older releases of Arakoon can be found on the `Archives page`_.

.. _Archives page: http://...

"""

parser = argparse.ArgumentParser()
parser.add_argument('--version', required= True)
options = parser.parse_args()
version = options.version 
hg_id = '80878f0d8b8b'
base_url = "https://bitbucket.org/despiegk/arakoon" 
deb_url = "%s/downloads/arakoon_%s-1_amd64.deb" % (base_url, version)
egg_url = "%s/downloads/arakoon-%s-py2.7.egg" % (base_url, version)
source_url = "%s/get/%s.tar.bz2" % (base_url,hg_id)
lib_url = "%s/downloads/libarakoon-ocaml-dev_%s-1.amd64.deb" % (base_url, version)

def check_download(url):
    f = urllib.urlopen(url)
    data =  f.read()
    size = len(data)
    size_kb = size / 1024.0
    m = md5.new()
    m.update(data)
    digest = m.hexdigest()
    return size_kb, digest


deb_size, deb_md5 = check_download(deb_url)
egg_size, egg_md5 = check_download(egg_url)
source_size, source_md5 = check_download(source_url)
lib_size, lib_md5 = check_download(lib_url)

f = string.Formatter()
params = {
    'version': version,
    'deb_url': deb_url,
    'deb_size': deb_size,
    'deb_md5': deb_md5,
    'egg_url': egg_url,
    'egg_size': egg_size,
    'egg_md5': egg_md5,
    'source_url': source_url,
    'source_size': source_size,
    'source_md5': source_md5,
    'lib_url' : lib_url,
    'lib_size': lib_size,
    'lib_md5': lib_md5
    }
result =  f.format(template, **params)

with open('download-%s-test.rst' % version,'w') as file:
    file.write(result)
