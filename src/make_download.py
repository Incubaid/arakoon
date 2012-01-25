import string
import urllib
import md5

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
+-------------------------+---------------+----------------+---------------------------------------+
| Debian Package x86_64   | {version}__  | {deb_size:.0f} KB   | MD5: {deb_md5}                    |
+-------------------------+---------------+----------------+---------------------------------------+
| Ubuntu x86_64 Q-Package | {version}    | name: arakoon  | domain: pylabs.org                     |
+-------------------------+---------------+----------------+---------------------------------------+

.. __: {deb_url}

Python Client
-------------
+-------------------------+---------------+----------------------+---------------------------------------+
| Python 2.6 egg          | {version}__  | {egg_size:.0f} KB     | MD5: {egg_md5}                        |
+-------------------------+---------------+----------------------+---------------------------------------+
| Ubuntu x86_64 Q-Package | {version}    | name: arakoon_client  | domain: pylabs.org                    |
+-------------------------+---------------+----------------------+---------------------------------------+

.. __: {egg_url}

OCaml client
------------
+-------------------------+----------+-------+---------------------------------------+
| Debian Package x86_64   | {version}__ | {lib_size:.2f} KB | MD5: {lib_md5}         |
+-------------------------+----------+-------+---------------------------------------+

.. __: http://...

Sources
-------
+---------+--------------+--------+---------------------------------------+
| Archive | {version}__  | {source_size:.2f} KB | MD5: {source_md5}       |
+---------+--------------+--------+---------------------------------------+

.. __: {source_url}

Download Archives
=================
Older releases of Arakoon can be found on the `Archives page`_.

.. _Archives page: http://...

"""

version = "1.0.1"
deb_url = "https://bitbucket.org/despiegk/arakoon/downloads/arakoon_1.0.1-1_amd64.deb"
egg_url = "https://bitbucket.org/despiegk/arakoon/downloads/arakoon-1.0.1-py2.6.egg"
source_url = "https://bitbucket.org/despiegk/arakoon/get/77db3bc403b8.tar.bz2"
lib_url = "???"

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
lib_size, lib_md5 = 42.0,"???"

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
