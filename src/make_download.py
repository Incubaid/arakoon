import string
import urllib
import md5
import argparse

template = """
=========
Downloads
=========
On this page you can find all the required packages to install Arakoon Release {version}

Arakoon {version} can only be installed on Linux. We provide packages for Debian amd64.

If you want support for other distros, please contact us and we'll see what we can do.

Licensing
=========
Arakoon is released under a dual license model as specified on the `licensing page`_.

.. _licensing page: licensing.html

Arakoon version {version}
======================
`Release notes`_

.. _Release notes: releases/{version}.html

Server
------
+-------------------------+------------------+----------------------+------------------------------------------+
| Debian Package x86_64   | {version:>13}__  | {deb_size:>17.0f} KB | MD5: {deb_md5:>32}    |
+-------------------------+------------------+----------------------+------------------------------------------+

.. __: {deb_url}

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

.. _Archives page: archives.html

"""

parser = argparse.ArgumentParser()
parser.add_argument('--version', required= True)
parser.add_argument('--rev', required= True)

# python make_download.py
# --version 1.6.5
# --rev a8792674f01e064ea4e1414250b0bbb8d072a4f3

options = parser.parse_args()
version = options.version
revision = options.rev

base_url = "https://github.com/Incubaid/arakoon"
#https://github.com/Incubaid/arakoon/releases/1.6.0/2444/arakoon_1.6.0_amd64.deb
deb_url = "%s/releases/download/%s/arakoon_%s_amd64.deb" % (base_url, version,version)
#egg_url = "%s/releases/%s/.../arakoon-%s-py2.7.egg" % (base_url, version,version)
#https://github.com/Incubaid/arakoon/archive/1.6.0.tar.gz
source_url = "%s/archive/%s.tar.gz" % (base_url, version)
lib_url = "%s/releases/download/%s/libarakoon-ocaml-dev_%s_amd64.deb" % (base_url,
                                                                         version,
                                                                         version)

def check_download(url):
    f = urllib.urlopen(url)
    code = f.getcode()
    if 200 != code:
        raise RuntimeError('Failed to download URL "%s"' % url)
    data = f.read()
    size = len(data)
    size_kb = size / 1024.0
    m = md5.new()
    m.update(data)
    digest = m.hexdigest()
    return size_kb, digest


deb_size, deb_md5 = check_download(deb_url)
#egg_size, egg_md5 = check_download(egg_url)
source_size, source_md5 = check_download(source_url)
lib_size, lib_md5 = check_download(lib_url)

f = string.Formatter()
params = {
    'version': version,
    'deb_url': deb_url,
    'deb_size': deb_size,
    'deb_md5': deb_md5,
#    'egg_url': egg_url,
#    'egg_size': egg_size,
#    'egg_md5': egg_md5,
    'source_url': source_url,
    'source_size': source_size,
    'source_md5': source_md5,
    'lib_url' : lib_url,
    'lib_size': lib_size,
    'lib_md5': lib_md5
    }
result =  f.format(template, **params)

with open('download-%s.rst' % version,'w') as file:
    file.write(result)
