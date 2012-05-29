from setuptools import setup


from subprocess import Popen, PIPE

def shell(cmd):
    v = Popen(cmd, stdout = PIPE).communicate()[0]
    v2 = v.strip()
    return v2

def get_info():
    s = shell(["git","describe","--all","--long","--always","--dirty"])
    return s

def get_version():
    info = get_info()
    return info

def get_branch():
    info = get_info()
    i0 = info.find('/') +1
    return info[i0:]

def get_license():
    data = None
    with open('COPYING','r') as f:
        data = f.read()
    return data

description =\
"""Arakoon is a simple distributed key value store.
This package provides a pure python client for Arakoon.

Git info: %s
""" % (get_version(),)

setup(name='arakoon',
      version=get_branch(),
      package_dir={'arakoon':'src/client/python'},
      packages=['arakoon'],
      data_files = [('license',['COPYING'])],
      url='http://www.arakoon.org',
      description=description,
      zip_safe=True,
      license= get_license()
      )

