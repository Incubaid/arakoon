from setuptools import setup


from subprocess import Popen, PIPE

def shell(cmd):
    v = Popen(cmd, stdout = PIPE).communicate()[0]
    v2 = v.strip()
    return v2

def get_info(option):
    return shell(["hg","id",option])

def get_tag():
    return get_info('-t')

def get_version():
    return get_info('-i')

def get_branch():
    return shell(["hg","branch"])

def get_license():
    data = None
    with open('COPYING','r') as f:
        data = f.read()
    return data

description =\
"""Arakoon is a simple distributed key value store.
This package provides a pure python client for Arakoon.

Mercurial version: %s
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

