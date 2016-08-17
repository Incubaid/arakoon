from setuptools import setup


from subprocess import Popen, PIPE

def shell(cmd):
    v = Popen(cmd, stdout = PIPE).communicate()[0]
    v2 = v.strip()
    return v2

def get_version():
    return shell(["git", "describe", "--tags", "--exact-match", "--dirty"])

def get_license():
    data = None
    with open('LICENSE','r') as f:
        data = f.read()
    return data

description =\
"""Arakoon is a simple distributed key value store.
This package provides a pure python client for Arakoon.

Git version: %s
""" % (get_version(),)

setup(name='arakoon',
      version=get_version(),
      package_dir={'arakoon':'src/client/python'},
      packages=['arakoon'],
      data_files = [('license',['LICENSE'])],
      url='https://github.com/openvstorage/arakoon',
      description=description,
      author='openvstorage.com & incubaid',
      classifiers=['Development Status :: 5 - Production/Stable',
                   'Operating System :: OS Independent',
                   'Topic :: Database',
                   ],
      zip_safe=True,
      license= get_license()
      )
