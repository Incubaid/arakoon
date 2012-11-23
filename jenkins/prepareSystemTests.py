from pymonkey.InitBase import i,q
import os
import subprocess

packages = [
            ('qbase_extra','3.2'),
            ('testrunner','1.1'),
           ]

for (p,v) in packages:
    pkg = i.qp.find(p,version = v)
    pkg.install()

