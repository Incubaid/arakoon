import os
import nose
import subprocess
import sys

root = "/tmp/X"
bin_dir = '%s/apps/arakoon/bin' % root
bin = bin_dir +'/arakoon'

def prologue():
    if not os.path.exists(bin):
        if not os.path.exists(bin_dir):
            os.makedirs(bin_dir)
        subprocess.call(['cp','./arakoon.native',
                         bin])


prologue()

env = os.environ
pwd = os.getcwd()
paths = ':'.join(map (lambda x: pwd + x, ['/pylabs','/pylabs/extensions']))
print paths
env ['PYTHONPATH'] = paths
cmd = ['nosetests']
rest = sys.argv[1:]
cmd.extend(sys.argv[1:])
subprocess.call(cmd, 
                cwd = './pylabs',
                env = env
                )

