import os
import nose
import subprocess
import sys

root = "/tmp/X"
bin_dir = '%s/apps/arakoon/bin' % root
bin = bin_dir +'/arakoon'

if not os.path.exists(bin):
    if not os.path.exists(bin_dir):
        os.makedirs(bin_dir)
    subprocess.call(['cp','./arakoon.native',
                     bin])
env = os.environ
env ['PYTHONPATH'] = './server:./client'
cmd = ['nosetests']
rest = sys.argv[1:]
cmd.extend(sys.argv[1:])
subprocess.call(cmd, 
                cwd = './extension',
                env = env
                )

