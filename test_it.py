import os
import nose
import subprocess
import sys

root = "/tmp/X"
bin_dir = '%s/apps/arakoon/bin' % root
bin = bin_dir +'/arakoon'

def prologue():
    if not os.path.exists(bin_dir):
        os.makedirs(bin_dir)
    if not os.path.exists(bin):
        subprocess.call(['cp','./barakoon.native', bin])            
    else:
        version = subprocess.check_output(['./barakoon.native','--version'])
        version2 = subprocess.check_output([bin,'--version'])
        if version <> version2:
            print version,version2
            print "=> copying exec"
            subprocess.call(['cp','./barakoon.native', bin])


prologue()

env = os.environ
pwd = os.getcwd()
paths = ':'.join(map (lambda x: pwd + x, ['/pylabs','/pylabs/extensions']))
print paths
env ['PYTHONPATH'] = paths
cmd = ['nosetests', '-w', 'test']
ldlib = 'LD_LIBRARY_PATH'
if env.has_key(ldlib):
    print "LD_LIBRARY_PATH=%s" % env[ldlib] 
rest = sys.argv[1:]
cmd.extend(rest)
subprocess.call(cmd, 
                cwd = './pylabs',
                env = env
                )

