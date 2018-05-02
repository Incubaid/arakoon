import os
import nose
import subprocess
import sys

root = os.environ.get('TEST_HOME')
if root is None:
    root = os.environ['HOME']

bin_dir = '%s/apps/arakoon/bin' % root
bin = bin_dir +'/arakoon'

def prologue():
    if not os.path.exists(bin_dir):
        os.makedirs(bin_dir)
    if not os.path.exists(bin):
        subprocess.call(['cp','./arakoon.native', bin])
        print "=> copying exec to: %s" % bin
    else:
        version = subprocess.check_output(['./arakoon.native','--version'])
        version2 = subprocess.check_output([bin,'--version'])
        if version <> version2:
            print version,version2
            print "=> copying exec"
            subprocess.call(['cp','./arakoon.native', bin])
        else:
            print "not copying arakoon executable"


prologue()

env = os.environ
pwd = os.getcwd()
paths = ':'.join(map (lambda x: pwd + x, ['/pylabs','/pylabs/extensions','/pylabs/pyrakoon']))
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
