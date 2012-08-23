from pymonkey.InitBase import i,q
import os
import subprocess

packages = [
            ('testrunner','1.1'),
            ('arakoon_dev','0.10.0'),
            ('mercurial', '1.9.2'),
           ]

for (p,v) in packages:
    pkg = i.qp.find(p,version = v)
    pkg.install()

for (k,v) in os.environ.items():
    print k


def run_it(cmd):
    p = subprocess.Popen(cmd, stdout = subprocess.PIPE)
    r = p.communicate()[0]
    rc = p.returncode
    if rc != 0:
        e = Exception("cmd: %s returned %i" % (cmd, rc))
        raise e
    return r


which_hg = run_it(['which','hg'])
hg_version = run_it(['hg', 'version'])
v = run_it(['hg','branch'])
branch = v.strip()

coDir = "/".join( [q.dirs.tmpDir, "arakoon-x"] )

print "which_hg=%s\nhg_version=%s\nbranch=%s\ncoDir=%s" % (which_hg, hg_version, branch, coDir) 

q.system.fs.createDir( coDir )

run_it(["hg","clone","https://bitbucket.org/despiegk/arakoon", coDir]) 
run_it(["hg","update", branch], cwd = coDir)
