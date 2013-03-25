from pymonkey.InitBase import i,q
import os
import subprocess

packages = [
            ('testrunner','1.1'),
            ('arakoon_dev','0.10.0'),
           ]

for (p,v) in packages:
    pkg = i.qp.find(p,version = v)
    pkg.install()

for (k,v) in os.environ.items():
    print k


def run_it(cmd, cwd = None):
    p = subprocess.Popen(cmd, stdout = subprocess.PIPE, cwd = cwd)
    r = p.communicate()[0]
    rc = p.returncode
    if rc != 0:
        e = Exception("cmd: %s returned %i" % (cmd, rc))
        raise e
    return r


which_git = run_it(['which','git'])
git_version = run_it(['git', '--version'])
rev = run_it(['git','rev-parse', 'HEAD']).strip()

coDir = "/".join( [q.dirs.tmpDir, "arakoon-x"] )

print "which_git=%s\ngit_version=%s\nrev=%s\ncoDir=%s" % (which_git, git_version, rev, coDir)

q.system.fs.createDir( coDir )

run_it(["git","clone","git://github.com/Incubaid/arakoon", coDir])
run_it(["git","checkout", rev], cwd = coDir)
