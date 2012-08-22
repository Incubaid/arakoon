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


v = subprocess.Popen(['hg','branch'], stdout = subprocess.PIPE).communicate()[0]
branch = v.strip()

coDir = "/".join( [q.dirs.tmpDir, "arakoon-x"] )

print "branch=%s, coDir=%s" % (branch,coDir) 

q.system.fs.createDir( coDir )
q.system.process.run( "hg clone https://bitbucket.org/despiegk/arakoon %s" % coDir ) 
q.system.process.run( "hg update %s" % branch, cwd = coDir)
