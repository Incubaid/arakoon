from pymonkey.InitBase import i,q
import os

packages = [
            ('testrunner','1.1'),
            ('arakoon_dev','0.10.0'),
           ]

for (p,v) in packages:
    pkg = i.qp.find(p,version = v)
    pkg.install()

for (k,v) in os.environ.items():
    print k


branch = "1.3"
coDir = "/".join( [q.dirs.tmpDir, "arakoon-x"] )
q.system.fs.createDir( coDir )
q.system.process.run( "hg clone https://bitbucket.org/despiegk/arakoon %s" % coDir ) 
q.system.process.run( "hg update %s" % branch, cwd = coDir)
