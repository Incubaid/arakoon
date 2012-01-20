from pymonkey.InitBase import i,q

packages = [
            ('testrunner','1.1'),
            ('arakoon_dev','0.10.0'),
           ]

for (p,v) in packages:
    pkg = i.qp.find(p,version = v)
    pkg.install()

coDir = "/".join( [q.dirs.tmpDir, "arakoon-1.0"] )
q.system.fs.createDir( coDir )
q.system.process.run( "hg clone https://bitbucket.org/despiegk/arakoon %s" % coDir ) 

