import os
import ConfigParser
import shutil
import logging
import subprocess
import StringIO
import fnmatch
import sys

class Status:
    HALTED = 'HALTED'
    RUNNING = 'RUNNING'

    def __init__(self):
        pass


"""
    cluster_id = 'sturdy'
    node_names = [ "sturdy_0", "sturdy_1", "sturdy_2" ]
    


    daemon_name = "arakoon"
    binary_full_path = "arakoon"
    lease_duration = 2.0
    tlog_entries_per_tlog = 1000
    
    nursery_nodes = {
        'nurse_0' : [ 'nurse_0_0', 'nurse_0_1', 'nurse_0_2'],
        'nurse_1' : [ 'nurse_1_0', 'nurse_1_1', 'nurse_1_2'],
        'nurse_2' : [ 'nurse_2_0', 'nurse_2_1', 'nurse_2_2']
        }
    nursery_cluster_ids = nursery_nodes.keys()
    nursery_cluster_ids.sort()

"""    

class Compat:
    def __init__(self):
        self.logging = logging
        self._count = 0

    def cfg2str(self, cfg):
        io = StringIO.StringIO()
        cfg.write(io)
        v = io.getvalue()
        io.close()
        return v

    def fileExists(self,fn):
        return os.path.exists(fn)

    def createDir(self,fn):
        if not os.path.exists(fn):
            return os.makedirs(fn)


    def isDir(self,fn):
        return os.path.isdir(fn)

    def removeDirTree(self,fn):
        try:
            return shutil.rmtree(fn)
        except:
            pass

    def removeFile(self,fn):
        return os.unlink(fn)

    def listFilesInDir(self, d, filter):
        files = os.listdir(d)
        logging.debug("original=%s", files)
        r = ["%s/%s" % (d,x) for x in files if fnmatch.fnmatch(x, filter)]
        logging.debug("filtered = %s", r)
        return r

    def raiseError(self,x):
        raise Exception(x)


    def getConfig(self, h):
        fn = h + '.cfg'
        logging.debug("reading %s",fn)
        p = ConfigParser.ConfigParser()
        p.read(fn)
        return p

    def writeConfig(self, p, h):
        self._count = self._count + 1
        fn = h + '.cfg'
        #logging.debug("writing (%i)%s:\n%s", self._count, fn, self.cfg2str(p))
        with open(fn,'w') as cfg:
            p.write(cfg)


    AppStatusType = Status()

    subprocess = subprocess


    _base = '/tmp/X'
    tmpDir = _base + '/tmp'
    appDir = _base + '/apps'
    cfgDir = _base + '/cfg'
    logDir = _base + '/log'
    varDir = _base + '/var'


class Q: # (Compat)
    
    def __init__(self):
        self.tmpDir = q.dirs.tmpDir
        self.appDir = q.dirs.appDir
        self.cfgDir = q.dirs.cfgDir
        self.logDir = q.dirs.logDir
        self.varDir = q.dirs.varDir


def which_compat():
    g = globals()
    if g.has_key('q') and g['q'].__class__.__name__ == 'PYMONKEY':
        print "in q's hell"
        r = Q()
    else:
        r = Compat()

    return r



X = which_compat()
