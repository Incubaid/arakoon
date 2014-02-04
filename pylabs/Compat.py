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

VAR = 'ARAKOON_PYTHON_CLIENT'

if os.environ.has_key(VAR) and os.environ[VAR] == 'pyrakoon':
    logging.info("opting for pyrakoon")
    print "pyrakoon"
    from pyrakoon import compat
    arakoon_client = compat
else:
    logging.info("opting for normal client")
    print "arakoon"
    from arakoon import Arakoon
    arakoon_client = Arakoon


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
def _cfg2str(self, cfg):
    io = StringIO.StringIO()
    cfg.write(io)
    v = io.getvalue()
    io.close()
    return v

def _getConfig(self, h):
    fn = h + '.cfg'
    self.logging.debug("reading %s",fn)
    p = ConfigParser.ConfigParser()
    p.read(fn)
    #logging.debug("config file=\n%s", self.cfg2str(p))
    return p

def sectionAsDict(config, name):
    d = {}
    for option in config.options(name):
        d[option] = config.get(name,option,False)
    return d

def _writeConfig(self, p, h):
    self._count = self._count + 1
    fn = h + '.cfg'
    logging.debug("writing (%i)%s:\n%s", self._count, fn, self.cfg2str(p))
    with open(fn,'w') as cfg:
        p.write(cfg)
        

def _run(self,cmd):
    #['mount', '-t', 'tmpfs', '-o', 'size=20m', 'tmpfs', '/opt/qbase3/var/tmp//arakoon_system_tests/test_disk_full_on_slave/sturdy_0/db']
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr = subprocess.PIPE)
    proc.wait()
    output = proc.stdout.read()    
    err = proc.stderr.read()
    return (proc.returncode, output,err)

class Compat:
    def __init__(self):
        self.logging = logging
        self._count = 0

    def fileExists(self,fn):
        return os.path.exists(fn)

    def createDir(self,fn):
        self.logging.debug("createDir(%s)", fn)
        if not os.path.exists(fn):
            return os.makedirs(fn)


    def isDir(self,fn):
        return os.path.isdir(fn)

    def removeDirTree(self,fn):
        self.logging.debug("removeDirTree(%s)", fn)
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

    def getFileContents(self, fn):
        with open(fn,'r') as f:
            data = f.read()
            return data

    def raiseError(self,x):
        raise Exception(x)

    def copyDirTree(self,src,dst):
        shutil.copytree(src,dst)

    run = _run

    cfg2str = _cfg2str
    writeConfig = _writeConfig
    getConfig = _getConfig

    AppStatusType = Status()

    subprocess = subprocess


    _base = '/tmp/X'
    tmpDir = _base + '/tmp'
    appDir = _base + '/apps'
    cfgDir = _base + '/cfg'
    logDir = _base + '/log'
    varDir = _base + '/var'


class _LOGGING:
    def __init__(self,q):
        self._q = q
        
    def info(self,template,*rest):
        message = template % rest
        self._q.logger.log(message,level = 10)

    def debug(self,template,*rest):
        message = template % rest
        self._q.logger.log(message,level = 5)
            

class Q: # (Compat)
    def __init__(self):
        try:
            from pymonkey import q
        except ImportError:
            from pylabs import q

        self.tmpDir = q.dirs.tmpDir
        self.appDir = q.dirs.appDir
        self.cfgDir = q.dirs.cfgDir
        self.logDir = q.dirs.logDir
        self.varDir = q.dirs.varDir
        self._q = q
        self._count = 0
        self.logging = _LOGGING(q)
        self.AppStatusType = q.enumerators.AppStatusType
        self.listFilesInDir = q.system.fs.listFilesInDir

        self.subprocess = subprocess 
        def check_output(cmd, **kwargs):
            return subprocess.Popen(cmd, stdout=subprocess.PIPE, **kwargs).communicate()[0]
        self.subprocess.check_output = check_output

    def fileExists(self,fn):
        return self._q.system.fs.exists(fn)

    getConfig = _getConfig
    writeConfig = _writeConfig
            
    def removeDirTree(self,path):
        return self._q.system.fs.removeDirTree(path)

    def createDir(self,path):
        return self._q.system.fs.createDir(path)

    def removeFile(self,path):
        os.unlink(path)
    
    def getFileContents(self, path):
        data = self._q.system.fs.fileGetContents(path)
        return data

    def raiseError(self, s):
        raise Exception(s)

    def copyDirTree(self, source, destination):
        self._q.system.fs.copyDirTree(source,destination)
    cfg2str = _cfg2str
    run = _run
    

def which_compat():
    print "which_compat"
    g = globals()
    if sys.prefix == '/opt/qbase3':
        r = Q()
    else:
        r = Compat()
    return r



X = which_compat()
X.arakoon_client = arakoon_client
