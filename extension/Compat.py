import os
import ConfigParser
import shutil
import logging
import subprocess


class Status:
    HALTED = 'HALTED'
    RUNNING = 'RUNNING'

    def __init__(self):
        pass

class Compat:
    def __init__(self):
        self.logging = logging

    cluster_id = 'sturdy'
    node_names = [ "sturdy_0", "sturdy_1", "sturdy_2" ]
    node_ips = [ "127.0.0.1", "127.0.0.1", "127.0.0.1"]
    node_client_base_port = 7080
    node_msg_base_port = 10000
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
    nursery_keeper_id = nursery_cluster_ids[0]
    
    
    def fileExists(self,fn):
        return os.path.exists(fn)

    def createDir(self,fn):
        if not os.path.exists(fn):
            return os.makedirs(fn)


    def removeDirTree(self,fn):
        try:
            return shutil.rmtree(fn)
        except:
            pass


    def raiseError(self,x):
        raise Exception(x)


    def getConfig(self, h):
        fn = h + '.cfg'
        logging.debug("reading %s",fn)
        p = ConfigParser.ConfigParser()
        p.read(fn)
        return p

    def writeConfig(self, p, h):
        fn = h + '.cfg'
        with open(fn,'w') as cfg:
            p.write(cfg)


    AppStatusType = Status()

    Popen = subprocess.Popen
    subprocess = subprocess


    _base = '/tmp/X'
    tmpDir = _base + '/tmp'
    appDir = _base + '/apps'
    cfgDir = _base + '/cfg'
    logDir = _base + '/log'
    varDir = _base + '/var'

X = Compat()
