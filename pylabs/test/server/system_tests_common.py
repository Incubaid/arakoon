"""
Copyright (2010-2014) INCUBAID BVBA

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""




from Compat import X
from nose.tools import *
from functools import wraps
import traceback
import sys
import struct
import subprocess
import signal
import gzip
import logging
import os
import random
import threading
import time

from arakoon_ext.server import ArakoonManagement
from arakoon_ext.client import ArakoonClient

test_failed = False

class Config:
    def __init__(self):
        self.lease_duration = 2.0
        self.tlog_entries_per_tlog = 1000
        self.node_names = [ "sturdy_0", "sturdy_1", "sturdy_2" ]
        self.cluster_id = 'sturdy'
        self.key_format_str = "key_%012d"
        self.value_format_str = "value_%012d"
        self.data_base_dir = None
        self.binary_full_path = ArakoonManagement.which_arakoon()
        self.nursery_nodes = {
            'nurse_0' : [ 'nurse_0_0', 'nurse_0_1', 'nurse_0_2'],
            'nurse_1' : [ 'nurse_1_0', 'nurse_1_1', 'nurse_1_2'],
            'nurse_2' : [ 'nurse_2_0', 'nurse_2_1', 'nurse_2_2']
            }
        self.nursery_cluster_ids = self.nursery_nodes.keys()
        self.node_client_base_port = 7080
        self.node_msg_base_port = 10000
        self.nursery_keeper_id = self.nursery_cluster_ids[0]
        self.node_ips = [ "127.0.0.1", "127.0.0.1", "127.0.0.1"]

CONFIG = Config()

class with_custom_setup ():

    def __init__ (self, setup, teardown):
        self.__setup = setup
        self.__teardown = teardown

    def __call__ (self, func ):
        @wraps(func)
        def decorate(*args,**kwargs):

            global data_base_dir
            data_base_dir = '/'.join([X.tmpDir,'arakoon_system_tests' , func.func_name])
            global test_failed
            test_failed = False
            fatal_ex = None
            home_dir = data_base_dir
            if X.fileExists( data_base_dir):
                remove_dirs ()
            self.__setup( home_dir )
            try:
                func(*args,**kwargs)
            except Exception, outer :
                tb = traceback.format_exc()
                logging.fatal( tb )
                fatal_ex = outer
            finally:
                self.__teardown( fatal_ex is None )

            if fatal_ex is not None:
                raise fatal_ex
        return decorate


data_base_dir = None
cluster_id = 'sturdy'
node_names = ['sturdy_%d' % n for n in xrange(5)]
node_ips = ["127.0.0.1"] * len(node_names)
node_client_base_port = 7080
node_msg_base_port = 10000
daemon_name = "arakoon"

lease_duration = 8.0 # was 2.0 s but is not enough on our virtualized test env.
tlog_entries_per_tlog = 1000

nursery_nodes = {
   'nurse_0' : [ 'nurse_0_0', 'nurse_0_1', 'nurse_0_2'],
   'nurse_1' : [ 'nurse_1_0', 'nurse_1_1', 'nurse_1_2'],
   'nurse_2' : [ 'nurse_2_0', 'nurse_2_1', 'nurse_2_2']
}
nursery_cluster_ids = nursery_nodes.keys()
nursery_cluster_ids.sort()
nursery_keeper_id = nursery_cluster_ids[0]

key_format_str = "key_%012d"
value_format_str = "value_%012d"

def generate_lambda( f, *args, **kwargs ):
    return lambda: f( *args, **kwargs )

def _getCluster( c_id = None):
    if c_id is None:
        c_id = cluster_id
    mgmt = ArakoonManagement.ArakoonManagement()
    return mgmt.getCluster(c_id)

def call_arakoon(*params):
    cmd = [CONFIG.binary_full_path] + list(params)
    r = X.subprocess.check_output(cmd)
    return r

def dump_tlog (node_id, tlog_number) :
    cluster = _getCluster()
    node_home_dir = cluster.getNodeConfig(node_id ) ['home']
    tlog_full_path =  '/'.join ([node_home_dir, "%03d.tlog" % tlog_number]  )
    cmd = [CONFIG.binary_full_path, "--dump-tlog", tlog_full_path]
    logging.debug( "Dumping file %s" % tlog_full_path )
    logging.debug("Command is : '%s'" % cmd )
    stdout = X.subprocess.check_output(cmd)
    return stdout

def get_arakoon_binary() :
    return '/'.join([get_arakoon_bin_dir(), 'arakoon'])

def get_arakoon_bin_dir():
    return '/'.join([X.appDir, "arakoon", "bin"])

def get_tcbmgr_path ():
    return '/'.join([get_arakoon_bin_dir(), "tcbmgr"] )

def get_diff_path():
    return "/usr/bin/diff"

def get_node_db_file( node_id ) :
    cluster = _getCluster()
    node_home_dir = cluster.getNodeConfig(node_id ) ['home']
    db_file = '/'.join([node_home_dir, node_id + ".db" ])
    return db_file

def dump_store( node_id ):
    cluster = _getCluster()
    stat = cluster.getStatusOne(node_id )
    msg = "Can only dump the store of a node that is not running (status is %s)" % stat
    assert_equals( stat, X.AppStatusType.HALTED, msg)

    db_file = get_node_db_file ( node_id )
    dump_file = '/'.join([X.tmpDir,"%s.dump" % node_id])
    cmd = get_tcbmgr_path() + " list -pv " + db_file
    try:
        dump_fd = open( dump_file, 'w' )
        logging.info( "Dumping store of %s to %s" % (node_id, dump_file) )
        stdout= X.subprocess.check_output(cmd)
        dump_fd.write(stdout)
        dump_fd.close()
    except OSError as ose:
        if ose.errno == 2:
            logging.info("store :%s is empty" % db_file)
        else:
            raise ose
    except Exception as ex:
        logging.info("Unexpected error: %s" % sys.exc_info()[0])
        raise ex
    return dump_file

def flush_store(node_name):
    cluster = _getCluster()
    cluster.flushStore(node_name)

def flush_stores(nodes = None):
    if nodes is None:
        nodes = _getCluster().listNodes()

    for node in nodes:
        flush_store( node )


def get_i(node_id, head=False):
    cluster = _getCluster()
    stat = cluster.getStatusOne(node_id )
    assert_equals( stat, X.AppStatusType.HALTED, "Can only dump the store of a node that is not running")
    if head:
        (home_dir, _, _, head_dir) = build_node_dir_names(node_id)
        db_file = head_dir + "/head.db"
    else:
        db_file = get_node_db_file(node_id)

    logging.debug("%s exists: %s" % (db_file, os.path.exists(db_file)))

    cmd = [get_arakoon_binary(), "--dump-store", db_file]
    logging.debug("get_i:cmd = %s" % str(cmd))
    stdout=  X.subprocess.check_output(cmd)
    print "\nstdout=\n"
    print stdout
    i_line = stdout.split("\n") [0]
    logging.info("i_line='%s'", i_line)

    if i_line.find("None") <> -1:
        r = 0
    else:
        i_str = i_line.split("(")[1][:-1]
        i_str2 = i_str[1:-1]
        r =  int(i_str2)

    return r

def compare_stores( node1_id, node2_id ):

    keys_to_skip = [ "*lease", "*lease2", "*i", "*master" ]
    dump1 = dump_store( node1_id )
    dump2 = dump_store( node2_id )

    # Line 2 contains the master lease, can be different as it contains a timestamp
    d1_fd = open ( dump1, "r" )
    d2_fd = open ( dump2, "r" )

    i1 = get_i (node1_id)
    logging.debug("Counter value for store of %s: %d" % (node1_id,i1))
    i2 = get_i (node2_id)
    logging.debug("Counter value for store of %s: %d" % (node2_id,i2))
    if( abs (i1 - i2) > 1 ):
        logging.error( "Store counters differ too much (%s: %d and %s: %d)" % (node1_id,i1,node2_id,i2) )

    i1_line = d1_fd.readline()
    i2_line = d2_fd.readline()

    diffs = { node1_id : {} , node2_id : {} }

    def get_next_kv ( fd ):

        line = fd.readline()
        if line == "" :
            return (None,None)
        parts = line.split("\t")
        if len( parts ) < 2 :
            return get_next_kv( fd )
        else :
            return ( parts[0], "\t".join(parts[1:]))

    iter = 0
    (k1,v1) = get_next_kv( d1_fd )
    (k2,v2) = get_next_kv( d2_fd )

    while k1 != None and k2 != None :
        iter+=1
        if( ( k1 == k2 and v1 == v2) ) :
            (k1,v1) = get_next_kv( d1_fd )
            (k2,v2) = get_next_kv( d2_fd )
        else :

            if k1 == k2 :
                if k1 not in keys_to_skip:
                    diffs[node1_id][k1] = v1
                    diffs[node2_id][k2] = v2
                    logging.debug( "Stores have different values for %s" % (k1) )
                (k1,v1) = get_next_kv( d1_fd )
                (k2,v2) = get_next_kv( d2_fd )
            if k1 < k2 :
                logging.debug( "Store of %s has a value for, store of %s doesn't" % (node1_id, node2_id) )
                diffs[node1_id][k1] = v1
                (k1,v1) = get_next_kv( d1_fd )
            if k1 > k2 :
                logging.debug( "Store of %s has a value for, store of %s doesn't" % (node2_id, node1_id) )
                diffs[node2_id][k2] = v2
                (k2,v2) = get_next_kv( d2_fd )

    if k1 != None :
        logging.debug ( "Store of %s contains more keys, store of %s is EOF" %  (node1_id, node2_id) )
        while k1 != None:
            diffs[node1_id][k1] = v1
            (k1,v1) = get_next_kv( d1_fd )
    if k2 != None:
        logging.debug ( "Store of %s contains more keys, store of %s is EOF" %  (node2_id, node1_id) )
        while k2 != None:
            diffs[node2_id][k2] = v2
            (k2,v2) = get_next_kv ( d2_fd )

    max_diffs = 0

    if ( i1 != i2 ):
        max_diffs = 1

    diff_cnt = len( set( diffs[node1_id].keys() ).union( set(diffs[node2_id].keys() ) ) )
    if diff_cnt > max_diffs :
        raise Exception ( "Found too many differences between stores (%d > %d)\n%s" % (diff_cnt, max_diffs,diffs) )

    logging.debug( "Stores of %s and %s are valid" % (node1_id,node2_id))
    return True

def get_tlog_count (node_id ):
    cluster = _getCluster()
    node_home_dir = cluster.getNodeConfig(node_id ) ['home']
    ls = X.listFilesInDir
    tlogs =      ls( node_home_dir, filter="*.tlog" )
    tlogs.extend(ls( node_home_dir, filter="*.tlc" ) )
    tlogs.extend(ls( node_home_dir, filter="*.tlf" ) )
    tlogs.extend(ls( node_home_dir, filter="*.tls" ) )
    return len(tlogs)

def get_last_tlog_id ( node_id ):
    cluster = _getCluster()
    node_home_dir = cluster.getNodeConfig(node_id ) ['home']
    tlog_max_id = 0
    tlog_id = None
    tlogs_for_node = X.listFilesInDir( node_home_dir, filter="*.tlog" )
    for tlog in tlogs_for_node:
        tlog = tlog [ len(node_home_dir):]
        tlog = tlog.strip('/')
        tlog_id = tlog.split(".")[0]
        tlog_id = int( tlog_id )
        if tlog_id > tlog_max_id :
            tlog_max_id = tlog_id
    if tlog_id is not None:
        logging.debug("get_last_tlog_id('%s') => %s" % (node_id, tlog_id))
    else :
        raise Exception ("Not a single tlog found in %s" % node_home_dir )

    return tlog_max_id

def get_last_i_tlog2(node_id):
    """ should be way faster """
    number = get_last_tlog_id(node_id)
    cluster = _getCluster()
    home = cluster.getNodeConfig(node_id )['home']
    tlog_full_path =  '/'.join([home, "%03d.tlog" % number])
    logging.info("reading i from : %s" % tlog_full_path)
    f = open(tlog_full_path,'rb')
    data = f.read()
    f.close()
    index = 0
    dlen = len(data)
    sn = None

    while index < dlen -16:
        sn = struct.unpack_from("q", data, index)[0]
        index = index + 8
        index = index + 4 # skip crc32
        elen = struct.unpack_from("I", data,index)[0]
        index = index + 4 + elen
    return sn

def last_entry_code(node_id):
    number = get_last_tlog_id(node_id)
    cluster = _getCluster()
    home = cluster.getNodeConfig(node_id )['home']
    tlog_full_path =  '/'.join([home, "%03d.tlog" % number])
    f = open(tlog_full_path,'rb')
    data = f.read()
    f.close()
    index = 0
    dlen = len(data)
    sn = None
    while index < dlen:
        sn = struct.unpack_from("q", data, index)[0]
        index = index + 8
        index = index + 4 # skip crc32
        elen = struct.unpack_from("I", data,index)[0]
        index = index + 4
        typ = struct.unpack_from("I", data, index)[0]
        index = index + elen
    return typ

def get_last_i_tlog ( node_id ):
    tlog_dump = dump_tlog ( node_id, get_last_tlog_id(node_id) )
    tlog_dump_list = tlog_dump.split("\n")
    tlog_last_entry = tlog_dump_list [-2]
    tlog_last_i = tlog_last_entry.split(":") [0].lstrip( " 0" )
    return tlog_last_i

def stopOne(name):
    cluster = _getCluster()
    rc = cluster.stopOne(name)
    assert (rc == 0)

def startOne(name, extraParams = None):
    cluster = _getCluster()
    cluster.startOne(name, extraParams)

def dropMaster(name):
    cluster = _getCluster()
    cluster.dropMaster(name)

def copyDbToHead(name, n):
    cluster = _getCluster()
    cluster.copyDbToHead(name, n)

def optimizeDb(name):
    cluster = _getCluster()
    cluster.optimizeDb(name)

def defragDb(name):
    cluster = _getCluster()
    cluster.defragDb(name)

def catchupOnly(name, sourceNode = None):
    cluster = _getCluster()
    cluster.catchupOnly(name, sourceNode)

def restart_all():
    cluster = _getCluster()
    cluster.restart()

def rotate_logs( max_logs_to_keep = 5, compress_old_files = True):
    for node_name in node_names:
        rotate_log( node_name, max_logs_to_keep, compress_old_files)

def send_signal ( node_name, signal ):
    cluster = _getCluster()
    pid = cluster._getPid(node_name)
    if pid is not None:
        X.subprocess.call(['kill', str(pid), '-s', str(signal)] )

def rotate_log(node_name, max_logs_to_keep, compress_old_files ):
    cfg = getConfig(node_name)
    log_dir = cfg['log_dir']

    log_file = '/'.join([log_dir, "%s.log" % (node_name) ])
    if compress_old_files:
        old_log_fmt = '/'.join([log_dir, "%s.log.%%d.gz" % (node_name) ])
    else :
        old_log_fmt = '/'.join([log_dir, "%s.log.%%d" % (node_name) ])

    tmp_log_file = log_file + ".1"

    def shift_logs ( ) :
        log_to_remove = old_log_fmt % (max_logs_to_keep - 1)
        if X.fileExists ( log_to_remove ) :
            fs.unlink(log_to_remove)

        for i in range( 1, max_logs_to_keep - 1) :
            j = max_logs_to_keep - 1 - i
            log_to_move = old_log_fmt % j
            new_log_name = old_log_fmt % (j + 1)
            if X.fileExists( log_to_move ) :
                os.rename ( log_to_move, new_log_name )
    cluster = _getCluster()
    shift_logs()
    if X.fileExists( log_file ):
        os.rename ( log_file, tmp_log_file )
        if cluster.getStatusOne(node_name) == X.AppStatusType.RUNNING:
            send_signal ( node_name, signal.SIGUSR1 )

        if compress_old_files:
            cf = gzip.open( old_log_fmt % 1 , 'w')
            orig = open(tmp_log_file, 'r' )
            cf.writelines(orig)
            cf.close()
            orig.close()
            os.remove(tmp_log_file)


def getConfig(name):
    cluster = _getCluster()
    return cluster.getNodeConfig(name)


def regenerateClientConfig( cluster_id ):
    h = '/'.join([X.cfgDir,'arakoonclients'])
    p = X.getConfig(h)

    if cluster_id in p.sections():
        clusterDir = p.get(cluster_id, "path")
        clientCfgFile = '/'.join([clusterDir, "%s_client.cfg" % cluster_id])
        if X.fileExists(clientCfgFile):
            X.removeFile(clientCfgFile)

    client = ArakoonClient.ArakoonClient()
    cliCfg = client.getClientConfig( cluster_id )
    cliCfg.generateFromServerConfig()

def wipe(name):
    config = getConfig(name)
    data_dir = config['home']
    dirs = [data_dir]
    def wipe_dir(d):
        X.removeDirTree(d)
        X.createDir(d)
    wipe_dir(data_dir)
    tlf_dir = config.get('tlf_dir')
    if tlf_dir:
        wipe_dir(tlf_dir)
        dirs.append(tlf_dir)

    logging.info("wiped %s (dirs=%s)",name, str(dirs))

def get_memory_usage(node_name):
    cluster = _getCluster()
    pid = cluster._getPid(node_name )
    if pid is None:
        return 0
    cmd = ["ps", "-p", str(pid), "-o", "vsz"]
    try:
        stdout = X.subprocess.check_output( cmd )
    except Exception,e:
        logging.error( "Coud not determine memory usage: %s" % e)
        return 0

    try:
        size_str = stdout.split("\n") [1]
        return int( size_str )
    except Exception as ex:
        logging.error( "Coud not determine memory usage: %s" % ex )
        return 0

def collapse(name, n = 1):
    global cluster_id
    config = getConfig(name)
    ip = config['ip']
    port = config['client_port']
    rc = X.subprocess.call([CONFIG.binary_full_path, '--collapse-remote',cluster_id,ip,port,str(n)])
    return rc

def local_collapse(name, n=1):
    cluster = _getCluster()
    cfg = "%s.cfg" % cluster._getConfigFileName()
    rc = X.subprocess.call([ CONFIG.binary_full_path, '--collapse-local', name, str(n),
                             '-config', cfg ])
    return rc

def add_node ( i ):
    ni = node_names[i]
    logging.info( "Adding node %s to config", ni )
    (db_dir,log_dir,tlf_dir,head_dir) = build_node_dir_names(ni)
    cluster = _getCluster()
    cluster.addNode (
        ni,
        node_ips[i],
        clientPort = node_client_base_port + i,
        messagingPort= node_msg_base_port + i,
        logDir = log_dir,
        logLevel = 'debug',
        home = db_dir,
        tlfDir = tlf_dir,
        headDir = head_dir)
    cluster.disableFsync([ni])
    cluster.addLocalNode (ni )
    cluster.createDirs(ni)

def start_all(clusterId = None) :
    cluster = _getCluster(clusterId )
    cluster.start()
    time.sleep(3.0)

def start_nursery( nursery_size ):
    for i in range(nursery_size):
        clu = _getCluster( nursery_cluster_ids[i])
        clu.start()
    time.sleep(0.2)

def stop_all(clusterId = None ):
    logging.info("stop_all")
    cluster = _getCluster( clusterId )
    rcs = cluster.stop()
    for nn in rcs.keys():
        v = rcs[nn]
        logging.info("rcs[%s] = %i", nn, v)
        assert (v == 0)

def stop_nursery( nursery_size ):
    for i in range(nursery_size):
        clu = _getCluster( nursery_cluster_ids[i])
        clu.stop()

def restart_nursery( nursery_size ):
    stop_nursery(nursery_size)
    start_nursery(nursery_size)

def restart_all(clusterId = None):
    stop_all(clusterId)
    start_all(clusterId)

def restart_random_node():
    node_index = random.randint(0, len(node_names) - 1)
    node_name = node_names [node_index ]
    delayed_restart_nodes( [ node_name ] )

def delayed_restart_all_nodes() :
    delayed_restart_nodes( node_names )

def delayed_restart_nodes(node_list) :
    downtime = random.random() * 60.0
    for node_name in node_list :
        stopOne(node_name )
    time.sleep( downtime )
    for node_name in node_list :
        startOne(node_name )

def delayed_restart_1st_node ():
    delayed_restart_nodes( [ node_names[0] ] )

def delayed_restart_2nd_node ():
    delayed_restart_nodes( [ node_names[1] ] )

def delayed_restart_3rd_node ():
    delayed_restart_nodes( [ node_names[2] ] )

def restart_nodes_wf_sim( n ):
    wf_step_duration = 0.2

    for i in range (n):
        stopOne(node_names[i] )
        time.sleep( wf_step_duration )

    for i in range (n):
        startOne(node_names[i] )
        time.sleep( wf_step_duration )

def getRandomString( length = 16 ) :
    def getRC ():
        return chr(random.randint(0,25) + ord('A'))

    retVal = ""
    for i in xrange( length ) :
        retVal += getRC()
    return retVal

def build_node_dir_names ( nodeName, base_dir = None ):
    if base_dir is None:
        global data_base_dir
        base_dir = data_base_dir
    data_dir = '/'.join([ base_dir, nodeName])
    db_dir = '/'.join([data_dir, "db"])
    log_dir = '/'.join([data_dir, "log"])
    tlf_dir = '/'.join([data_dir, "tlf"])
    head_dir = '/'.join([data_dir, "head"])
    return (db_dir,log_dir,tlf_dir,head_dir)

def setup_n_nodes_base(c_id, node_names, force_master,
                       base_dir, base_msg_port, base_client_port,
                       extra = None, nodes_extra = None,
                       witness_nodes = False, useIPV6=False,
                       slowCollapser = False):

    running_arakoons = X.subprocess.check_output("pgrep -c arakoon || true",
                                                 shell = True).strip()
    print "\n\tpgrep -c arakoon =>%s" % running_arakoons
    logging.info("pgrep -c arakoon =>%s", running_arakoons)

    X.subprocess.check_call("sudo /sbin/iptables -F".split(' ') )

    cluster = _getCluster( c_id )
    cluster.tearDown()

    cluster = _getCluster(c_id)
    logging.info( "Creating data base dir %s" % base_dir )
    X.createDir ( base_dir )

    n = len(node_names)
    ip = "127.0.0.1"
    if useIPV6:
        ip = "::1"

    for i in range (n) :
        is_witness = witness_nodes & (i % 2 != 0)
        nodeName = node_names[ i ]
        (db_dir,log_dir,tlf_dir,head_dir) = build_node_dir_names( nodeName )
        if slowCollapser and (i % 2 == 1):
            collapseSlowdown = 3
        else:
            collapseSlowdown = None
        cluster.addNode(name=nodeName,
                        ip = ip,
                        clientPort = base_client_port+i,
                        messagingPort = base_msg_port+i,
                        logDir = log_dir,
                        home = db_dir,
                        tlfDir = tlf_dir,
                        headDir = head_dir,
                        isWitness = is_witness,
                        collapseSlowdown = collapseSlowdown)

        cluster.addLocalNode(nodeName)
        cluster.createDirs(nodeName)

    cluster.disableFsync()

    if force_master:
        logging.info( "Forcing master to %s", node_names[0] )
        cluster.forceMaster(node_names[0] )
    else :
        logging.info( "Using master election" )
        cluster.forceMaster(None )

    config = cluster._getConfigFile()
    for i in range (n):
        nodeName = node_names[ i ]
        config.set(nodeName, '__tainted_fsync_tlog_dir', 'false')
        if nodes_extra and nodes_extra.get(nodeName):
            for k,v in nodes_extra[nodeName].items():
                config.set(nodeName, k, v)

    #
    #
    #
    if extra :
        logging.info("EXTRA!")
        for k,v in extra.items():
            logging.info("%s -> %s", k, v)
            config.set("global", k, v)

    fn = cluster._getConfigFileName()
    X.writeConfig(config, fn)


    logging.info( "Creating client config" )
    regenerateClientConfig( c_id )

    logging.info( "Changing log level to debug for all nodes" )
    cluster.setLogLevel("debug")

    lease = int(lease_duration)
    logging.info( "Setting lease expiration to %d" % lease)
    cluster.setMasterLease( lease )
    return cluster


def setup_n_nodes ( n, force_master, home_dir,
                    extra = None, nodes_extra = None,
                    witness_nodes = False, useIPV6 = False,
                    slowCollapser = False):

    setup_n_nodes_base(cluster_id, node_names[0:n], force_master, data_base_dir,
                       node_msg_base_port, node_client_base_port,
                       extra = extra, nodes_extra = nodes_extra,
                       witness_nodes = witness_nodes, useIPV6 = useIPV6,
                       slowCollapser = slowCollapser)

    logging.info( "Starting cluster" )
    start_all( cluster_id )
    time.sleep(1.0)

    logging.info( "Setup complete" )


def setup_3_nodes_forced_master (home_dir):
    setup_n_nodes( 3, True, home_dir, witness_nodes = True)

def setup_3_nodes_forced_master_normal_slaves (home_dir):
    setup_n_nodes( 3, True, home_dir)

def setup_3_nodes_forced_master_slow_collapser(home_dir):
    setup_n_nodes( 3, True, home_dir, witness_nodes = True, slowCollapser = True)

def setup_2_nodes_forced_master (home_dir):
    setup_n_nodes( 2, True, home_dir, witness_nodes = True)

def setup_2_nodes_forced_master_mini (home_dir):
    extra = {'tlog_max_entries':'500'}
    setup_n_nodes( 2, True, home_dir, extra)

def setup_3_nodes_forced_master_mini_rollover_on_size(home_dir):
    extra = {'tlog_max_entries': '100_000',
             'tlog_max_size'   : '64000'
            }

    setup_n_nodes( 3, True, home_dir, extra)

def setup_3_nodes_forced_master_mini (home_dir):
    extra = {'tlog_max_entries':'1000'}
    setup_n_nodes( 3, True, home_dir, extra)

def setup_2_nodes_forced_master_normal_slaves (home_dir):
    setup_n_nodes( 2, True, home_dir)

def setup_1_node_forced_master (home_dir):
    setup_n_nodes( 1, True, home_dir)

def setup_3_nodes_witness_slave (home_dir):
    setup_n_nodes( 3, False, home_dir, witness_nodes = True)

def setup_3_nodes_mini(home_dir):
    extra = {'tlog_max_entries':'1000'}
    setup_n_nodes( 3, False, home_dir, extra)

def setup_2_nodes_mini(home_dir):
    extra = {'tlog_max_entries':'1000'}
    setup_n_nodes(2, False, home_dir, extra)

def setup_3_nodes_mini_forced_master(home_dir):
    extra = {'tlog_max_entries':'1000'}
    setup_n_nodes( 3, True, home_dir, extra)

def setup_3_nodes (home_dir) :
    setup_n_nodes( 3, False, home_dir)

def setup_2_nodes (home_dir) :
    setup_n_nodes( 2, False, home_dir)

def setup_1_node (home_dir):
    setup_n_nodes( 1, False, home_dir )

def setup_1_node_mini (home_dir):
    extra = {'tlog_max_entries':'1000'}
    setup_n_nodes(1, False, home_dir, extra)

default_setup = setup_3_nodes

def setup_3_nodes_ipv6(home_dir):
    setup_n_nodes(3, False, home_dir, useIPV6 = True)

def setup_nursery_n (n, home_dir):

    for i in range(n):
        c_id = nursery_cluster_ids[i]
        base_dir = '/'.join([data_base_dir, c_id])
        setup_n_nodes_base( c_id, nursery_nodes[c_id], False, base_dir,
                            node_msg_base_port + 3*i, node_client_base_port+3*i)
        clu = _getCluster(c_id)
        clu.setNurseryKeeper(nursery_keeper_id)

        logging.info("Starting cluster %s", c_id)
        clu.start()

    logging.info("Initializing nursery to contain %s" % nursery_keeper_id )

    time.sleep(5.0)
    n = q.manage.nursery.getNursery( nursery_keeper_id )
    n.initialize( nursery_keeper_id )

    logging.info("Setup complete")

def setup_nursery_2 (home_dir):
    setup_nursery_n(2, home_dir)

def setup_nursery_3 (home_dir):
    setup_nursery_n(3, home_dir)

def dummy_teardown(removeDirs):
    pass


def common_teardown( removeDirs, cluster_ids):
    for cluster_id in cluster_ids:
        logging.info( "Stopping arakoon daemons for cluster %s" % cluster_id )
        stop_all (cluster_id )

        cluster = _getCluster( cluster_id)
        cluster.tearDown(removeDirs )
        cluster.remove()

    if removeDirs:
        remove_dirs ()

def remove_dirs():
    X.removeDirTree( data_base_dir )

def basic_teardown( removeDirs ):
    logging.info("basic_teardown(%s)" % removeDirs)
    common_teardown( False, [cluster_id])
    for i in range( len(node_names) ):
        destroy_ram_fs( i )
    if removeDirs:
        remove_dirs ()
    logging.info( "Teardown complete" )

def nursery_teardown( removeDirs ):
    common_teardown(removeDirs, nursery_cluster_ids)

def get_client ( c_id = None):
    if c_id is None:
        c_id = cluster_id
    ext = ArakoonClient.ArakoonClient()
    client = ext.getClient(c_id)
    return client

def get_nursery_client():
    client = q.clients.nursery.getClient(nursery_keeper_id)
    return client

def get_nursery():
    return q.manage.nursery.getNursery(nursery_keeper_id)

def iterate_n_times (n, f, startSuffix = 0, failure_max=0, valid_exceptions=None ):
    client = get_client ()
    failure_count = 0
    client.recreate = False

    if valid_exceptions is None:
        valid_exceptions = []

    global test_failed

    for i in xrange ( n ) :
        if test_failed :
            logging.error( "Test marked as failed. Aborting.")
            break
        suffix = ( i + startSuffix )
        key = key_format_str % suffix
        value = value_format_str % suffix

        try:
            f(client, key, value )
        except Exception, ex:
            logging.info("%i:Exception: %s for key=%s", i,ex,key)
            failure_count += 1
            fatal = True
            for valid_ex in valid_exceptions:
                if isinstance(ex, valid_ex ) :
                    fatal = False
            if failure_count > failure_max or fatal :
                client.dropConnections()
                test_failed = True
                logging.critical( "!!! Failing test")
                tb = traceback.format_exc()
                logging.critical( tb )
                raise
        if client.recreate :
            client.dropConnections()
            client = get_client()
            client.recreate = False

    client.dropConnections()


def create_and_start_thread (f ):
    class MyThread ( threading.Thread ):

        def __init__ (self, f, *args, **kwargs ):
            threading.Thread.__init__ ( self )
            self._f = f
            self._args = args
            self._kwargs = kwargs

        def run (self):
            try:
                self._f ( *(self._args), **(self._kwargs) )
            except Exception, ex:
                global test_failed
                logging.critical("!!! Failing test")
                tb = traceback.format_exc()
                logging.critical( tb )
                test_failed = True
                raise

    t = MyThread( f )
    t.start ()
    return t

def create_and_start_thread_list ( f_list ):
    return map ( create_and_start_thread, f_list )

def create_and_wait_for_thread_list ( f_list , timeout=None, assert_failure=True ):

    class SyncThread ( threading.Thread ):
        def __init__ (self, thr_list):
            threading.Thread.__init__ ( self )
            self.thr_list = thr_list

        def run (self):
            for thr in thr_list :
                thr.join()

    global test_failed
    test_failed = False

    thr_list = create_and_start_thread_list ( f_list )

    sync_thr = SyncThread ( thr_list )
    sync_thr.start()
    sync_thr.join( timeout )
    assert_false( sync_thr.isAlive() )
    if assert_failure :
        assert_false( test_failed )


def create_and_wait_for_threads ( thr_cnt, iter_cnt, f, timeout=None ):

    f_list = []
    for i in range( thr_cnt ) :
        g = lambda : iterate_n_times(iter_cnt, f )
        f_list.append( g )

    create_and_wait_for_thread_list( f_list, timeout)

def simple_set(client, key, value):
    client.set( key, value )

def assert_get( client, key, value):
    assert_equals( client.get(key), value )

def set_get_and_delete( client, key, value):
    client.set( key, value )
    assert_equals( client.get(key), value )
    client.delete( key )
    assert_raises ( X.arakoon_client.ArakoonNotFound, client.get, key )

def generic_retrying_ ( client, f, is_valid_ex, duration = 5.0 ) :
    start = time.time()
    failed = True
    tryCnt = 0

    global test_failed

    last_ex = None

    while ( failed and time.time() < start + duration ) :
        try :
            tryCnt += 1
            f ()
            failed = False
            last_ex = None
        except Exception, ex:
            logging.debug( "Caught an exception => %s: %s", ex.__class__.__name__, ex )
            time.sleep( 0.5 )
            last_ex = ex
            if not is_valid_ex( ex, tryCnt ) :
                # test_failed = True
                logging.debug( "Re-raising exception => %s: %s (tryCnt=%i)", ex.__class__.__name__, ex, tryCnt )
                raise
            logging.debug("recreating client")
            client.recreate = True
            client.dropConnections()
            client = get_client()

    if last_ex is not None:
        raise last_ex

def generic_retrying_set_get_and_delete( client, key, value, is_valid_ex ):
    generic_retrying_ (client, (lambda : client.set( key,value ) ), is_valid_ex, duration = 60.0 )
    generic_retrying_ (client, (lambda : assert_equals( client.get(key), value ) ) , is_valid_ex, duration = 60.0 )
    try:
        generic_retrying_ (client, (lambda : client.delete( key ) ) , is_valid_ex, duration = 60.0 )
    except X.arakoon_client.ArakoonNotFound:
        pass

def retrying_set_get_and_delete( client, key, value ):
    valids = (X.arakoon_client.ArakoonSockNotReadable,
              X.arakoon_client.ArakoonSockReadNoBytes,
              X.arakoon_client.ArakoonSockRecvError,
              X.arakoon_client.ArakoonSockRecvClosed,
              X.arakoon_client.ArakoonSockSendError,
              X.arakoon_client.ArakoonNotConnected,
              X.arakoon_client.ArakoonNodeNotMaster,
              X.arakoon_client.ArakoonNodeNoLongerMaster,
              )
    def validate_ex ( ex, tryCnt ):
        ex_msg = "%s" % ex

        validEx = isinstance(ex, valids)
        if validEx:
            logging.debug( "Ignoring exception: %s", ex_msg )
        return validEx

    generic_retrying_set_get_and_delete( client, key, value, validate_ex)

def add_node_scenario ( node_to_add_index ):
    iterate_n_times( 100, simple_set )
    stop_all()
    add_node( node_to_add_index )
    regenerateClientConfig(cluster_id)
    start_all()
    iterate_n_times( 100, assert_get )
    iterate_n_times( 100, set_get_and_delete, 100)

def assert_key_value_list( start_suffix, list_size, list ):
    assert_equals( len(list), list_size )
    for i in xrange( list_size ) :
        suffix = start_suffix + i
        key = key_format_str % (suffix )
        value = value_format_str % (suffix )
        assert_equals ( (key,value) , list [i] )

def assert_last_i_in_sync ( node_1, node_2 ):
    last_i_1 = get_last_i_tlog2(node_1)
    last_i_2 = get_last_i_tlog2(node_2)
    i1 = int(last_i_1)
    i2 = int(last_i_2)
    if i1 > i2:
        hi = i1
        hi_node = node_1
        lo = i2
    else:
        hi = i2
        hi_node = node_2
        lo = i1

    if hi - lo > 1:
        code = last_entry_code(hi_node) # masterset = 4
        masterSet = 4
        assert_equals(code,
                      masterSet,
                      "Values for i are invalid %i %i code:%i" % (i1, i2,code) )
    else:
        pass


def assert_running_nodes(n):
    try:
        output = X.subprocess.check_output(['pgrep',
                                            '-a',
                                            '-f', "%s --node" % (daemon_name)],
                                           stderr = X.subprocess.STDOUT
        )
        lines = output.strip().split('\n')
        logging.info("assert_running_nodes:%s", lines)
        lines2=filter(lambda x : x.find('/bin/bash') == -1, lines)
        logging.info("lines2:%s", lines2)
        count = len(lines2)
    except subprocess.CalledProcessError,ex:
        logging.info("ex:%s => count = 0" % ex)
        count = 0

    assert_equals(
        count, n,
        "Number of expected running nodes mismatch: expected=%s, actual=%s" % (n,count)
    )

def assert_value_list ( start_suffix, list_size, list ) :
    assert_list( value_format_str, start_suffix, list_size, list )

def assert_key_list ( start_suffix, list_size, list ) :
    assert_list( key_format_str, start_suffix, list_size, list )

def assert_list ( format_str, start_suffix, list_size, list ) :
    assert_equals( len(list), list_size )

    for i in xrange( list_size ) :
        elem = format_str % (start_suffix + i)
        assert_equals ( elem , list [i] )

def dir_to_fs_file_name (dir_name):
    return dir_name.replace( "/", "_")

def destroy_ram_fs( node_index ) :
    (mount_target,log_dir,tlf_dir,head_dir) = build_node_dir_names( node_names[node_index] )
    if os.path.isdir(mount_target) and os.path.ismount(mount_target):
        cmd = ["sudo", "/bin/umount", mount_target]
        (rc,out,err) = X.run(cmd)
        if rc:
            raise Exception("cmd:%s failed (%s,%s,%s)" % (str(cmd), rc,out,err))

def delayed_master_restart_loop ( iter_cnt, delay ) :
    for i in xrange( iter_cnt ):
        global test_failed
        try:
            time.sleep( delay )
            cli = get_client()
            cli.set('delayed_master_restart_loop','delayed_master_restart_loop')
            master_id = cli.whoMaster()
            cli.dropConnections()
            stopOne( master_id )
            cli.set('delayed_master_restart_loop', 'slaves elect new master and can make progress')
            startOne( master_id )
        except:
            logging.critical("!!!! Failing test. Exception in restart loop.")
            test_failed = True
            raise

def restart_loop( node_index, iter_cnt, int_start_stop, int_stop_start ) :
    for i in xrange (iter_cnt) :
        node = node_names[node_index]
        time.sleep( 1.0 * int_start_stop )
        stopOne(node)
        time.sleep( 1.0 * int_stop_start )
        startOne(node)


def restart_single_slave_scenario( restart_cnt, set_cnt, compare_store ) :
    start_stop_wait = 3.0
    stop_start_wait = 1.0
    slave_loop = lambda : restart_loop( 1, restart_cnt, start_stop_wait, stop_start_wait )
    set_loop = lambda : iterate_n_times( set_cnt, set_get_and_delete )
    create_and_wait_for_thread_list( [slave_loop, set_loop] )

    # Give the slave some time to catch up
    time.sleep( 5.0 )
    flush_stores()
    stop_all()
    assert_last_i_in_sync ( node_names[0], node_names[1] )
    if compare_store:
        compare_stores( node_names[0], node_names[1] )

def get_entries_per_tlog():
    cmd = [CONFIG.binary_full_path, '--version']
    stdout = X.subprocess.check_output(cmd)
    lines = stdout.split('\n')
    k = 'tlogEntriesPerFile:'
    for line in lines:
        i = line.find(k)
        if i > -1:
            n = int(line[i+len(k):])
            return n
    raise KeyError(k)

def prefix_scenario( start_suffix ):
    iterate_n_times( 100, simple_set, startSuffix = start_suffix )

    test_key_pref = key_format_str  % ( start_suffix + 90 )
    test_key_pref = test_key_pref [:-1]

    client = get_client()

    key_list = client.prefix( test_key_pref )
    assert_key_list ( start_suffix + 90, 10, key_list)

    key_list = client.prefix( test_key_pref, 7 )
    assert_key_list ( start_suffix + 90, 7, key_list)

    client.dropConnections ()

def range_scenario ( start_suffix ):

    iterate_n_times( 100, simple_set, startSuffix = start_suffix )

    client = get_client()

    start_key = key_format_str % (start_suffix )
    end_key = key_format_str % (start_suffix + 100 )
    test_key = key_format_str % (start_suffix + 25)
    test_key_2 = key_format_str % (start_suffix + 50)

    key_list = client.range( test_key , True, end_key , False )
    assert_key_list ( start_suffix+25, 75, key_list )

    key_list = client.range( test_key , False, end_key , False )
    assert_key_list ( start_suffix+26, 74, key_list )

    key_list = client.range( test_key, True, end_key , False, 10 )
    assert_key_list ( start_suffix+25, 10, key_list )

    key_list = client.range( start_key, True, test_key , False )
    assert_key_list ( start_suffix, 25, key_list)

    key_list = client.range( start_key, True, test_key , True )
    assert_key_list ( start_suffix, 26, key_list)

    key_list = client.range( start_key, True, test_key , False, 10 )
    assert_key_list ( start_suffix, 10, key_list )

    key_list = client.range( test_key, True, test_key_2 , False )
    assert_key_list ( start_suffix+25, 25, key_list )

    key_list = client.range( test_key, False, test_key_2 , True )
    assert_key_list ( start_suffix+26, 25, key_list )

    key_list = client.range( test_key, True, test_key_2 , False, 10 )
    assert_key_list ( start_suffix+25, 10, key_list )

def range_entries_scenario( start_suffix ):

    iterate_n_times( 100, simple_set, startSuffix = start_suffix )

    client = get_client()

    start_key = key_format_str % (start_suffix )
    end_suffix = key_format_str % ( start_suffix + 100 )
    test_key = key_format_str % (start_suffix + 25)
    test_key_2 = key_format_str % (start_suffix + 50)
    try:
        key_value_list = client.range_entries ( test_key , True, end_suffix , False )
        assert_key_value_list ( start_suffix + 25, 75, key_value_list )

        key_value_list = client.range_entries( test_key , False, end_suffix , False )
        assert_key_value_list ( start_suffix + 26, 74, key_value_list )

        key_value_list = client.range_entries( test_key, True, end_suffix , False, 10 )
        assert_key_value_list ( start_suffix + 25, 10, key_value_list )

        key_value_list = client.range_entries( start_key, True, test_key , False )
        assert_key_value_list ( start_suffix, 25, key_value_list)

        key_value_list = client.range_entries( start_key, True, test_key , True )
        assert_key_value_list ( start_suffix, 26, key_value_list)

        key_value_list = client.range_entries( start_key, True, test_key , False, 10 )
        assert_key_value_list ( start_suffix, 10, key_value_list )

        key_value_list = client.range_entries( test_key, True, test_key_2 , False )
        assert_key_value_list ( start_suffix + 25, 25, key_value_list )

        key_value_list = client.range_entries( test_key, False, test_key_2 , True )
        assert_key_value_list ( start_suffix + 26, 25, key_value_list )

        key_value_list = client.range_entries( test_key, True, test_key_2 , False, 10 )
        assert_key_value_list ( start_suffix + 25, 10, key_value_list )
    except Exception, ex:
        logging.info("on failure moment, master was: %s", client._masterId)
        raise ex

def heavy_range_entries_scenario( start_suffix, count, queries, valid_exceptions ):
    value_format_str_ = "p" * 4200 +  "value_%012d"
    client = get_client()

    def validate_ex(ex, tryCnt):
        ex_msg = str(ex)
        validEx = False
        for valid_ex in valid_exceptions:
            if isinstance(ex, valid_ex ) :
                validEx = True

        if validEx:
            logging.debug( "Ignoring exception: %s", ex_msg )
        return validEx


    for i in xrange(count):
        suffix = ( i + start_suffix )
        key = key_format_str % suffix
        value = value_format_str_ % suffix

        generic_retrying_(client, lambda: client.set(key, value), validate_ex, duration = 60.0)


    start_key = key_format_str % (start_suffix )
    end_suffix = key_format_str % (start_suffix + count)
    for i in xrange(queries) :
        key_value_list = client.range_entries ( start_key , True, end_suffix , False, -1 )

def reverse_range_entries_scenario(start_suffix):
    iterate_n_times(100, simple_set, startSuffix = start_suffix)
    client = get_client ()
    start_key = key_format_str % (start_suffix)
    end_key = key_format_str % (start_suffix + 100)
    try:
        kv_list0 = client.range_entries("a", True,"z", True, 10)
        for t in kv_list0:
            logging.info("t=%s",t)
        logging.info("now reverse")
        kv_list = client.rev_range_entries("z", True, "a", True, 10)
        for t in kv_list:
            logging.info("t=%s", t)
        assert_equals( len(kv_list), 10)
        assert_equals(kv_list[0][0], 'key_000000001099')
    except Exception, ex:
        raise ex


def compare_files(fn0, fn1):
    with open(fn0, 'r') as f0:
        with open(fn1, 'r') as f1:
            go_on = True
            count = 0
            while go_on:
                f0_line = f0.readline()
                f1_line = f1.readline()
                logging.debug("l0:%s\nl1:%s\n", f0_line.strip(), f1_line.strip())
                if f0_line <> f1_line :
                    raise Exception("%s and %s diff on line %i" % (fn0,fn1, count))
                if len(f0_line) == 0:
                    assert(len(f1_line) == 0)
                    go_on = False
                count = count + 1


def inspect_cluster(cluster):

    # cleanup inspect dumps
    cluster_id = cluster._getClusterId()
    node_names = cluster.listLocalNodes()
    for node_name in node_names:
        dn = '%s/%s/' % (cluster_id, node_name,)
        logging.info( "deleting %s", dn)
        X.removeDirTree(dn)

    cfg_fn = cluster._getConfigFileName()
    r = call_arakoon("--inspect-cluster", "-config", "%s.cfg" % cfg_fn)


    dump_paths = []
    for node_name in node_names:
        dump_path = './%s/%s/store.db.dump' % (cluster_id, node_name)
        dump_paths.append(dump_path)

    size = len(node_names)
    for i in xrange(size -1):
        for j in xrange(i+1, size):
            fn0 = dump_paths[i]
            fn1 = dump_paths[j]
            compare_files(fn0,fn1)
            logging.info("%s == %s",fn0,fn1)
    logging.info("inspect_cluster(%s): ok", cluster_id)
