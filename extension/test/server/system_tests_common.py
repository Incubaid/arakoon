'''
This file is part of Arakoon, a distributed key-value store. Copyright
(C) 2010 Incubaid BVBA

Licensees holding a valid Incubaid license may use this file in
accordance with Incubaid's Arakoon commercial license agreement. For
more information on how to enter into this agreement, please contact
Incubaid (contact details can be found on www.arakoon.org/licensing).

Alternatively, this file may be redistributed and/or modified under
the terms of the GNU Affero General Public License version 3, as
published by the Free Software Foundation. Under this license, this
file is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.

See the GNU Affero General Public License for more details.
You should have received a copy of the
GNU Affero General Public License along with this program (file "COPYING").
If not, see <http://www.gnu.org/licenses/>.
'''


from pymonkey import q, i
from nose.tools import *
from functools import wraps
import traceback

class with_custom_setup ():
    
    def __init__ (self, setup, teardown):
        self.__setup = setup
        self.__teardown = teardown
    
    def __call__ (self, func ):
        @wraps(func)
        def decorate(*args,**kwargs):
            
            global data_base_dir 
            data_base_dir = q.system.fs.joinPaths( q.dirs.tmpDir, 'arakoon_system_tests' , func.func_name )
            fatal_ex = None
            home_dir = data_base_dir
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


import os
import random
import threading
import time
import arakoon.ArakoonProtocol  
import logging

test_failed = False 

from arakoon.ArakoonExceptions import * 
 
if __name__ == "__main__" :
    from pymonkey import InitBase

data_base_dir = None
node_names = [ "arakoon_0", "arakoon_1", "arakoon_2" ]
node_ips = [ "127.0.0.1", "127.0.0.1", "127.0.0.1"]
node_client_base_port = 7080
node_msg_base_port = 10000
daemon_name = "arakoond"
binary_full_path = "/opt/qbase3/apps/arakoon/bin/arakoond"
lease_duration = 10.0

key_format_str = "key_%012d"
value_format_str = "value_%012d"

fs = q.system.fs
proc = q.system.process

def generate_lambda( f, *args, **kwargs ):
    return lambda: f( *args, **kwargs )

def dump_tlog (node_id, tlog_number) :
    node_home_dir = q.config.arakoon.getNodeConfig( node_id ) ['home']
    tlog_full_path =  q.system.fs.joinPaths ( node_home_dir, "%03d.tlog" % tlog_number  )
    cmd = "%s --dump-tlog %s" % (binary_full_path, tlog_full_path)
    logging.debug( "Dumping file %s" % tlog_full_path )
    logging.debug("Command is : '%s'" % cmd )
    (exit,stdout,stderr) = q.system.process.run(cmd)
    assert_equals( exit, 0, "Could not dump tlog for node %s" % node_id )
    return stdout

def get_arakoon_binary() :
    return fs.joinPaths( get_arakoon_bin_dir(), 'arakoond')

def get_arakoon_bin_dir():
    return fs.joinPaths( q.dirs.appDir, "arakoon", "bin")

def get_tcbmgr_path ():
    return fs.joinPaths( get_arakoon_bin_dir(), "tcbmgr" )

def get_diff_path():
    return "/usr/bin/diff"

def get_node_db_file( node_id ) :
    node_home_dir = q.config.arakoon.getNodeConfig( node_id ) ['home']
    db_file = fs.joinPaths( node_home_dir, node_id + ".db" )
    return db_file
    
def dump_store( node_id ): 
    stat = q.cmdtools.arakoon.getStatusOne( node_id )
    assert_equals( stat, q.enumerators.AppStatusType.HALTED, "Can only dump the store of a node that is not running")
    db_file = get_node_db_file ( node_id )
    dump_file = db_file + ".dump" 
    cmd = get_tcbmgr_path() + " list -pv " + db_file
    dump_fd = open( dump_file, 'w' )
    logging.debug( "Dumping store of %s to %s" % (node_id, dump_file) )
    (exit,stdout,stderr) = proc.run( cmd , captureOutput=True, stdout=dump_fd )
    dump_fd.close()
    return dump_file

def compare_stores( node1_id, node2_id ):

    keys_to_skip = [ "*lease", "*i", "*master" ]
    dump1 = dump_store( node1_id )
    dump2 = dump_store( node2_id )
    
    # Line 2 contains the master lease, can be different as it contains a timestamp
    d1_fd = open ( dump1, "r" )
    d2_fd = open ( dump2, "r" )
    
    def get_i ( node_id ):
        stat = q.cmdtools.arakoon.getStatusOne( node_id )
        assert_equals( stat, q.enumerators.AppStatusType.HALTED, "Can only dump the store of a node that is not running")
        db_file = get_node_db_file(node_id)
        cmd = " ".join( [get_arakoon_binary(), "--dump-store", db_file])
        (exit,stdout,stderr) = proc.run( cmd, captureOutput=True )
        i_line = stdout.split("\n") [0]
        i_str = i_line.split("(")[1][:-1]
        return int(i_str)
     
    

    i1 = get_i (node1_id)
    logging.debug("Counter value for store of %s: %d" % (node1_id,i1))
    i2 = get_i (node2_id)
    logging.debug("Counter value for store of %s: %d" % (node2_id,i2))
    if( abs (i1 - i2) > 1 ):
        raise Exception( "Store counters differ too much (%d : %d)" % (i1,i2) )
    
    i1_line = d1_fd.readline()
    i2_line = d2_fd.readline()
    
    diffs = { node1_id : {} , node2_id : {} }
    
    def extract_key_value (line):
        kv = line.split("\t")
        try:
            k = kv [0]
            v = kv [1]
        except Exception as ex :
            logging.debug("Mal-formed line in dump: '%s'. Aborting.)" % line )
            raise ex
        return (k,v)
    
    iter = 0 
    while i1_line != '' and i2_line != '' :
        iter+=1
        if( i1_line == i2_line ) :
            i1_line = d1_fd.readline()
            i2_line = d2_fd.readline()
        else :
            (k1,v1) = extract_key_value(i1_line)
            (k2,v2) = extract_key_value(i2_line)
            
            if k1 == k2 :
                if k1 not in keys_to_skip :
                    diffs[node1_id][k1] = v1
                    diffs[node2_id][k2] = v2 
                    logging.debug( "Stores have different values for %s" % (k1) )
                i1_line = d1_fd.readline()
                i2_line = d2_fd.readline() 
            if k1 < k2 :
                logging.debug( "Store of %s has a value for, store of %s doesn't" % (node1_id, k1) )
                diffs[node1_id][k1] = v1
                i1_line = d1_fd.readline()
            if k1 > k2 :
                logging.debug( "Store of %s has a value for, store of %s doesn't" % (node2_id, k2) )
                diffs[node2_id][k2] = v2
                i2_line = d2_fd.readline()
    
    if i1_line != '':
        logging.debug ( "Store of %s contains more keys, other store is EOF" %  (node1_id) )
        while i1_line != '':
            if i1_line != "\n":
                (k,v) = extract_key_value (i1_line)
                diffs[node1_id][k] = v
            i1_line = d1_fd.readline()
    if i2_line != '':
        logging.debug ( "Store of %s contains more keys, other store is EOF" %  (node2_id) )
        while i2_line != '':
            if i2_line != "\n" :
                (k,v) = extract_key_value (i2_line)
                diffs[node2_id][k] = v
            i2_line = d2_fd.readline() 
            
    max_diffs = 0
    
    if ( i1 != i2 ):
        max_diffs = 1
        
    diff_cnt = len( set( diffs[node1_id].keys() ).union( set(diffs[node2_id].keys() ) ) )
    if diff_cnt > max_diffs :
        raise Exception ( "Found too many differences between stores (%d > %d)\n%s" % (diff_cnt, max_diffs,diffs) )

    logging.debug( "Stores of %s and %s are valid" % (node1_id,node2_id))
    return True

def get_last_tlog_id ( node_id ):
    node_home_dir = q.config.arakoon.getNodeConfig( node_id ) ['home']
    tlog_max_id = 0
    tlog_id = None
    tlogs_for_node = q.system.fs.listFilesInDir( node_home_dir, filter="*.tlog" )
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
        raise Exception( "Not a single tlog found in %s" % node_home_dir )
    return tlog_max_id
    
def get_last_i_tlog ( node_id ):
    tlog_dump = dump_tlog ( node_id, get_last_tlog_id(node_id) ) 
    tlog_dump_list = tlog_dump.split("\n")
    tlog_last_entry = tlog_dump_list [-2]
    tlog_last_i = tlog_last_entry.split(":") [0].lstrip( " 0" )
    return tlog_last_i


def add_node ( i ):
    logging.info( "Adding node %s to config", node_names[ i ] )
    
    (db_dir,log_dir) = build_node_dir_names( node_names[ i ] )
    
    q.config.arakoon.addNode ( node_names[i], node_ips[i], 
                               client_port=node_client_base_port + i,
                               messaging_port=node_msg_base_port + i, 
                               log_dir = log_dir,
                               log_level = 'debug',
                               home = db_dir)
    q.config.arakoon.addLocalNode ( node_names[i] )
    q.config.arakoon.createDirs( node_names[i] )

def start_all() :
    q.cmdtools.arakoon.start()
    time.sleep(5.0)  

def restart_random_node():
    node_index = random.randint(0, len(node_names) - 1)
    node_name = node_names [node_index ]
    delayed_restart_nodes( [ node_name ] )

def delayed_restart_all_nodes() :
    delayed_restart_nodes( node_names )
    
def delayed_restart_nodes(node_list) :
    downtime = random.random() * 60.0
    for node_name in node_list :
        q.cmdtools.arakoon.stopOne( node_name )
    time.sleep( downtime )
    for node_name in node_list :
        q.cmdtools.arakoon.startOne( node_name )

def delayed_restart_1st_node ():
    delayed_restart_nodes( [ node_names[0] ] )

def delayed_restart_2nd_node ():
    delayed_restart_nodes( [ node_names[1] ] )

def delayed_restart_3rd_node ():
    delayed_restart_nodes( [ node_names[2] ] )
    
def restart_nodes_wf_sim( n ):
    wf_step_duration = 0.2
    
    for i in range (n):
        q.cmdtools.arakoon.stopOne( node_names[i] )
        time.sleep( wf_step_duration )
    
    for i in range (n):    
        q.cmdtools.arakoon.startOne( node_names[i] )
        time.sleep( wf_step_duration )

def getRandomString( length = 16 ) :
    def getRC ():
        return chr(random.randint(0,25) + ord('A'))

    retVal = ""
    for i in range( length ) :
        retVal += getRC()        
    return retVal

def build_node_dir_names ( nodeName ):
    global data_base_dir
    data_dir = q.system.fs.joinPaths( data_base_dir, nodeName)
    db_dir = q.system.fs.joinPaths( data_dir, "db")
    log_dir = q.system.fs.joinPaths( data_dir, "log")
    return (db_dir,log_dir)

def setup_n_nodes ( n, force_master, home_dir ):

    q.system.process.run( "/sbin/iptables -F" )
    cfg_list = q.config.list()
    
    if "arakoon" in cfg_list :
        logging.info( "Clearing server config" )
        q.config.remove("arakoon")
    
    if "arakoonnodes" in cfg_list :
        logging.info( "Clearing client config" )
        q.config.remove("arakoonnodes")
        
    if "arakoonservernodes" in cfg_list :
        logging.info( "Clearing client config" )
        q.config.remove("arakoonservernodes")
    
    if q.system.fs.exists( home_dir ) :
        logging.info( "Removing home dir" )
        q.system.fs.removeDirTree( data_base_dir )
                    
    logging.info( "Creating data base dir %s" % data_base_dir )
    
    q.system.fs.createDir ( data_base_dir )
    
    for i in range (n) :
        nodeName = node_names[ i ]
        (db_dir,log_dir) = build_node_dir_names( nodeName )
        q.config.arakoon.addNode(name=nodeName, client_port=7080+i,messaging_port=10000+i,
               log_dir = log_dir, home = db_dir )
        q.config.arakoon.addLocalNode(nodeName)
        q.config.arakoon.createDirs(nodeName)

    if force_master:
        logging.info( "Forcing master to %s", node_names[0] )
        q.config.arakoon.forceMaster( node_names[0] )
    else :
        logging.info( "Using master election" )
        q.config.arakoon.forceMaster( None )
    
        
    logging.info( "Creating client config" )
    q.config.arakoonnodes.generateClientConfigFromServerConfig()
    
    logging.info( "Changing log level to debug for all nodes" )
    config = q.config.getInifile("arakoon")
    
    for i in range(n) :
        config.setParam(node_names[i],"log_level","debug")
    config.setParam( 'global', 'lease_expiry', str(int(lease_duration)) )    
    config.write()
    
    
    logging.info( "Starting arakoon" )
    start_all() 
   
    logging.info( "Setup complete" )
    

def setup_3_nodes_forced_master (home_dir):
    setup_n_nodes( 3, True, home_dir)
    
def setup_2_nodes_forced_master (home_dir):
    setup_n_nodes( 2, True, home_dir)

def setup_1_node_forced_master (home_dir):
    setup_n_nodes( 1, True, home_dir)

def setup_3_nodes (home_dir) :
    setup_n_nodes( 3, False, home_dir)

def setup_2_nodes (home_dir) :
    setup_n_nodes( 2, False, home_dir)
    
def setup_1_node (home_dir):
    setup_n_nodes( 1, False, home_dir )

default_setup = setup_3_nodes

def dummy_teardown():
    pass

def basic_teardown( removeDirs ):
    
    if "arakoon" in q.config.list():
        logging.info( "Stopping arakoon daemons" )
        q.cmdtools.arakoon.stop()
    for i in range( len(node_names) ):
        destroy_ram_fs( i )
        
    q.config.arakoon.tearDown( removeDirs )
    if removeDirs:
        q.system.fs.removeDirTree( data_base_dir )
        
    logging.info( "Teardown complete" )


def get_client ():
    return q.clients.arakoon.getClient() 


def iterate_n_times (n, f, startSuffix = 0, failure_max=0, valid_exceptions=None ):
    client = get_client ()
    failure_count = 0
    client.recreate = False
    
    if valid_exceptions is None:
        valid_exceptions = []
        
    global test_failed
    
    for i in range ( n ) :
        suffix = ( i + startSuffix )
        key = key_format_str % suffix
        value = value_format_str % suffix
        
        try:
            f(client, key, value )
        except Exception, ex:
            failure_count += 1
            fatal = True
            for valid_ex in valid_exceptions:
                if isinstance(ex, valid_ex ) :
                    fatal = False
            if failure_count > failure_max or fatal :
                client._dropConnections()
                test_failed = True
                logging.critical( "!!! Failing test")
                tb = traceback.format_exc()
                logging.critical( tb )
                raise
        if client.recreate :
            client._dropConnections()
            client = get_client()
            client.recreate = False
            
    client._dropConnections()
        

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
    
def mindless_simple_set( client, key, value):
    try:
        client.set( key, value)
    except Exception, ex:
        logging.info( "Error while setting => %s: %s" , ex.__class__.__name__, ex)

def simple_set(client, key, value):
    client.set( key, value )

def assert_get( client, key, value):
    assert_equals( client.get(key), value )

def set_get_and_delete( client, key, value):
    client.set( key, value )
    assert_equals( client.get(key), value )
    client.delete( key )
    assert_raises ( ArakoonNotFound, client.get, key )

def mindless_retrying_set_get_and_delete( client, key, value ):
    def validate_ex ( ex, tryCnt ):
        return True
    
    generic_retrying_set_get_and_delete( client, key, value, validate_ex )
    
    
def generic_retrying_set_get_and_delete( client, key, value, is_valid_ex ):
    start = time.time()
    failed = True
    tryCnt = 0
    
    global test_failed
    
    last_ex = None 
    
    while ( failed and time.time() < start + 5.0 ) :
        try :
            tryCnt += 1
            client.set( key,value )
            assert_equals( client.get(key), value )
            client.delete( key )
            # assert_raises ( ArakoonNotFound, client.get, key )
            failed = False
            last_ex = None
        except (ArakoonNoMaster, ArakoonNodeNotMaster), ex:
            if isinstance(ex, ArakoonNoMaster) :
                logging.debug("No master in cluster. Recreating client.")
            else :
                logging.debug("Old master is not yet ready to succumb. Recreating client")
            
            # Make sure we propagate the need to recreate the client 
            # (or the next iteration we are back to using the old one)
            client.recreate = True
            client._dropConnections()
            client = get_client() 
            
        except Exception, ex:
            logging.debug( "Caught an exception => %s: %s", ex.__class__.__name__, ex )
            time.sleep( 0.5 )
            last_ex = ex
            if not is_valid_ex( ex, tryCnt ) :
                # test_failed = True
                logging.debug( "Re-raising exception => %s: %s", ex.__class__.__name__, ex )
                raise
    
    if last_ex is not None:
        raise last_ex

    
def retrying_set_get_and_delete( client, key, value ):
    def validate_ex ( ex, tryCnt ):
        ex_msg = "%s" % ex
        validEx = False
        
        validEx = validEx or isinstance( ex, ArakoonSockNotReadable )
        validEx = validEx or isinstance( ex, ArakoonSockReadNoBytes )
        validEx = validEx or isinstance( ex, ArakoonSockRecvError )
        validEx = validEx or isinstance( ex, ArakoonSockRecvClosed )
        validEx = validEx or isinstance( ex, ArakoonSockSendError )
        validEx = validEx or isinstance( ex, ArakoonNotConnected ) 
        validEx = validEx or isinstance( ex, ArakoonNodeNotMaster )
        if validEx:
            logging.debug( "Ignoring exception: %s", ex_msg )
        return validEx
    
    generic_retrying_set_get_and_delete( client, key, value, validate_ex) 
    
def add_node_scenario ( node_to_add_index ):
    iterate_n_times( 100, simple_set )
    
    q.cmdtools.arakoon.stop()
    
    add_node( node_to_add_index )
    
    if "arakoonnodes" in q.config.list():
        q.config.remove("arakoonnodes")   
    q.config.arakoonnodes.generateClientConfigFromServerConfig()
    
    start_all()

    iterate_n_times( 100, assert_get )
    
    iterate_n_times( 100, set_get_and_delete, 100)

def assert_key_value_list( start_suffix, list_size, list ):
    assert_equals( len(list), list_size )
    for i in range( list_size ) :
        suffix = start_suffix + i
        key = key_format_str % (suffix )
        value = value_format_str % (suffix )
        assert_equals ( (key,value) , list [i] )

def assert_last_i_in_sync ( node_1, node_2 ):
    last_i_0 = get_last_i_tlog( node_1 )
    last_i_1 = get_last_i_tlog( node_2 )    
    abs_diff = abs(int(last_i_0) - int(last_i_1) )
    assert_equals( max(abs_diff,1) , 1, 
                   "Values for i are invalid %s %s" % (last_i_0, last_i_1) )  

def assert_running_nodes ( n ):
    assert_not_equals ( q.system.process.checkProcess( daemon_name, n), 1, "Number of expected running nodes missmatch" )

def assert_value_list ( start_suffix, list_size, list ) :
    assert_list( value_format_str, start_suffix, list_size, list )

def assert_key_list ( start_suffix, list_size, list ) :
    assert_list( key_format_str, start_suffix, list_size, list )
 
def assert_list ( format_str, start_suffix, list_size, list ) :
    assert_equals( len(list), list_size )
    
    for i in range( list_size ) :
        elem = format_str % (start_suffix + i)
        assert_equals ( elem , list [i] )

def run_cmd (cmd, display_output = True) :
    q.system.process.execute( cmd, outputToStdout = display_output )
    
def dir_to_fs_file_name (dir_name):
    return dir_name.replace( "/", "_")

def destroy_ram_fs( node_index ) :
    (mount_target,log_dir) = build_node_dir_names( node_names[node_index] ) 
    
    try :
        cmd = "umount %s" % mount_target
        run_cmd ( cmd )
    except :
        pass
    
def delayed_master_restart_loop ( iter_cnt, delay ) :
    for i in range( iter_cnt ):
        time.sleep( delay )
        cli = get_client()
        cli.set('delayed_master_restart_loop','delayed_master_restart_loop')
        master_id = cli.whoMaster()
        cli._dropConnections()
        q.cmdtools.arakoon.stopOne( master_id )
        q.cmdtools.arakoon.startOne( master_id )
                     
def restart_loop( node_index, iter_cnt, int_start_stop, int_stop_start ) :

    for i in range (iter_cnt) :
        time.sleep( 1.0 * int_start_stop )
        q.cmdtools.arakoon.stopOne ( node_names[node_index] )
        time.sleep( 1.0 * int_stop_start )
        q.cmdtools.arakoon.startOne ( node_names[node_index] )
        

def restart_single_slave_scenario( restart_cnt, set_cnt ) :
    start_stop_wait = 3.0
    stop_start_wait = 1.0
    slave_loop = lambda : restart_loop( 1, restart_cnt, start_stop_wait, stop_start_wait )
    set_loop = lambda : iterate_n_times( set_cnt, set_get_and_delete )
    create_and_wait_for_thread_list( [slave_loop, set_loop] )
    
    # Give the slave some time to catch up 
    time.sleep( 5.0 )
    
    assert_last_i_in_sync ( node_names[0], node_names[1] )
    q.cmdtools.arakoon.stop()
    compare_stores( node_names[0], node_names[1] )

