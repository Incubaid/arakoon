"""
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
"""


from .. import system_tests_common as Common
from ..system_tests_common import q
from .. import system_tests_common

from arakoon.ArakoonExceptions import *
import arakoon
import nose.tools as NT

import logging
import time
import sys

if __name__ == '__main__' :
    from pymonkey import InitBase
    from pymonkey import i,q
    
def mount_ram_fs ( node_index ) :
    
    (mount_target,log_dir,tlf_dir) = Common.build_node_dir_names( Common.node_names[node_index] )
    fs = q.system.fs
    if fs.exists( mount_target ) :
        try:
            Common.stopOne( Common.node_names[node_index] )
        except:
            pass
        try :
            cmd = "umount %s" % mount_target
            Common.run_cmd ( cmd )
        except :
            pass
        fs.removeDirTree( mount_target )
    
    fs.createDir ( mount_target )
    
    if not fs.isDir( mount_target ) :
        raise Exception( "%s is not valid mount target as it is not a directory")
    try :
        cmd = "mke2fs -q -m 0 /dev/ram%d" % (node_index)
        Common.run_cmd ( cmd )
        
        cmd = "mount /dev/ram%d %s" % (node_index, mount_target)
        Common.run_cmd ( cmd )
        
    except Exception, ex :
        logging.error( "Caught exception: %s" , ex )
        Common.stopOne( Common.node_names[node_index] )
        destroy_ram_fs ( node_index )
    
    
def setup_3_nodes_ram_fs ( home_dir ):
    cfg_list = q.config.list()

    cluster = q.manage.arakoon.getCluster(Common.cluster_id)
    cluster.remove()
    
    cluster = q.manage.arakoon.getCluster(Common.cluster_id)    
    
    logging.info( "Creating data base dir %s" % home_dir )
    
    q.system.fs.createDir ( home_dir )
    
    try :
        for i in range( len(Common.node_names) ):
            mount_ram_fs ( i )
            nodeName = Common.node_names[i]
            (db_dir,log_dir,tlf_dir) = Common.build_node_dir_names( Common.node_names[ i ] )
            cluster.addNode (
                nodeName, Common.node_ips[i], 
                clientPort = Common.node_client_base_port + i,
                messagingPort = Common.node_msg_base_port + i, 
                logDir = log_dir,
                tlfDir = tlf_dir,
                home = db_dir)
            cluster.addLocalNode(nodeName)
            cluster.createDirs(nodeName)

    except Exception as ex:
        teardown_ram_fs( True )
        (a,b,c) = sys.exc_info()
        raise a, b, c
    
    logging.info( "Changing log level to debug for all nodes" )
    cluster.setLogLevel("debug")
    cluster.setMasterLease(int(Common.lease_duration))

    logging.info( "Creating client config" )
    Common.regenerateClientConfig(Common.cluster_id)
            
    Common.start_all()

def teardown_ram_fs ( removeDirs ):

    logging.info( "Tearing down" )
    Common.stop_all()
    
    # Copy over log files
    if not removeDirs:
        destination = q.dirs.tmpDir + Common.data_base_dir [1:]
        if q.system.fs.exists( destination ):
            q.system.fs.removeDirTree( destination )
        q.system.fs.createDir(destination)
        q.system.fs.copyDirTree( system_tests_common.data_base_dir, destination)
        
    for i in range ( len( Common.node_names ) ):
        Common.destroy_ram_fs( i )

    cluster = q.manage.arakoon.getCluster(Common.cluster_id)
    cluster.tearDown()


def fill_disk ( file_to_write ) :
    cmd = "dd if=/dev/zero of=%s bs=1M" % file_to_write
    try :
        Common.run_cmd ( cmd )
    except Exception, ex:
        logging.error( "Caught exception => %s: %s", ex.__class__.__name__, ex )


def build_iptables_block_rules( tcp_port ) :
    rules = list()
    rules.append( "-A INPUT -p tcp -m tcp --dport %s -m state --state NEW,ESTABLISHED -j DROP " % tcp_port )
    rules.append( "-A OUTPUT -p tcp -m tcp --sport %s -m state --state NEW,ESTABLISHED -j DROP " % tcp_port )
    return rules

def get_current_iptables_rules (): 
    rules_file = q.system.fs.joinPaths( q.dirs.tmpDir, "iptables-rules-save")
    cmd = "sudo /sbin/iptables-save > %s" % rules_file
    Common.run_cmd( cmd )
    rules_file_contents = q.system.fs.fileGetContents( rules_file )
    return rules_file_contents.split( "\n") [5:-3]

def apply_iptables_rules ( rules ) :
    
    flush_all_rules()
    
    for rule in rules :
        lines = rule.split("\n")
        for line in lines :
            if line.strip() != "" :
                cmd = "sudo /sbin/iptables %s" % line
                logging.info("cmd=%s", cmd)
                Common.run_cmd( cmd, False )
        
        
def block_tcp_port ( tcp_port ):
    
    rules = build_iptables_block_rules( tcp_port )
    current_rules = get_current_iptables_rules ()

    for rule in rules :
        if rule in current_rules:
            return
        else :
            current_rules.append( rule )
        
    apply_iptables_rules( current_rules ) 
    
@Common.with_custom_setup(setup_3_nodes_ram_fs, 
                          teardown_ram_fs )    
def test_disk_full_on_slave ():
    cli = Common.get_client()
    master_id = cli.whoMaster()
    slave_id = Common.node_names[0]
    if slave_id == master_id:
        slave_id = Common.node_names[1]

    logging.info( "Got master '%s' and slave '%s'", master_id, slave_id  )
    
    disk_full_scenario( slave_id, cli )
    
    
def disk_full_scenario( node_id, cli ):
    cluster = q.manage.arakoon.getCluster(Common.cluster_id)
    node_home = cluster.getNodeConfig(node_id ) ['home']
    
    disk_filling = q.system.fs.joinPaths( node_home , "filling")
    disk_filler = lambda: fill_disk ( disk_filling )
    set_loop = lambda: Common.iterate_n_times( 500, Common.simple_set, 500 )
    set_get_delete_loop = lambda: Common.iterate_n_times(10000, 
                                                         Common.set_get_and_delete,
                                                         1000 )
    
    Common.iterate_n_times( 500, Common.simple_set )
    
    try :
        Common.create_and_wait_for_thread_list ( [ disk_filler, 
                                                   set_loop, 
                                                   set_get_delete_loop ] )
    except Exception, ex:
        logging.error( "Caught exception when disk was full => %s: %s", ex.__class__.__name__, ex ) 
    
    q.system.fs.unlink( disk_filling )
    cli.dropConnections()
   
    # Make sure the node with a full disk is no longer running
    NT.assert_equals( cluster.getStatusOne(node_id),
                      q.enumerators.AppStatusType.HALTED,
                      'Node with full disk is still running') 
    time.sleep( 2* Common.lease_duration )
    
    cli2 = Common.get_client()
    cli2.whoMaster()
    cli2.dropConnections()
   
    Common.start_all()
 
    time.sleep( Common.lease_duration )
    
    if node_id == Common.node_names[0] :
        node_2 = Common.node_names[0]
    else :
        node_2 = Common.node_names[1]
    
    Common.stop_all()
    Common.assert_last_i_in_sync ( node_id, node_2 )
    Common.compare_stores( node_id, node_2 )
    Common.start_all()
    
    Common.iterate_n_times( 500, Common.set_get_and_delete , 100000 )
    key_list = cli.prefix( "key_", 500 )
    Common.assert_key_list( 0, 500, key_list )
    
    
def unblock_node_ports ( node_id ):
    current_rules = set( get_current_iptables_rules()) 
    node_block_rules = set( get_node_block_rules( node_id ))
    apply_iptables_rules( current_rules.difference(node_block_rules) )
    
    
def block_node_ports ( node_id ):
    current_rules = set( get_current_iptables_rules())
    node_block_rules = set( get_node_block_rules( node_id ))
    apply_iptables_rules( current_rules.union( node_block_rules ))

def get_node_block_rules( node_id ):
    node_ports = get_node_ports( node_id )
    NT.assert_not_equals( len(node_ports), 0, "Could not determine node ports")
    node_block_rules = list()
    
    for node_port in node_ports:
        node_block_rules.extend( build_iptables_block_rules( node_port ) )
        
    return node_block_rules
    
def get_node_ports ( node_id ):
    cluster = q.manage.arakoon.getCluster(Common.cluster_id)
    node_pid = cluster._getPid(node_id)
    cmd = "netstat -natp | grep %s\/arakoon | awk '// {print $4}' | cut -d ':' -f 2 | sort -u" % node_pid 
    (exitcode,stdout,stderr) = q.system.process.run( cmd )
    print stdout
    print stderr
    NT.assert_equals( exitcode, 0, "Could not determine node ports for %s" % node_id )
    port_list = stdout.strip().split("\n")
    
    return port_list

def iterate_block_unblock_nodes ( iter_cnt, node_ids, period = 1.0 ):
    for i in range( iter_cnt ) :
        time.sleep( period )
        for node_id in node_ids:
            block_node_ports( node_id )
        time.sleep( period )
        for node_id in node_ids:
            unblock_node_ports( node_id )

def iterate_block_unblock_single_slave ( ):
    cli = get_client() 
    master_id = cli.whoMaster()
    cli.dropConnections()
    
    if ( master_id is None ) :
        logging.error( "Cannot determine who is master skipping this method")
        return
    nns = Common.node_names
    slave_index = ( nns.index( master_id ) + 1 ) % len ( nns )
    slave_id = nns[ slave_index ]    
    iterate_block_unblock_nodes ( 60, [ slave_id ] )

def iterate_block_unblock_both_slaves ( ):
    cli = get_client() 
    master_id = cli.whoMaster()
    cli.dropConnections()
    
    if ( master_id is None ) :
        logging.error( "Cannot determine who is master skipping this method")
        return
    
    slave_list = list()
    for node in Common.node_names :
        if node == master_id :
            continue
        slave_list.append( node )
        
    iterate_block_unblock_nodes ( 60, slave_list )
    
def iterate_block_unblock_master ( ):
    cli = get_client() 
    master_id = cli.whoMaster()
    cli.dropConnections()
    
    if ( master_id is None ) :
        logging.error( "Cannot determine who is master skipping this method")
        return
    
    iterate_block_unblock_nodes ( 60, [ master_id ] )

def flush_all_rules() :
    cmd = "sudo /sbin/iptables -F"
    Common.run_cmd( cmd, False )
    
def iptables_teardown( removeDirs ) :
    flush_all_rules ()
    Common.basic_teardown ( removeDirs )    
    logging.info( "iptables teardown complete" )

@Common.with_custom_setup(Common.setup_3_nodes_forced_master, 
                          iptables_teardown )
def test_block_single_slave_ports_loop () :
    master_id = Common.node_names [0]
    # Node 0 is fixed master 
    slave_id = Common.node_names[1]
    
    write_loop = lambda: Common.iterate_n_times( 10000, 
                                                 Common.set_get_and_delete )
    block_loop = lambda: iterate_block_unblock_nodes ( 10, [ slave_id ] )
    
    Common.create_and_wait_for_thread_list( [write_loop, block_loop] )
    
    # Make sure the slave is notified of running behind
    cli = Common.get_client()
    cli.set( 'key', 'value')
    
    # Give the slave some time to catchup
    time.sleep(5.0)
    Common.stop_all()
    Common.assert_last_i_in_sync ( master_id, slave_id )
    Common.compare_stores( master_id, slave_id )

@Common.with_custom_setup(Common.setup_3_nodes, 
                          iptables_teardown )
def test_block_master_ports () :
    global cluster_id
    
    def validate_reelection( old_master_id) :
        
        # Leave some time for re-election
        time.sleep( 1.5 * Common.lease_duration )
    
        cli_cfg_no_master = dict ( cli._config.getNodes() )
        # Kick out old master out of config, he is unaware of who is master
        cli_cfg_no_master.pop(old_master_id) 
        new_cli = arakoon.Arakoon.ArakoonClient(
            arakoon.Arakoon.ArakoonClientConfig(Common.cluster_id, 
                                                cli_cfg_no_master) )
        
        new_master_id = new_cli.whoMaster()
            
        NT.assert_not_equals( new_master_id, None, "No new master elected." )
        new_cli.dropConnections()
        
        cli_cfg_only_master = dict()
        for key in cli._config.getNodes().keys() :
            if key == old_master_id :
                cli_cfg_only_master [ key ] = cli._config.getNodeLocations( key  )
        new_cli = arakoon.Arakoon.ArakoonClient(
            arakoon.Arakoon.ArakoonClientConfig(Common.cluster_id, 
                                                cli_cfg_only_master ) )
        NT.assert_raises( ArakoonNoMaster, new_cli.whoMaster )
        new_cli.dropConnections()
        
        return new_master_id
    
    cli = Common.get_client()
    old_master_id = cli.whoMaster()

    master_ports = get_node_ports( old_master_id )
    cluster = q.manage.arakoon.getCluster(Common.cluster_id)
    master_client_port = cluster.getNodeConfig(old_master_id ) ["client_port"]
    master_ports.remove( master_client_port )
    block_rules = list()
    for master_port in master_ports:
        block_rules.extend (build_iptables_block_rules (master_port) )
        
    apply_iptables_rules( block_rules )
    
    NT.assert_raises( ArakoonException, cli.set, "key", "value" )
    
    new_master_id = validate_reelection( old_master_id )
    
    flush_all_rules()
    
    cli._masterId = None
    Common.set_get_and_delete( cli, "k1", "v1")
    cli.dropConnections()
    
