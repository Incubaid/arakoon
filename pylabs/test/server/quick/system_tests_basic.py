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



from .. import system_tests_common as C
import time
import subprocess
import logging
from nose.tools import *

from Compat import X

CONFIG = C.CONFIG
from arakoon.ArakoonProtocol import AtLeast

try:
    assert_in
except NameError:
    def assert_in(member, container, msg=None):
        assert_true(member in container,
            '%r not found in %r' % (member, container))

@C.with_custom_setup ( C.setup_1_node_forced_master, C.basic_teardown )
def test_start_stop_single_node_forced () :
    C.assert_running_nodes ( 1 )
    cluster = C._getCluster()
    cluster.stop()
    C.assert_running_nodes ( 0 )
    cluster.start()
    time.sleep(1.0)
    C.assert_running_nodes ( 1 )

@C.with_custom_setup ( C.setup_3_nodes_forced_master, C.basic_teardown )
def test_start_stop_three_nodes_forced () :
    cluster = C._getCluster()
    C.assert_running_nodes ( 3 )
    cluster.stop()
    C.assert_running_nodes ( 0 )
    cluster.start()
    time.sleep(1.0)
    C.assert_running_nodes ( 3 )

def test_start_stop_wrapper():
    cluster = C._getCluster()
    fn = "/tmp/my_wrapper.sh"
    with open(fn,'w') as f:
        f.write('#!/bin/bash\n')
        f.write("logger wrapper called with '$@'\n")
        f.write('$@\n')
    subprocess.call(['chmod','+x',fn])
    nn = "wrapper"
    cluster.addNode(nn, "127.0.0.1", 8000, wrapper = fn)
    cluster.addLocalNode(nn)
    cluster.createDirs(nn)
    C.assert_running_nodes(0)
    cluster.start()
    C.assert_running_nodes(1)
    cluster.stop()
    C.assert_running_nodes(0)
    cluster.tearDown()

def test_start_arakoon383():
    cluster = C._getCluster()
    wrapper = 'false'
    name = 'wrapper'
    cluster.addNode(name, '127.0.0.1', 8000, wrapper=wrapper)
    cluster.addLocalNode(name)
    C.assert_running_nodes(0)
    assert_equals(cluster.startOne(name), 1)
    C.assert_running_nodes(0)
    assert_equals(cluster.restartOne(name), 1)
    C.assert_running_nodes(0)
    cluster.tearDown()

@C.with_custom_setup(C.setup_1_node, C.basic_teardown)
def test_max_value_size_tinkering ():
    cluster = C._getCluster()
    C.assert_running_nodes(1)
    key = "key_not_so_big_v"
    value = "xxxxxxxxxx" * 2000
    client = C.get_client()
    client.set(key,value)
    cluster.stop()
    logging.debug("set succeeded")
    cfg = cluster._getConfigFile()
    cfg.set("global", "__tainted_max_value_size", "1024")
    X.writeConfig(cfg, cluster._getConfigFileName())
    cluster.start()
    time.sleep(1.0)
    C.assert_running_nodes(1)
    client = C.get_client()
    assert_raises (X.arakoon_client.ArakoonException, client.set, key, value)


@C.with_custom_setup(C.setup_1_node,C.basic_teardown)
def test_marker_presence_required ():
    _arakoon = CONFIG.binary_full_path
    C.assert_running_nodes(1)
    client = C.get_client()
    for x in xrange(100):
        client.set("x%i" % x,"X%i" % x)
    cluster = C._getCluster()
    cluster.stop()

    # remove the marker
    nn = C.node_names[0]
    cfg = cluster.getNodeConfig(nn)
    home = cfg['home']
    tlog = home + '/000.tlog'
    subprocess.call([_arakoon,'--strip-tlog', tlog])

    # this will fail
    cluster.start()
    time.sleep(1.0)
    C.assert_running_nodes(0)

    #check the exit code:
    cfgp = "%s.cfg" % (cluster._getConfigFileName()) # OMG
    logging.debug("cfgp=%s",cfgp)
    try:
       subprocess.check_call([_arakoon, '--node', nn, '-config', cfgp])
    except subprocess.CalledProcessError,e:
        assert_equals(e.returncode,42)

    # add the marker and start again:
    subprocess.call([_arakoon,'--mark-tlog', tlog, 'closed:%s' % nn])
    cluster.start()
    time.sleep(1.0)
    C.assert_running_nodes(1)

    # now do the same steps to test ./arakoon --mark-tlog /path/to/tlog nodename -close
    cluster.stop()
    subprocess.call([_arakoon,'--strip-tlog', tlog])

    try:
       subprocess.check_call([_arakoon, '--node', nn, '-config', cfgp])
    except subprocess.CalledProcessError,e:
        assert_equals(e.returncode,42)

    # add the marker and start again:
    subprocess.call([_arakoon,'--unsafe-close-tlog', tlog, nn])
    cluster.start()
    time.sleep(1.0)
    C.assert_running_nodes(1)


@C.with_custom_setup( C.default_setup, C.basic_teardown )
def test_single_client_100_set_get_and_deletes() :
    C.iterate_n_times( 100, C.set_get_and_delete )

@C.with_custom_setup( C.setup_1_node_forced_master, C.basic_teardown)
def test_deploy_1_to_2():
    C.add_node_scenario (1)

@C.with_custom_setup( C.setup_2_nodes_forced_master, C.basic_teardown)
def test_deploy_2_to_3():
    C.add_node_scenario (2)

@C.with_custom_setup( C.default_setup, C.basic_teardown )
def test_range ():
    C.range_scenario ( 1000 )

@C.with_custom_setup( C.default_setup, C.basic_teardown)
def test_reverse_range_entries():
    C.reverse_range_entries_scenario(1000)


@C.with_custom_setup ( C.setup_1_node_forced_master, C.basic_teardown )
def test_large_value ():
    value = 'x' * (10 * 1024 * 1024)
    client = C.get_client()
    try:
        client.set ('some_key', value)
        raise Exception('this should have failed')
    except X.arakoon_client.ArakoonException as inst:
        logging.info('inst=%s', inst)

@C.with_custom_setup( C.default_setup, C.basic_teardown )
def test_range_entries ():
    C.range_entries_scenario( 1000 )

@C.with_custom_setup(C.default_setup, C.basic_teardown)
def test_aSSert_scenario_1():
    client = C.get_client()
    client.set('x','x')
    try:
        client.aSSert('x','x')
    except X.arakoon_client.ArakoonException as ex:
        logging.error ( "Bad stuff happened: %s" % ex)
        assert_equals(True,False)

@C.with_custom_setup(C.default_setup, C.basic_teardown)
def test_aSSert_scenario_2():
    client = C.get_client()
    client.set('x','x')
    assert_raises( X.arakoon_client.ArakoonAssertionFailed, client.aSSert, 'x', None)

@C.with_custom_setup(C.default_setup, C.basic_teardown)
def test_aSSert_scenario_3():
    client = C.get_client()
    client.set('x','x')
    seq = client.makeSequence()
    seq.addAssert('x','x')
    client.sequence(seq)

@C.with_custom_setup(C.setup_1_node_forced_master, C.basic_teardown)
def test_aSSert_sequences():
    client = C.get_client()
    client.set ('test_assert','test_assert')
    client.aSSert('test_assert', 'test_assert')
    assert_raises(X.arakoon_client.ArakoonAssertionFailed,
                  client.aSSert,
                  'test_assert',
                  'something_else')

    seq = client.makeSequence()
    seq.addAssert('test_assert','test_assert')
    seq.addSet('test_assert','changed')
    client.sequence(seq)

    v = client.get('test_assert')

    assert_equals(v, 'changed', "first_sequence failed")

    seq2 = client.makeSequence()
    seq2.addAssert('test_assert','test_assert')
    seq2.addSet('test_assert','changed2')
    assert_raises(X.arakoon_client.ArakoonAssertionFailed,
                  client.sequence,
                  seq2)

    v = client.get('test_assert')
    assert_equals(v, 'changed', 'second_sequence: %s <> %s' % (v,'changed'))


@C.with_custom_setup(C.default_setup, C.basic_teardown)
def test_aSSert_exists_scenario_1():
    client = C.get_client()
    client.set('x_e','x_e')
    try:
        client.aSSert_exists('x_e')
    except X.arakoon_client.ArakoonException as ex:
        logging.error ( "Bad stuff happened: %s" % ex)
        assert_equals(True,False)

@C.with_custom_setup(C.default_setup, C.basic_teardown)
def test_aSSert_exists_scenario_2():
    client = C.get_client()
    client.set('x_e','x_e')
    assert_raises( X.arakoon_client.ArakoonAssertionFailed, client.aSSert_exists, 'no_x')

@C.with_custom_setup(C.default_setup, C.basic_teardown)
def test_aSSert_exists_scenario_3():
    client = C.get_client()
    client.set('x_e','x_e')
    seq = client.makeSequence()
    ass = seq.addAssertExists('x_e')
    client.sequence(seq)

@C.with_custom_setup(C.setup_1_node_forced_master, C.basic_teardown)
def test_aSSert_exists_sequences():
    client = C.get_client()
    client.set ('test_assert_exists','test_assert_exists')
    client.aSSert_exists('test_assert_exists')
    assert_raises(X.arakoon_client.ArakoonAssertionFailed,
                  client.aSSert_exists,
                  'test_assert_not_set')

    seq = client.makeSequence()
    seq.addAssertExists('test_assert_exists')
    seq.addSet('test_assert','changed')
    client.sequence(seq)

    v = client.get('test_assert')

    assert_equals(v, 'changed', "first_sequence failed")

    seq2 = client.makeSequence()
    seq2.addAssertExists('test_assert_exists_not_set_2')
    seq2.addSet('test_assert','changed2')
    assert_raises(X.arakoon_client.ArakoonAssertionFailed,
                  client.sequence,
                  seq2)

    v = client.get('test_assert')
    assert_equals(v, 'changed', 'second_sequence: %s <> %s' % (v,'changed'))

@C.with_custom_setup( C.default_setup, C.basic_teardown )
def test_prefix ():
    C.prefix_scenario(1000)

def tes_and_set_scenario( start_suffix ): #tes is deliberate
    client = C.get_client()

    old_value_prefix = "old_"
    new_value_prefix = "new_"

    for i in range ( 1000 ) :

        old_value = old_value_prefix + C.value_format_str % ( i+start_suffix )
        new_value = new_value_prefix + C.value_format_str % ( i+start_suffix )
        key = C.key_format_str % ( i+start_suffix )

        client.set( key, old_value )
        set_value = client.testAndSet( key, old_value , new_value )
        assert_equals( set_value, old_value )

        set_value = client.get ( key )
        assert_equals( set_value, new_value )

        set_value = client.testAndSet( key, old_value, old_value )
        assert_equals( set_value, new_value )

        set_value = client.get ( key )
        assert_not_equals( set_value, old_value )

        try:
            client.delete( key )
        except ArakoonNotFound:
            logging.error ( "Caught not found for key %s" % key )
        assert_raises( X.arakoon_client.ArakoonNotFound, client.get, key )

    client.dropConnections()


@C.with_custom_setup (C.setup_1_node, C.basic_teardown)
def test_drop_master_singleton():
    client = C.get_client()
    master = client.whoMaster()
    cluster = C._getCluster()
    #assert_raises(Exception, cluster.dropMaster, master) # want to see assert rc too
    try:
        cluster.dropMaster(master)
        raise Error
    except Exception, e:
        rc = e.args[0]
        ARA_ERR_NOT_SUPPORTED = 32
        assert_equals (rc,ARA_ERR_NOT_SUPPORTED, "wrong rc %i" % rc)




@C.with_custom_setup( C.default_setup, C.basic_teardown )
def test_test_and_set() :
    tes_and_set_scenario( 100000 )

@C.with_custom_setup( C.default_setup, C.basic_teardown)
def test_replace():
    client = C.get_client()
    old = client.replace("xxx", "xxx")
    assert_equals(old, None)
    old2 = client.replace("xxx","yyy")
    assert_equals(old2, "xxx")
    old3 = client.replace("xxx",None)
    assert_equals(old3,"yyy")
    e = client.exists("xxx")
    assert_equals(e,False)


@C.with_custom_setup( C.setup_3_nodes_forced_master , C.basic_teardown )
def test_who_master_fixed () :
    client = C.get_client()
    node = client.whoMaster()
    assert_equals ( node, C.node_names[0] )
    client.dropConnections()

@C.with_custom_setup( C.setup_3_nodes , C.basic_teardown )
def test_who_master () :
    client = C.get_client()
    node = client.whoMaster()
    assert_true ( node in C.node_names )
    client.dropConnections()

@C.with_custom_setup(C.setup_3_nodes, C.basic_teardown)
def test_nop():
    client = C.get_client()
    for i in xrange(10):
        client.nop()
    client.dropConnections()

@C.with_custom_setup(C.setup_3_nodes, C.basic_teardown)
def test_consistency():
    client = C.get_client ()
    client.set('x','X')
    client.set('z','Z')
    m = client.get_txid()
    logging.debug("m = %s", m)
    assert_equals(str(m).find("AtLeast"),0)
    time.sleep(5)
    client.setConsistency(m)
    v = client.get('x')
    assert_equals(v,'X')
    m2 = AtLeast(1000)
    client.setConsistency(m2)
    try:
        v2 = client.get('z')
        raise Exception()
    except X.arakoon_client.ArakoonException as e:
        logging.debug(e._msg)

    client.dropConnections()

@C.with_custom_setup (C.setup_3_nodes, C.basic_teardown)
def test_get_version():
    client = C.get_client()
    #first on master:

    vt = client.getVersion()
    logging.debug("tuple = %s", str(vt))
    (major,minor,patch, info) = vt
    majors = [1, 2 ** 32 - 1] # 2 ** 32 - 1 ~= -1
    assert_in(major, majors)
    #then on specific level:
    vt2 = client.getVersion(C.node_names[0])
    logging.debug("tuple = %s", str(vt2))
    client.dropConnections() # needed?


@C.with_custom_setup(C.setup_3_nodes, C.basic_teardown)
def test_current_state():
    client = C.get_client()
    for nn in C.node_names[:3]:
        s = client.getCurrentState(nn)
        logging.debug("node %s => %s", nn, s)
    client.dropConnections()

@C.with_custom_setup( C.setup_3_nodes_forced_master_normal_slaves, C.basic_teardown )
def test_restart_single_slave_short ():
    C.restart_single_slave_scenario( 2, 100, True )

@C.with_custom_setup( C.setup_3_nodes_forced_master, C.basic_teardown )
def test_restart_single_slave_short_2 ():
    C.restart_single_slave_scenario( 2, 100, False )

def test_daemon_version () :
    cmd = [CONFIG.binary_full_path,"--version" ]
    stdout = subprocess.check_output(cmd)
    logging.debug( "STDOUT: \n%s" % stdout )
    version = stdout.split('"') [1]
    assert_not_equals( "000000000000", version, "Invalid version 000000000000" )
    local_mods = version.find ("+")
    assert_equals( local_mods, -1, "Invalid daemon, built with local modifications")

@C.with_custom_setup( C.default_setup, C.basic_teardown )
def test_delete_non_existing() :
    cli = C.get_client()
    try :
        cli.delete( 'non-existing' )
    except X.arakoon_client.ArakoonNotFound as ex:
        ex_msg = "%s" % ex
        assert_equals( "'non-existing'", ex_msg, "Delete did not return the key, got: %s" % ex_msg)
    C.set_get_and_delete( cli, "k", "v")


@C.with_custom_setup( C.default_setup, C.basic_teardown )
def test_delete_non_existing_sequence() :
    cli = C.get_client()
    seq = cli.makeSequence()
    seq.addDelete( 'non-existing' )
    try :
        cli.sequence( seq )
    except X.arakoon_client.ArakoonNotFound as ex:
        ex_msg = "%s" % ex
        assert_equals( "'non-existing'", ex_msg, "Sequence did not return the key, got: %s" % ex_msg)
    C.set_get_and_delete( cli, "k", "v")


def sequence_scenario( start_suffix ):
    iter_size = 1000
    cli = C.get_client()

    start_key = C.key_format_str % start_suffix
    end_key = C.key_format_str % ( start_suffix + iter_size - 1 )
    seq = cli.makeSequence()
    for i in range( iter_size ) :
        k = C.key_format_str % (i+start_suffix)
        v = C.value_format_str % (i+start_suffix)
        seq.addSet(k, v)

    cli.sequence( seq )

    key_value_list = cli.range_entries( start_key, True, end_key, True )
    C.assert_key_value_list(start_suffix, iter_size , key_value_list )

    seq = cli.makeSequence()
    for i in range( iter_size ) :
        k = C.key_format_str % (start_suffix + i)
        seq.addDelete(k)
    cli.sequence( seq )

    key_value_list = cli.range_entries( start_key, True, end_key, True )
    assert_equal( len(key_value_list), 0,
                  "Still keys in the store, should have been deleted" )

    for i in range( iter_size ) :
        k= C.key_format_str % (start_suffix + i)
        v = C.value_format_str % (start_suffix + i)
        seq.addSet(k, v)

    seq.addDelete( "non-existing" )
    assert_raises( X.arakoon_client.ArakoonNotFound, cli.sequence, seq )
    key_value_list = cli.range_entries( start_key, True, end_key, True )
    assert_equal( len(key_value_list), 0, "There are keys in the store, should not be the case" )

    cli.dropConnections()

@C.with_custom_setup( C.default_setup, C.basic_teardown )
def test_sequence ():
    sequence_scenario( 10000 )


@C.with_custom_setup( C.setup_3_nodes , C.basic_teardown )
def test_3_nodes_stop_all_start_slaves ():

    key = C.getRandomString()
    value = C.getRandomString()

    cli = C.get_client()
    cli.set(key,value)
    master = cli.whoMaster()
    slaves = filter( lambda node: node != master, C.node_names[:3] )
    logging.debug("slaves=%s", slaves)
    C.stop_all()
    for slave in slaves:
        C.startOne( slave )

    cli.dropConnections()
    cli = C.get_client()
    stored_value = cli.get( key )
    assert_equals( stored_value, value,
                   "Stored value mismatch for key '%s' ('%s' != '%s')" %
                   (key, value, stored_value) )

@C.with_custom_setup( C.default_setup, C.basic_teardown )
def test_get_storage_utilization():
    cl = C._getCluster()
    cli = C.get_client()
    cli.set('key','value')
    time.sleep(0.2)
    C.stop_all()

    def get_total_size( d ):
        assert_not_equals( d['log'], 0, "Log dir cannot be empty")
        assert_not_equals( d['db'], 0 , "Db dir cannot be empty")
        assert_not_equals( d['tlog'], 0, "Tlog dir cannot be empty")
        return d['log'] + d['tlog'] + d['db']

    logging.debug("Testing global utilization")
    d = cl.getStorageUtilization()
    total = get_total_size(d)
    logging.debug("Testing node 0 utilization")
    d = cl.getStorageUtilization( C.node_names[0] )
    n1 = get_total_size(d)
    logging.debug("Testing node 1 utilization")
    d = cl.getStorageUtilization( C.node_names[1] )
    n2 = get_total_size(d)
    logging.debug("Testing node 2 utilization")
    d = cl.getStorageUtilization( C.node_names[2] )
    n3 = get_total_size(d)
    sum = n1+n2+n3
    assert_equals(sum, total,
                  "Sum of storage size per node (%d) should be same as total (%d)" %
                  (sum,total))


@C.with_custom_setup( C.default_setup, C.basic_teardown )
def _test_gather_evidence():
    """ disabled, because of upstream inertia"""
    data_base_dir = C.data_base_dir
    q = C.q
    cluster = C._getCluster()
    dest_file = '/'.join([data_base_dir, "evidence.tgz"])
    dest_dir =  '/'.join([data_base_dir, "evidence"] )
    destination = 'file://%s' % dest_file
    cluster.gatherEvidence(destination, test = True)

    fs.targzUncompress(dest_file, dest_dir)
    nodes = fs.listDirsInDir(dest_dir)
    files = []
    for node in nodes:
        files.extend(fs.listFilesInDir(node))
    assert_equals(len(nodes),3, "Did not get data from all nodes")
    assert_equals(len(files) > 12, True , "Did not gather enough files")

@C.with_custom_setup( C.default_setup, C.basic_teardown )
def test_get_key_count():
    cli = C.get_client()
    c = cli.getKeyCount()
    assert_equals(c, 0, "getKeyCount should return 0 but got %d" % c)
    test_size = 100
    C.iterate_n_times( test_size, C.simple_set )
    c = cli.getKeyCount()
    assert_equals(c, test_size, "getKeyCount should return %d but got %d" %
                  (test_size, c) )

@C.with_custom_setup (C.default_setup, C.basic_teardown)
def test_get_key_count_on_slave():
    #
    cli = C.get_client()
    m = cli.whoMaster()
    slaves = filter(lambda x: x <> m, C.node_names)
    s0 = slaves[0]
    cluster = C._getCluster()
    port = cluster.getNodeConfig(s0)['client_port']
    s0_coords = ["127.0.0.1", port]
    # evil: point everything to the slave
    cfg = X.arakoon_client.ArakoonClientConfig(C.cluster_id,
                                               { 'sturdy_0' : s0_coords,
                                                 'sturdy_1' : s0_coords,
                                                 'sturdy_2' : s0_coords,
                                             })

    slave_only_client = X.arakoon_client.ArakoonClient(cfg)
    try:
        count = slave_only_client.getKeyCount()
        logging.debug("count = %i", count)
        assert_true(False)
    except X.arakoon_client.ArakoonException, e:
        pass



@C.with_custom_setup (C.default_setup, C.basic_teardown)
def test_close_on_sigterm():
    n0 = C.node_names[0]
    C.stopOne(n0)
    cfg = C.getConfig(n0)
    log_file = "/".join([cfg['log_dir'], n0 + ".log"])
    with open(log_file,'r') as f:
        lines = f.readlines()
        tail = lines[-20:]
        found = False
        for l in tail:
            print l
            if l.find("fatal") > 0 and l.find ("OK") >0:
                found = True
                break
    assert_true(found)

@C.with_custom_setup (C.setup_2_nodes, C.basic_teardown )
def test_download_db():
    C.iterate_n_times( 100, C.simple_set )
    cli = C.get_client()
    m = cli.whoMaster()
    clu = C._getCluster()
    assert_raises(Exception, clu.backupDb, m, "/tmp/backup")
    n0 = C.node_names[0]
    n1 = C.node_names[1]

    C.flush_store(n1)
    C.stop_all()

    C.startOne(n0)
    time.sleep(1.0)
    db_file = C.get_node_db_file (n1)

    # since there is only 1 node running, there can't be a master and
    # we should be able to do this without problems:

    C.assert_running_nodes(1)
    cli2 = C.get_client()
    assert_raises(X.arakoon_client.ArakoonNoMaster, cli2.whoMaster)

    clu.backupDb(n0, db_file)
    C.assert_running_nodes(1)

    C.flush_store(n0)
    C.stop_all()
    C.compare_stores(n0,n1)

@C.with_custom_setup(C.setup_3_nodes, C.basic_teardown)
def test_delete_prefix():
    """ deletion of all key value pairs with same prefix """
    cli = C.get_client()
    for i in xrange(100):
        cli.set("key_%04i" % i, "whatever")
    prefix = "key_004"
    keys = cli.prefix(prefix)
    l = len(keys)
    l2 = cli.deletePrefix(prefix)
    assert_equals(l2,l, "wrong number of keys deleted")
    l3 = cli.deletePrefix(prefix)
    assert_equals(l3,0)
    logging.debug("done")

@C.with_custom_setup(C.setup_3_nodes, C.basic_teardown)
def test_multi_get():
    cli = C.get_client()
    keys = []
    for i in xrange(10):
        k = "key_%04i" % i
        keys.append(k)
        cli.set(k,k)
    vs = cli.multiGet(keys)
    for i in xrange(10):
        v = vs[i]
        assert_equals(v, keys[i])
    logging.debug("done")


@C.with_custom_setup(C.setup_3_nodes, C.basic_teardown)
def test_multi_get_option():
    cli = C.get_client()
    keys = []
    for i in xrange(10):
        k = "key_%04i" % i
        keys.append(k)
        cli.set(k,k)
    keys.append('not_present')
    vos = cli.multiGetOption(keys)
    for i in xrange(11):
        v = vos[i]
        if i < 10:
            assert_true(v == keys[i])
        else:
            assert_true(v is None)
    logging.debug("done")

@C.with_custom_setup(C.setup_3_nodes_ipv6, C.basic_teardown)
def test_ipv6():
    cli = C.get_client()
    cli.set("x","X")
    x = cli.get("x")
    assert_equals(x, "X")

@C.with_custom_setup(C.setup_3_nodes, C.basic_teardown)
def test_rev_range_entries_arakoon368():
    """ assert ARAKOON-368 bugfix """
    cli = C.get_client()
    cli.set("key0", "value0")
    cli.set("key1", "value1")
    cli.set("key2", "value2")
    l1 = cli.rev_range_entries("kez",False, "a", True, 10)
    correct = [("key2","value2"), ("key1","value1"), ("key0","value0") ]
    assert_equals(l1, correct)

@C.with_custom_setup( C.setup_3_nodes, C.basic_teardown )
def test_statistics():
    cli = C.get_client()
    stat_dict = cli.statistics()

    required_keys = [
       "start",
       "last",
       "set_info",
       "get_info",
       "del_info",
       "seq_info",
       "mget_info",
       "tas_info",
       "op_info",
       "node_is",
    ]
    required_timing_keys = [
        "n",
        "min",
        "max",
        "avg",
        "var"
    ]

    for k in required_keys:
        assert_true( stat_dict.has_key(k), "Required key missing: %s" % k)
        if k.startswith("n_"):
            assert_equals( stat_dict[k], 0, "Operation counter should be 0, but is %d" % stat_dict[k] )
        if k.endswith( "_timing"):
            timing = stat_dict[k]
            for tk in required_timing_keys:
                assert_true( timing.has_key(tk), "Required key (%s) missing in time info (%s)" % (tk, k) )
                assert_equals( timing["max"], 0.0,
                    "Wrong value for max timing of %s: %f != 0.0" %(k, timing["max"]))
                assert_equals( timing["avg"], 0.0,
                    "Wrong value for avg timing of %s: %f != 0.0" %(k, timing["avg"]))
                assert_equals( timing["var"], 0.0,
                    "Wrong value for var timing of %s: %f != 0.0" %(k, timing["var"]))
                assert_not_equals( timing["min"], 0.0,
                    "Wrong value for min timing of %s: 0.0" %(k))


    key_list = list()
    seq = cli.makeSequence()

    for i in range(10) :
        key = "key_%d" % i
        key2 = "key2_%d" % i
        val = "val_%d" % i
        key_list.append( key )
        cli.set(key, val)
        cli.get(key )
        cli.multiGet(key_list)
        seq.addSet(key2,val)
        seq.addDelete(key2)
        cli.sequence( seq )
        cli.testAndSet(key,None,val)
    for i in range(10):
        key = "key_%d" % i
        cli.delete(key)

    stat_dict = cli.statistics()

    for k in required_keys:
        assert_true( stat_dict.has_key(k), "Required key missing: %s" % k)
        if k.startswith("n_"):
            assert_not_equals( stat_dict[k], 0,
                "Operation counter %s should be not be 0, but is." % k )
        if k.endswith( "_timing"):
            timing = stat_dict[k]
            for tk in required_timing_keys:
                assert_true( timing.has_key(tk), "Required key (%s) missing in time info (%s)" % (tk, k) )
                assert_not_equals( timing["max"], 0.0,
                    "Wrong value for max timing of %s == 0.0" %k)
                assert_not_equals( timing["avg"], 0.0,
                    "Wrong value for avg timing of %s == 0.0" % k)
                assert_not_equals( timing["var"], 0.0,
                    "Wrong value for var timing of %s == 0.0" % k)
                assert_not_equals( timing["min"], 0.0,
                    "Wrong value for min timing of %s == 0.0" % k)
