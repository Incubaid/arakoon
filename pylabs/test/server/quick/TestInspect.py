'''
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
'''

import os
import logging
from .. import system_tests_common as C
import time
from Compat import X

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
    r = C.call_arakoon("--inspect-cluster", "-config", "%s.cfg" % cfg_fn)
    

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



@C.with_custom_setup(C.setup_3_nodes_mini, C.basic_teardown)
def test_inspect():
    client = C.get_client()
    for i in xrange(1500):
        k = "key_%04i" % i
        client.set(k,k)
    cluster = C._getCluster()
    cfg_fn = cluster._getConfigFileName()

    
    node_id = C.node_names[0]
    C.dropMaster(node_id)
    C.copyDbToHead(node_id, 1)
    C.stop_all()

    # tamper with node_0's store.
    
    node_cfg = cluster.getNodeConfig(node_id )
    print node_cfg.keys()
    head_dir = node_cfg['head_dir']
    
    head_file_name= '%s/head.db' % (head_dir)
    logging.info("head_file_name=%s", head_file_name)
    
    r = C.call_arakoon("--dev-set-store", head_file_name, "@AAAAA", "AAA" * 300)
    r = C.call_arakoon("--dev-set-store", head_file_name, "@zzzzz", "ZZZ" * 300)

    

    
    C.start_all()

    logging.info("%s's head has been updated", node_id)
    
    cli = C.get_client()
    for i in xrange(1500,3001):
        k = "key_%04i" % i
        cli.set(k,k)

    #time.sleep(100)
    logging.info("inspect .... which should fail!")
    ok = False
    try:
        inspect_cluster(cluster)
    except Exception:
        ok = True
    assert(ok)

