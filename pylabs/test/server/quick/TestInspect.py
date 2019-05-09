'''
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
'''

import os
import logging
from .. import system_tests_common as C

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

    logging.info("inspect .... which should fail!")
    ok = False
    try:
        inspect_cluster(cluster)
    except Exception:
        ok = True
    assert(ok)

