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



from ..system_tests_common import with_custom_setup

from ..system_tests_common import setup_1_node, setup_2_nodes
from ..system_tests_common import setup_2_nodes_forced_master
from ..system_tests_common import basic_teardown

from .. import system_tests_common as C

from nose.tools import *

import time
from threading import Thread
import os
import sys
import logging
import arakoon_ext.server.RemoteControlProtocol as RCP
from Compat import X

@with_custom_setup(setup_2_nodes_forced_master, basic_teardown)
def test_catchup_only():
    C.iterate_n_times(123000, C.simple_set)
    n0 = C.node_names[0]
    n1 = C.node_names[1]
    logging.info("stopping %s", n1)
    C.stopOne(n1)
    C.wipe(n1)
    logging.info("catchup-only")
    C.catchupOnly(n1)
    logging.info("done with catchup-only")
    C.flush_store(n0)
    C.stopOne(n0)
    C.compare_stores(n1,n0)
    C.start_all()
    C.assert_running_nodes(2)




def fake_download(sn):
    config = C.getConfig(sn)
    sn_ip = config['ip']
    sn_port = int(config['client_port'])
    s = RCP.make_socket(sn_ip,sn_port)
    RCP._prologue(C.cluster_id,s)
    cmd = RCP._int_to(RCP._DOWNLOAD_DB | RCP._MAGIC)
    s.send(cmd)
    RCP.check_error_code(s)
    db_size = RCP._receive_int64(s)
    logging.debug("%s:db_size=%i", sn, db_size)
    fn = '/tmp/%s.txt' % sn

    try:
        os.unlink(fn)
    except:
        pass

    tmpf = file(fn,'w')
    i = 0
    n = 60
    buf = db_size / n
    total = 0
    while i < n:
        data = s.recv(buf)
        ldata = len(data)
        total += ldata
        tmpf.write("%s read %i\t%i: (%i/%i)\n" % (sn, ldata, i,total, db_size))
        tmpf.flush()
        if ldata == 0:
            tmpf.write("stopping")
            break
        time.sleep(1.0)
        i = i + 1
    tmpf.close()

    logging.debug("done")
    s.close()

@with_custom_setup(C.default_setup, C.basic_teardown)
def test_download_db_dead_master():

    n = 43210
    C.iterate_n_times(n, C.simple_set)
    logging.debug("%s sets", n)
    client = C.get_client()
    master = client.whoMaster()
    logging.debug("master=%s", master)
    cluster = C._getCluster()
    slaves = filter(lambda node: node !=master, C.node_names)[:2]
    logging.debug("slaves = %s", slaves)
    # connect to 1 slave, do a fake download db
    #(send command and read slowly)
    # then kill master, wait and see if another surfaces
    #

    s0 = slaves[0]
    s1 = slaves[1]
    t0 = Thread(target = fake_download,args=[s0])
    t1 = Thread(target = fake_download,args=[s1])

    t0.start()
    t1.start()
    C.stopOne(master)
    t0.join()
    t1.join()
    time.sleep(15.0)
    cl2 = C.get_client()
    m2 = cl2.whoMaster()
    logging.debug("master2 = %s", m2)

@with_custom_setup(C.default_setup, C.basic_teardown)
def test_download_db_with_load():
    "NOTE: this test has never failed."
    global exn
    exn = None

    n = 43210
    C.iterate_n_times(n, C.simple_set)
    logging.debug("%s sets", n)
    client = C.get_client()
    master = client.whoMaster()
    logging.debug("master=%s", master)
    cluster = C._getCluster()
    slaves = filter(lambda node: node !=master, C.node_names)[:2]
    logging.debug("slaves = %s", slaves)

    # download db from slave, slowly
    s0 = slaves[0]
    t0 = Thread(target = fake_download,args=[s0])

    # and put some load on the system too
    def load():
        C.iterate_n_times(200, C.simple_set)
    t1 = Thread(target = load)
    t2 = Thread(target = load)

    t0.start()
    t1.start()
    t2.start()

    t0.join()
    t1.join()
    t2.join()

@with_custom_setup(C.setup_2_nodes_mini, C.basic_teardown)
def test_mixed_tlog_formats():
    cluster = C._getCluster()
    cluster.disableFsync(C.node_names[:2])
    s0 = 10500
    logging.info("going to do %i sets",s0)
    C.iterate_n_times(s0,C.simple_set)
    C.stop_all()
    cluster.enableTlogCompression(compressor = 'bz2')

    C.start_all()
    logging.info("another %i sets", s0)
    C.iterate_n_times(s0,C.simple_set)
    C.stop_all()

    # do we have both .tlf and .tlx files?
    n0 = C.node_names[0]
    n1 = C.node_names[1]
    config = C.getConfig(n0)

    tlx_dir = config.get('tlf_dir')
    if not tlx_dir:
        tlx_dir = config.get('home')
    files = os.listdir(tlx_dir)

    tls = filter(lambda x:x.endswith(".tlx"), files)
    tlf = filter(lambda x:x.endswith(".tlf"), files)
    assert_true(len(tls) > 5, "we should have .tlx files" )
    assert_true(len(tlf) > 5, "we should have .tlf files" )

    # does catchup still work?

    C.wipe(n0)
    C.startOne(n1)

    #wait for n1 to respond to client requests...
    time.sleep(5)


    rc = cluster.catchupOnly(n0)
    logging.info("catchup had rc=%i", rc)

    C.flush_store(n1)
    C.stop_all()


    C.compare_stores(n0,n1)

    C.start_all()
    rc = C.collapse(name=n1,n = 2)
    logging.info("collapse had rc=%i", rc)
    assert_true(rc == 0, "this should not have failed")
    head_dir = C.build_node_dir_names(n1)[3]

    db_file = head_dir + "/head.db"
    time.sleep(1.0) # give it time to move
    assert_true(X.fileExists(db_file), "%s should exist" % db_file)
