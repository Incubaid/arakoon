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

from ..system_tests_common import with_custom_setup

from ..system_tests_common import setup_1_node, setup_2_nodes
from ..system_tests_common import setup_2_nodes_forced_master
from ..system_tests_common import basic_teardown

from .. import system_tests_common as C

from nose.tools import *

import time
import logging
import subprocess
import threading



@with_custom_setup(setup_2_nodes, basic_teardown)
def test_collapse():
    zero = C.node_names[0]
    one = C.node_names[1]
    n = 298765
    C.iterate_n_times(n, C.simple_set)
    logging.info("did %i sets, now going into collapse scenario" % n)
    C.collapse(zero,1)
    logging.info("collapsing done")
    C.stopOne(one)
    C.wipe(one)
    C.startOne(one)
    cli = C.get_client()
    assert_false(cli.expectProgressPossible())
    up2date = False
    counter = 0
    while not up2date and counter < 100:
        time.sleep(1.0)
        counter = counter + 1
        up2date = cli.expectProgressPossible()
    logging.info("catchup from collapsed node finished")

@with_custom_setup(setup_1_node, basic_teardown)
def test_concurrent_collapse_fails():
    """ assert only one collapse goes through at any time (eta : 450s) """
    zero = C.node_names[0]
    n = 298765
    logging.info("going to do %i sets to fill tlogs", n)
    C.iterate_n_times(n, C.simple_set)
    logging.info("Did %i sets, now going into collapse scenario", n)
    
    class SecondCollapseThread(threading.Thread):
        def __init__(self, sleep_time):
            threading.Thread.__init__(self)
            
            self.sleep_time = sleep_time
            self.exception_received = False
    
        def run(self):
            logging.info('Second collapser thread started, sleeping...')
            time.sleep(self.sleep_time)
            
            logging.info('Starting concurrent collapse')
            rc = C.collapse(zero, 1)
   
            logging.info('Concurrent collapse returned %d', rc)

            if rc == 255:
                self.exception_received = True

    s = SecondCollapseThread(8)
    assert not s.exception_received
       
    logging.info('Launching second collapser thread')
    s.start()

    logging.info('Launching main collapse')
    C.collapse(zero, 1)
       
    logging.info("collapsing finished")
    assert_true(s.exception_received)



@with_custom_setup(setup_2_nodes_forced_master, basic_teardown)
def test_catchup_exercises():
    time.sleep(C.lease_duration) # ??
    
    def do_one(n, max_wait):
        logging.info("do_one(%i,%f)", n, max_wait)
        C.iterate_n_times(n, C.simple_set)
        logging.info("sets done, stopping")
        C.stop_all()
        logging.info("stopped all nodes")
        nn = C.node_names[1]
        C.wipe(nn)

        C.start_all()
        logging.info("started all")
        
        counter = 0
        up2date = False
        cli = C.get_client ()
        while not up2date and counter < max_wait :
            time.sleep( 1.0 )
            counter += 1
            up2date = cli.expectProgressPossible()
            logging.info("up2date=%s", up2date)
        
        cli.dropConnections()
        if counter >= max_wait :
            raise Exception ("Node did not catchup in a timely fashion")

    n = 20000
    w = 80 # 250/s should be more than enough even for dss driven vmachines
    for i in range(5):
        do_one(n,w)
        n = n * 2
        w = w * 2


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
    C.stopOne(n0)
    C.compare_stores(n1,n0)
    C.start_all()
    C.assert_running_nodes(2)


import sys
sys.path.append('/opt/qbase3/lib/pymonkey/extensions/arakoon/server/')
import RemoteControlProtocol as RCP
from threading import Thread
import os

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
