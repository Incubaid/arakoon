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
from Compat import X
from nose.tools import *
import subprocess
import os
import time
import logging

class TestCmdTools:
    def __init__(self):
        cid = 'sturdy'
        self._clusterId = cid
        self._n0 = "%s_0" % cid
        self._n1 = "%s_1" % cid
        self._n2 = "%s_2" % cid

    def _serverNodes(self):
        return "%s_servernodes" % self._clusterId

    def _getCluster(self):
        return C._getCluster(self._clusterId)
    
    def setup(self):
        logging.info('setup')
        c1 = self._getCluster()
        c1.remove()
        c2 = self._getCluster()
        c2.setUp(3)

    def teardown(self):
        logging.info('teardown')
        cluster = self._getCluster()
        cluster.stop()
        cluster.tearDown()
        cluster.remove()

    def _assert_n_running(self,n):
        cluster = self._getCluster()
        status = cluster.getStatus()
        logging.debug('status=%s', status)
        c = 0
        for key in status.keys():
            if status[key] == X.AppStatusType.RUNNING:
                c = c + 1
        if c != n:
            for node in status.keys():
                if status[node] == X.AppStatusType.HALTED:
                    cfg = cluster._getConfigFile()
                    logDir = cfg.get(node, "log_dir")
                    fn = "%s/%s.log" % (logDir, node)
                    logging.info("fn=%s",fn)
                    def cat_log(fn):
                        with open(fn,'r') as f:
                            lines = f.readlines()
                            logging.info("fn:%s for %s", fn, node)
                            for l in lines:
                                ls = l.strip()
                                logging.info(ls)

                    #crash log ?
                    cat_log(fn)
                    filter = '%s.debug.*' % node    
                    crash = X.listFilesInDir(logDir,filter = filter)
                    if len(crash):
                        crash_fn = crash[0]
                        cat_log(crash_fn)
                                             
        msg = "expected %i running nodes, but got %i" % (n,c)
        assert_equals(c, n, msg = msg)

    def testStart(self):
        cluster = self._getCluster()
        rcs = cluster.start()

        assert set(rcs.iterkeys()) == set(cluster.listNodes())

        self._assert_n_running(3)

        #starting twice should not throw anything
        cluster.start()
        self._assert_n_running(3)



    def testStop(self):
        cluster = self._getCluster()
        cluster.start()
        self._assert_n_running(3)

        cluster.stop()
        self._assert_n_running(0)

        #stopping twice should not throw anything
        cluster.stop()
        self._assert_n_running(0)

    def testRestart(self):
        cluster = self._getCluster()
        rcs = cluster.start()

        assert set(rcs.iterkeys()) == set(cluster.listNodes())

        self._assert_n_running(3)

        cluster.restart()
        #@TODO check if the pids are different
        self._assert_n_running(3)
        cluster.stop()
        self._assert_n_running(0)

        cluster.restart()
        self._assert_n_running(3)

    def testStartOne(self):
        cluster = self._getCluster()
        cluster.startOne(self._n0)
        self._assert_n_running(1)
        cluster.startOne(self._n1)
        self._assert_n_running(2)

    def testStartOneUnkown(self):
        cluster = self._getCluster()
        assert_raises(Exception, cluster.startOne, "arakoon0")

    def testStopOne(self):
        cluster = self._getCluster()
        cluster.start()
        t0 = time.clock()
        cluster.stopOne(self._n0)
        t1 = time.clock()
        assert_true(t1 - t0 < 2.0, "took too long")
        self._assert_n_running(2)

    def testStopOneUnknown(self):
        cluster = self._getCluster()
        assert_raises(Exception, cluster.stopOne, "arakoon0")

    def testGetStatus(self):
        logging.info('1')
        cluster = self._getCluster()
        logging.info('2')
        cluster.start()
        logging.info('3')
        assert_equals(cluster.getStatus(),
                      {self._n0: X.AppStatusType.RUNNING,
                       self._n1: X.AppStatusType.RUNNING,
                       self._n2: X.AppStatusType.RUNNING})
        logging.info('4')
        cluster.stopOne(self._n0)
        assert_equals(cluster.getStatus(),
                      {self._n0: X.AppStatusType.HALTED, 
                       self._n1: X.AppStatusType.RUNNING,
                       self._n2: X.AppStatusType.RUNNING})
        
    def testGetStatusOne(self):
        cluster = self._getCluster()
        cluster.start()
        cluster.stopOne(self._n0)
        gos = cluster.getStatusOne
        assert_equals(gos(self._n0), X.AppStatusType.HALTED)
        assert_equals(gos(self._n1), X.AppStatusType.RUNNING)

    def testGetStatusOneUnknown(self):
        cluster = self._getCluster()
        assert_raises(Exception, cluster.getStatusOne, "whatever")

    def testRestartOne(self):
        cluster = self._getCluster()
        cluster.start()
        cluster.stopOne(self._n0)
        cluster.restartOne(self._n1)
        self._assert_n_running(2)


    def testRestartOneUnknown(self):
        cluster = self._getCluster()
        assert_raises(Exception, cluster.restartOne, "arakoon0")

