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

from pymonkey import q
from nose.tools import *

import os

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
        return q.config.arakoon.getCluster(self._clusterId)
    
    def setup(self):
        if self._clusterId in q.config.list():
            q.config.remove(self._clusterId)
        
        servernodes = self._serverNodes()
        if servernodes in q.config.list():
            q.config.remove(servernodes)

        cluster = self._getCluster()
        cluster.setUp(3)

    def teardown(self):
        cluster = self._getCluster()
        q.cmdtools.arakoon.stop(self._clusterId)
        cluster.tearDown()

    def testStart(self):
        cid = self._clusterId
        q.cmdtools.arakoon.start(cid)
        assert_equals(q.system.process.checkProcess( "arakoond", 3), 0)

        #starting twice should not throw anything
        q.cmdtools.arakoon.start(cid)
        assert_equals(q.system.process.checkProcess( "arakoond", 3), 0)



    def testStop(self):
        cid = self._clusterId
        q.cmdtools.arakoon.start(cid)
        assert_equals(q.system.process.checkProcess( "arakoond", 3), 0)

        q.cmdtools.arakoon.stop(cid)
        assert_equals(q.system.process.checkProcess( "arakoond"), 1)

        #stopping twice should not throw anything
        q.cmdtools.arakoon.stop(cid)
        assert_equals(q.system.process.checkProcess( "arakoond"), 1)

    def testRestart(self):
        cid = self._clusterId
        q.cmdtools.arakoon.start(cid)
        assert_equals(q.system.process.checkProcess( "arakoond", 3), 0)

        q.cmdtools.arakoon.restart(cid)
        #@TODO check if the pids are different
        assert_equals(q.system.process.checkProcess( "arakoond", 3), 0)

        q.cmdtools.arakoon.stop(cid)
        assert_equals(q.system.process.checkProcess( "arakoond"), 1)

        q.cmdtools.arakoon.restart(cid)
        assert_equals(q.system.process.checkProcess( "arakoond", 3), 0)

    def testStartOne(self):
        cid = self._clusterId
        q.cmdtools.arakoon.startOne(cid, self._n0)
        assert_equals(q.system.process.checkProcess( "arakoond", 1), 0)

        q.cmdtools.arakoon.startOne(cid, self._n1)
        assert_equals(q.system.process.checkProcess( "arakoond", 2), 0)

    def testStartOneUnkown(self):
        assert_raises(Exception, q.cmdtools.arakoon.startOne, "arakoon0")

    def testStopOne(self):
        cid = self._clusterId
        q.cmdtools.arakoon.start(cid)
        q.cmdtools.arakoon.stopOne(cid, self._n0)
        assert_equals(q.system.process.checkProcess( "arakoond", 2), 0)
        assert_equals(q.system.process.checkProcess( "arakoond", 3), 1)


    def testStopOneUnknown(self):
         assert_raises(Exception, q.cmdtools.arakoon.stopOne, "arakoon0")

    def testGetStatus(self):
        cid = self._clusterId
        q.cmdtools.arakoon.start(cid)
        assert_equals(q.cmdtools.arakoon.getStatus(cid),
                      {self._n0: q.enumerators.AppStatusType.RUNNING,
                       self._n1: q.enumerators.AppStatusType.RUNNING,
                       self._n2: q.enumerators.AppStatusType.RUNNING})

        q.cmdtools.arakoon.stopOne(cid, self._n0)
        assert_equals(q.cmdtools.arakoon.getStatus(cid),
                      {self._n0: q.enumerators.AppStatusType.HALTED, 
                       self._n1: q.enumerators.AppStatusType.RUNNING,
                       self._n2: q.enumerators.AppStatusType.RUNNING})
        
    def testGetStatusOne(self):
        cid = self._clusterId
        q.cmdtools.arakoon.start(cid)
        q.cmdtools.arakoon.stopOne(cid,self._n0)
        gos = q.cmdtools.arakoon.getStatusOne
        assert_equals(gos(cid, self._n0), q.enumerators.AppStatusType.HALTED)
        assert_equals(gos(cid, self._n1), q.enumerators.AppStatusType.RUNNING)

    def testGetStatusOneUnknown(self):
        assert_raises(Exception, q.cmdtools.arakoon.getStatusOne, "whatever")

    def testRestartOne(self):
        cid = self._clusterId
        q.cmdtools.arakoon.start(cid)
        q.cmdtools.arakoon.stopOne(cid,self._n0)

        q.cmdtools.arakoon.restartOne(cid, self._n1)
        assert_equals(q.system.process.checkProcess( "arakoond", 2), 0)
        assert_equals(q.system.process.checkProcess( "arakoond", 3), 1)


    def testRestartOneUnknown(self):
        assert_raises(Exception, q.cmdtools.arakoon.restartOne, "arakoon0")

if __name__ == '__main__' :
    from pymonkey import InitBase
    pass
