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

from pymonkey import q
from nose.tools import *

import os

class TestCmdTools:
    def setup(self):
        if "arakoon" in q.config.list():
            q.config.remove("arakoon")
        if "arakoonservernodes" in q.config.list():
            q.config.remove("arakoonservernodes")

        q.config.arakoon.setUp(3)

    def teardown(self):
        q.cmdtools.arakoon.stop()
        q.config.arakoon.tearDown()

    def testStart(self):
        q.cmdtools.arakoon.start()
        assert_equals(q.system.process.checkProcess( "arakoond", 3), 0)

        #starting twice should not throw anything
        q.cmdtools.arakoon.start()
        assert_equals(q.system.process.checkProcess( "arakoond", 3), 0)

    def testGetStatus(self):
        q.cmdtools.arakoon.start()
        assert_equals(q.cmdtools.arakoon.getStatus(), {"arakoon_0": q.enumerators.AppStatusType.RUNNING, \
                                                       "arakoon_1": q.enumerators.AppStatusType.RUNNING, \
                                                       "arakoon_2": q.enumerators.AppStatusType.RUNNING})

        q.cmdtools.arakoon.stopOne("arakoon_0")
        assert_equals(q.cmdtools.arakoon.getStatus(), {"arakoon_0": q.enumerators.AppStatusType.HALTED, \
                                                       "arakoon_1": q.enumerators.AppStatusType.RUNNING, \
                                                       "arakoon_2": q.enumerators.AppStatusType.RUNNING})
    def testStop(self):
        q.cmdtools.arakoon.start()
        assert_equals(q.system.process.checkProcess( "arakoond", 3), 0)

        q.cmdtools.arakoon.stop()
        assert_equals(q.system.process.checkProcess( "arakoond"), 1)

        #stopping twice should not throw anything
        q.cmdtools.arakoon.stop()
        assert_equals(q.system.process.checkProcess( "arakoond"), 1)

    def testRestart(self):
        q.cmdtools.arakoon.start()
        assert_equals(q.system.process.checkProcess( "arakoond", 3), 0)

        q.cmdtools.arakoon.restart()
        #@TODO check if the pids are different
        assert_equals(q.system.process.checkProcess( "arakoond", 3), 0)

        q.cmdtools.arakoon.stop()
        assert_equals(q.system.process.checkProcess( "arakoond"), 1)

        q.cmdtools.arakoon.restart()
        assert_equals(q.system.process.checkProcess( "arakoond", 3), 0)

    def testStartOne(self):
        q.cmdtools.arakoon.startOne("arakoon_0")
        assert_equals(q.system.process.checkProcess( "arakoond", 1), 0)

        q.cmdtools.arakoon.startOne("arakoon_1")
        assert_equals(q.system.process.checkProcess( "arakoond", 2), 0)

    def testStartOneUnkown(self):
        assert_raises(Exception, q.cmdtools.arakoon.startOne, "arakoon0")

    def testStopOne(self):
        q.cmdtools.arakoon.start()
        q.cmdtools.arakoon.stopOne("arakoon_0")
        assert_equals(q.system.process.checkProcess( "arakoond", 2), 0)
        assert_equals(q.system.process.checkProcess( "arakoond", 3), 1)


    def testStopOneUnknown(self):
         assert_raises(Exception, q.cmdtools.arakoon.stopOne, "arakoon0")

    def testGetStatusOne(self):
        q.cmdtools.arakoon.start()
        q.cmdtools.arakoon.stopOne("arakoon_0")

        assert_equals(q.cmdtools.arakoon.getStatusOne("arakoon_0"), q.enumerators.AppStatusType.HALTED)
        assert_equals(q.cmdtools.arakoon.getStatusOne("arakoon_1"), q.enumerators.AppStatusType.RUNNING)

    def testGetStatusOneUnknown(self):
        assert_raises(Exception, q.cmdtools.arakoon.getStatusOne, "arakoon0")

    def testRestartOne(self):
        q.cmdtools.arakoon.start()
        q.cmdtools.arakoon.stopOne("arakoon_0")

        q.cmdtools.arakoon.restartOne("arakoon_1")
        assert_equals(q.system.process.checkProcess( "arakoond", 2), 0)
        assert_equals(q.system.process.checkProcess( "arakoond", 3), 1)


    def testRestartOneUnknown(self):
        assert_raises(Exception, q.cmdtools.arakoon.restartOne, "arakoon0")

if __name__ == '__main__' :
    from pymonkey import InitBase
    pass
