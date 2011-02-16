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

class TestConfig:
    def setup(self):
        if "arakoon" in q.config.list():
            q.config.remove("arakoon")
        if "arakoonservernodes" in q.config.list():
            q.config.remove("arakoonservernodes")

    def teardown(self):
        if "arakoon" in q.config.list():
            q.config.remove("arakoon")
        if "arakoonservernodes" in q.config.list():
            q.config.remove("arakoonservernodes")

    def testAddNode(self):
        q.config.arakoon.addNode("arakoon_0")
        
        config = q.config.getInifile("arakoon")
        assert_equals(config.getSectionAsDict("global"), {'nodes': 'arakoon_0'})
        assert_equals(config.getSectionAsDict("arakoon_0"), { 'client_port': '7080', \
                                                         'home': '/opt/qbase3/var/db/arakoon/arakoon_0', \
                                                         'ip': '127.0.0.1', \
                                                         'log_dir': '/opt/qbase3/var/log/arakoon/arakoon_0', \
                                                         'log_level': 'info', \
                                                         'messaging_port': '10000', \
                                                         'name': 'arakoon_0'})

        q.config.arakoon.addNode("arakoon_1",
                                 "192.168.0.1",
                                 7081,12345,"debug",
                                 "/tmp","/tmp/joe")
        config = q.config.getInifile("arakoon")
        assert_equals(config.getSectionAsDict("global"), {'nodes': 'arakoon_0,arakoon_1'})
        assert_equals(config.getSectionAsDict("arakoon_0"), { 'client_port': '7080', \
                                                         'home': '/opt/qbase3/var/db/arakoon/arakoon_0', \
                                                         'ip': '127.0.0.1', \
                                                         'log_dir': '/opt/qbase3/var/log/arakoon/arakoon_0', \
                                                         'log_level': 'info', \
                                                         'messaging_port': '10000', \
                                                         'name': 'arakoon_0'})
        assert_equals(config.getSectionAsDict("arakoon_1"), { 'client_port': '7081', \
                                                         'home': '/tmp/joe', \
                                                         'ip': '192.168.0.1', \
                                                         'log_dir': '/tmp', \
                                                         'log_level': 'debug', \
                                                         'messaging_port': '12345', \
                                                         'name': 'arakoon_1'})

    def testAddNodeInvalidName(self):
        assert_raises(Exception, q.config.arakoon.addNode,"arak oon")
        assert_raises(Exception, q.config.arakoon.addNode,"arak#oon")
        assert_raises(Exception, q.config.arakoon.addNode,"arako,on")

    def testAddNodeInvalidLogLevel(self):
        assert_raises(Exception, q.config.arakoon.addNode,"arakoon_0","192.168.0.1",7081,12345,"depug")

    def testAddNodeDuplicateName(self):
        q.config.arakoon.addNode("arakoon_0")
        assert_raises(Exception, q.config.arakoon.addNode,"arakoon_0")

    def testRemoveNode(self):
        q.config.arakoon.addNode("arakoon_0")
        q.config.arakoon.addNode("arakoon_1")

        q.config.arakoon.removeNode("arakoon_0")

        config = q.config.getInifile("arakoon")
        assert_equals(config.getSectionAsDict("global"), {'nodes': 'arakoon_1'})
        assert_false(config.checkSection("arakoon_0"))

    def testRemoveUnknownNode(self):
        q.config.arakoon.setUp(3)

        assert_raises(Exception, q.config.arakoon.removeNode, "arakoon_4")

    def testForceMaster(self):
        for i in range(0,3):
            q.config.arakoon.addNode("arakoon_%s" % i)

        q.config.arakoon.forceMaster("arakoon_0")

        config = q.config.getInifile("arakoon")
        assert_equals(config.getValue("global",'master'), 'arakoon_0')

        q.config.arakoon.forceMaster("arakoon_1")

        config = q.config.getInifile("arakoon")
        assert_equals(config.getValue("global",'master'), 'arakoon_1')

        q.config.arakoon.forceMaster()

        config = q.config.getInifile("arakoon")
        assert_false(config.checkParam("global",'master'))

    def testForceUnknownMaster(self):
        for i in range(0,3):
           q.config.arakoon.addNode("arakoon_%s" % i)

        assert_raises(Exception, q.config.arakoon.forceMaster,"arakoon_4")

    def testSetQuorum(self):
        for i in range(0,3):
           q.config.arakoon.addNode("arakoon_%s" % i)

        q.config.arakoon.setQuorum(1)
        config = q.config.getInifile("arakoon")
        assert_equals(config.getValue("global", 'quorum'), '1')

        q.config.arakoon.setQuorum(2)
        config = q.config.getInifile("arakoon")
        assert_equals(config.getValue("global", 'quorum'), '2')

    def testSetIllegalQuorum(self):
        for i in range(0,3):
           q.config.arakoon.addNode("arakoon_%s" % i)

        assert_raises(Exception, q.config.arakoon.setQuorum, "bla")
        assert_raises(Exception, q.config.arakoon.setQuorum, -1)
        assert_raises(Exception, q.config.arakoon.setQuorum, 4)


    def testGetClientConfig(self):
        q.config.arakoon.setUp(3)

        assert_equals(q.config.arakoon.getClientConfig(), {"arakoon_0": ("127.0.0.1", 7080), "arakoon_1": ("127.0.0.1", 7081),"arakoon_2": ("127.0.0.1", 7082)})

    def testListNodes(self):
        q.config.arakoon.setUp(3)

        assert_equals(q.config.arakoon.listNodes(), ["arakoon_0", "arakoon_1", "arakoon_2"])

    def testGetNodeConfig(self):
        q.config.arakoon.setUp(3)

        assert_equals(q.config.arakoon.getNodeConfig("arakoon_0"), \
                      {'client_port': '7080', \
                       'home': '/opt/qbase3/var/db/arakoon/arakoon_0', \
                       'ip': '127.0.0.1', \
                       'log_dir': '/opt/qbase3/var/log/arakoon/arakoon_0', \
                       'log_level': 'info', \
                       'messaging_port': '10000', \
                       'name': 'arakoon_0'})

    def testGetNodeConfigUnknownNode(self):
        q.config.arakoon.setUp(3)

        assert_raises(Exception, q.config.arakoon.getNodeConfig, "arakoon")

    def testCreateDirs(self):
        q.config.arakoon.addNode("arakoon_0")
        q.config.arakoon.createDirs("arakoon_0")

        assert_true(q.system.fs.exists(q.system.fs.joinPaths(q.dirs.logDir, \
                        "arakoon", \
                        "arakoon_0")))
        assert_true(q.system.fs.exists(q.system.fs.joinPaths(q.dirs.varDir, \
                        "db", \
                        "arakoon", \
                        "arakoon_0")))

    def testRemoveDirs(self):
        q.config.arakoon.addNode("arakoon_0")
        q.config.arakoon.createDirs("arakoon_0")
        q.config.arakoon.removeDirs("arakoon_0")

        assert_false(q.system.fs.exists(q.system.fs.joinPaths(q.dirs.logDir, \
                        "arakoon", \
                        "arakoon_0")))
        assert_false(q.system.fs.exists(q.system.fs.joinPaths(q.dirs.varDir, \
                        "db", \
                        "arakoon", \
                        "arakoon_0")))

    def testAddLocalNode(self):
        for i in range(0,3):
            q.config.arakoon.addNode("arakoon_%s" % i)

        q.config.arakoon.addLocalNode("arakoon_1")

        config = q.config.getInifile("arakoonservernodes")
        assert_equals(config.getSectionAsDict("global"), {'nodes': 'arakoon_1'})

        q.config.arakoon.addLocalNode("arakoon_0")

        config = q.config.getInifile("arakoonservernodes")
        assert_equals(config.getSectionAsDict("global"), {'nodes': 'arakoon_1,arakoon_0'})

    def testAddLocalNodeUnknownNode(self):
        assert_raises(Exception, q.config.arakoon.addLocalNode, "arakoon_0")

        q.config.arakoon.addNode("arakoon_0")
        q.config.arakoon.addLocalNode("arakoon_0")

        assert_raises(Exception, q.config.arakoon.addLocalNode, "arakoon_1")

    def testAddLocalNodeDuplicateNode(self):
        q.config.arakoon.addNode("arakoon_0")
        q.config.arakoon.addLocalNode("arakoon_0")

        assert_raises(Exception, q.config.arakoon.addLocalNode, "arakoon_0")

    def testRemoveLocalNode(self):
        for i in range(0,3):
            q.config.arakoon.addNode("arakoon_%s" % i)

        q.config.arakoon.addLocalNode("arakoon_1")

        q.config.arakoon.removeLocalNode("arakoon_1")


    def testListLocalNodes(self):
        for i in range(0,3):
            q.config.arakoon.addNode("arakoon_%s" % i)
            q.config.arakoon.addLocalNode("arakoon_%s" % i)

        assert_equals(q.config.arakoon.listLocalNodes(), ["arakoon_0", "arakoon_1", "arakoon_2"])

if __name__ == '__main__' :
    from pymonkey import InitBase
    pass
