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
    def __init__(self):
        self._cluster_id = 'sturdy'

    def __servernodes(self):
        return '%s_servernodes' % self._cluster_id

    def setup(self):
        cid = self._cluster_id
        if cid in q.config.list():
            q.config.remove(cid)

        sn = self.__servernodes()
        if sn in q.config.list():
            q.config.remove(sn)

    def teardown(self):
        cid = self._cluster_id
        if cid in q.config.list():
            q.config.remove(cid)

        sn = self.__servernodes()
        if sn in q.config.list():
            q.config.remove(sn)

    def testAddNode(self):
        cid = self._cluster_id
        n0 = '%s_0' % cid
        n1 = '%s_1' % cid
        
        q.config.arakoon.addNode(cid, n0)
        
        config = q.config.getInifile(cid)
        assert_equals(config.getSectionAsDict("global"),
                      {'nodes': n0,
                       'cluster_id': cid
                       })
        assert_equals(config.getSectionAsDict(n0),
                      { 'client_port': '7080', 
                        'home': '/opt/qbase3/var/db/%s/%s' % (cid,n0), 
                        'ip': '127.0.0.1', 
                        'log_dir': '/opt/qbase3/var/log/%s/%s' % (cid,n0), 
                        'log_level': 'info', 
                        'messaging_port': '10000', 
                        'name': n0})
        
        q.config.arakoon.addNode(cid,
                                 n1,
                                 "192.168.0.1",
                                 7081,
                                 12345,
                                 "debug",
                                 "/tmp",
                                 "/tmp/joe")
    
        config = q.config.getInifile(cid)
        
        assert_equals(config.getSectionAsDict("global"),
                      {'nodes': '%s,%s' % (n0,n1),
                       'cluster_id':cid})

        assert_equals(config.getSectionAsDict(n0),
                      { 'client_port': '7080', 
                        'home': '/opt/qbase3/var/db/%s/%s' % (cid,n0), 
                        'ip': '127.0.0.1', 
                        'log_dir': '/opt/qbase3/var/log/%s/%s' % (cid,n0), 
                        'log_level': 'info', 
                        'messaging_port': '10000', 
                        'name': n0})
    
        assert_equals(config.getSectionAsDict(n1),
                      { 'client_port': '7081', 
                        'home': '/tmp/joe', 
                        'ip': '192.168.0.1', 
                        'log_dir': '/tmp', 
                        'log_level': 'debug', 
                        'messaging_port': '12345', 
                        'name': n1})

    def testAddNodeInvalidName(self):
        assert_raises(Exception, q.config.arakoon.addNode,"arak oon")
        assert_raises(Exception, q.config.arakoon.addNode,"arak#oon")
        assert_raises(Exception, q.config.arakoon.addNode,"arako,on")

    def testAddNodeInvalidLogLevel(self):
        assert_raises(Exception, q.config.arakoon.addNode,"arakoon_0","192.168.0.1",7081,12345,"depug")

    def testAddNodeDuplicateName(self):
        cid = self._cluster_id
        n0 = '%s_%i' % (cid,0)
        q.config.arakoon.addNode(cid,n0)
        assert_raises(Exception, q.config.arakoon.addNode,cid,n0)

    def testRemoveNode(self):
        cid = self._cluster_id
        n0 = '%s_%i' % (cid,0)
        n1 = '%s_%i' % (cid,1)
        
        q.config.arakoon.addNode(cid,n0)
        q.config.arakoon.addNode(cid,n1)

        q.config.arakoon.removeNode(cid,n0)

        config = q.config.getInifile(cid)
        assert_equals(config.getSectionAsDict("global")['nodes'],n1)
        assert_false(config.checkSection(n0))

    def testRemoveUnknownNode(self):
        cid = self._cluster_id
        q.config.arakoon.setUp(cid,3)

        assert_raises(Exception, q.config.arakoon.removeNode, "arakoon_4")

    def testForceMaster(self):
        cid = self._cluster_id
        n0 = '%s_%i' % (cid,0)
        n1 = '%s_%i' % (cid,1)

        for i in range(0,3):
            ni = '%s_%i' % (cid,i)
            q.config.arakoon.addNode(cid, ni)

        q.config.arakoon.forceMaster(cid, n0)

        config = q.config.getInifile(cid)
        assert_equals(config.getValue("global",'master'), n0)

        q.config.arakoon.forceMaster(cid,n1)

        config = q.config.getInifile(cid)
        assert_equals(config.getValue("global",'master'), n1)

        q.config.arakoon.forceMaster(cid)

        config = q.config.getInifile("arakoon")
        assert_false(config.checkParam("global",'master'))

    def testForceUnknownMaster(self):
        for i in range(0,3):
           q.config.arakoon.addNode("arakoon_%s" % i)

        assert_raises(Exception, q.config.arakoon.forceMaster,"arakoon_4")

    def testSetQuorum(self):
        cid = self._cluster_id 
        for i in range(0,3):
           ni = '%s_%i' % (cid, i)
           q.config.arakoon.addNode(cid, ni)

        q.config.arakoon.setQuorum(cid,1)
        config = q.config.getInifile(cid)
        assert_equals(config.getValue("global", 'quorum'), '1')

        q.config.arakoon.setQuorum(cid,2)
        config = q.config.getInifile(cid)
        assert_equals(config.getValue("global", 'quorum'), '2')

    def testSetIllegalQuorum(self):
        cid = self._cluster_id
        for i in range(0,3):
            ni = '%s_%i' % (cid, i)
            q.config.arakoon.addNode(cid, ni)

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
        cid = self._cluster_id
        n0 = '%s_%i' % (cid, 0)
        q.config.arakoon.addNode(cid, n0)
        q.config.arakoon.createDirs(cid,n0)

        assert_true(q.system.fs.exists(q.system.fs.joinPaths(q.dirs.logDir, cid, n0)))
        assert_true(q.system.fs.exists(q.system.fs.joinPaths(q.dirs.varDir, "db", cid, n0)))

    def testRemoveDirs(self):
        cid = self._cluster_id
        n0 = '%s_%i' % (cid, 0)
        q.config.arakoon.addNode(cid,n0)
        q.config.arakoon.createDirs(cid,n0)
        q.config.arakoon.removeDirs(cid,n0)

        assert_false(q.system.fs.exists(q.system.fs.joinPaths(q.dirs.logDir, cid, n0)))
        assert_false(q.system.fs.exists(q.system.fs.joinPaths(q.dirs.varDir, "db",
                                                              cid, n0)))

    def testAddLocalNode(self):
        
        cid = self._cluster_id
        for i in range(0,3):
            q.config.arakoon.addNode(cid, "%s_%s" % (cid,i))

        n0 = '%s_0' % cid
        n1 = '%s_1' % cid
        
        q.config.arakoon.addLocalNode(cid, n1)

        sn = self.__servernodes()
        
        config = q.config.getInifile(sn)
        assert_equals(config.getSectionAsDict("global"),
                      {'nodes': n1})

        q.config.arakoon.addLocalNode(cid, n0)

        config = q.config.getInifile(sn)
        assert_equals(config.getSectionAsDict("global"),
                      {'nodes': '%s,%s' % (n1,n0)})

    def testAddLocalNodeUnknownNode(self):
        cid = self._cluster_id
        n0 = '%s_%i' % (cid,0)
        assert_raises(Exception, q.config.arakoon.addLocalNode, cid, n0)

        q.config.arakoon.addNode(cid, n0)
        q.config.arakoon.addLocalNode(cid, n0)

        n1 = '%s_%i' % (cid,1)
        assert_raises(Exception, q.config.arakoon.addLocalNode, cid,n1)

    def testAddLocalNodeDuplicateNode(self):
        cid = self._cluster_id
        n0 = '%s_%i' % (cid,0)
        q.config.arakoon.addNode(cid, n0)
        q.config.arakoon.addLocalNode(cid,n0)

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
