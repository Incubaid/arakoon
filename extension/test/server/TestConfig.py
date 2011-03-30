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
        self._clusterId = 'sturdy'

    def __servernodes(self):
        return '%s_local_nodes' % self._clusterId

    def _getCluster(self):
        return q.manage.arakoon.getCluster(self._clusterId)
    
    def setup(self):
        cid = self._clusterId
        cl = q.manage.arakoon.getCluster( cid )
        cl.remove()
        
    def teardown(self):
        cid = self._clusterId
        cl = q.manage.arakoon.getCluster( cid )
        cl.remove()
        
    def testAddNode(self):
        cid = self._clusterId
        n0 = '%s_0' % cid
        n1 = '%s_1' % cid

        cluster = self._getCluster()
        cluster.addNode(n0)
        
        config = cluster._getConfigFile()
        assert_equals(config.getSectionAsDict("global"),
                      {'cluster': n0,
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
        
        cluster.addNode(n1,
                        "192.168.0.1",
                        7081,
                        12345,
                        "debug",
                        "/tmp",
                        "/tmp/joe")
    
        config = cluster._getConfigFile()
        
        assert_equals(config.getSectionAsDict("global"),
                      {'cluster': '%s,%s' % (n0,n1),
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
        cluster = self._getCluster()
        assert_raises(Exception, cluster.addNode,"arak oon")
        assert_raises(Exception, cluster.addNode,"arak#oon")
        assert_raises(Exception, cluster.addNode,"arako,on")

    def testAddNodeInvalidLogLevel(self):
        cluster = self._getCluster()
        assert_raises(Exception, cluster.addNode,
                      'sturdy_0',
                      "192.168.0.1",7081,
                      12345,
                      "depug")

    def testAddNodeDuplicateName(self):
        cid = self._clusterId
        n0 = '%s_%i' % (cid,0)
        cluster = self._getCluster()
        cluster.addNode(n0)
        assert_raises(Exception, cluster.addNode,n0)

    def testRemoveNode(self):
        cid = self._clusterId
        n0 = '%s_%i' % (cid,0)
        n1 = '%s_%i' % (cid,1)
        cluster = self._getCluster()
        cluster.addNode(n0)
        cluster.addNode(n1)

        cluster.removeNode(n0)

        config = cluster._getConfigFile()
        assert_equals(config.getSectionAsDict("global")['cluster'],n1)
        assert_false(config.checkSection(n0))

    def testRemoveUnknownNode(self):
        cluster = self._getCluster()
        cluster.setUp(3)

        assert_raises(Exception, cluster.removeNode, "arakoon_4")

    def testForceMaster(self):
        cid = self._clusterId
        n0 = '%s_%i' % (cid,0)
        n1 = '%s_%i' % (cid,1)

        cluster = self._getCluster()
        
        for i in range(0,3):
            ni = '%s_%i' % (cid,i)
            cluster.addNode(ni)

        cluster.forceMaster(n0)

        config = cluster._getConfigFile()
        assert_equals(config.getValue("global",'master'), n0)

        cluster.forceMaster(n1)

        config = cluster._getConfigFile()
        assert_equals(config.getValue("global",'master'), n1)

        cluster.forceMaster(None)

        config = cluster._getConfigFile()
        assert_false(config.checkParam("global",'master'))


        
    def testForceUnknownMaster(self):
        cid = self._clusterId
        cluster = self._getCluster()
        for i in range(0,3):
            ni = '%s_%i' % (cid,i)
            cluster.addNode(ni)

        assert_raises(Exception, cluster.forceMaster, "arakoon_4")

    def testPreferredMaster(self):
        cid = self._clusterId
        cluster = self._getCluster()
        for i in range(0,3):
            ni = '%s_%i' % (cid,i)
            cluster.addNode(ni)
        cluster.forceMaster('%s_0' % cid, preferred = True)
        config = cluster._getConfigFile()
        ok = config.checkParam("global", "preferred_master")
        assert_true(ok)
        p = config.getParam("global", "preferred_master")
        assert_equals(p,'true')
        
    def testSetQuorum(self):
        cid = self._clusterId
        cluster = self._getCluster()
        for i in range(0,3):
           ni = '%s_%i' % (cid, i)
           cluster.addNode(ni)

        cluster.setQuorum(1)
        config = cluster._getConfigFile()
        assert_equals(config.getValue("global", 'quorum'), '1')

        cluster.setQuorum(2)
        config = cluster._getConfigFile()
        assert_equals(config.getValue("global", 'quorum'), '2')

    def testSetIllegalQuorum(self):
        cid = self._clusterId
        cluster = self._getCluster()
        for i in range(0,3):
            ni = '%s_%i' % (cid, i)
            cluster.addNode(ni)

        assert_raises(Exception, cluster.setQuorum, "bla")
        assert_raises(Exception, cluster.setQuorum, -1)
        assert_raises(Exception, cluster.setQuorum, 4)


    def testGetClientConfig(self):
        cid = self._clusterId
        cluster = self._getCluster()
        cluster.setUp(3)
        n0 = '%s_%i' % (cid,0)
        n1 = '%s_%i' % (cid,1)
        n2 = '%s_%i' % (cid,2)
        assert_equals(cluster.getClientConfig(),
                      {n0: ("127.0.0.1", 7080),
                       n1: ("127.0.0.1", 7090),
                       n2: ("127.0.0.1", 7100)})

    def testListNodes(self):
        cid = self._clusterId
        cluster = self._getCluster()
        cluster.setUp(3)
        templates = ["%s_0", "%s_1", "%s_2"]
        ns = map(lambda t: t % cid,templates)
        assert_equals(cluster.listNodes(), ns)

    def testGetNodeConfig(self):
        cid = self._clusterId
        cluster = self._getCluster()
        cluster.setUp(3)
        n0 = '%s_%i' % (cid, 0)
        
        assert_equals(cluster.getNodeConfig(n0),
                      {'client_port': '7080', 
                       'home': '/opt/qbase3/var/db/%s/%s' % (cid, n0), 
                       'ip': '127.0.0.1', 
                       'log_dir': '/opt/qbase3/var/log/%s/%s' % (cid,n0), 
                       'log_level': 'info', 
                       'messaging_port': '7081', 
                       'name': n0})

    def testGetNodeConfigUnknownNode(self):
        cid = self._clusterId
        cluster = self._getCluster()
        cluster.setUp(3)

        assert_raises(Exception, cluster.getNodeConfig, "arakoon")

    def testCreateDirs(self):
        cid = self._clusterId
        n0 = '%s_%i' % (cid, 0)
        cluster = self._getCluster()
        cluster.addNode(n0)
        cluster.createDirs(n0)
        p0 = q.system.fs.joinPaths(q.dirs.logDir, cid, n0)
        p1 = q.system.fs.joinPaths(q.dirs.varDir, "db", cid, n0)
        print p0
        print p1
        assert_true(q.system.fs.exists(p0))
        assert_true(q.system.fs.exists(p1))

    def testRemoveDirs(self):
        cid = self._clusterId
        n0 = '%s_%i' % (cid, 0)
        cluster = self._getCluster()
        cluster.addNode(n0)
        cluster.createDirs(n0)
        cluster.removeDirs(n0)
        p0 = q.system.fs.joinPaths(q.dirs.logDir, cid, n0)
        p1 = q.system.fs.joinPaths(q.dirs.varDir, "db", cid, n0)
        assert_false(q.system.fs.exists(p0))
        assert_false(q.system.fs.exists(p1))

    def testAddLocalNode(self):
        
        cid = self._clusterId
        cluster = self._getCluster()
        for i in range(0,3):
            cluster.addNode("%s_%s" % (cid,i))

        n0 = '%s_0' % cid
        n1 = '%s_1' % cid
        
        cluster.addLocalNode(n1)

        sn = self.__servernodes()
        
        cfgPath = q.system.fs.joinPaths( q.dirs.cfgDir, "qconfig", "arakoon", cid, "%s_local_nodes" % cid)        
        config = q.config.getInifile( cfgPath )
        assert_equals(config.getSectionAsDict("global"),
                      {'cluster': n1})

        cluster.addLocalNode(n0)

        config = q.config.getInifile( cfgPath )
        assert_equals(config.getSectionAsDict("global"),
                      {'cluster': '%s,%s' % (n1,n0)})

    def testAddLocalNodeUnknownNode(self):
        cid = self._clusterId
        n0 = '%s_%i' % (cid,0)
        cluster = self._getCluster()
        assert_raises(Exception, cluster.addLocalNode, n0)

        cluster.addNode(n0)
        cluster.addLocalNode(n0)

        n1 = '%s_%i' % (cid,1)
        assert_raises(Exception, cluster.addLocalNode, n1)

    def testAddLocalNodeDuplicateNode(self):
        cid = self._clusterId
        n0 = '%s_%i' % (cid,0)
        cluster = self._getCluster()
        cluster.addNode(n0)
        cluster.addLocalNode(n0)

        assert_raises(Exception, cluster.addLocalNode, n0)

    def testRemoveLocalNode(self):
        cid = self._clusterId
        n1 = '%s_%i' % (cid,1)
        cluster = self._getCluster()
        for i in range(0,3):
            ni = '%s_%i' % (cid,i)
            cluster.addNode(ni)

        cluster.addLocalNode(n1)

        cluster.removeLocalNode(n1)


    def testListLocalNodes(self):
        cid = self._clusterId
        names = []
        cluster = self._getCluster()
        for i in range(0,3):
            ni = '%s_%i' % (cid, i)
            names.append(ni)
            cluster.addNode(ni)
            cluster.addLocalNode(ni)

        assert_equals(cluster.listLocalNodes(), names)

if __name__ == '__main__' :
    from pymonkey import InitBase
    pass
