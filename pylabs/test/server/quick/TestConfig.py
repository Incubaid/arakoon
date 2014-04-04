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



import Compat
from Compat import X
from .. import system_tests_common as C
from nose.tools import *
import os
import logging

class TestConfig:
    def __init__(self):
        self._clusterId = 'sturdy'

    def __servernodes(self):
        return '%s_local_nodes' % self._clusterId

    def _getCluster(self):
        return C._getCluster(self._clusterId)

    def setup(self):
        cid = self._clusterId
        cl = C._getCluster( cid )
        cl.remove()

    def teardown(self):
        cid = self._clusterId
        cl = C._getCluster( cid )
        cl.remove()

    def testAddNode(self):
        cid = self._clusterId
        n0 = '%s_0' % cid
        n1 = '%s_1' % cid

        cluster = self._getCluster()
        cluster.addNode(n0)

        config = cluster._getConfigFile()
        assert_equals(Compat.sectionAsDict(config, "global"),
                      {'cluster': n0,
                       'cluster_id': cid
                       })
        assert_equals(Compat.sectionAsDict(config, n0),
                      { 'client_port': '7080',
                        'home': '%s/db/%s/%s' % (X.varDir, cid, n0),
                        'ip': '127.0.0.1',
                        'log_dir': '%s/%s/%s' % (X.logDir, cid, n0),
                        'log_level': 'info',
                        'messaging_port': '10000'})

        cluster.addNode(n1,
                        "192.168.0.1",
                        7081,
                        12345,
                        "debug",
                        "/tmp",
                        "/tmp/joe")

        config = cluster._getConfigFile()

        assert_equals(Compat.sectionAsDict(config, "global"),
                      {'cluster': '%s,%s' % (n0,n1),
                       'cluster_id':cid})

        assert_equals(Compat.sectionAsDict(config, n0),
                      { 'client_port': '7080',
                        'home': '%s/db/%s/%s' % (X.varDir, cid, n0),
                        'ip': '127.0.0.1',
                        'log_dir': '%s/%s/%s' % (X.logDir, cid, n0),
                        'log_level': 'info',
                        'messaging_port': '10000'})

        assert_equals(Compat.sectionAsDict(config, n1),
                      { 'client_port': '7081',
                        'home': '/tmp/joe',
                        'ip': '192.168.0.1',
                        'log_dir': '/tmp',
                        'log_level': 'debug',
                        'messaging_port': '12345'})

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

    def testAddNodeMultipleIps(self):
        cluster = self._getCluster()
        name = "whatever"
        ips = ["127.0.0.1","192.168.0.1"]
        cluster.addNode(name = name, ip=ips)
        config = cluster._getConfigFile()
        d = Compat.sectionAsDict(config, name)
        assert_equals(d['ip'],'127.0.0.1,192.168.0.1')

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
        assert_equals(Compat.sectionAsDict(config, "global")['cluster'],n1)
        assert_false(config.has_section(n0))

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
        assert_equals(config.get("global",'master'), n0)

        cluster.forceMaster(n1)

        config = cluster._getConfigFile()
        assert_equals(config.get("global",'master'), n1)

        cluster.forceMaster(None)

        config = cluster._getConfigFile()
        assert_false(config.has_option("global",'master'))



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
        ok = config.has_option("global", "preferred_master")
        assert_true(ok)
        p = config.get("global", "preferred_master")
        assert_equals(p,'true')


    def testPreferredMastersNoMaster(self):
        cid = self._clusterId
        cluster = self._getCluster()

        for i in xrange(3):
            cluster.addNode('%s_%i' % (cid, i))

        cluster.preferredMasters(['%s_1' % cid, '%s_2' % cid])

        config = cluster._getConfigFile()
        assert_true(config.has_option('global', 'preferred_masters'))
        pms = map(str.strip,
                config.get('global', 'preferred_masters').split(','))
        assert_equals(pms, ['%s_1' % cid, '%s_2' % cid])

    def testPreferredMastersForcedMaster(self):
        cid = self._clusterId
        cluster = self._getCluster()

        for i in xrange(3):
            cluster.addNode('%s_%i' % (cid, i))

        cluster.forceMaster('%s_0' % cid, preferred=False)

        assert_raises(Exception, cluster.preferredMasters, ['%s_1' % cid])

    def testPreferredMastersPreferredMaster(self):
        cid = self._clusterId
        cluster = self._getCluster()

        for i in xrange(3):
            cluster.addNode('%s_%i' % (cid, i))

        cluster.forceMaster('%s_0' % cid, preferred=True)

        cluster.preferredMasters(['%s_0' % cid, '%s_1' % cid])

        config = cluster._getConfigFile()
        assert_false(config.has_option('global', 'master'))
        assert_false(config.has_option('global', 'preferred_master'))
        assert_true(config.has_option('global', 'preferred_masters'))
        pms = map(str.strip,
                config.get('global', 'preferred_masters').split(','))
        assert_equals(pms, ['%s_0' % cid, '%s_1' % cid])

    def testUnsetPreferredMaster(self):
        cid = self._clusterId
        cluster = self._getCluster()

        for i in xrange(3):
            cluster.addNode('%s_%i' % (cid, i))

        cluster.preferredMasters(['%s_0' % cid, '%s_1' % cid])

        config = cluster._getConfigFile()
        assert_true(config.has_option('global', 'preferred_masters'))
        pms = map(str.strip,
                config.get('global', 'preferred_masters').split(','))
        assert_equals(pms, ['%s_0' % cid, '%s_1' % cid])

        cluster.preferredMasters([])
        config = cluster._getConfigFile()
        assert_false(config.has_option('global', 'preferred_masters'))


    def testSetQuorum(self):
        cid = self._clusterId
        cluster = self._getCluster()
        for i in range(0,3):
           ni = '%s_%i' % (cid, i)
           cluster.addNode(ni)

        cluster.setQuorum(1)
        config = cluster._getConfigFile()
        assert_equals(config.get("global", 'quorum'), '1')

        cluster.setQuorum(2)
        config = cluster._getConfigFile()
        assert_equals(config.get("global", 'quorum'), '2')

    def testReadOnly(self):
        cluster = self._getCluster()
        cluster.addNode("x1")
        cluster.setReadOnly(True)
        config = cluster._getConfigFile()
        assert_equals(config.get("global","readonly"), 'true')
        cluster.setReadOnly(False)
        config = cluster._getConfigFile()
        ok = config.has_option("global","readonly")
        assert_false(ok)

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
                      {n0: (["127.0.0.1"], 7080),
                       n1: (["127.0.0.1"], 7090),
                       n2: (["127.0.0.1"], 7100)})

    def testGetClientConfigMultipleIPs(self):
        cluster = self._getCluster()
        name = "whatever"
        ips = ["127.0.0.1","192.168.0.1"]
        cluster.addNode(name = name, ip=ips)
        ccfg = cluster.getClientConfig()
        ip_list,port = ccfg[name]
        assert_equals(ip_list,['127.0.0.1', '192.168.0.1'])

    def testWrapper(self):
        cluster = self._getCluster()
        name = "wrappertest"
        wrapper = 'sh'
        cluster.addNode(name = name,
                        wrapper = wrapper)
        cfg = cluster.getNodeConfig(name)
        wl = cfg.get('wrapper')
        assert_equals(wl, wrapper)

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
                       'home': '%s/db/%s/%s' % (X.varDir, cid, n0),
                       'ip': '127.0.0.1',
                       'log_dir': '%s/%s/%s' % (X.logDir, cid,n0),
                       'log_level': 'info',
                       'messaging_port': '7081'})

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
        p0 = '/'.join([X.logDir, cid, n0])
        p1 = '/'.join([X.varDir, "db", cid, n0])
        print p0
        print p1
        assert_true(X.fileExists(p0))
        assert_true(X.fileExists(p1))

    def testRemoveDirs(self):
        cid = self._clusterId
        n0 = '%s_%i' % (cid, 0)
        cluster = self._getCluster()
        cluster.addNode(n0)
        cluster.createDirs(n0)
        cluster.removeDirs(n0)
        p0 = '/'.join([X.logDir, cid, n0])
        p1 = '/'.join([X.varDir, "db", cid, n0])
        assert_false(X.fileExists(p0))
        assert_false(X.fileExists(p1))

    def testAddLocalNode(self):

        cid = self._clusterId
        cluster = self._getCluster()
        for i in range(0,3):
            cluster.addNode("%s_%s" % (cid,i))

        n0 = '%s_0' % cid
        n1 = '%s_1' % cid

        cluster.addLocalNode(n1)

        sn = self.__servernodes()

        cfgPath = '/'.join( [X.cfgDir, "qconfig", "arakoon", cid, "%s_local_nodes" % cid])
        config = X.getConfig( cfgPath )
        assert_equals(Compat.sectionAsDict(config, "global"),
                      {'cluster': n1})

        cluster.addLocalNode(n0)

        config = X.getConfig( cfgPath )
        assert_equals(Compat.sectionAsDict(config, "global"),
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


    def testNameDiffersFromId(self):
        name = self._clusterId
        id_inside = "id_inside"
        cluster = C._getCluster(name)
        cfg_name = cluster._getConfigFileName()
        logging.debug("cfg_name = %s", cfg_name)
        #/opt/qbase3/cfg//qconfig/arakoon/cluster_name
        ok = cfg_name.endswith(name)
        assert_true(ok)
        cluster.addNode('node_0')

        cfg = X.getConfig(cfg_name)
        logging.debug("cfg = %s", X.cfg2str(cfg))
        id0 = cfg.get('global', 'cluster_id')
        assert_equals(id0, name)
        # now set it to id
        cfg.set('global', 'cluster_id',id_inside)
        X.writeConfig(cfg,cfg_name)
        logging.debug('cfg_after = %s', X.cfg2str(cfg))

        cluster = C._getCluster(name)
        #ccfg = cluster.getClientConfig()
        #print ccfg

        client = cluster.getClient()
        ccfg2 = client._config
        logging.debug("ccfg2=%s", ccfg2)
        ccfg_id = ccfg2.getClusterId()

        assert_equals(ccfg_id, id_inside)
