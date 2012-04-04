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

from Compat import X

from arakoon import Arakoon 
from arakoon.ArakoonProtocol import ArakoonClientConfig
from arakoon import Nursery

class ArakoonClientExtConfig:
    """
    Configuration of Arakoon nodes
    """
    def __init__(self, clusterId, configPath):
        self._clusterId = clusterId
        self._configPath = configPath

    def addNode(self, name, ip, clientPort):
        """
        Add a node to the client configuration

        @param name: the name of the node
        @param ip: the ip  the node
        @param clientPort: the port of the node
        """
        self.__validateName(name)
        
        clusterId = self._clusterId
        inifile_path = self._configPath
        print "inifilepath:" , inifile_path
        
        p = X.getConfig(inifile_path)

        if not p.has_section("global"):
            p.add_section("global")
            p.set("global", "cluster_id", clusterId)
            p.set("global","cluster", "")

        nodes = self.__getNodes(p)

        if name in nodes:
            raise Exception("There is already a node with name %s configured" % name)

        nodes.append(name)
        p.add_section(name)
        p.set(name, "name", name)
        p.set(name, "ip", ip) 
        p.set(name, "client_port", clientPort)

        p.set("global","cluster", ",".join(nodes))
        X.writeConfig(p,inifile_path)

    def removeNode(self, name):
        """
        Remove a node

        @param name the name of the node
        """
        self.__validateName(name)
        
        config = X.getConfig(self._configPath)

        if not config.has_section("global"):
            raise Exception("No node with name %s configured" % name)

        nodes = self.__getNodes(config)

        if name in nodes:
            config.remove_section(name)

            nodes.remove(name)
            config.set("global","cluster", ",".join(nodes))
            X.writeConfig(config,self._configPath)
            return

        raise Exception("No node with name %s configured" % name)

    def getNodes(self):
        """
        Get an object that contains all node information
        @return dict the dict can be used as param for the ArakoonConfig object
        """
        config = X.getConfig(self._configPath)
        clientconfig = dict()

        if config.has_section("global"):
            nodes = self.__getNodes(config)

            for name in nodes:
                ip = config.get(name, "ip")
                client_port = config.get(name, "client_port")
                clientconfig[name] = (ip,client_port)
                                      

        return clientconfig

    def generateFromServerConfig(self):
        """
        Generate the client config file from the servers
        """
        clusterId = self._clusterId
        fn = '/'.join([X.cfgDir, 'arakoonclusters'])
        p = X.getConfig(fn)
        clusterExists = p.has_section(clusterId)
            
        if not clusterExists:
            X.raiseError("No server cluster '%s' is defined." % clusterId)

        serverConfigDir = p.get(clusterId, "path")
        serverConfigPath =  '/'.join([serverConfigDir, clusterId])
        serverConfig = X.getConfig(serverConfigPath)
        if serverConfig.has_section("global"):
            nodes = self.__getNodes(serverConfig)

            for name in nodes:
                if name in self.getNodes():
                    self.removeNode(name)

                self.addNode(name,
                             serverConfig.get(name, "ip"),
                             serverConfig.get(name, "client_port"))
        

    def __getNodes(self, config):
        if not config.has_section("global"):
            return []

        nodes = config.get("global", "cluster").strip()
        # "".split(",") -> ['']
        if nodes == "":
            return []
        nodes = nodes.split(",")
        nodes = map(lambda x: x.strip(), nodes)

        return nodes


    def __validateName(self, name):
        if name is None:
            raise Exception("A name should be passed. None is not an option")

        if not type(name) == type(str()):
            raise Exception("Name should be of type string")

        for char in [' ', ',', '#']:
            if char in name:
                raise Exception("name should not contain %s" % char)
            
class ArakoonClient:
    """
    Arakoon client management
    """
    @staticmethod
    def _getClientConfig(clusterId, configName = None):
        """
        Gets an Arakoon client object for an existing cluster
        @type clusterId: string
        @param clusterId: the name of the cluster for which you want to get a client
        @return arakoon client object
        """
        clientConfig = X.getConfig('/'.join ([X.cfgDir,"arakoonclients"]))
        if not clientConfig.has_section(clusterId):
            X.raiseError("No such client configured for cluster [%s]" % clusterId)
        else:
            node_dict = {}
            clientCfg = ArakoonClient._getConfig(clusterId, configName)
            cfgFile = X.getConfig( clientCfg )
            if not cfgFile.has_section("global") :
                if configName is not None:
                    msg = "Named client '%s' for cluster '%s' does not exist" % (configName, clusterId)
                else :
                    msg = "No client available for cluster '%s'" % clusterId 
                X.raiseError(msg )
            clusterParam = cfgFile.get("global", "cluster")
            for node in clusterParam.split(",") :
                node = node.strip()
                ip  = cfgFile.get(node, "ip") 
                port = cfgFile.get(node, "client_port") 
                ip_port = (ip, port)
                node_dict.update({node: ip_port})
            config = ArakoonClientConfig(clusterId, node_dict)
            return config
            
    def getClient(self, clusterId, configName=None):
        config = self._getClientConfig(clusterId, configName)
        return Arakoon.ArakoonClient(config)

    def listClients(self):
        """
        Returns a list with the existing clients.
        """
        config = X.getConfig("arakoonclients")
        return config.sections() 

    def getClientConfig (self, clusterId, configName = None):
        """
        Adds an Arakoon client to the configuration.
        @type clusterId: string
        @param clusterId: the name of the cluster for which you want to add a client
        @type configName: optional string
        @param configName: the name of the client configuration for this cluster
        """
        
        #clusterConfig = q.config.getInifile("arakoonclusters")
        #if not clusterConfig.checkSection(clusterId):
        #    q.errorconditionhandler.raiseError("No cluster with that name is configured.")
        fn = '/'.join([X.cfgDir, 'arakoonclients'])
        p = X.getConfig(fn)
        if not p.has_section(clusterId):
            p.add_section(clusterId)
            cfgDir = '/'.join([X.cfgDir, "qconfig", "arakoon", clusterId])
            p.set(clusterId, "path", cfgDir)
            X.writeConfig(p, fn)
        
        cfgFile = self._getConfig(clusterId, configName)
        return ArakoonClientExtConfig(clusterId, cfgFile)
        
    @staticmethod
    def _getConfig(clusterId, configName):
        fn = '/'.join([X.cfgDir, 'arakoonclients'])
        p = X.getConfig(fn)
        clusterDir = p.get(clusterId, "path")
        last = None
        if configName is None:
            last = "%s_client" % clusterId
        else:
            last = "%s_client_%s" % (clusterId, configName)

        return '/'.join([clusterDir, last])


class NurseryClient:
    def getClient(self, cluster_id):
        cfg = ArakoonClient._getClientConfig( cluster_id, None )
        return Nursery.NurseryClient(cfg)
    
    
