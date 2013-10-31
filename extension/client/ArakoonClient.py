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

try:
    from pymonkey import q
except ImportError:
    from pylabs import q

from arakoon.ArakoonProtocol import ArakoonClientConfig
from arakoon import Nursery

from pyrakoon import compat

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
        
        config = q.config.getInifile(inifile_path)

        if not config.checkSection("global"):
            config.addSection("global")
            config.addParam("global", "cluster_id", clusterId)
            config.addParam("global","cluster", "")

        nodes = self.__getNodes(config)

        if name in nodes:
            raise Exception("There is already a node with name %s configured" % name)

        nodes.append(name)
        config.addSection(name)
        config.addParam(name, "name", name)
        config.addParam(name, "ip", ip) 
        config.addParam(name, "client_port", clientPort)

        config.setParam("global","cluster", ",".join(nodes))

        config.write()

    def removeNode(self, name):
        """
        Remove a node

        @param name the name of the node
        """
        self.__validateName(name)

        config = q.config.getInifile(self._configPath)

        if not config.checkSection("global"):
            raise Exception("No node with name %s configured" % name)

        nodes = self.__getNodes(config)

        if name in nodes:
            config.removeSection(name)

            nodes.remove(name)
            config.setParam("global","cluster", ",".join(nodes))

            config.write()
            return

        raise Exception("No node with name %s configured" % name)

    def getNodes(self):
        """
        Get an object that contains all node information
        @return dict the dict can be used as param for the ArakoonConfig object
        """

        config = q.config.getInifile(self._configPath)
    
        clientconfig = dict()

        if config.checkSection("global"):
            nodes = self.__getNodes(config)

            for name in nodes:
                clientconfig[name] = (config.getValue(name, "ip"),
                                      config.getValue(name, "client_port"))

        return clientconfig

    def generateFromServerConfig(self):
        """
        Generate the client config file from the servers
        """
        clusterId = self._clusterId
        
        try:
            clustersCfg = q.config.getConfig('arakoonclusters')
            clusterExists = clustersCfg.has_key(clusterId)
        except Exception as ex:
            raise ex
            clusterExists = False
            
        if not clusterExists:
            q.errorconditionhandler.raiseError("No server cluster %s is defined." % clusterId)

        serverConfigDir = clustersCfg[clusterId]["path"]
        serverConfigPath =  q.system.fs.joinPaths( serverConfigDir, clusterId)
        serverConfig = q.config.getInifile(serverConfigPath)

        if serverConfig.checkSection("global"):
            nodes = self.__getNodes(serverConfig)

            for name in nodes:
                if name in self.getNodes():
                    self.removeNode(name)

                self.addNode(name,
                             serverConfig.getValue(name, "ip"),
                             serverConfig.getValue(name, "client_port"))

    def __getNodes(self, config):
        if not config.checkSection("global"):
            return []

        nodes = config.getValue("global", "cluster").strip()
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
    def _getClientConfig(clusterName, configName = None):
        """
        Gets an Arakoon client object for an existing cluster
        @type clusterName: string
        @param clusterName: the name of the cluster for which you want to get a client
        @return arakoon client object
        """
        clientConfig = q.config.getInifile("arakoonclients")
        if not clientConfig.checkSection(clusterName):
            q.errorconditionhandler.raiseError("No such client configured for cluster [%s]." % clusterName)
        else:
            node_dict = {}
            clientCfg = ArakoonClient._getConfig(clusterName, configName)
            cfgFile = q.config.getInifile( clientCfg )
            if not cfgFile.checkSection("global") :
                if configName is not None:
                    msg = "Named client '%s' for cluster '%s' does not exist" % (configName, clusterName)
                else :
                    msg = "No client available for cluster '%s'" % clusterName
                q.errorconditionhandler.raiseError(msg )
            clusterParam = cfgFile.getValue("global", "cluster")
            for node in clusterParam.split(",") :
                node = node.strip()
                ips  = cfgFile.getValue(node, "ip") 
                ip_list = ips.split(",")
                port = cfgFile.getValue(node, "client_port") 
                node_dict.update({node: (ip_list,port)})
            cluster_id = cfgFile.getValue("global", "cluster_id")
            config = ArakoonClientConfig(cluster_id, node_dict)
            return config
            
    def getClient(self, clusterId, configName=None):
        config = self._getClientConfig(clusterId, configName)
        return compat.ArakoonClient(config)

    def listClients(self):
        """
        Returns a list with the existing clients.
        """
        config = q.config.getInifile("arakoonclients")
        return config.getSections() 

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

        clientConfig = q.config.getInifile("arakoonclients")
        if not clientConfig.checkSection(clusterId):
            clientConfig.addSection(clusterId)
            cfgDir = q.system.fs.joinPaths(q.dirs.cfgDir, "qconfig", "arakoon", clusterId)
            clientConfig.addParam(clusterId, "path", cfgDir)
            clientConfig.write()
        
        cfgFile = self._getConfig(clusterId, configName)
        return ArakoonClientExtConfig(clusterId, cfgFile)
        
    @staticmethod
    def _getConfig(clusterId, configName):
        clientsCfg = q.config.getConfig( "arakoonclients")
        clusterDir = clientsCfg[clusterId]["path"]
        if configName is None:
            return q.system.fs.joinPaths( clusterDir, "%s_client" % clusterId)
        else:
            return q.system.fs.joinPaths( clusterDir, "%s_client_%s" % (clusterId, configName))


class NurseryClient:
    def getClient(self, cluster_id):
        cfg = ArakoonClient._getClientConfig( cluster_id, None )
        return Nursery.NurseryClient(cfg)
    
    
