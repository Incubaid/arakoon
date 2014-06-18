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

from Compat import X


def make_client(config):
    client = X.arakoon_client.ArakoonClient(config)
    return client

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

        if isinstance(ip, basestring):
            ip = [ip]

        clusterId = self._clusterId
        inifile_path = self._configPath

        config = X.getConfig(inifile_path)

        if not config.has_section("global"):
            config.add_section("global")
            config.set("global", "cluster_id", clusterId)
            config.set("global","cluster", "")

        nodes = self.__getNodes(config)

        if name in nodes:
            raise Exception("There is already a node with name %s configured" % name)

        nodes.append(name)
        config.add_section(name)
        config.set(name, "ip", ', '.join(ip))
        config.set(name, "client_port", clientPort)

        config.set("global","cluster", ",".join(nodes))

        X.writeConfig(config,inifile_path)

    def removeNode(self, name):
        """
        Remove a node

        @param name the name of the node
        """
        self.__validateName(name)

        config = q.config.getInifile(self._configPath)

        if not config.has_section("global"):
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

        config = X.getConfig(self._configPath)

        clientconfig = {}

        if config.has_section("global"):
            nodes = self.__getNodes(config)

            for name in nodes:
                ips = config.get(name, 'ip')
                ip_list = map(lambda x: x.strip(), ips.split(','))
                clientconfig[name] = (ip_list,
                                      config.get(name, "client_port"))

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
        serverConfigPath = '/'.join([serverConfigDir, clusterId])
        serverConfig = X.getConfig(serverConfigPath)
        if serverConfig.has_section("global"):
            nodes = self.__getNodes(serverConfig)

            for name in nodes:
                if name in self.getNodes():
                    self.removeNode(name)

                ips = serverConfig.get(name, 'ip')
                ip_list = map(lambda x: x.strip(), ips.split(','))
                self.addNode(name,
                             ip_list,
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
    def _getClientConfig(clusterName, configName = None):
        """
        Gets an Arakoon client object for an existing cluster
        @type clusterName: string
        @param clusterName: the name of the cluster for which you want to get a client
        @return arakoon client object
        """
        clientConfig = X.getConfig('/'.join ([X.cfgDir,"arakoonclients"]))
        if not clientConfig.has_section(clusterName):
            X.raiseError("No such client configured for cluster [%s]" % clusterName)
        else:
            node_dict = {}
            clientCfg = ArakoonClient._getConfig(clusterName, configName)
            cfgFile = X.getConfig( clientCfg )
            if not cfgFile.has_section("global") :
                if configName is not None:
                    msg = "Named client '%s' for cluster '%s' does not exist" % (configName, clusterName)
                else :
                    msg = "No client available for cluster '%s'" % clusterName
                X.raiseError(msg )
            clusterParam = cfgFile.get("global", "cluster")
            for node in clusterParam.split(",") :
                node = node.strip()
                ips = cfgFile.get(node, "ip")
                ip_list = map(lambda x: x.strip(), ips.split(','))
                port = cfgFile.get(node, "client_port")
                ip_port = (ip_list, port)
                node_dict.update({node: ip_port})
            clusterId = cfgFile.get('global', 'cluster_id')
            config = X.arakoon_client.ArakoonClientConfig(clusterId, node_dict)
            return config

    def getClient(self, clusterName, configName=None):
        config = self._getClientConfig(clusterName, configName)
        client = make_client(config)
        return client

    def listClients(self):
        """
        Returns a list with the existing clients.
        """
        config = X.getConfig("arakoonclients")
        return config.sections()

    def getClientConfig (self, clusterName, configName = None):
        """
        Adds an Arakoon client to the configuration.
        @type clusterName: string
        @param clusterName: the name of the cluster for which you want to add a client
        @type configName: optional string
        @param configName: the name of the client configuration for this cluster
        """
        fn = '/'.join([X.cfgDir, 'arakoonclients'])
        p = X.getConfig(fn)
        if not p.has_section(clusterName):
            p.add_section(clusterName)
            cfgDir = '/'.join([X.cfgDir, "qconfig", "arakoon", clusterName])
            p.set(clusterName, "path", cfgDir)
            X.writeConfig(p, fn)

        cfgFile = self._getConfig(clusterName, configName)
        return ArakoonClientExtConfig(clusterName, cfgFile)

    @staticmethod
    def _getConfig(clusterName, configName):
        fn = '/'.join([X.cfgDir, 'arakoonclients'])
        p = X.getConfig(fn)
        clusterDir = p.get(clusterName, "path")
        last = None
        if configName is None:
            last = "%s_client" % clusterName
        else:
            last = "%s_client_%s" % (clusterName, configName)

        return '/'.join([clusterDir, last])


class NurseryClient:
    def getClient(self, clusterName):
        cfg = ArakoonClient._getClientConfig( clusterName, None )
        return Nursery.NurseryClient(cfg)
