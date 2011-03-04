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
import os 

class ArakoonConfig:
    def getCluster(self, clusterId):
        """
        @type clusterId: string
        @return a helper to config that cluster
        """
        return ArakoonCluster(clusterId)

class ArakoonCluster:

    def __init__(self, clusterId):
        self.__validateName(clusterId)
        self._clusterId = clusterId
    
    def _servernodes(self):
        return '%s_servernodes' % self._clusterId

    def __repr__(self):
        return "<ArakoonCluster:%s>" % self._clusterId

    def _getConfigFile(self):
        config = q.config.getInifile(self._clusterId)
        return config
    
    def addNode(self,
                name,
                ip = "127.0.0.1",
                clientPort = 7080,
                messagingPort = 10000,
                logLevel = "info",
                logDir = None,
                home = None,
                tlogDir = None,
                user = None,
                group = None,
                isLocal = True):
        """
        Add a node to the configuration of the supplied cluster

        The function also creates 
        @param name the name of the node, should be unique across the environment
        @param ip the ip this node shoulc be contacted on
        @param clientPort the port the clients should use to contact this node
        @param messagingPort the port the other nodes should use to contact this node
        @param logLevel the loglevel (debug info notice warning error fatal)
        @param logDir the directory used for logging
        @param home the directory used for the nodes data
        @param tlogDir the directory used for tlogs (if none, home will be used)

        """
        self.__validateName(name)
        self.__validateLogLevel(logLevel)


        config = self._getConfigFile()
        if not config.checkSection("global"):
            config.addSection("global")
            config.addParam("global","cluster_id",self._clusterId)
            config.addParam("global","nodes", "")

        nodes = self.__getNodes(config)

        if name in nodes:
            raise Exception("node %s already present" % name )

        nodes.append(name)
        config.addSection(name)
        config.addParam(name, "name", name)
        config.addParam(name, "ip", ip)
        self.__validateInt("clientPort", clientPort)
        config.addParam(name, "client_port", clientPort)
        self.__validateInt("messagingPort", messagingPort)
        config.addParam(name, "messaging_port", messagingPort)
        config.addParam(name, "log_level", logLevel)
        
        if user is not None:
            config.addParam(name, "user", user)
        
        if group is not None:
            config.addParam(name, "group", group)

        if logDir is None:
            logDir = q.system.fs.joinPaths(q.dirs.logDir, self._clusterId, name)
        config.addParam(name, "log_dir", logDir) 

        if home is None:
            home = q.system.fs.joinPaths(q.dirs.varDir, "db", self._clusterId, name)
        config.addParam(name, "home", home)

        if tlogDir:
            config.addParam(name,"tlog_dir", tlogDir)
        
        config.setParam("global","nodes", ",".join(nodes))

        config.write()

    def removeNode(self, name):
        """
        Remove a node from the configuration of the supplied cluster

        @param name the name of the node as configured in the config file
        """
        self.__validateName(name)

        config = self._getConfigFile()
        if not config.checkSection("global"):
            raise Exception("No node with name %s" % name)

        nodes = self.__getNodes(config)
        
        if name in nodes:
            self.removeLocalNode(name)
            config.removeSection(name)
            nodes.remove(name)
            config.setParam("global","nodes", ",".join(nodes))
            config.write()
            return

        raise Exception("No node with name %s" % name)

    def setMasterLease(self, duration=None):
        """
        Set the master lease duration in the supplied cluster

        @param duration The duration of the master lease in seconds
        @param clusterId the id of the arakoon cluster
        """
        section = "global"
        key = "lease_period"
        
        config = q.config.getInifile(clusterId)

        if not config.checkSection( section ):
            raise Exception("Section '%s' not found in config" % section )

        if duration:
            if not isinstance( duration, int ) :
                raise AttributeError( "Invalid value for lease duration (expected int, got: '%s')" % duration)
            if config.checkParam(section, key):
                config.setParam(section, key, duration)
            else:
                config.addParam(section, key, duration)
        else:
            config.removeParam(section, key)

        config.write()
        
    def forceMaster(self, name=None):
        """
        Force a master in the supplied cluster

        @param name the name of the master to force. If None there is no longer a forced master
        @param clusterId: the id of the arakoon cluster
        """
        config = self._getConfigFile()
        if not config.checkSection("global"):
            raise Exception("No node with name %s configured" % name)

        if name:
            nodes = self.__getNodes(config)

            self.__validateName(name)
            if not name in nodes:
                raise Exception("No node with name %s configured in cluster %s" % name)

            if config.checkParam("global", "master"):
                config.setParam("global", "master", name)
            else:
                config.addParam("global", "master", name)
        else:
            config.removeParam("global", "master")

        config.write()

    def setQuorum(self, quorum=None):
        """
        Set the quorum for the supplied cluster

        The quorum dictates on how many nodes need to acknowledge the new value before it becomes accepted.
        The default is (nodes/2)+1

        @param quorum the forced quorom. If None, the default is used 
        """
        config = self._getConfigFile()

        if not config.checkSection("global"):
            config.addSection("global")
            config.addParam("global","nodes", "")

        if quorum:
            try :
                if ( int(quorum) != quorum or
                     quorum < 0 or
                     quorum > len( self.listNodes())) :
                    raise Exception ( "Illegal value for quorum %s" % quorum )
                
            except:
                raise Exception("Illegal value for quorum %s " % quorum)
            
            if config.checkParam("global", "quorum"):
                config.setParam("global", "quorum", int(quorum))
            else:
                config.addParam("global", "quorum", int(quorum))
        else: 
            config.removeParam("global", "quorum")
            
        config.write()


    def getClientConfig(self):
        """
        Get an object that contains all node information in the supplied cluster
        @return dict the dict can be used as param for the ArakoonConfig object
        """
        config = self._getConfigFile()
        clientconfig = dict()

        if config.checkSection("global"):
            nodes = self.__getNodes(config)

            for name in nodes:
                clientconfig[name] = (config.getValue(name, "ip"),
                                      int(config.getValue(name, "client_port")))

        return clientconfig

    def listNodes(self):
        """
        Get a list of all node names in the supplied cluster
        @return list of strings containing the node names
        """
        config = self._getConfigFile()
        return self.__getNodes(config)

    def getNodeConfig(self,name):
        """
        Get the parameters of a node section 

        @param name the name of the node
        @return dict keys and values of the nodes parameters
        """
        self.__validateName(name)

        config = self._getConfigFile()

        nodes = self.__getNodes(config)

        if name in nodes:
            return config.getSectionAsDict(name)
        else:
            raise Exception("No node with name %s configured" % name)


    def createDirs(self, name):
        """
        Create the Directories for a local arakoon node in the supplied cluster

        @param name the name of the node as configured in the config file
        """
        self.__validateName(name)

        config = self._getConfigFile()
        if not config.checkSection("global"):
            raise Exception("No node with name %s configured" % name)

        nodes = self.__getNodes(config)

        if name in nodes:
            home = config.getValue(name, "home")
            q.system.fs.createDir(home)

            if config.checkParam(name, "tlog_dir"):
                tlogDir = config.getValue(name, "tlog_dir")
                q.system.fs.createDir(tlogDir)

            logDir = config.getValue(name, "log_dir")
            q.system.fs.createDir(logDir)

            return

        msg = "No node %s configured" % name
        raise Exception(msg)

    def removeDirs(self, name):
        """
        Remove the Directories for a local arakoon node in the supplied cluster

        @param name the name of the node as configured in the config file
        """
        self.__validateName(name)

        config = self._getConfigFile()
        if not config.checkSection('global'):
            raise Exception("No node with name %s" % name )


        nodes = self.__getNodes(config)

        if name in nodes:
            home = config.getValue(name, "home")
            q.system.fs.removeDirTree(home)

            if config.checkParam(name, "tlog_dir"):
                tlogDir = config.getValue(name, "tlog_dir")
                q.system.fs.removeDirTree(tlogDir)
            
            logDir = config.getValue(name, "log_dir")
            q.system.fs.removeDirTree(logDir)
            return
        
        raise Exception("No node %s" % tpl )



    def addLocalNode(self, name):
        """
        Add a node to the list of nodes that have to be started locally
        from the supplied cluster

        @param name the name of the node as configured in the config file
        """
        self.__validateName(name)

        config = self._getConfigFile()

        if not config.checkSection("global"):
            raise Exception("No node %s" % name )

        nodes = self.__getNodes(config)
        config_name = self._servernodes()
        if name in nodes:
            nodesconfig = q.config.getInifile(config_name)

            if not nodesconfig.checkSection("global"):
                nodesconfig.addSection("global")
                nodesconfig.addParam("global","nodes", "")

            nodes = self.__getNodes(nodesconfig)
            if name in nodes:
                raise Exception("node %s already present" % name)
            nodes.append(name)
            nodesconfig.setParam("global","nodes", ",".join(nodes))

            nodesconfig.write()

            return
        
        raise Exception("No node %s" % name)

    def removeLocalNode(self, name):
        """
        Remove a node from the list of nodes that have to be started locally
        from the supplied cluster

        @param name the name of the node as configured in the config file
        """
        self.__validateName(name)
        config_name = self._servernodes()
        config = q.config.getInifile(config_name)

        if not config.checkSection("global"):
            return

        nodes = self.__getNodes(config)

        if name in nodes:
            nodes.remove(name)
            config.setParam("global","nodes", ",".join(nodes))
            config.write()

    def listLocalNodes(self):
        """
        Get a list of the local nodes in the supplied cluster

        @return list of strings containing the node names
        """
        config_name = self._servernodes()
        config = q.config.getInifile(config_name)

        return self.__getNodes(config)

    def setUp(self, numberOfNodes):
        """
        Sets up a local environment

        @param numberOfNodes the number of nodes in the environment
        @return the dict that can be used as a param for the ArakoonConfig object
        """
        cid = self._clusterId
        for i in range(0, numberOfNodes):
            nodeName = "%s_%i" %(cid, i)
            self.addNode(name = nodeName,
                         clientPort = 7080+i,
                         messagingPort = 10000+i)
            self.addLocalNode(nodeName)
            self.createDirs(nodeName)

        if numberOfNodes > 0:
            self.forceMaster("%s_0" % cid)
        
        config = self._getConfigFile()
        config.addParam( 'global', 'cluster_id', cid)

    def tearDown(self, removeDirs=True ):
        """
        Tears down a local environment

        @param removeDirs remove the log and home dir
        @param cluster the name of the arakoon cluster
        """
        config = self._getConfigFile()

        if not config.checkSection('global'):
            return


        nodes = self.__getNodes(config)
        
        for node in nodes:
            if removeDirs:
                self.removeDirs(node)

            self.removeNode(node)
        
        if self.__getForcedMaster(config):
            self.forceMaster(None)

    def __getForcedMaster(self, config):
        if not config.checkSection("global"):
            return []
        
        if config.checkParam("global", "master"):
            return config.getValue("global", "master").strip()
        else:
            return []

    def __getNodes(self, config):
        if not config.checkSection("global"):
            return []

        nodes = config.getValue("global", "nodes").strip()
        # "".split(",") -> ['']
        if nodes == "":
            return []
        nodes = nodes.split(",")
        nodes = map(lambda x: x.strip(), nodes)

        return nodes

    def __validateInt(self,name, value):
        typ = type(value)
        if not typ == type(1):
            raise Exception("%s=%s (type = %s) but should be an int" % (name, value, typ))
    def __validateName(self, name):
        if name is None:
            raise Exception("A name should be passed. None is not an option")

        if not type(name) == type(str()):
            raise Exception("Name should be of type strinq.config.getInifile(clusterId)g")

        for char in [' ', ',', '#']:
            if char in name:
                raise Exception("name should not contain %s" % char)

    def __validateLogLevel(self, name):
        if not name in ["info", "debug", "notice", "warning", "error", "fatal"]:
            raise Exception("%s is not a valid log level" % name)
