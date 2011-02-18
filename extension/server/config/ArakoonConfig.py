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
    """
    Class for automated setup of Arakoon nodes
    """
    def __servernodes(self, clusterId):
        return '%s_servernodes' % clusterId
    
    def addNode(self, name,
                clusterId,
                ip = "127.0.0.1",
                client_port = 7080,
                messaging_port = 10000,
                log_level = "info",
                log_dir = None,
                home = None,
                tlog_dir = None,

                user = None,
                group = None ):
        """
        Add a node to the configuration of the supplied cluster

        The function also creates 
        @param clusterId the arakoon cluster name
        @param name the name of the node, should be unique across the environment
        @param ip the ip this node shoulc be contacted on
        @param client_port the port the clients should use to contact this node
        @param messaging_port the port the other nodes should use to contact this node
        @param log_level the loglevel (debug info notice warning error fatal)
        @param log_dir the directory used for logging
        @param home the directory used for the nodes data
        @param tlog_dir the directory used for tlogs (if none, home will be used)

        """
        self.__validateName(clusterId)
        self.__validateName(name)

        self.__validateLogLevel(log_level)

        config = q.config.getInifile(clusterId)

        if not config.checkSection("global"):
            config.addSection("global")
            config.addParam("global","cluster_id",clusterId)
            config.addParam("global","nodes", "")

        nodes = self.__getNodes(config)

        if name in nodes:
            raise Exception("There is already a node with name %s configured in cluster %s" % (name, clusterId) )

        nodes.append(name)
        config.addSection(name)
        config.addParam(name, "name", name)
        config.addParam(name, "ip", ip) 
        config.addParam(name, "client_port", client_port)
        config.addParam(name, "messaging_port", messaging_port)
        config.addParam(name, "log_level", log_level)
        
        if user is not None:
            config.addParam(name, "user", user)
        
        if group is not None:
            config.addParam(name, "group", group)

        if log_dir is None:
            log_dir = q.system.fs.joinPaths(q.dirs.logDir, "arakoon", name)
        config.addParam(name, "log_dir", log_dir) 

        if home is None:
            home = q.system.fs.joinPaths(q.dirs.varDir, "db", "arakoon", name)
        config.addParam(name, "home", home)

        if tlog_dir:
            config.addParam(name,"tlog_dir", tlog_dir)
        
        config.setParam("global","nodes", ",".join(nodes))

        config.write()

    def removeNode(self, clusterId, name):
        """
        Remove a node from the configuration of the supplied cluster

        @param name the name of the node as configured in the config file
        @param clusterId: the name of the arakoon cluster
        """
        self.__validateName(name)

        config = q.config.getInifile(clusterId)

        if not config.checkSection("global"):
            raise Exception("No node with name %s configured in cluster %s" % (name, cluster) )

        nodes = self.__getNodes(config)

        if name in nodes:
            self.removeLocalNode(name, clusterId)

            config.removeSection(name)

            nodes.remove(name)
            config.setParam("global","nodes", ",".join(nodes))

            config.write()
            return

        raise Exception("No node with name %s configured in cluster %s" % (name, cluster) )

    def setMasterLease(self, clusterId, duration=None):
        """
        Set the master lease duration in the supplied cluster

        @param duration The duration of the master lease in seconds
        @param clusterId the id of the arakoon cluster
        """
        section = "global"
        key = "lease_expiry"
        
        config = q.config.getInifile(clusterId)

        if not config.checkSection( section ):
            raise Exception("Section '%s' not found in config for cluster %s" % (section,cluster) )

        if duration is not None:
            if not isinstance( duration, int ) :
                raise AttributeError( "Invalid value for lease duration (expected int, got: '%s')" % duration)
            if config.checkParam(section, key):
                config.setParam(section, key, duration)
            else:
                config.addParam(section, key, duration)
        else:
            config.removeParam(section, key)

        config.write()
        
    def forceMaster(self, clusterId, name=None):
        """
        Force a master in the supplied cluster

        @param name the name of the master to force. If None there is no longer a forced master
        @param clusterId: the id of the arakoon cluster
        """
        config = q.config.getInifile(clusterId)

        if not config.checkSection("global"):
            raise Exception("No node with name %s configured in cluster %s" % (name,clusterId) )

        if name:
            nodes = self.__getNodes(config)

            self.__validateName(name)
            if not name in nodes:
                raise Exception("No node with name %s configured in cluster %s" % (name,clusterId) )

            if config.checkParam("global", "master"):
                config.setParam("global", "master", name)
            else:
                config.addParam("global", "master", name)
        else:
            config.removeParam("global", "master")

        config.write()

    def setQuorum(self, clusterId, quorum=None):
        """
        Set the quorum for the supplied cluster

        The quorum dictates on how many nodes need to acknowledge the new value before it becomes accepted.
        The default is (nodes/2)+1

        @param quorum the forced quorom. If None, the default is used 
        @param cluster the name of the arakoon cluster
        """
        config = q.config.getInifile(cluster)

        if not config.checkSection("global"):
            config.addSection("global")
            config.addParam("global","nodes", "")

        if quorum:
            try :
                if ( int(quorum) != quorum or quorum < 0 or quorum > len( self.listNodes(cluster = clusterId))) :
                    raise Exception ( "Illegal value for quorum" )
                
            except:
                raise Exception( "Illegal value for quorum" )
            
            if config.checkParam("global", "quorum"):
                config.setParam("global", "quorum", int(quorum))
            else:
                config.addParam("global", "quorum", int(quorum))
        else: 
            config.removeParam("global", "quorum")
            
        config.write()


    def getClientConfig(self, clusterId):
        """
        Get an object that contains all node information in the supplied cluster

        @param clusterId the name of the arakoon cluster
        @return dict the dict can be used as param for the ArakoonConfig object
        """
        config = q.config.getInifile(clusterId)
    
        clientconfig = dict()

        if config.checkSection("global"):
            nodes = self.__getNodes(config)

            for name in nodes:
                clientconfig[name] = (config.getValue(name, "ip"), int(config.getValue(name, "client_port")))

        return clientconfig

    def listNodes(self, clusterId):
        """
        Get a list of all node names in the supplied cluster

        @param cluster the name of the arakoon cluster
        @return list of strings containing the node names
        """
        config = q.config.getInifile(clusterId)

        return self.__getNodes(config)

    def getNodeConfig(self, clusterId, name):
        """
        Get the parameters of a node section 

        @param name the name of the node
        @param cluster the name of the arakoon cluster

        @return dict keys and values of the nodes parameters
        """
        self.__validateName(name)

        config = q.config.getInifile(clusterId)

        nodes = self.__getNodes(config)

        if name in nodes:
            return config.getSectionAsDict(name)
        else:
            raise Exception("No node with name %s configured in cluster %s" % (name,clusterId) )


    def createDirs(self, clusterId, name):
        """
        Create the Directories for a local arakoon node in the supplied cluster

        @param name the name of the node as configured in the config file
        @param cluster the name of the arakoon cluster
        """
        self.__validateName(name)

        config = q.config.getInifile(clusterId)

        if not config.checkSection("global"):
            raise Exception("No node with name %s configured in cluster %s" % (name,clusterId) )

        nodes = self.__getNodes(config)

        if name in nodes:
            home = config.getValue(name, "home")
            q.system.fs.createDir(home)

            if config.checkParam(name, "tlog_dir"):
                tlog_dir = config.getValue(name, "tlog_dir")
                q.system.fs.createDir(tlog_dir)

            log_dir = config.getValue(name, "log_dir")
            q.system.fs.createDir(log_dir)

            return

        raise Exception("No node with name %s configured in cluster %s" % (name,cluster) )

    def removeDirs(self, clusterId, name):
        """
        Remove the Directories for a local arakoon node in the supplied cluster

        @param name the name of the node as configured in the config file
        @param cluster the name of the arakoon cluster
        """
        self.__validateName(name)

        config = q.config.getInifile(clusterId)

        if not config.checkSection('global'):
            raise Exception("No node with name %s configured in cluster %s" % (name,clusterId) )


        nodes = self.__getNodes(config)

        if name in nodes:
            home = config.getValue(name, "home")
            q.system.fs.removeDirTree(home)

            if config.checkParam(name, "tlog_dir"):
                tlog_dir = config.getValue(name, "tlog_dir")
                q.system.fs.removeDirTree(tlog_dir)
            
            log_dir = config.getValue(name, "log_dir")
            q.system.fs.removeDirTree(log_dir)
            return
        
        raise Exception("No node with name %s configured in cluster %s" % (name,cluster) )



    def addLocalNode(self, clusterId, name):
        """
        Add a node to the list of nodes that have to be started locally
        from the supplied cluster

        @param name the name of the node as configured in the config file
        @param cluster the name of the arakoon cluster
        """
        self.__validateName(name)

        config = q.config.getInifile(clusterId)

        if not config.checkSection("global"):
            raise Exception("No node with name %s configured in cluster %s" % (name,clusterId) )

        nodes = self.__getNodes(config)
        config_name = self.__servernodes(clusterId)
        if name in nodes:
            nodesconfig = q.config.getInifile(config_name)

            if not nodesconfig.checkSection("global"):
                nodesconfig.addSection("global")
                nodesconfig.addParam("global","nodes", "")

            nodes = self.__getNodes(nodesconfig)
            if name in nodes:
                raise Exception("There is already a node with name %s configured in cluster %s" % (name, clusterId) )
            nodes.append(name)
            nodesconfig.setParam("global","nodes", ",".join(nodes))

            nodesconfig.write()

            return
        
        raise Exception("No node with name %s configured in cluster %s" % (name,clusterId) )

    def removeLocalNode(self, clusterId, name):
        """
        Remove a node from the list of nodes that have to be started locally
        from the supplied cluster

        @param name the name of the node as configured in the config file
        @param cluster the name of the arakoon cluster
        """
        self.__validateName(name)
        config_name = self.__servernodes(clusterId)
        config = q.config.getInifile(config_name)

        if not config.checkSection("global"):
            return

        nodes = self.__getNodes(config)

        if name in nodes:
            nodes.remove(name)
            config.setParam("global","nodes", ",".join(nodes))
            config.write()

    def listLocalNodes(self, clusterId):
        """
        Get a list of the local nodes in the supplied cluster

        @param cluster the name of the arakoon cluster
        @return list of strings containing the node names
        """
        config_name = self.__servernodes(clusterId)
        config = q.config.getInifile(config_name)

        return self.__getNodes(config)

    def setUp(self, clusterId, number_of_nodes ):
        """
        Sets up a local environment

        @param number_of_nodes the number of nodes in the environment
        @param clusterId: the id of the arakoon cluster

        @return the dict that can be used as a param for the ArakoonConfig object
        """
        
        for i in range(0, number_of_nodes):
            nodeName = "%s_%i" %(clusterId, i)
            self.addNode(clusterId = clusterId,
                         name=nodeName,
                         client_port=7080+i,
                         messaging_port=10000+i)
            self.addLocalNode(clusterId,nodeName)
            self.createDirs(clusterId, nodeName)

        if number_of_nodes > 0:
            self.forceMaster(clusterId, "%s_0" % clusterId)
        
        config = q.config.getInifile(clusterId)
        config.addParam( 'global', 'cluster_id', clusterId)

    def tearDown(self, clusterId, removeDirs=True ):
        """
        Tears down a local environment

        @param removeDirs remove the log and home dir
        @param cluster the name of the arakoon cluster
        """
        config = q.config.getInifile(clusterId)

        if not config.checkSection('global'):
            return


        nodes = self.__getNodes(config)

        for node in nodes:
            if removeDirs:
                self.removeDirs(clusterId, node)

            self.removeNode(clusterId, node)
        
        if self.__getForcedMaster(config):
            self.forceMaster(clusterId)

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

    def __validateName(self, name):
        if name is None:
            raise Exception("A name should be passed. None is not an option")

        if not type(name) == type(str()):
            raise Exception("Name should be of type string")

        for char in [' ', ',', '#']:
            if char in name:
                raise Exception("name should not contain %s" % char)

    def __validateLogLevel(self, name):
        if not name in ["info", "debug", "notice", "warning", "error", "fatal"]:
            raise Exception("%s is not a valid log level" % name)
