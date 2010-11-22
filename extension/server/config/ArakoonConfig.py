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

class ArakoonConfig:
    """
    Class for auto-mated setup of Arakoon nodes
    """
    def addNode(self, name, ip = "127.0.0.1", client_port = 7080, messaging_port = 10000, log_level = "info", log_dir = None, home = None):
        """
        Add a node to the configuration

        The function also creates 

        @param name the name of the node, should be unique across the environment
        @param ip the ip this node shoulc be contacted on
        @param client_port the port the clients should use to contact this node
        @param messaging_port the port the other nodes should use to contact this node
        @param log_level the loglevel (debug info notice warning error fatal)
        @param log_dir the directory used for logging
        @param home the directory used for the nodes data 
        """
        self.__validateName(name)
        self.__validateLogLevel(log_level)

        config = q.config.getInifile("arakoon")

        if not config.checkSection("global"):
            config.addSection("global")
            config.addParam("global","nodes", "")

        nodes = self.__getNodes(config)

        if name in nodes:
            raise Exception("There is already a node with name %s configured" % name)

        nodes.append(name)
        config.addSection(name)
        config.addParam(name, "name", name)
        config.addParam(name, "ip", ip) 
        config.addParam(name, "client_port", client_port)
        config.addParam(name, "messaging_port", messaging_port)
        config.addParam(name, "log_level", log_level)

        if log_dir is None:
            log_dir = q.system.fs.joinPaths(q.dirs.logDir, "arakoon", name)
        config.addParam(name, "log_dir", log_dir) 

        if home is None:
            home = q.system.fs.joinPaths(q.dirs.varDir, "db", "arakoon", name)
        config.addParam(name, "home", home)

        config.setParam("global","nodes", ",".join(nodes))

        config.write()

    def removeNode(self, name):
        """
        Remove a node from the configuration"

        @param name the name of the node as configured in the config file
        """
        self.__validateName(name)

        config = q.config.getInifile("arakoon")

        if not config.checkSection("global"):
            raise Exception("No node with name %s configured" % name)

        nodes = self.__getNodes(config)

        if name in nodes:
            self.removeLocalNode(name)

            config.removeSection(name)

            nodes.remove(name)
            config.setParam("global","nodes", ",".join(nodes))

            config.write()
            return

        raise Exception("No node with name %s configured" % name)

    def setMasterLease(self, duration=None):
        """
        Force a master

        @param name the name of the master to force. If None there is no longer a forced master
        """
        section = "global"
        key = "lease_expiry"
        
        config = q.config.getInifile("arakoon")

        if not config.checkSection( section ):
            raise Exception("No node with name %s configured" % name)

        if duration is not None:
            if not isinstance( duration, int ) :
                raise AttributeError( "Invalid value for lease duration (expected int, got: '%s')" % duration)
            if config.checkParam(section, key):
                config.setParam(section, key, duration)
            else:
                config.addParam(section, key, name)
        else:
            config.removeParam(section, key)

        config.write()
        
    def forceMaster(self, name=None):
        """
        Force a master

        @param name the name of the master to force. If None there is no longer a forced master
        """
        config = q.config.getInifile("arakoon")

        if not config.checkSection("global"):
            raise Exception("No node with name %s configured" % name)

        if name:
            nodes = self.__getNodes(config)

            self.__validateName(name)
            if not name in nodes:
                raise Exception("No node with name %s configured" % name)

            if config.checkParam("global", "master"):
                config.setParam("global", "master", name)
            else:
                config.addParam("global", "master", name)
        else:
            config.removeParam("global", "master")

        config.write()

    def setQuorum(self, quorum=None):
        """
        Set the quorom

        The quorom dictates on how many nodes need to acknowlegde the new value beforce it becomes accepted.
        The default is (nodes/2)+1

        @param quorm the forced quorom. If None, the default is used 
        """
        config = q.config.getInifile("arakoon")

        if not config.checkSection("global"):
            config.addSection("global")
            config.addParam("global","nodes", "")

        if quorum:
            try :
                if ( int(quorum) != quorum or quorum < 0 or quorum > len( self.listNodes())) :
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


    def getClientConfig(self):
        """
        Get an object that contains all node information

        @return dict the dict can be used as param for the ArakoonConfig object
        """
        config = q.config.getInifile("arakoon")
    
        clientconfig = dict()

        if config.checkSection("global"):
            nodes = self.__getNodes(config)

            for name in nodes:
                clientconfig[name] = (config.getValue(name, "ip"), int(config.getValue(name, "client_port")))

        return clientconfig

    def listNodes(self):
        """
        Get a list of all node names

        @return list of strings containing the node names
        """
        config = q.config.getInifile("arakoon")

        return self.__getNodes(config)

    def getNodeConfig(self, name):
        """
        Get the parameters of a node section

        @param name the name of the node

        @return dict keys and values of the nodes parameters
        """
        self.__validateName(name)

        config = q.config.getInifile("arakoon")

        nodes = self.__getNodes(config)

        if name in nodes:
            return config.getSectionAsDict(name)
        else:
            raise Exception("No node with name %s configured" % name)


    def createDirs(self, name):
        """
        Create the Directories for a local arakoon node

        @param name the name of the node as configured in the config file
        """
        self.__validateName(name)

        config = q.config.getInifile("arakoon")

        if not config.checkSection("global"):
            raise Exception("No node with name %s configured" % name)

        nodes = self.__getNodes(config)

        if name in nodes:
            home = config.getValue(name, "home")
            q.system.fs.createDir(home)

            log_dir = config.getValue(name, "log_dir")
            q.system.fs.createDir(log_dir)

            return

        raise Exception("No node with name %s configured" % name)

    def removeDirs(self, name):
        """
        Remove the Directories for a local arakoon node

        @param name the name of the node as configured in the config file
        """
        self.__validateName(name)

        config = q.config.getInifile('arakoon')

        if not config.checkSection('global'):
            raise Exception("No node with name %s configured" % name)

        nodes = self.__getNodes(config)

        if name in nodes:
            home = config.getValue(name, "home")
            q.system.fs.removeDirTree(home)

            log_dir = config.getValue(name, "log_dir")
            q.system.fs.removeDirTree(log_dir)
            return

        raise Exception("No node with name %s configured" % name)


    def addLocalNode(self, name):
        """
        Add a node to the list of nodes that have to be started locally

        @param name the name of the node as configured in the config file
        """
        self.__validateName(name)

        config = q.config.getInifile("arakoon")

        if not config.checkSection("global"):
            raise Exception("No node with name %s configured" % name)

        nodes = self.__getNodes(config)

        if name in nodes:
            nodesconfig = q.config.getInifile("arakoonservernodes")

            if not nodesconfig.checkSection("global"):
                nodesconfig.addSection("global")
                nodesconfig.addParam("global","nodes", "")

            nodes = self.__getNodes(nodesconfig)
            if name in nodes:
                raise Exception("There is already a node with name %s configured" % name)
            nodes.append(name)
            nodesconfig.setParam("global","nodes", ",".join(nodes))

            nodesconfig.write()

            return

        raise Exception("No node with name %s configured" % name)

    def removeLocalNode(self, name):
        """
        Remove a node from the list of nodes that have to be started locally

        @param name the name of the node as configured in the config file
        """
        self.__validateName(name)

        config = q.config.getInifile("arakoonservernodes")

        if not config.checkSection("global"):
            return

        nodes = self.__getNodes(config)

        if name in nodes:
            nodes.remove(name)
            config.setParam("global","nodes", ",".join(nodes))
            config.write()

    def listLocalNodes(self):
        """
        Get a list of the local nodes

        @return list of strings containing the node names
        """
        config = q.config.getInifile("arakoonservernodes")

        return self.__getNodes(config)

    def setUp(self, number_of_nodes):
        """
        Sets up a local environment

        @param number_of_nodes the number of nodes in the environment

        @return the dict that can be used as a param for the ArakoonConfig object
        """
        for i in range(0, number_of_nodes):
            nodeName = "arakoon_%s" %i
            self.addNode(name=nodeName, client_port=7080+i,messaging_port=10000+i)
            self.addLocalNode(nodeName)
            self.createDirs(nodeName)

        if number_of_nodes > 0:
            self.forceMaster("arakoon_0")

    def tearDown(self, removeDirs=True):
        """
        Tears down a local environment

        @param removeDirs remove the log and home dir
        """
        config = q.config.getInifile('arakoon')

        if not config.checkSection('global'):
            return


        nodes = self.__getNodes(config)

        for node in nodes:
            if removeDirs:
                self.removeDirs(node)

            self.removeNode(node)
        
        if self.__getForcedMaster(config):
            self.forceMaster()

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
