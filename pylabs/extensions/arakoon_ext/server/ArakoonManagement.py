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

import ArakoonRemoteControl
from arakoon_ext.client import ArakoonClient

import itertools
import time
import string
import subprocess
import os.path

from arakoon.ArakoonExceptions import ArakoonNodeNotLocal

def which_arakoon():
    path = '%s/arakoon/bin/arakoon' % (X.appDir)
    print "testing path:", path
    if X.fileExists(path):
        return path
    else:
        return "arakoon"

class ArakoonManagement:
    def getCluster(self, clusterId):
        """
        @type clusterId: string
        @return a helper to config that cluster
        """
        return ArakoonCluster(clusterId)

    def listClusters(self):
        """
        Returns a list with the existing clusters.
        """
        config = q.config.getInifile("arakoonclusters")
        return config.getSections()

    def start(self):
        """
        Starts all clusters.
        """
        [clus.start() for clus in [self.getCluster(cluster) for cluster in self.listClusters()]]

    def stop(self):
        """
        Stops all clusters.
        """
        [clus.stop() for clus in [self.getCluster(cluster) for cluster in self.listClusters()]]

    def restart(self):
        """
        Restarts all clusters.
        """
        self.stop()
        self.start()
       

class ArakoonCluster:

    def __init__(self, clusterId):
        self.__validateName(clusterId)
        self._clusterId = clusterId
        self._binary = which_arakoon()
        self._arakoonDir = '%s/%s' % (X.cfgDir, 'arakoon')
        self._clustersFNH = '%s/%s' % (X.cfgDir, 'arakoonclusters')
        p = X.getConfig(self._clustersFNH)

        if not p.has_section(self._clusterId):   
            X.logging.debug("No section %s: adding", self._clusterId)
            clusterPath = '/'.join([X.cfgDir,"qconfig", "arakoon", clusterId])
            p.add_section(self._clusterId)
            p.set(self._clusterId, "path", clusterPath)
            if not X.fileExists(self._arakoonDir):
                X.createDir(self._arakoonDir)

            if not X.fileExists(clusterPath):
                X.createDir(clusterPath)
            X.writeConfig(p, self._clustersFNH)

        self._clusterPath = p.get( self._clusterId, "path", False)
        
        
    def _servernodes(self):
        return '%s_local_nodes' % self._clusterId

    def __repr__(self):
        return "<ArakoonCluster:%s (%s) >" % (self._clusterId, self._arakoonDir)

    def _getConfigFileName(self):
        #clusterPath = '/'.join([X.cfgDir,"qconfig", "arakoon", self._clusterId])
        p = X.getConfig(self._clustersFNH)
        if not p.has_section(self._clusterId):
            raise Exception()
        cfgDir = p.get( self._clusterId, "path",False)
        cfgFile = '/'.join ([cfgDir, self._clusterId])
        return cfgFile

    def _getConfigFile(self):
        h = self._getConfigFileName()
        p = X.getConfig(h)
        return p
        
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
                isLearner = False,
                targets = None,
                isLocal = True):
        """
        Add a node to the configuration of the supplied cluster

        The function also creates 
        @param name  the name of the node, should be unique across the environment
        @param ip   the ip this node shoulc be contacted on
        @param clientPort   the port the clients should use to contact this node
        @param messagingPort  the port the other nodes should use to contact this node
        @param logLevel   the loglevel (debug info notice warning error fatal)
        @param logDir   the directory used for logging
        @param home   the directory used for the nodes data
        @param tlogDir   the directory used for tlogs (if none, home will be used)
        @param isLearner   whether this node is a learner node or not
        @param targets   for a learner node the targets (string list) it learns from
        """
        self.__validateName(name)
        self.__validateLogLevel(logLevel)


        config = self._getConfigFile()
        nodes = self.__getNodes(config)

        if name in nodes:
            raise Exception("node %s already present" % name )
        if not isLearner:
            nodes.append(name)
        
        config.add_section(name)
        config.set(name, "name", name)
        config.set(name, "ip", ip)
        self.__validateInt("clientPort", clientPort)
        config.set(name, "client_port", clientPort)
        self.__validateInt("messagingPort", messagingPort)
        config.set(name, "messaging_port", messagingPort)
        config.set(name, "log_level", logLevel)
        
        if user is not None:
            config.set(name, "user", user)
        
        if group is not None:
            config.set(name, "group", group)

        if logDir is None:
            logDir = '/'.join([X.logDir, self._clusterId, name])
        config.set(name, "log_dir", logDir) 

        if home is None:
            home = '/'.join([X.varDir, "db", self._clusterId, name])
        config.set(name, "home", home)

        if tlogDir:
            config.set(name,"tlog_dir", tlogDir)

        if isLearner:
            config.set(name, "learner", "true")
            if targets is None:
                targets = self.listNodes()
            config.set(name, "targets", string.join(targets,","))

        if not config.has_section("global") :
            config.add_section("global")        
            config.set("global", "cluster_id", self._clusterId)
        config.set("global","cluster", ",".join(nodes))

        h = self._getConfigFileName()

        X.writeConfig(config,h)

    def removeNode(self, name):
        """
        Remove a node from the configuration of the supplied cluster

        @param name the name of the node as configured in the config file
        """
        self.__validateName(name)

        config = self._getConfigFile()
        nodes = self.__getNodes(config)
        
        if name in nodes:
            self.removeLocalNode(name)
            config.remove_section(name)
            nodes.remove(name)
            config.set("global","cluster", ",".join(nodes))
            X.writeConfig(config, self._getConfigFileName())
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
        
        config = self._getConfigFile()

        if not config.has_section(section):
            raise Exception("Section '%s' not found in config" % section )

        if duration:
            if not isinstance( duration, int ) :
                raise AttributeError( "Invalid value for lease duration (expected int, got: '%s')" % duration)
            config.set(section, key, duration)
        else:
            config.remove_option(section, key)

        with open(self._getConfigFileName(),'w') as pf:
            config.write(pf)
        
    def forceMaster(self, name=None, preferred = False):
        """
        Force a master in the supplied cluster

        @param name the name of the master to force. If None there is no longer a forced master
        @param preferred: Set given node to be preferred master
        @type preferred: `bool`
        """
        config = self._getConfigFile()
        g = 'global'
        pm = 'preferred_master'
        m = 'master'
        def _set(s,a,v):
            config.set(s,a,v)
        if name:
            nodes = self.__getNodes(config)

            self.__validateName(name)
            if not name in nodes:
                raise Exception("No node with name %s configured in cluster %s" % (name,self._clusterId) )
            _set(g,m,name)
            if preferred:
                _set(g,pm,'true')
        else:
            config.remove_option(g, m)
            if config.has_option(g, pm):
                config.remove_option(g, pm)
        X.writeConfig(config, self._getConfigFileName())

    def setLogLevel(self, level, nodes=None):
        if nodes is None:
            nodes = self.listNodes()
        else:
            for n in nodes :
                self.__validateName( n )
        self.__validateLogLevel( level )
        config = self._getConfigFile()
        
        for n in nodes:
            config.set(n, "log_level", level )
        
        X.writeConfig(config, self._getConfigFileName())

    def setReadOnly(self, flag = True):
        config = self._getConfigFile()
        if flag and len(self.listNodes()) <> 1:
            raise Exception("only for clusters of size 1")

        g = "global"
        p = "readonly"
        if config.has_option(g,p):
            config.remove_option(g, p)
        if flag :
            config.set(g, p, "true")

        X.writeConfig(config, self._getConfigFileName())
        
    def setQuorum(self, quorum=None):
        """
        Set the quorum for the supplied cluster

        The quorum dictates on how many nodes need to acknowledge the new value before it becomes accepted.
        The default is (nodes/2)+1

        @param quorum the forced quorum. If None, the default is used 
        """
        fn = self._getConfigFileName()
        config = X.getConfig(fn)
        if quorum:
            try :
                if ( int(quorum) != quorum or
                     quorum < 0 or
                     quorum > len( self.listNodes())) :
                    raise Exception ( "Illegal value for quorum %s" % quorum )
                
            except:
                raise Exception("Illegal value for quorum %s " % quorum)
            
            config.set("global", "quorum", int(quorum))
        else: 
            config.remove_option("global", "quorum")
            
        X.writeConfig(config, fn)


    def getClientConfig(self):
        """
        Get an object that contains all node information in the supplied cluster
        @return dict the dict can be used as param for the ArakoonConfig object
        """
        config = self._getConfigFile()
        clientconfig = dict()

        nodes = self.__getNodes(config)

        for name in nodes:
            clientconfig[name] = (config.get(name, "ip"),
                                  int(config.get(name, "client_port")))

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

        if config.has_section(name):
            d = {}
            for option in config.options(name):
                d[option] = config.get(name,option,False)
            return d
        else:
            raise Exception("No node with name %s configured" % name)


    def createDirs(self, name):
        """
        Create the Directories for a local arakoon node in the supplied cluster

        @param name the name of the node as configured in the config file
        """
        self.__validateName(name)

        config = self._getConfigFile()
        
        if config.has_section(name):
            home = config.get(name, "home")
            X.createDir(home)
            self._initialize(name)
            logDir = config.get(name, "log_dir")
            X.createDir(logDir)

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
        nodes = self.__getNodes(config)

        if name in nodes:
            home = config.get(name, "home")
            X.removeDirTree(home)

            if config.has_option(name, "tlog_dir"):
                tlogDir = config.get(name, "tlog_dir")
                X.removeDirTree(tlogDir)
            
            logDir = config.get(name, "log_dir")
            X.removeDirTree(logDir)
            return
        
        raise Exception("No node %s" % name )



    def addLocalNode(self, name):
        """
        Add a node to the list of nodes that have to be started locally
        from the supplied cluster

        @param name the name of the node as configured in the config file
        """
        self.__validateName(name)

        config = self._getConfigFile()
        nodes = self.__getNodes(config)
        config_name = self._servernodes()
        if config.has_section(name):
            config_name_path = '/'.join([self._clusterPath, config_name])
            p = X.getConfig(config_name_path)

            if not p.has_section("global"):
                p.add_section("global")
                p.set("global","cluster", "")

            nodes = self.__getNodes(p)
            if name in nodes:
                raise Exception("node %s already present" % name)
            nodes.append(name)
            p.set("global","cluster", ",".join(nodes))
            X.writeConfig(p, config_name_path)
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
        config_name_path = '/'.join([self._clusterPath, config_name])

        p = X.getConfig(config_name_path)
        if p.has_section("global"):
            return
        node_str = ''
        if p.has_option('global', 'cluster'):
            node_str = p.get("global", "cluster").strip()

        nodes = node_str.split(',')
        if name in nodes:
            nodes.remove(name)
            node_str = ','.join(nodes)
            p.set("global","cluster", node_str)
        X.writeConfig(p, config_name_path)

    def listLocalNodes(self):
        """
        Get a list of the local nodes in the supplied cluster

        @return list of strings containing the node names
        """
        config_name = self._servernodes()
        config_name_path = '/'.join([self._clusterPath, config_name])
        config = X.getConfig(config_name_path)

        return self.__getNodes(config)

    def setUp(self, numberOfNodes, basePort = 7080):
        """
        Sets up a local environment

        @param numberOfNodes the number of nodes in the environment
        @return the dict that can be used as a param for the ArakoonConfig object
        """
        cid = self._clusterId
        clientPort = basePort
        messagingPort = basePort + 1
        for i in range(0, numberOfNodes):
            nodeName = "%s_%i" %(cid, i)
            
            self.addNode(name = nodeName,
                         clientPort = clientPort,
                         messagingPort = messagingPort)
            self.addLocalNode(nodeName)
            self.createDirs(nodeName)
            clientPort += 10
            messagingPort += 10

        if numberOfNodes > 0:
            self.forceMaster("%s_0" % cid)
        
        config = self._getConfigFile()
        config.set( 'global', 'cluster_id', cid)
        X.writeConfig(config, self._getConfigFileName())
        

    def tearDown(self, removeDirs=True ):
        """
        Tears down a local environment

        @param removeDirs remove the log and home dir
        @param cluster the name of the arakoon cluster
        """
        config = self._getConfigFile()
        nodes = self.__getNodes(config)
        
        for node in nodes:
            if removeDirs:
                self.removeDirs(node)
            self.removeNode(node)
        
        if self.__getForcedMaster(config):
            self.forceMaster(None)
        
        self.remove()
        
    def remove(self):
        def _removeClusterId(fn):
            p = X.getConfig(fn)
            p.remove_section(self._clusterId)
            X.writeConfig(p,fn)
        
        clients = '%s/%s' % (X.cfgDir, "arakoonclients")
        _removeClusterId(clients)
        _removeClusterId(self._clustersFNH)
        X.removeDirTree(self._clusterPath)

    def __getForcedMaster(self, config):
        if not config.has_section("global"):
            return []
        
        if config.has_option("global", "master"):
            return config.get("global", "master", False).strip()
        else:
            return []

    def __getNodes(self, config):
        if not config.has_section("global"):
            return []
        nodes = []
        try:
            line = config.get("global", "cluster", False).strip()
            # "".split(",") -> ['']
            if line == "":
                nodes =  []
            else:
                nodes = line.split(",")
                nodes = map(lambda x: x.strip(), nodes)
        except LookupError:
            pass
        return nodes

    def __validateInt(self,name, value):
        typ = type(value)
        if not typ == type(1):
            raise Exception("%s=%s (type = %s) but should be an int" % (name, value, typ))

    def __validateName(self, name):
        if name is None or name.strip() == "":
            raise Exception("A name should be passed.  An empty name is not an option")

        if not type(name) == type(str()):
            raise Exception("Name should be of type strinq.config.getInifile(clusterId)g")

        for char in [' ', ',', '#']:
            if char in name:
                raise Exception("name should not contain %s" % char)

    def __validateLogLevel(self, name):
        if not name in ["info", "debug", "notice", "warning", "error", "fatal"]:
            raise Exception("%s is not a valid log level" % name)


    def start(self):
        """
        start all nodes in the cluster
        """
        for name in self.listLocalNodes():
            self._startOne(name)

    def stop(self):
        """
        stop all nodes in the supplied cluster
        
        @param cluster the arakoon cluster name
        """
        for name in self.listLocalNodes():
            self._stopOne(name)


    def restart(self):
        """
        Restart all nodes in the supplied cluster
        
        @param clusterId the arakoon cluster name
        """
        for name in self.listLocalNodes():
            self._restartOne(name)

    def getStatus(self):
        """
        Get the status the cluster's nodes running on this machine

        @return dict node name -> status (q.enumerators.AppStatusType)
        """
        status = {}
        for name in self.listLocalNodes():
            status[name] = self._getStatusOne(name)

        return status

    def _requireLocal(self, nodeName):
        if not nodeName in self.listLocalNodes():
            raise ArakoonNodeNotLocal( nodeName)
    
    def startOne(self, nodeName):
        """
        Start the node with a given name
        @param nodeName The name of the node

        """
        self._requireLocal(nodeName)
        self._startOne(nodeName)

    
    def catchupOnly(self, nodeName):
        """
        make the node catchup, but don't start it.
        (This is handy if you want to minimize downtime before you,
         go from a 1 node setup to a 2 node setup)
        """
        self._requireLocal(nodeName)
        cmd = [self._binary,
               '-config',
               '%s/%s.cfg' % (self._clusterPath, self._clusterId),
               '--node',
               nodeName,
               '-catchup-only']
        X.subprocess.call(cmd)
        
    def stopOne(self, nodeName):
        """
        Stop the node with a given name
        @param nodeName The name of the node

        """
        self._requireLocal(nodeName)
        self._stopOne(nodeName)

    def remoteCollapse(self, nodeName, n):
        """
        Tell the targetted node to collapse all but n tlog files
        @type nodeName: string
        @type n: int
        """
        config = self.getNodeConfig(nodeName)
        ip = config['ip']
        port = int(config['client_port'])
        ArakoonRemoteControl.collapse(ip,port,self._clusterId, n)
        
    def restartOne(self, nodeName):
        """
        Restart the node with a given name in the supplied cluster
        @param nodeName The name of the node

        """
        self._requireLocal( nodeName)
        self._restartOne(nodeName)

    def getStatusOne(self, nodeName):
        """
        Get the status node with a given name in the supplied cluster
        @param nodeName The name of the node
        """
        self._requireLocal(nodeName)
        return self._getStatusOne(nodeName)

    def backupDb(self, nodeName, location):
        """
        Make a backup the live database to the specified file
        
        @param nodeName The name of the node you want to backup
        @param location The path to the file where the backup should be stored
        @return void
        """
        config = self.getNodeConfig(nodeName)
        ip = config['ip']
        port = int(config['client_port'])
        ArakoonRemoteControl.downloadDb(ip,port,self._clusterId, location)
        

    def _cmd(self, name):
        r =  [self._binary,
              '--node', name, 
              '-config', '%s/%s.cfg' % (self._clusterPath, self._clusterId),
              '-daemonize']
        return r

    def _initialize(self,name):
        r = [self._binary, 
             '-config', '%s/%s.cfg' % (self._clusterPath, self._clusterId),
             '--init-db', name]
        print ' '.join(r)
        rc = X.subprocess.call(r)
        return rc
        
    def _cmdLine(self, name):
        cmd = self._cmd(name)
        cmdLine = string.join(cmd, ' ')
        return cmdLine
    
    def _startOne(self, name):
        if self._getStatusOne(name) == X.AppStatusType.RUNNING:
            return
        
        config = self.getNodeConfig(name)
        cmd = []
        if 'user' in config :
            cmd = ['sudo']
            cmd.append('-u')
            cmd.append(config['user'])
            
        #if 'group' in config :
        #    kwargs ['group'] = config ['group']
        # ???
        command = self._cmd(name)
        cmd.extend(command)
        X.logging.debug('calling: %s', str(cmd))
        X.subprocess.call(cmd, close_fds = True)


    def _stopOne(self, name):
        line = self._cmdLine(name)
        cmd = ['pkill', '-fx',  line]
        X.logging.debug("stopping '%s' with: %s",name, string.join(cmd, ' '))
        rc = X.subprocess.call(cmd, close_fds = True)
        X.logging.debug("%s=>rc=%i" % (cmd,rc))
        i = 0
        while(self._getStatusOne(name) == X.AppStatusType.RUNNING):
            rc = X.subprocess.call(cmd, close_fds = True)
            X.logging.debug("%s=>rc=%i" % (cmd,rc))
            time.sleep(1)
            i += 1
            X.logging.debug("'%s' is still running... waiting" % name)
            if i == 10:
                X.logging.debug("stopping '%s' with kill -9" % name)
                X.subprocess.call(['pkill', '-9', '-fx', line], close_fds = True)
                cnt = 0
                while (self._getStatusOne(name) == X.AppStatusType.RUNNING ) :
                    X.logging.debug("'%s' is STILL running... waiting" % name)
                    time.sleep(1)
                    cnt += 1
                    if( cnt > 10):
                        break
                break
            else:
                X.subprocess.call(cmd, close_fds=True)
    
    def _restartOne(self, name):
        self._stopOne(name)
        self._startOne(name)


    def _getPid(self, name):
        if self._getStatusOne(name) == X.AppStatusType.HALTED:
            return None
        line = self._cmdLine(name)
        cmd = 'pgrep -o -fx "%s"' % line
        (exitCode, stdout, stderr) = X.run( cmd )
        if exitCode != 0 :
            return None
        else: 
            return int(stdout)
                
    def _getStatusOne(self,name):
        line = self._cmdLine(name)
        cmd = ['pgrep','-fx', line]
        proc = X.subprocess.Popen(cmd,
                                  close_fds = True,
                                  stdout=subprocess.PIPE)
        pids = proc.communicate()[0]
        pid_list = pids.split()
        lenp = len(pid_list)
        result = None
        if lenp == 1:
            result = X.AppStatusType.RUNNING
        elif lenp == 0:
            result = X.AppStatusType.HALTED
        else:
            for pid in pid_list:
                try:
                    f = open('/proc/%s/cmdline' % pid,'r')
                    startup = f.read()
                    f.close()
                    X.logging.debug("pid=%s; cmdline=%s", pid, startup)
                except:
                    pass
            raise Exception("multiple matches", pid_list)
        return result

    def getStorageUtilization(self, node = None):
        """Calculate and return the disk usage of the supplied arakoon cluster on the system

        When no node name is given, the aggregate consumption of all nodes
        configured in the supplied cluster on the system is returned.

        Return format is a dictionary containing 2 keys: 'db' and
        'log', whose values denote the size of database files
        (*.bs ) and log files (*).

        :param node: Name of the node to check
        :type node: `str`
        
        :param cluster: Name of the arakoon cluster
        :type cluster: `str`

        :return: Storage utilization of the node(s)
        :rtype: `dict`

        :raise ValueError: No such local node
        """
        local_nodes = self.listLocalNodes()

        if node is not None and node not in local_nodes:
            raise ArakoonNodeNotLocal ( node )

        def helper(config):
            home = config['home']
            log_dir = config['log_dir']

            files_in_dir = lambda dir_: itertools.ifilter(os.path.isfile,
                (os.path.join(dir_, name) for name in os.listdir(dir_)))
            matching_files = lambda *exts: lambda files: \
                (file_ for file_ in files
                    if any(file_.endswith(ext) for ext in exts))

            db_files = matching_files('.bs')
            log_files = matching_files('') # Every string ends with ''

            sum_size = lambda files: sum(os.path.getsize(file_)
                for file_ in files)

            return {
                'db': sum_size(db_files(files_in_dir(home))),
                'log': sum_size(log_files(files_in_dir(log_dir)))
            }

        nodes = (node, ) if node is not None else local_nodes
        stats = (helper(self.getNodeConfig(node)) for node in nodes)

        result = {}
        for stat in stats:
            for key, value in stat.iteritems():
                result[key] = result.get(key, 0) + value

        return result

    def gatherEvidence(self,
                       destination,
                       clusterCredentials=None,
                       includeLogs=True,
                       includeDB=True,
                       includeTLogs=True,
                       includeConfig=True, test = False):
        """
        @param destination : path INCLUDING FILENAME where the evidence archive is saved. Can be URI, in other words, ftp://..., smb://, /tmp, ...
        @param clusterCredentials : dict of tuples e.g. {"node1" ('login', 'password'), "node2" ('login', 'password'), "node3" ('login', 'password')}
        @param includeLogs : Boolean value indicating that the logs need to be included in the evidence archive, default is True
        @param includeDB : Boolean value indicating that the Tokyo Cabinet db and db.wall files need to be included in the evidence archive, default is True
        @param includeTLogs : Boolean value indicating that the tlogs need to be included in the evidence archive, default is True
        @param includeConfig : Boolean value indicating that the arakoon configuration files should be included in the resulting archive
        
        """
        nodes_list = self.listNodes()
        diff_list = self.listNodes()
        
        if q.qshellconfig.interactive or test:
            
            if not clusterCredentials:
                clusterCredentials = self._getClusterCredentials(nodes_list,diff_list,test)
            
            elif len(clusterCredentials) < len(nodes_list):
                nodes_list = [x for x in nodes_list if x not in clusterCredentials]
                diff_list = [x for x in nodes_list if x not in clusterCredentials]
                sub_clusterCredentials = self._getClusterCredentials(nodes_list, diff_list, test)
                clusterCredentials.update(sub_clusterCredentials)
                
            else:
                q.gui.dialog.message("All Nodes have Credentials.")
            
            self._transferFiles(destination,
                                clusterCredentials,
                                includeLogs,
                                includeDB,
                                includeTLogs,
                                includeConfig)
        
        else:
            if not clusterCredentials or len(clusterCredentials) < len(nodes_list):
                raise NameError('Error: QShell is Not interactive')
            
            else:
                q.gui.dialog.message("All Nodes have Credentials.")
                self._transferFiles(destination,
                                    clusterCredentials,
                                    includeLogs,
                                    includeDB,
                                    includeTLogs,
                                    includeConfig)


    def _getClusterCredentials(self, 
                               nodes_list,
                               diff_list, test):
        clusterCredentials = dict()
        same_credentials_nodes = list()
        
        for nodename in nodes_list:
            node_passwd = ''
            if not test:
                if nodename in diff_list:
                    
                    node_config = self.getNodeConfig(nodename)
                    node_ip = node_config['ip']
               
                    node_login = q.gui.dialog.askString("Please provide login name for %s @ %s default 'root'" % (nodename, node_ip))
                    if node_login == '':
                        node_login = 'root'
                    
                    while node_passwd == '':
                        node_passwd = q.gui.dialog.askPassword('Please provide password for %s @ %s' % (nodename, node_ip))
                        if node_passwd == '':
                            q.gui.dialog.message("Error: Password is Empty.")
                    
                    clusterCredentials[nodename] = (node_login, node_passwd)
                    
                    if len(diff_list) > 1:
                        same_credentials = q.gui.dialog.askYesNo('Do you want to set the same credentials for any other node?')
                        
                        diff_list.remove(nodename)
                        
                        if same_credentials:
                            
                            same_credentials_nodes = q.gui.dialog.askChoiceMultiple("Please choose node(s) that will take same credentials:",diff_list)
                            
                            for node in same_credentials_nodes:
                                clusterCredentials[node] = (node_login, node_passwd)
                            #end for
                            if len(same_credentials_nodes) == len(diff_list):
                                break
                            else:
                                diff_list = list(set(diff_list).difference(set(same_credentials_nodes)))
            if test:
                clusterCredentials[nodename] = ('hudson', 'hudson')
        #end for
        return clusterCredentials


    def _transferFiles(self,
                       destination,
                       clusterCredentials,
                       includeLogs=True,
                       includeDB=True,
                       includeTLogs=True,
                       includeConfig=True):
        """
        
        This function copies the logs, db, tlog and config files to a Temp folder on the machine running the script then compresses the Temp 
        folder and places a copy at the destination provided at the beginning
        """
        nodes_list = self.listNodes()
        archive_name = self._clusterId + "_cluster_details"
        archive_folder = q.system.fs.joinPaths(q.dirs.tmpDir , archive_name)
        
        cfs = q.cloud.system.fs 
        sfs = q.system.fs
        
        for nodename in nodes_list:    
            node_folder  = sfs.joinPaths( archive_folder, nodename)
            
            sfs.createDir(node_folder)
            configDict = self.getNodeConfig(nodename)
            source_ip = configDict['ip']
            
            userName = clusterCredentials[nodename][0]
            password = clusterCredentials[nodename][1]
            
            source_path = 'sftp://' + userName + ':' + password + '@' + source_ip
            
            if includeDB:
                db_files = cfs.listDir( source_path + configDict['home'] )
                files2copy = filter ( lambda fn : fn.startswith( nodename ), db_files )
                for fn in files2copy :
                    full_db_file = source_path + configDict['home'] + "/" + fn
                    cfs.copyFile(full_db_file , 'file://' + node_folder)
                
            
            if includeLogs:
                
                for fname in cfs.listDir(source_path + configDict['log_dir']):
                    if fname.startswith(nodename):
                        fileinlog = q.system.fs.joinPaths(configDict['log_dir'] ,fname)
                        cfs.copyFile(source_path + fileinlog, 'file://' + node_folder)
            
            if includeTLogs:
                
                source_dir = None
                if configDict.has_key('tlog_dir'):
                    source_dir = configDict['tlog_dir']
                else:
                    source_dir = configDict['home']
                full_source_dir = source_path + source_dir
                    
                for fname in q.cloud.system.fs.listDir( full_source_dir ):
                    if fname.endswith('.tlog') or fname.endswith('.tlc') or fname.endswith('.tlf'):
                        tlogfile = q.system.fs.joinPaths(source_dir ,fname)
                        cfs.copyFile(source_path + tlogfile, 'file://' + node_folder)

            
            clusterName = self._clusterId + '.cfg'
            clusterNodes = self._clusterId + '_local_nodes.cfg'

            clusterPath = q.system.fs.joinPaths(self._clusterPath, clusterName)
            q.cloud.system.fs.copyFile(source_path + clusterPath, 'file://' + node_folder)

            clusterNodesPath = q.system.fs.joinPaths(self._clusterPath, clusterNodes)
            if q.cloud.system.fs.sourcePathExists('file://' + clusterNodesPath):
                q.cloud.system.fs.copyFile(source_path +  clusterNodesPath, 'file://' + node_folder)
            
            
        archive_file = sfs.joinPaths( q.dirs.tmpDir, self._clusterId + '_cluster_evidence.tgz')
        q.system.fs.targzCompress( archive_folder,  archive_file)
        cfs.copyFile('file://' + archive_file , destination)
        q.system.fs.removeDirTree( archive_folder )
        q.system.fs.unlink( archive_file )
    
    
    def setNurseryKeeper(self, clusterId): 
        """
        Updates the cluster configuration file to the correct nursery keeper cluster. 
        If the keeper needs to be removed from the cluster config, specify None as clusterId
        
        This requires a valid client configuration on the system that can be used to access the keeper cluster.
        
        @param clusterId:  The id of the cluster that will function as nursery keeper
        @type clusterId:   string / None
        
        @return void
        """
        
        config = self._getConfigFile()
        
        if clusterId is None:
            config.remove_section("nursery")
            return
            
        clientMgmt = ArakoonClient.ArakoonClient()
        cliCfg = clientMgmt.getClientConfig(clusterId)
        nurseryNodes = cliCfg.getNodes()
        
        if len(nurseryNodes) == 0:
            raise RuntimeError("A valid client configuration is required for cluster '%s'" % (clusterId) )
        
        config.add_section("nursery")
        config.set("nursery", "cluster_id", clusterId)
        config.set("nursery", "cluster", ",".join( nurseryNodes.keys() ))
        
        for (id,(ip,port)) in nurseryNodes.iteritems() :
            if not config.has_section(id):
                config.add_section(id)
            config.set(id,"ip",ip)
            config.set(id,"client_port",port)
        X.writeConfig(config, self._getConfigFileName())
