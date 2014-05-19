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
import os
import ArakoonRemoteControl
import os.path
import itertools
import subprocess
import time
import types
import signal
import string
import logging

from arakoon import Arakoon
from arakoon.ArakoonExceptions import ArakoonNodeNotLocal

def which_arakoon():
    path = '/'.join([X.appDir,"arakoon/bin/arakoon"])
    if X.fileExists(path):
        return path
    else:
        return "arakoon"

class ArakoonManagement:
    def getCluster(self, clusterName):
        """
        @type clusterName: string
        @return a helper to config that cluster
        """
        return ArakoonCluster(clusterName)

    def listClusters(self):
        """
        Returns a list with the existing clusters.
        """
        fn = '/'.join ([X.cfgDir, "arakoonclusters"])
        config = X.getConfig(fn)
        return config.sections()

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

    def __init__(self, clusterName):
        self.__validateName(clusterName)
        """
        There's a difference between the clusterId and the cluster's name.
        The name is used to construct the path to find the config file.
        the id is what's inside the cfg file and
               what you need to provide to a client that want's to talk to the cluster.
        """
        self._clusterName = clusterName
        self._binary = which_arakoon()
        self._arakoonDir = '/'.join([X.cfgDir, "arakoon"])
        self._clustersFNH = '/'.join([X.cfgDir, 'arakoonclusters'])

        clusterConfig = X.getConfig(self._clustersFNH)

        if not clusterConfig.has_section(self._clusterName):

            clusterPath = '/'.join([X.cfgDir,"qconfig", "arakoon", clusterName])
            clusterConfig.add_section(clusterName)
            clusterConfig.set(clusterName, "path", clusterPath)

            if not X.fileExists(self._arakoonDir):
                X.createDir(self._arakoonDir)

            if not X.fileExists(clusterPath):
                X.createDir(clusterPath)
            X.writeConfig(clusterConfig, self._clustersFNH)
        self._clusterPath = clusterConfig.get(clusterName, "path" )

    def _servernodes(self):
        return '%s_local_nodes' % self._clusterName

    def __repr__(self):
        return "<ArakoonCluster:%s>" % self._clusterName

    def _getConfigFileName(self):
        p = X.getConfig(self._clustersFNH)
        if not p.has_section(self._clusterName):
            raise Exception("%s not present in %s" % (self._clusterName, self._clustersFNH))
        cfgDir = p.get( self._clusterName, "path", False)
        cfgFile = '/'.join([cfgDir, self._clusterName])
        return cfgFile

    def _saveConfig(self,config):
        fn = self._getConfigFileName()
        X.writeConfig(config,fn)

    def _getConfigFile(self):
        h = self._getConfigFileName()
        return X.getConfig(h)

    def _getClusterId(self):
        clusterId = self._clusterName
        try:
            config = self._getConfigFile()
            clusterId = config.get("global", "cluster_id")
        except:
            logging.info("setting cluster_id to %s", clusterId)
            config.set("global","cluster_id",clusterId)
        return clusterId

    def addBatchedTransactionConfig(self,
                                    name,
                                    max_entries = None,
                                    max_size = None):
        """
        Add a batched transaction config section to the configuration of the supplied cluster

        @param name the name of the batched transaction config section
        @param max_entries the maximum amount of entries before the batched store will persist the changes to tokyo cabinet; default is None, which results in 200.
        @param max_size the maximum combined size of the entries (in bytes) before the batched store will persist the changes to tokyo cabinet; default is None, which results in 100_000.
        """
        config = self._getConfigFile()

        config.addSection(name)

        if max_entries is not None:
            config.set(name, "max_entries", max_entries)
        if max_size is not None:
            config.set(name, "max_size", max_size)

        config.write()

    def addLogConfig(self,
                     name,
                     client_protocol = None,
                     paxos = None,
                     tcp_messaging = None):
        """
        Add a log config section to the configuration of the supplied cluster

        @param name the name of the log config section
        @param client_protocol the log level for the client_protocol log section
        @param paxos the log level for the paxos log section
        @param tcp_messaging the log level for the tcp_messaging log section
        """
        config = self._getConfigFile()

        config.addSection(name)

        if client_protocol is not None:
            config.set(name, "client_protocol", client_protocol)
        if paxos is not None:
            config.set(name, "paxos", paxos)
        if tcp_messaging is not None:
            config.set(name, "tcp_messaging", tcp_messaging)

        config.write()

    def addNode(self,
                name,
                ip = "127.0.0.1",
                clientPort = 7080,
                messagingPort = 10000,
                logLevel = "info",
                logDir = None,
                home = None,
                tlogDir = None,
                wrapper = None,
                isLearner = False,
                targets = None,
                isLocal = False,
                logConfig = None,
                batchedTransactionConfig = None,
                tlfDir = None,
                headDir = None,
                isWitness = False,
                collapseSlowdown = None):
        """
        Add a node to the configuration of the supplied cluster

        @param name : the name of the node, should be unique across the environment
        @param ip : the ip(s) this node should be contacted on (string or string list)
        @param clientPort : the port the clients should use to contact this node
        @param messagingPort : the port the other nodes should use to contact this node
        @param logLevel : the loglevel (debug info notice warning error fatal)
        @param logDir : the directory used for logging
        @param home : the directory used for the nodes data
        @param tlogDir : the directory used for tlogs (if none, home will be used)
        @param wrapper : wrapper line for the executable (for example 'softlimit -o 8192')
        @param isLearner : whether this node is a learner node or not
        @param targets : for a learner node the targets (string list) it learns from
        @param isLocal : whether this node is a local node and should be added to the local nodes list
        @param logConfig : specifies the log config to be used for this node
        @param batchedTransactionConfig : specifies the batched transaction config to be used for this node
        @param tlfDir : the directory used for tlfs (if none, tlogDir will be used)
        @param headDir : the directory used for head.db (if none, tlfDir will be used)
        @param isWitness : whether this node is a witness or not
        @param collapseSlowdown : the factor with which collapsing should be slowed down
        """
        self.__validateName(name)
        self.__validateLogLevel(logLevel)

        if isinstance(ip, basestring):
            ip = [ip]

        config = self._getConfigFile()
        nodes = self.__getNodes(config)

        if name in nodes:
            raise Exception("node %s already present" % name )
        if not isLearner:
            nodes.append(name)

        config.add_section(name)

        config.set(name, "ip", ', '.join(ip))

        self.__validateInt("clientPort", clientPort)
        config.set(name, "client_port", clientPort)
        self.__validateInt("messagingPort", messagingPort)
        config.set(name, "messaging_port", messagingPort)
        config.set(name, "log_level", logLevel)

        if logConfig is not None:
            config.set(name, "log_config", logConfig)

        if batchedTransactionConfig is not None:
            config.set(name, "batched_transaction_config", batchedTransactionConfig)

        if wrapper is not None:
            config.set(name, "wrapper", wrapper)

        if logDir is None:
            logDir = '/'.join([X.logDir, self._clusterName, name])
        config.set(name, "log_dir", logDir)

        if home is None:
            home = '/'.join([X.varDir, "db", self._clusterName, name])
        config.set(name, "home", home)

        if tlogDir:
            config.set(name,"tlog_dir", tlogDir)

        if tlfDir:
            config.set(name,"tlf_dir", tlfDir)

        if headDir:
            config.set(name,"head_dir", headDir)

        if isLearner:
            config.set(name, "learner", "true")
            if targets is None:
                targets = self.listNodes()
            config.set(name, "targets", string.join(targets,","))

        if isWitness:
            config.set(name, "witness", "true")

        if collapseSlowdown:
            config.set(name, "collapse_slowdown", collapseSlowdown)

        if not config.has_section("global") :
            config.add_section("global")
            config.set("global", "cluster_id", self._clusterName)
        config.set("global","cluster", ",".join(nodes))

        self._saveConfig(config)

        if isLocal:
            self.addLocalNode(name)

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
            self._saveConfig(config)
            return

        raise Exception("No node with name %s" % name)

    def setMasterLease(self, duration=None):
        """
        Set the master lease duration in the supplied cluster

        @param duration The duration of the master lease in seconds
        """
        section = "global"
        key = "lease_period"

        config = self._getConfigFile()

        if not config.has_section( section ):
            raise Exception("Section '%s' not found in config" % section )

        if duration:
            if not isinstance( duration, int ) :
                raise AttributeError( "Invalid value for lease duration (expected int, got: '%s')" % duration)
            config.set(section, key, duration)
        else:
            config.remove_option(section, key)

        self._saveConfig(config)

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
        if name:
            nodes = self.__getNodes(config)

            self.__validateName(name)
            if not name in nodes:
                raise Exception("No node with name %s configured in cluster %s" % (name,self._clusterName) )
            config.set(g,m,name)
            if preferred:
                config.set(g,pm,'true')
        else:
            config.remove_option(g, m)
            if config.has_option(g, pm):
                config.remove_option(g, pm)

        self._saveConfig(config)

    def preferredMasters(self, nodes):
        '''
        Set a list of preferred master nodes

        When the given list is empty, the configuration item is unset.

        Since this option is incompatible with a fixed master, this method will

        - raise an exception if 'master' is set and 'preferred_master' is false
          (or not set, which defaults to false)
        - unset 'master' and 'preferred_master' if both are set and
          'preferred_master' is true

        @param nodes: Names of preferred master nodes
        @type nodes: `list` of `str`
        '''

        if isinstance(nodes, basestring):
            raise TypeError('Expected list of strings, not string')

        config = self._getConfigFile()

        if not nodes:
            if config.has_option('global', 'preferred_masters'):
                config.remove_option('global', 'preferred_masters')
                self._saveConfig(config)
                return


        section = 'global'
        master = 'master'
        preferred_master = 'preferred_master'

        # Check existing master/preferred_master configuration. Bail out if
        # incompatible.
        if config.has_option(section, master):
            preferred_master_setting = \
                config.get(section, preferred_master).lower() \
                if config.has_option(section, preferred_master) \
                else 'false'

            if preferred_master_setting != 'true':
                raise Exception(
                    'Can\'t set both \'master\' and \'preferred_masters\'')

            # If reached, 'master' was set and 'preferred_master' was true.
            # We're free to remove both, since they're replaced by the
            # 'preferred_masters' setting.
            config.remove_option(section, master)
            if config.has_option(section, preferred_master):
                config.remove_option(section, preferred_master)

        # Set up preferred_masters
        preferred_masters = 'preferred_masters'

        config.set(section, preferred_masters, ', '.join(nodes))
        self._saveConfig(config)

    def setLogConfig(self, logConfig, nodes=None):
        if nodes is None:
            nodes = self.listNodes()
        else:
            for n in nodes :
                self.__validateName( n )

        config = self._getConfigFile()
        for n in nodes:
            config.set(n, "log_config", logConfig)

        config.write()

    def setLogLevel(self, level, nodes=None):
        if nodes is None:
            nodes = self.listNodes()
        else:
            for n in nodes :
                self.__validateName( n )
        self.__validateLogLevel( level )
        config = self._getConfigFile()

        for n in nodes:
            config.set( n, "log_level", level )
        self._saveConfig(config)

    def setCollapseSlowdown(self, collapseSlowdown, nodes=None):
        if nodes is None:
            nodes = self.listNodes()
        else:
            for n in nodes :
                self.__validateName( n )
        config = self._getConfigFile()

        for n in nodes:
            if collapseSlowdown:
                config.set(n, "collapse_slowdown", collapseSlowdown)
            else:
                config.remove_option(n, "collapse_slowdown")

        self._saveConfig(config)

    def _setTlogCompression(self,nodes, compressor):
        if nodes is None:
            nodes = self.listNodes()
        else:
            for n in nodes:
                self.__validateName(n)
        config = self._getConfigFile()
        for n in nodes:
            config.remove_option(n, "disable_tlog_compression")
            config.set(n, "tlog_compression", compressor)
        self._saveConfig(config)

    def enableTlogCompression(self, nodes=None, compressor='bz2'):
        """
        Enables tlog compression for the given nodes (this is enabled by default)
        @param nodes List of node names
        @param compressor one of 'bz2', 'snappy', 'none'
        """
        self._setTlogCompression(nodes,compressor)

    def disableTlogCompression(self, nodes=None):
        """
        Disables tlog compression for the given nodes
        @param nodes List of node names
        """
        self._setTlogCompression(nodes,"none")

    def _changeFsync(self, nodes, value):
        if nodes is None:
            nodes = self.listNodes()
        else:
            for n in nodes:
                self.__validateName(n)

        config = self._getConfigFile()

        for node in nodes:
            config.set(node, 'fsync', value)
        self._saveConfig(config)

    def enableFsync(self, nodes=None):
        '''Enable fsync'ing of tlogs after every operation'''
        self._changeFsync(nodes, 'true')

    def disableFsync(self, nodes=None):
        '''Disable fsync'ing of tlogs after every operation'''
        self._changeFsync(nodes, 'false')


    def setTLSCACertificate(self, ca_cert_path):
        '''Configure path to TLS CA certificate

        This corresponds to the `tls_ca_cert` entry in the `global` section.

        Set to `None` to unset/disable.

        The path should point to a valid file, otherwise a `ValueError` will be
        raised.

        :param ca_cert_path: Path to CA certificate
        :type ca_cert_path: `str`
        '''

        global_ = 'global'
        tls_ca_cert = 'tls_ca_cert'

        config = self._getConfigFile()
        if ca_cert_path is None:
            if config.has_option(global_, tls_ca_cert):
                config.remove_option(global_, tls_ca_cert)

            self._saveConfig(config)
            return

        if not os.path.isfile(ca_cert_path):
            raise ValueError(
                'Invalid ca_cert_path \'%s\': no such file' % ca_cert_path)

        config.set(global_, tls_ca_cert, ca_cert_path)

        self._saveConfig(config)

    def enableTLSService(self):
        '''Enable TLS on the client service

        This corresponds to the `tls_service` entry in the `global` section.

        Note `tls_ca_cert` should be configured before calling this method,
        otherwise an `Exception` will be raised.
        '''

        global_ = 'global'
        tls_ca_cert = 'tls_ca_cert'
        tls_service = 'tls_service'

        config = self._getConfigFile()
        if not config.has_option(global_, tls_ca_cert):
            raise Exception('No tls_ca_cert configured')

        if config.has_option(global_, tls_service):
            config.remove_option(global_, tls_service)

        config.set(global_, tls_service, 'true')
        self._saveConfig(config)

    def disableTLSService(self):
        '''Disable TLS on the client service

        This corresponds to the `tls_service` entry in the `global` section.
        '''

        global_ = 'global'
        tls_service = 'tls_service'

        config = self._getConfigFile()

        if config.has_option(global_, tls_service):
            config.remove_option(global_, tls_service)

        self._saveConfig(config)

    def enableTLSServiceValidatePeer(self):
        '''Enable TLS peer verification on the client service

        This corresponds to the `tls_service_validate_peer` entry in the
        `global` section.

        Note `tls_service` should be enabled before calling this method,
        otherwise an `Exception` is raised.
        '''

        global_ = 'global'
        tls_service = 'tls_service'
        tls_service_validate_peer = 'tls_service_validate_peer'

        config = self._getConfigFile()
        if (not config.has_option(global_, tls_service)) \
            or (config.get(global_, tls_service).lower() != 'true'):
            raise Exception('tls_service not enabled')

        if config.has_option(global_, tls_service_validate_peer):
            config.remove_option(global_, tls_service_validate_peer)

        config.set(global_, tls_service_validate_peer, 'true')
        self._saveConfig(config)

    def disableTLSServiceValidatePeer(self):
        '''Disable TLS peer verification on the client service

        This corresponds to the `tls_service_validate_peer` entry in the
        `global` section.
        '''

        global_ = 'global'
        tls_service_validate_peer = 'tls_service_validate_peer'

        config = self._getConfigFile()

        if config.has_option(global_, tls_service_validate_peer):
            config.remove_option(global_, tls_service_validate_peer)

        self._saveConfig(config)

    def setTLSCertificate(self, node, cert_path, key_path):
        '''Set the TLS certificate & key paths for a node

        This corresponds to the `tls_cert` and `tls_key` entries in a node
        section.

        Set both `cert_path` and `key_path` to `None` to unset the setting and
        disable TLS usage.

        Both paths should point to valid files, otherwise a `ValueError` is
        raised.

        `tls_ca_cert` should be configured before calling this method,
        otherwise an `Exception` is raised.

        :param node: Node name
        :type node: `str`
        :param cert_path: Path to node certificate file
        :type cert_path: `str`
        :param key_path: Path to node key file
        :type key_path: `str`
        '''

        self.__validateName(node)

        if cert_path is None and key_path is not None:
            raise ValueError('cert_path is None but key_path isn\'t')

        if cert_path is not None and key_path is None:
            raise ValueError('key_path is None but cert_path isn\'t')

        global_ = 'global'
        tls_ca_cert = 'tls_ca_cert'
        tls_cert = 'tls_cert'
        tls_key = 'tls_key'

        config = self._getConfigFile()
        if cert_path is None and key_path is None:
            if config.has_option(node, tls_cert):
                config.remove_option(node, tls_cert)

            if config.has_option(node, tls_key):
                config.remove_option(node, tls_key)

            self._saveConfig(config)

            return


        if not config.has_option(global_, tls_ca_cert):
            raise Exception('No tls_ca_cert configured')

        if not os.path.isfile(cert_path):
            raise ValueError(
                'Invalid cert_path \'%s\': no such file' % cert_path)

        if not os.path.isfile(key_path):
            raise ValueError(
                'Invalid key_path \'%s\': no such file' % key_path)

        if config.has_option(node, tls_cert):
            config.remove_option(node, tls_cert)

        if config.has_option(node, tls_key):
            config.remove_option(node, tls_key)

        config.set(node, tls_cert, cert_path)
        config.set(node, tls_key, key_path)
        self._saveConfig(config)


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
        self._saveConfig(config)

    def setQuorum(self, quorum=None):
        """
        Set the quorum for the supplied cluster

        The quorum dictates on how many nodes need to acknowledge the new value before it becomes accepted.
        The default is (nodes/2)+1

        @param quorum the forced quorum. If None, the default is used
        """
        config = self._getConfigFile()
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
            config.remove("global", "quorum")

        self._saveConfig(config)

    def getClientConfig(self):
        """
        Get an object that contains all node information in the supplied cluster
        @return dict the dict can be used as param for the ArakoonConfig object
        """
        config = self._getConfigFile()
        clientconfig = dict()

        nodes = self.__getNodes(config)

        for name in nodes:
            ips = config.get(name, "ip")
            ip_list = map(lambda x: x.strip(), ips.split(","))
            port = int(config.get(name, "client_port"))
            clientconfig[name] = (ip_list, port)


        return clientconfig

    def getClient(self):
        config = self.getClientConfig()
        id = self._getClusterId()
        client = Arakoon.ArakoonClient(Arakoon.ArakoonClientConfig(id, config))
        return client

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

            if config.has_option(name, "tlog_dir"):
                tlogDir = config.get(name, "tlog_dir")
                X.createDir(tlogDir)

            if config.has_option(name, "tlf_dir"):
                tlfDir = config.get(name, "tlf_dir")
                X.createDir(tlfDir)

            if config.has_option(name, "head_dir"):
                headDir = config.get(name, "head_dir")
                X.createDir(headDir)

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
            nodesconfig = X.getConfig(config_name_path)

            if not nodesconfig.has_section("global"):
                nodesconfig.add_section("global")
                nodesconfig.set("global","cluster", "")

            nodes = self.__getNodes(nodesconfig)
            if name in nodes:
                raise Exception("node %s already present" % name)
            nodes.append(name)
            nodesconfig.set("global","cluster", ",".join(nodes))

            X.writeConfig(nodesconfig,config_name_path)

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
        config = X.getConfig(config_name_path)

        if not config.has_section("global"):
            return

        node_str = config.get("global", "cluster").strip()
        nodes = node_str.split(',')
        if name in nodes:
            nodes.remove(name)
            node_str = ','.join(nodes)
            config.set("global","cluster", node_str)
            X.writeConfig(config, config_name_path)

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
        cid = self._clusterName
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
        self._saveConfig(config)

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
        clients_fn = "%s/%s" % (X.cfgDir, "arakoonclients")
        clientConf = X.getConfig(clients_fn)
        clientConf.remove_section(self._clusterName)
        X.writeConfig(clientConf,clients_fn)

        fn = self._clustersFNH
        clusterConf = X.getConfig(fn)
        clusterConf.remove_section(self._clusterName)
        X.writeConfig(clusterConf, fn)

        X.removeDirTree(self._clusterPath)

    def __getForcedMaster(self, config):
        if not config.has_section("global"):
            return []

        if config.has_option("global", "master"):
            return config.get("global", "master").strip()
        else:
            return []

    def __getNodes(self, config):
        if not config.has_section("global"):
            return []
        nodes = []
        try:
            if config.has_option("global", "cluster"):
                line = config.get("global", "cluster").strip()
                # "".split(",") -> ['']
                if line == "":
                    nodes =  []
                else:
                    nodes = line.split(",")
                    nodes = map(lambda x: x.strip(), nodes)
            else:
                nodes = []
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
            raise Exception("Name should be of type string")

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
        rcs = {}

        for name in self.listLocalNodes():
            rcs[name] = self._startOne(name)

        return rcs

    def stop(self):
        """
        stop all nodes in the supplied cluster

        @param cluster the arakoon cluster name
        """
        rcs = {}
        for name in self.listLocalNodes():
            rcs[name] = self._stopOne(name)

        return rcs


    def restart(self):
        """
        Restart all nodes in the supplied cluster

        """
        rcs = {}

        for name in self.listLocalNodes():
            rcs[name] = self._restartOne(name)

        return rcs

    def getStatus(self):
        """
        Get the status the cluster's nodes running on this machine

        @return dict node name -> status (AppStatusType)
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
        return self._startOne(nodeName)


    def catchupOnly(self, nodeName):
        """
        make the node catchup, but don't start it.
        (This is handy if you want to minimize downtime before you,
         go from a 1 node setup to a 2 node setup)
        """
        self._requireLocal(nodeName)
        cmd = [self._binary,
               '-config',
               '%s/%s.cfg' % (self._clusterPath, self._clusterName),
               '--node',
               nodeName,
               '-catchup-only']
        return subprocess.call(cmd)

    def stopOne(self, nodeName):
        """
        Stop the node with a given name
        @param nodeName The name of the node

        """
        self._requireLocal(nodeName)
        return self._stopOne(nodeName)

    def remoteCollapse(self, nodeName, n):
        """
        Tell the targetted node to collapse all but n tlog files
        @type nodeName: string
        @type n: int
        """
        config = self.getNodeConfig(nodeName)
        ip_mess = config['ip']
        ip = self._getIp(ip_mess)
        port = int(config['client_port'])
        clusterId = self._getClusterId()
        ArakoonRemoteControl.collapse(ip,port,clusterId, n)

    def copyDbToHead(self, nodeName, n):
        """
        Tell the targetted node to take a copy of it's db to be used as head, removing all but n tlogs
        @type nodeName: string
        @type n: int
        """
        config = self.getNodeConfig(nodeName)
        ip_mess = config['ip']
        ip = self._getIp(ip_mess)
        port = int(config['client_port'])
        clusterId = self._getClusterId()
        ArakoonRemoteControl.copyDbToHead(ip,port,clusterId, n)

    def optimizeDb(self, nodeName):
        """
        Tell a node to optimize its database (only works on slaves)

        @param nodeName The name of the node you want to optimize
        @return void
        """
        config = self.getNodeConfig(nodeName)
        ip_mess = config['ip']
        ip = self._getIp(ip_mess)
        port = int(config['client_port'])
        clusterId = self._getClusterId()
        ArakoonRemoteControl.optimizeDb(ip,port, clusterId)


    def injectAsHead(self, nodeName, newHead, force=False, inPlace=False):
        """
        tell the node to use the file as its new head database
        @param nodeName The (local) node where you want to inject the database
        @param newHead  a database file that can serve as head
        @param force forces the database to be injected even when the current head is corrupt
        @param inPlace Use in-place rename instead of copying `newHead`
        @return Return code of inject-as-head call
        """
        self._requireLocal(nodeName)

        cmd = [self._binary,'--inject-as-head', newHead, nodeName, '-config',
                  '%s/%s.cfg' % (self._clusterPath, self._clusterName) ]

        if force:
            cmd.append('--force')
        if inPlace:
            cmd.append('--inplace')

        p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        output = p.communicate()[0]
        rc = p.returncode
        logging.debug("injectAsHead returned [%d] %s", rc, output)
        return rc


    def defragDb(self, nodeName):
        """
        Tell a node to defrag its database (only works on slaves)
        @param nodeName The name of the node you want to optimize
        @return void
        """
        config = self.getNodeConfig(nodeName)
        ip_mess = config['ip']
        ip = self._getIp(ip_mess)
        port = int(config['client_port'])
        clusterId = self._getClusterId()
        ArakoonRemoteControl.defragDb(ip,port, clusterId)


    def dropMaster(self, nodeName):
        """
        Request a node to drop its master role
        @param nodeName The name of the node you want to drop its master role
        @return void
        """
        config = self.getNodeConfig(nodeName)
        ip_mess = config['ip']
        ip = self._getIp(ip_mess)
        port = int(config['client_port'])
        clusterId = self._getClusterId()
        ArakoonRemoteControl.dropMaster(ip,port, clusterId)


    def flushStore(self, nodeName):
        """
        Request a node to flush its batched store to disk
        @param nodeName The name of the node you want to perform the flush of its store
        @return void
        """
        config = self.getNodeConfig(nodeName)
        ip_mess = config['ip']
        ip = self._getIp(ip_mess)
        port = int(config['client_port'])
        clusterId = self._getClusterId()
        ArakoonRemoteControl.flushStore(ip,port, clusterId)


    def restartOne(self, nodeName):
        """
        Restart the node with a given name in the supplied cluster
        @param nodeName The name of the node

        """
        self._requireLocal( nodeName)
        return self._restartOne(nodeName)

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
        ip_mess = config['ip']
        ip = self._getIp(ip_mess)
        port = int(config['client_port'])
        clusterId = self._getClusterId()
        ArakoonRemoteControl.downloadDb(ip,port,clusterId, location)


    def _cmd(self, name):
        r =  [self._binary,'--node',name,'-config',
              '%s/%s.cfg' % (self._clusterPath, self._clusterName),
              '-start']
        return r

    def _cmdLine(self, name):
        cmd = self._cmd(name)
        cmdLine = string.join(cmd, ' ')
        return cmdLine

    def _startOne(self, name):
        if self._getStatusOne(name) == X.AppStatusType.RUNNING:
            return

        config = self.getNodeConfig(name)
        cmd = []
        if 'wrapper' in config :
            wrapperLine = config['wrapper']
            cmd = wrapperLine.split(' ')

        command = self._cmd(name)
        cmd.extend(command)
        cmd.append('-daemonize')
        logging.debug('calling: %s', str(cmd))
        return subprocess.call(cmd, close_fds = True)

    def _getIp(self,ip_mess):
        t_mess = type(ip_mess)
        if t_mess == types.StringType:
            parts = ip_mess.split(',')
            ip = string.strip(parts[0])
            return ip
        elif t_mess == types.ListType:
            return ip_mess[0]
        else:
            raise Exception("should '%s' be a string or string list")

    def _stopOne(self, name):
        line = self._cmdLine(name)
        cmd = ['pkill', '-f',  line]
        logging.debug("stopping '%s' with: %s",name, string.join(cmd, ' '))
        rc = subprocess.call(cmd, close_fds = True)
        logging.debug("%s=>rc=%i" % (cmd,rc))
        i = 0
        while(self._getStatusOne(name) == X.AppStatusType.RUNNING):
            rc = subprocess.call(cmd, close_fds = True)
            logging.debug("%s=>rc=%i" % (cmd,rc))
            time.sleep(1)
            i += 1
            logging.debug("'%s' is still running... waiting" % name)

            if i == 10:
                msg = "Requesting '%s' to dump crash log information" % name
                logging.debug(msg)
                X.subprocess.call(['pkill', '-%d' % signal.SIGUSR2, '-f', line], close_fds=True)
                time.sleep(1)

                logging.debug("stopping '%s' with kill -9" % name)
                rc = X.subprocess.call(['pkill', '-9', '-f', line], close_fds = True)
                if rc == 0:
                    rc = 9
                cnt = 0
                while (self._getStatusOne(name) == X.AppStatusType.RUNNING ) :
                    logging.debug("'%s' is STILL running... waiting" % name)
                    time.sleep(1)
                    cnt += 1
                    if( cnt > 10):
                        break
                break
            else:
                X.subprocess.call(cmd, close_fds=True)
        if rc < 9:
            rc = 0 # might be we looped one time too many.
        return rc

    def _restartOne(self, name):
        self._stopOne(name)
        return self._startOne(name)


    def _getPid(self, name):
        if self._getStatusOne(name) == X.AppStatusType.HALTED:
            return None
        line = self._cmdLine(name)
        cmd = ['pgrep', '-o' ,'-f' , line]
        try:
            stdout = X.subprocess.check_output( cmd )
            return int(stdout)
        except:
            return None

    def _getStatusOne(self,name):
        line = self._cmdLine(name)
        cmd = ['pgrep','-fn', line]
        proc = subprocess.Popen(cmd,
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
                    logging.debug("pid=%s; cmdline=%s", pid, startup)
                except:
                    pass
            raise Exception("multiple matches", pid_list)
        return result

    def getStorageUtilization(self, node = None):
        """Calculate and return the disk usage of the supplied arakoon cluster on the system

        When no node name is given, the aggregate consumption of all nodes
        configured in the supplied cluster on the system is returned.

        Return format is a dictionary containing 3 keys: 'db', 'tlog' and
        'log', whose values denote the size of database files
        (*.db, *.db.wall), TLog files (*.tlc, *.tlog) and log files (*).

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
            real_tlog_dir = config.get('tlog_dir', home)
            tlf_dir = config.get('tlf_dir', real_tlog_dir)
            head_dir = config.get('head_dir', real_tlog_dir)

            tlog_dirs = set([real_tlog_dir, tlf_dir])
            # 'head_dir' might have a place in here, but head.db wasn't counted
            # before (in most cases), so...
            db_dirs = set([home])
            log_dirs = set([log_dir])

            files_in_dir = lambda dir_: itertools.ifilter(os.path.isfile,
                (os.path.join(dir_, name) for name in os.listdir(dir_)))
            files_in_dirs = lambda dirs: itertools.chain(*(files_in_dir(dir_)
                for dir_ in dirs))
            matching_files = lambda *exts: lambda files: \
                (file_ for file_ in files
                    if any(file_.endswith(ext) for ext in exts))

            tlog_files = matching_files('.tlc', '.tlog','.tlf')
            db_files = matching_files('.db', '.db.wal')
            log_files = matching_files('') # Every string ends with ''

            sum_size = lambda files: sum(os.path.getsize(file_)
                for file_ in files)

            return {
                'tlog': sum_size(tlog_files(files_in_dirs(tlog_dirs))),
                'db': sum_size(db_files(files_in_dirs(db_dirs))),
                'log': sum_size(log_files(files_in_dirs(log_dirs)))
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
                    node_ip_mess = node_config['ip']
                    node_ip = self._getIp(node_ip_mess)
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
        archive_name = self._clusterName + "_cluster_details"
        archive_folder = q.system.fs.joinPaths(q.dirs.tmpDir , archive_name)

        cfs = q.cloud.system.fs
        sfs = q.system.fs

        for nodename in nodes_list:
            node_folder  = sfs.joinPaths( archive_folder, nodename)

            sfs.createDir(node_folder)
            configDict = self.getNodeConfig(nodename)
            source_ip_mess = configDict['ip']
            source_ip = self._getIp(source_ip_mess)
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


            clusterId = self._clusterName + '.cfg'
            clusterNodes = self._clusterName + '_local_nodes.cfg'

            clusterPath = '/'.join(self._clusterPath, clusterId)
            q.cloud.system.fs.copyFile(source_path + clusterPath, 'file://' + node_folder)

            clusterNodesPath = q.system.fs.joinPaths(self._clusterPath, clusterNodes)
            if q.cloud.system.fs.sourcePathExists('file://' + clusterNodesPath):
                q.cloud.system.fs.copyFile(source_path +  clusterNodesPath, 'file://' + node_folder)


        archive_file = sfs.joinPaths( q.dirs.tmpDir, self._clusterName + '_cluster_evidence.tgz')
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

        cliCfg = q.clients.arakoon.getClientConfig(clusterId)
        nurseryNodes = cliCfg.getNodes()

        if len(nurseryNodes) == 0:
            raise RuntimeError("A valid client configuration is required for cluster '%s'" % (clusterId) )

        config.add_section("nursery")
        config.set("nursery", "cluster_id", clusterId)
        config.set("nursery", "cluster", ",".join( nurseryNodes.keys() ))

        for (id,(ip,port)) in nurseryNodes.iteritems() :
            if isinstance(ip, basestring):
                ip = [ip]

            config.add_section(id)
            config.set(id, "ip", ', '.join(ip))
            config.set(id,"client_port",port)
        self._saveConfig(config)
