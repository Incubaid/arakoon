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
import os.path
import itertools

import subprocess
import time

EXC_MSG_NOT_LOCAL_FMT = "Node %s is not a local node in cluster %s" 

class ArakoonCmdtools:
    def __init__(self):
        self._binary = q.system.fs.joinPaths(q.dirs.appDir, "arakoon", "bin", "arakoond")
        self._cfgPath = q.system.fs.joinPaths(q.dirs.cfgDir, "qconfig")

    def start(self, cluster = "arakoon"):
        """
        start all nodes in the supplied cluster
        
        @param cluster the arakoon cluster name
        """
        for name in q.config.arakoon.listLocalNodes(cluster = cluster):
            self._startOne(name, cluster = cluster)

    def stop(self, cluster = "arakoon"):
        """
        stop all nodes in the supplied cluster
        
        @param cluster the arakoon cluster name
        """
        for name in q.config.arakoon.listLocalNodes(cluster = cluster):
            self._stopOne(name, cluster = cluster)


    def restart(self, cluster = "arakoon"):
        """
        Restart all nodes in the supplied cluster
        
        @param cluster the arakoon cluster name
        """
        for name in q.config.arakoon.listLocalNodes(cluster = cluster):
            self._restartOne(name, cluster = cluster)

    def getStatus(self, cluster = "arakoon"):
        """
        Get the status of all nodes in the supplied cluster

        @param cluster the arakoon cluster name
        @return dict node name -> status (q.enumerators.AppStatusType)
        """
        status = {}

        for name in q.config.arakoon.listLocalNodes(cluster = cluster):
            status[name] = self._getStatusOne(name, cluster = cluster)

        return status

    def startOne(self, nodeName, cluster = "arakoon"):
        """
        Start the node with a given name

        @param nodeName The name of the node
        @param cluster the arakoon cluster name
        """
        if not nodeName in q.config.arakoon.listLocalNodes(cluster = cluster):
            raise Exception(EXC_MSG_NOT_LOCAL_FMT % (nodeName, cluster))

        self._startOne(nodeName, cluster = cluster)

    def stopOne(self, nodeName, cluster = "arakoon"):
        """
        Stop the node with a given name

        @param nodeName The name of the node
        @param cluster the arakoon cluster name
        """
        if not nodeName in q.config.arakoon.listLocalNodes(cluster = cluster):
            raise Exception(EXC_MSG_NOT_LOCAL_FMT % (nodeName, cluster))

        self._stopOne(nodeName, cluster = cluster)

    def restartOne(self, nodeName, cluster = "arakoon"):
        """
        Restart the node with a given name in the supplied cluster

        @param nodeName The name of the node
        @param cluster the arakoon cluster name
        """
        if not nodeName in q.config.arakoon.listLocalNodes(cluster = cluster):
            raise Exception( EXC_MSG_NOT_LOCAL_FMT % (nodeName, cluster) )

        self._restartOne(nodeName, cluster = cluster)

    def getStatusOne(self, nodeName, cluster = "arakoon"):
        """
        Get the status node with a given name in the supplied cluster

        @param nodeName The name of the node
        @param cluster the arakoon cluster name
        """
        if not nodeName in q.config.arakoon.listLocalNodes(cluster = cluster):
            raise Exception( EXC_MSG_NOT_LOCAL_FMT % (nodeName, cluster) )

        return self._getStatusOne(nodeName, cluster = cluster)

    def _startOne(self, name, cluster = "arakoon"):
        if self._getStatusOne(name, cluster = cluster) == q.enumerators.AppStatusType.RUNNING:
            return
        
        kwargs = { }
        config = q.config.arakoon.getNodeConfig(name)
        if 'user' in config :
            kwargs['user'] = config ['user']
            
        if 'group' in config :
            kwargs ['group'] = config ['group']
        
        
        command = '%s -config %s --node %s' % (self._binary, 
             '%s/%s.cfg' % (self._cfgPath, cluster),  name)
            
        pid = q.system.process.runDaemon(command, **kwargs)


    def _stopOne(self, name, cluster = "arakoon"):
        subprocess.call(['pkill', \
                         '-f', \
                         '%s -config %s/%s.cfg --node %s' % (self._binary, self._cfgPath, cluster, name)], \
                        close_fds=True)

        i = 0
        while(self._getStatusOne(name, cluster = cluster) == q.enumerators.AppStatusType.RUNNING):
            time.sleep(1)
            i += 1

            if i == 10:
                subprocess.call(['pkill', \
                                 '-9', \
                                 '-f', \
                                 '%s -config %s/%s.cfg --node %s' % (self._binary, self._cfgPath, cluster, name)], \
                                close_fds=True)
                break
            else:
                subprocess.call(['pkill', \
                                 '-f', \
                                 '%s -config %s/%s.cfg --node %s' % (self._binary, self._cfgPath, cluster, name)], \
                            close_fds=True)
    def _restartOne(self,name, cluster = "arakoon"):
        self._stopOne(name, cluster = cluster)
        self._startOne(name, cluster = cluster)


    def _getPid(self, name, cluster="arakoon"):
        if self._getStatusOne(name, cluster) == q.enumerators.AppStatusType.HALTED:
            return None
        
        cmd = 'pgrep -o -f "%s -config %s/%s.cfg --node %s"' % (self._binary, self._cfgPath, cluster, name)
        (exitCode, stdout, stderr) = q.system.process.run( cmd )
        if exitCode != 0 :
            return None
        else :
            return int(stdout)
                
    def _getStatusOne(self,name, cluster = "arakoon"):
        ret = subprocess.call(['pgrep', \
                               '-f', \
                               '%s -config %s/%s.cfg --node %s' % (self._binary, self._cfgPath, cluster, name)], \
                               close_fds=True, \
                               stdout=subprocess.PIPE)

        if ret == 0:
            return q.enumerators.AppStatusType.RUNNING
        else:
            return q.enumerators.AppStatusType.HALTED

    @staticmethod
    def getStorageUtilization(node=None, cluster = "arakoon"):
        '''Calculate and return the disk usage of the supplied arakoon cluster on the system

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
        '''

        local_nodes = q.config.arakoon.listLocalNodes(cluster = cluster)

        if node is not None and node not in local_nodes:
            raise ValueError(EXC_MSG_NOT_LOCAL_FMT % (node, cluster))

        def helper(config):
            home = config['home']
            log_dir = config['log_dir']

            files_in_dir = lambda dir_: itertools.ifilter(os.path.isfile,
                (os.path.join(dir_, name) for name in os.listdir(dir_)))
            matching_files = lambda *exts: lambda files: \
                (file_ for file_ in files
                    if any(file_.endswith(ext) for ext in exts))

            tlog_files = matching_files('.tlc', '.tlog')
            db_files = matching_files('.db', '.db.wal')
            log_files = matching_files('') # Every string ends with ''

            sum_size = lambda files: sum(os.path.getsize(file_)
                for file_ in files)

            return {
                'tlog': sum_size(tlog_files(files_in_dir(home))),
                'db': sum_size(db_files(files_in_dir(home))),
                'log': sum_size(log_files(files_in_dir(log_dir)))
            }

        nodes = (node, ) if node is not None else local_nodes
        stats = (helper(q.config.arakoon.getNodeConfig(node, cluster = cluster))
            for node in nodes)

        result = {}
        for stat in stats:
            for key, value in stat.iteritems():
                result[key] = result.get(key, 0) + value

        return result

    def gatherEvidence(self, destination, clusterCredentials=None, includeLogs=True, includeDB=True, includeTLogs=True, includeConfig=True):
        """
        @param destination : path INCLUDING FILENAME where the evidence archive is saved. Can be URI, in other words, ftp://..., smb://, /tmp, ...
        @param clusterCredentials : dict of tuples e.g. {"node1" ('login', 'password'), "node2" ('login', 'password'), "node3" ('login', 'password')}
        @param includeLogs : Boolean value indicating that the logs need to be included in the evidence archive, default is True
        @param includeDB : Boolean value indicating that the Tokyo Cabinet db and db.wall files need to be included in the evidence archive, default is True
        @param includeTLogs : Boolean value indicating that the tlogs need to be included in the evidence archive, default is True
        @param includeConfig : Boolean value indicating that the arakoon configuration files should be included in the resulting archive
        
        """
        
        nodes_list = q.config.arakoon.listNodes()
        diff_list = q.config.arakoon.listNodes()
        
        if q.qshellconfig.interactive:
            
            if not clusterCredentials:
                clusterCredentials = self._getClusterCredentials(nodes_list, diff_list)
            
            elif len(clusterCredentials) < len(nodes_list):
                nodes_list = [x for x in nodes_list if x not in clusterCredentials]
                diff_list = [x for x in nodes_list if x not in clusterCredentials]
                sub_clusterCredentials = self._getClusterCredentials(nodes_list, diff_list)
                clusterCredentials.update(sub_clusterCredentials)
                
            else:
                q.gui.dialog.message("All Nodes have Credentials.")
            
            self._transferFiles(destination, clusterCredentials, includeLogs, includeDB, includeTLogs, includeConfig)
        
        else:
            if not clusterCredentials or len(clusterCredentials) < len(nodes_list):
                raise NameError('Error: QShel is Not interactive')
            
            else:
                q.gui.dialog.message("All Nodes have Credentials.")
                self._transferFiles(destination, clusterCredentials, includeLogs, includeDB, includeTLogs, includeConfig)


    def _getClusterCredentials(self, nodes_list, diff_list):
        clusterCredentials = dict()
        same_credentials_nodes = list()
        
        for nodename in nodes_list:
            node_passwd = ''
            
            if nodename in diff_list:
                
                node_config = q.config.arakoon.getNodeConfig(nodename)
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
        #end for
        return clusterCredentials


    def _transferFiles(self, destination, clusterCredentials, includeLogs=True, includeDB=True, includeTLogs=True, includeConfig=True):
        """
        
        This function copies the logs, db, tlog and config files to a Temp folder on the machine running the script then compresses the Temp 
        folder and places a copy at the destination provided at the beginning
        """
        
        nodes_list = q.config.arakoon.listNodes()
        archive_folder = q.system.fs.joinPaths(q.dirs.tmpDir , 'Archive')
        
        cfs = q.cloud.system.fs 
        sfs = q.system.fs
        
        for nodename in nodes_list:    
            node_folder  = sfs.joinPaths( archive_folder, nodename)
            
            sfs.createDir(node_folder)
            configDict = q.config.arakoon.getNodeConfig(nodename)
            source_ip = configDict['ip']
            
            userName = clusterCredentials[nodename][0]
            password = clusterCredentials[nodename][1]
            
            source_path = 'sftp://' + userName + ':' + password + '@' + source_ip
            
            if includeDB:
                dbfile = sfs.joinPaths(configDict['home'], (nodename +'.db'))
                db_files = cfs.listDir( source_path + configDict['home'] )
                files2copy = filter ( lambda fn : fn.startswith( nodename ), db_files )
                for fn in files2copy :
                    full_db_file = source_path + fn
                    cfs.copyFile(full_db_file , 'file://' + node_folder)
                
            
            if includeLogs:
                
                for fname in cfs.listDir(source_path + configDict['log_dir']):
                    if fname.startswith(nodename):
                        fileinlog = cfs.joinPaths(configDict['log_dir'] ,fname)
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
                
            q.cloud.system.fs.copyFile(source_path + '/opt/qbase3/cfg/qconfig/arakoon.cfg', 'file://' + node_folder)
            q.cloud.system.fs.copyFile(source_path + '/opt/qbase3/cfg/qconfig/arakoonnodes.cfg', 'file://' + node_folder)
            
        #end for 
        fs = q.system.fs
        archive_file = fs.joinPaths( q.dirs.tmpDir, 'Archive.tgz') 
        q.system.fs.targzCompress( archive_folder,  archive_file)
        
        q.cloud.system.fs.copyFile('file://' + archive_file , destination)
        
        q.system.fs.removeDirTree( archive_folder )
        q.system.fs.unlink( archive_file )
