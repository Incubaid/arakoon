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
import ArakoonRemoteControl
import subprocess
import time

EXC_MSG_NOT_LOCAL_FMT = "Node %s is not a local node in cluster %s" 

class ArakoonCmdtools:
    def __init__(self):
        self._binary = q.system.fs.joinPaths(q.dirs.appDir, "arakoon", "bin", "arakoond")
        self._cfgPath = q.system.fs.joinPaths(q.dirs.cfgDir, "qconfig")

    def start(self, clusterId):
        """
        start all nodes in the supplied cluster
        
        @param cluster the arakoon cluster name
        """
        for name in q.config.arakoon.listLocalNodes(clusterId):
            self._startOne(clusterId,name)

    def stop(self, clusterId):
        """
        stop all nodes in the supplied cluster
        
        @param cluster the arakoon cluster name
        """
        for name in q.config.arakoon.listLocalNodes(clusterId):
            self._stopOne(clusterId, name)


    def restart(self, clusterId):
        """
        Restart all nodes in the supplied cluster
        
        @param clusterId the arakoon cluster name
        """
        for name in q.config.arakoon.listLocalNodes(clusterId):
            self._restartOne(clusterId, name)

    def getStatus(self, clusterId):
        """
        Get the status of all nodes in the supplied cluster

        @param clusterId the arakoon cluster name
        @return dict node name -> status (q.enumerators.AppStatusType)
        """
        status = {}

        for name in q.config.arakoon.listLocalNodes(clusterId):
            status[name] = self._getStatusOne(clusterId, name)

        return status

    def _requireLocal(self, clusterId, nodeName):
        if not nodeName in q.config.arakoon.listLocalNodes(clusterId):
            raise Exception(EXC_MSG_NOT_LOCAL_FMT % (nodeName, clusterId))
    
    def startOne(self, clusterId, nodeName):
        """
        Start the node with a given name
        @param clusterId the arakoon cluster name
        @param nodeName The name of the node

        """
        self._requireLocal(clusterId, nodeName)
        self._startOne(clusterId, nodeName)

    
    def catchupOnly(self, clusterId, nodeName):
        """
        make the node catchup, but don't start it.
        (This is handy if you want to minimize downtime before you,
         go from a 1 node setup to a 2 node setup)
        """
        self._requireLocal(clusterId, nodeName)
        cmd = [self._binary,
               '-config',
               '%s/%s.cfg' % (self._cfgPath, clusterId),
               '--node',
               nodeName,
               '-catchup-only']
        subprocess.call(cmd)
        
    def stopOne(self, clusterId, nodeName):
        """
        Stop the node with a given name
        @param clusterId the arakoon cluster name
        @param nodeName The name of the node

        """
        self._requireLocal(clusterId,nodeName)
        self._stopOne(clusterId, nodeName)

    def remoteCollapse(self, clusterId, nodeName, n):
        """
        Tell the targetted node to collapse n tlog files
        @type clusterId: string
        @type nodeName: string
        @type n: int
        """
        config = q.config.arakoon.getNodeConfig(clusterId, nodeName)
        ip = config['ip']
        port = int(config['client_port'])
        ArakoonRemoteControl.collapse(ip,port,clusterId, n)
        
    def restartOne(self, clusterId, nodeName):
        """
        Restart the node with a given name in the supplied cluster
        @param clusterId the arakoon cluster name
        @param nodeName The name of the node

        """
        self._requireLocal(clusterId, nodeName)
        self._restartOne(clusterId, nodeName)

    def getStatusOne(self, clusterId, nodeName):
        """
        Get the status node with a given name in the supplied cluster
        @param clusterId the arakoon cluster name
        @param nodeName The name of the node

        """
        self._requireLocal(clusterId, nodeName)
        return self._getStatusOne(clusterId, nodeName)

    def _startOne(self, clusterId, name):
        if self._getStatusOne(clusterId, name) == q.enumerators.AppStatusType.RUNNING:
            return
        
        kwargs = { }
        config = q.config.arakoon.getNodeConfig(clusterId, name)
        if 'user' in config :
            kwargs['user'] = config ['user']
            
        if 'group' in config :
            kwargs ['group'] = config ['group']
        
        
        command = '%s -config %s --node %s' % (self._binary, 
             '%s/%s.cfg' % (self._cfgPath, clusterId),  name)
            
        pid = q.system.process.runDaemon(command, **kwargs)


    def _stopOne(self, clusterId, name):
        subprocess.call(['pkill', 
                         '-f', 
                         '%s -config %s/%s.cfg --node %s' % (self._binary,
                                                             self._cfgPath,
                                                             clusterId,
                                                             name)], 
                        close_fds=True)

        i = 0
        while(self._getStatusOne(clusterId, name) == q.enumerators.AppStatusType.RUNNING):
            time.sleep(1)
            i += 1

            if i == 10:
                subprocess.call(['pkill', 
                                 '-9', 
                                 '-f', 
                                 '%s -config %s/%s.cfg --node %s' % (self._binary, self._cfgPath, clusterId, name)], 
                                close_fds=True)
                break
            else:
                subprocess.call(['pkill', 
                                 '-f', 
                                 '%s -config %s/%s.cfg --node %s' % (self._binary,
                                                                     self._cfgPath,
                                                                     clusterId,
                                                                     name)], 
                                close_fds=True)
    
    def _restartOne(self,clusterId, name):
        self._stopOne(clusterId, name)
        self._startOne(clusterId, name)


    def _getPid(self, clusterId, name):
        if self._getStatusOne(clusterId, name) == q.enumerators.AppStatusType.HALTED:
            return None
        
        cmd = 'pgrep -o -f "%s -config %s/%s.cfg --node %s"' % (self._binary,
                                                                self._cfgPath,
                                                                clusterId, name)
        (exitCode, stdout, stderr) = q.system.process.run( cmd )
        if exitCode != 0 :
            return None
        else :
            return int(stdout)
                
    def _getStatusOne(self,clusterId, name):
        cmd = ['pgrep','-f',
               '%s -config %s/%s.cfg --node %s' %
               (self._binary, self._cfgPath, clusterId, name)]
        ret = subprocess.call(cmd,close_fds=True, stdout=subprocess.PIPE)
        result = q.enumerators.AppStatusType.HALTED
        if ret == 0:
            result = q.enumerators.AppStatusType.RUNNING

        return result

    @staticmethod
    def getStorageUtilization(clusterId, node=None):
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

        local_nodes = q.config.arakoon.listLocalNodes(clusterId)

        if node is not None and node not in local_nodes:
            raise ValueError(EXC_MSG_NOT_LOCAL_FMT % (node, clusterId))

        def helper(config):
            home = config['home']
            log_dir = config['log_dir']

            files_in_dir = lambda dir_: itertools.ifilter(os.path.isfile,
                (os.path.join(dir_, name) for name in os.listdir(dir_)))
            matching_files = lambda *exts: lambda files: \
                (file_ for file_ in files
                    if any(file_.endswith(ext) for ext in exts))

            tlog_files = matching_files('.tlc', '.tlog','.tlf')
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
                raise NameError('Error: QShell is Not interactive')
            
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
