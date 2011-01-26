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

class ArakoonCmdtools:
    def __init__(self):
        self._binary = q.system.fs.joinPaths(q.dirs.appDir, "arakoon", "bin", "arakoond")
        self._cfgPath = q.system.fs.joinPaths(q.dirs.cfgDir, "qconfig")

    def start(self, cluster = "arakoon"):
        """
        start all nodes as configured in arakoonservernodes.cfg
        
        @param cluster the arakoon cluster name
        """
        for name in q.config.arakoon.listLocalNodes(cluster = cluster):
            self._startOne(name, cluster = cluster)

    def stop(self, cluster = "arakoon"):
        """
        stop all nodes as configured in arakoonservernodes.cfg
        
        @param cluster the arakoon cluster name
        """
        for name in q.config.arakoon.listLocalNodes(cluster = cluster):
            self._stopOne(name, cluster = cluster)


    def restart(self, cluster = "arakoon"):
        """
        Restart all nodes as configured in arakoonservernodes.cfg
        
        @param cluster the arakoon cluster name
        """
        for name in q.config.arakoon.listLocalNodes(cluster = cluster):
            self._restartOne(name, cluster = cluster)

    def getStatus(self, cluster = "arakoon"):
        """
        Get the status of all nodes as configured in arakoonservernodes.cfg

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
            raise Exception("Node %s is not a local node")

        self._startOne(nodeName, cluster = cluster)

    def stopOne(self, nodeName, cluster = "arakoon"):
        """
        Stop the node with a given name

        @param nodeName The name of the node
        @param cluster the arakoon cluster name
        """
        if not nodeName in q.config.arakoon.listLocalNodes(cluster = cluster):
            raise Exception("Node %s is not a local node")

        self._stopOne(nodeName, cluster = cluster)

    def restartOne(self, nodeName, cluster = "arakoon"):
        """
        Restart the node with a given name

        @param nodeName The name of the node
        @param cluster the arakoon cluster name
        """
        if not nodeName in q.config.arakoon.listLocalNodes(cluster = cluster):
            raise Exception("Node %s is not a local node")

        self._restartOne(nodeName, cluster = cluster)

    def getStatusOne(self, nodeName, cluster = "arakoon"):
        """
        Get the status node with a given name

        @param nodeName The name of the node
        @param cluster the arakoon cluster name
        """
        if not nodeName in q.config.arakoon.listLocalNodes(cluster = cluster):
            raise Exception("Node %s is not a local node")

        return self._getStatusOne(nodeName, cluster = cluster)

    def _startOne(self, name, cluster = "arakoon"):
        if self._getStatusOne(name, cluster = cluster) == q.enumerators.AppStatusType.RUNNING:
            return

        ret = subprocess.call(['%s' % self._binary, \
                               '-daemonize', \
                               '-config', \
                               '%s/%s.cfg' % (self._cfgPath, cluster), \
                               '--node',
                               '%s' % name], \
                              close_fds=True)

        if ret != 0:
            raise Exception("Arakoon Daemon %s could not be started" % name)

    def _stopOne(self, name, cluster = "arakoon"):
        subprocess.call(['pkill', \
                         '-f', \
                         '%s -daemonize -config %s/%s.cfg --node %s' % (self._binary, self._cfgPath, cluster, name)], \
                        close_fds=True)

        i = 0
        while(self._getStatusOne(name, cluster = cluster) == q.enumerators.AppStatusType.RUNNING):
            time.sleep(1)
            i += 1

            if i == 10:
                subprocess.call(['pkill', \
                                 '-9', \
                                 '-f', \
                                 '%s -daemonize -config %s/%s.cfg --node %s' % (self._binary, self._cfgPath, cluster, name)], \
                                close_fds=True)
                break
            else:
                subprocess.call(['pkill', \
                                 '-f', \
                                 '%s -daemonize -config %s/%s.cfg --node %s' % (self._binary, self._cfgPath, cluster, name)], \
                            close_fds=True)
    def _restartOne(self,name, cluster = "arakoon"):
        self._stopOne(name, cluster = cluster)
        self._startOne(name, cluster = cluster)

    def _getStatusOne(self,name, cluster = "arakoon"):
        ret = subprocess.call(['pgrep', \
                               '-f', \
                               '%s -daemonize -config %s/%s.cfg --node %s' % (self._binary, self._cfgPath, cluster, name)], \
                               close_fds=True, \
                               stdout=subprocess.PIPE)

        if ret == 0:
            return q.enumerators.AppStatusType.RUNNING
        else:
            return q.enumerators.AppStatusType.HALTED

    @staticmethod
    def getStorageUtilization(node=None, cluster = "arakoon"):
        '''Calculate and return the disk usage of Arakoon on the system

        When no node name is given, the aggregate consumption of all nodes
        configured on the system is returned.

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
            raise ValueError('No such local node')

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
