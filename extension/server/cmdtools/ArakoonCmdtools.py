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

import subprocess
import time

class ArakoonCmdtools:
    def __init__(self):
        self._binary = q.system.fs.joinPaths(q.dirs.appDir, "arakoon", "bin", "arakoond")
        self._cfgFile = q.system.fs.joinPaths(q.dirs.cfgDir, "qconfig", "arakoon.cfg")

    def start(self):
        """
        start all nodes as configured in arakoonservernodes.cfg
        """
        for name in q.config.arakoon.listLocalNodes():
            self._startOne(name)

    def stop(self):
        """
        stop all nodes as configured in arakoonservernodes.cfg
        """
        for name in q.config.arakoon.listLocalNodes():
            self._stopOne(name)


    def restart(self):
        """
        Restart all nodes as configured in arakoonservernodes.cfg
        """
        for name in q.config.arakoon.listLocalNodes():
            self._restartOne(name)

    def getStatus(self):
        """
        Get the status of all nodes as configured in arakoonservernodes.cfg

        @return dict node name -> status (q.enumerators.AppStatusType)
        """
        status = {}

        for name in q.config.arakoon.listLocalNodes():
            status[name] = self._getStatusOne(name)

        return status

    def startOne(self, nodeName):
        """
        Start the node with a given name

        @param nodeName The name of the node
        """
        if not nodeName in q.config.arakoon.listLocalNodes():
            raise Exception("Node %s is not a local node")

        self._startOne(nodeName)

    def stopOne(self, nodeName):
        """
        Stop the node with a given name

        @param nodeName The name of the node
        """
        if not nodeName in q.config.arakoon.listLocalNodes():
            raise Exception("Node %s is not a local node")

        self._stopOne(nodeName)

    def restartOne(self, nodeName):
        """
        Restart the node with a given name

        @param nodeName The name of the node
        """
        if not nodeName in q.config.arakoon.listLocalNodes():
            raise Exception("Node %s is not a local node")

        self._restartOne(nodeName)

    def getStatusOne(self, nodeName):
        """
        Get the status node with a given name

        @param nodeName The name of the node
        """
        if not nodeName in q.config.arakoon.listLocalNodes():
            raise Exception("Node %s is not a local node")

        return self._getStatusOne(nodeName)

    def _startOne(self, name):
        if self._getStatusOne(name) == q.enumerators.AppStatusType.RUNNING:
            return

        ret = subprocess.call(['%s' % self._binary, \
                               '-daemonize', \
                               '-config', \
                               '%s' % self._cfgFile, \
                               '--node',
                               '%s' % name], \
                              close_fds=True)

        if ret != 0:
            raise Exception("Arakoon Daemon %s could not be started" % name)

    def _stopOne(self, name):
        subprocess.call(['pkill', \
                         '-f', \
                         '%s -daemonize -config %s --node %s' % (self._binary, self._cfgFile, name)], \
                        close_fds=True)

        i = 0
        while(self._getStatusOne(name) == q.enumerators.AppStatusType.RUNNING):
            time.sleep(1)
            i += 1

            if i == 10:
                subprocess.call(['pkill', \
                                 '-9', \
                                 '-f', \
                                 '%s -daemonize -config %s --node %s' % (self._binary, self._cfgFile, name)], \
                                close_fds=True)
                break
            else:
                subprocess.call(['pkill', \
                                 '-f', \
                                 '%s -daemonize -config %s --node %s' % (self._binary, self._cfgFile, name)], \
                            close_fds=True)
    def _restartOne(self,name):
        self._stopOne(name)
        self._startOne(name)

    def _getStatusOne(self,name):
        ret = subprocess.call(['pgrep', \
                               '-f', \
                               '%s -daemonize -config %s --node %s' % (self._binary, self._cfgFile, name)], \
                               close_fds=True)

        if ret == 0:
            return q.enumerators.AppStatusType.RUNNING
        else:
            return q.enumerators.AppStatusType.HALTED
