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


class NurseryManagement:
    def getNursery(self, clusterId ):
        """
        Retrieve the nursery manager for the nursery with the provided keeper
        
        @param clusterId:  The keeper of the nursery
        @type clusterId:   string
        
        @rtype:            NurseryManager
        """
        return NurseryManager( clusterId )

    @staticmethod
    def getConfigLocation(clusterId):
        fn = '%s/%s' % (X.cfgDir, 'arakoonclusters')
        cfg = X.getConfig( fn )
        if cfg.has_section( clusterId ):
            return "%s/%s.cfg" % (cfg.get( clusterId, "path"), clusterId)
        else:
            raise RuntimeError("Unknown nursery cluster '%s'" % clusterId)
           
class NurseryManager:
    def __init__(self,clusterId):
        self._keeperCfg = NurseryManagement.getConfigLocation(clusterId)
                
    def migrate(self, leftClusterId, separator, rightClusterId):
        """
        Trigger a migration of key range in the nursery.
        
        Either leftClusterId or rightClusterId must already be part of the nursery. 
        So it is not possible to add two clusters at the same time.
        
        The separator will serve as the boundary of the key range that is migrated.
        
        For more documentation see the docs on our portal (www.arakoon.org)
        
        @param leftClusterId:   The cluster that will be responsible for the key range up to the separator
        @type leftClusterId:    string
        @param separator:       The separator separating the key ranges between the two clusters
        @type separator:        string
        @param rightClusterId:  The cluster that will be responsible for the key range starting with the separator
        @type rightClusterId:   string
        @rtype:                 void
        """
        cmd = self.__getBaseCmd()
        cmd += ["--nursery-migrate", leftClusterId, separator, rightClusterId]
        cmd += self.__getConfigCmdline()
        self.__runCmd ( cmd )
    
    def initialize(self, firstClusterId):
        """
        Initialize the routing of the nursery so it only contains the provided cluster
        
        A nursery can only be initialized once
        
        @param firstClusterId:  The first cluster of the nursery
        @type firstClusterId:   string
        
        @rtype void
        """
        cmd = self.__getBaseCmd()
        cmd += ["--nursery-init", firstClusterId]
        cmd += self.__getConfigCmdline()
        self.__runCmd ( cmd )
    
    def delete(self, clusterId, separator = None):
        """
        Remove a cluster from the nursery. If the cluster is a boundary cluster, no separator can be provided.
        
        @param clusterId:  The cluster to be removed
        @type clusterId:   string
        
        @param separator:  Separator separating the clusters neighbouring the cluster that will be removed (not valid when deleting boundary clusters
        @type separator:   string
        
        @rtype void
        """
        cmd = self.__getBaseCmd()
        cmd += self.__getConfigCmdline()
        cmd += "--nursery-delete %s " % clusterId
        if separator is None:
            cmd +=  '""'
        else :
            cmd += separator
        self.__runCmd( cmd )
        
    def __runCmd(self, cmd):
        X.logging.debug("cmd: %s", cmd)
        try:
            output = X.subprocess.check_output( cmd )
            X.logging.debug("output: %s", output)
        except X.subprocess.CalledProcessError,e:
            X.logging.debug("output: %s", e.output)
            raise RuntimeError(e)
        
        
    def __getConfigCmdline(self):
        return ["-config", self._keeperCfg]
        
    def __getBaseCmd(self):
        path = '%s/arakoon/bin/arakoon' % (X.appDir)
        if X.fileExists(path):
            return [path]
        else:
            return ["arakoon"]
        
