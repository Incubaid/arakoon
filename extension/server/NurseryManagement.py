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

try:
    from pymonkey import q
except ImportError:
    from pylabs import q

ARAKOON_BINARY = "/opt/qbase3/apps/arakoon/bin/arakoon "

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
        cfg = q.config.getConfig( "arakoonclusters" )
        if cfg.has_key( clusterId ):
            return "%s/%s.cfg" % (cfg[ clusterId ]["path"], clusterId)
        else:
            raise RuntimeError("Unknown nursery cluster %s" % clusterId)
           
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
        cmd += "--nursery-migrate %s %s %s " % (leftClusterId, separator, rightClusterId)
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
        cmd += "--nursery-init %s " % firstClusterId
        cmd += self.__getConfigCmdline()
        self.__runCmd ( cmd )
        
    def __runCmd(self, cmd):
        (exit, stdout, stderr) = q.system.process.run( commandline = cmd, stopOnError=False)
        if exit :
            raise RuntimeError( stderr )
        
    def __getConfigCmdline(self):
        return "-config %s " % self._keeperCfg
        
    def __getBaseCmd(self):
        return ARAKOON_BINARY

    