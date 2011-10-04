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
                
    def migrate(self, leftCluster, separator, rightCluster):
        cmd = self.__getBaseCmd()
        cmd += "--nursery-migrate %s %s %s " % (leftCluster, separator, rightCluster)
        cmd += self.__getConfigCmdline()
        self.__runCmd ( cmd )
    
    def initialize(self, firstClusterId):
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

    