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

ARAKOON_BINARY = "/opt/qbase3/apps/arakoon/bin/arakoon "

class NurseryManagement:
    
    def __init__(self,clusterId):
        self._clusterId = clusterId
                
    def migrate(self, leftCluster, separator, rightCluster):
        cmd = _getBaseCmd()
        cmd += "--migrate %s %s %s " % (leftCluster, separator, rightCluster)
        cmd += self.__getConfigCmdline()
        q.system.process.run( cmd )
    
    def initialize(self, firstClusterId):
        cmd = _getBaseCmd()
        cmd += "--init %s " % firstClusterId
        cmd += self.__getConfigCmdline()
        q.system.process.run( cmd )
        
    def __getBaseCmd(self):
        return ARAKOON_BINARY
    
    def __getConfigCmdline(self):
        cfg = q.config.getConfig( "arakoonclustsers" )
        if cfg.has_key( self._clusterId ):
            return "-config %s " % ( cfg[ self._clusterId ] )
        else:
            raise RuntimeError("Unknown nursery cluster %s" % self._clusterId)
    