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

from pymonkey import q,i

from arakoon import Arakoon 
from arakoon.ArakoonProtocol import ArakoonClientConfig
from arakoon.ArakoonProtocol import ArakoonNotFound

class ArakoonClient:
    """
    Arakoon client management
    """
    
    def getClient(self, clusterId):
        """
        Get arakoonclient
        @type clusterId: string
        @param clusterId: cluster for that client
        @return arakoon client object
        """
        nodes = q.config.arakoonnodes.getNodes(clusterId)
        config = ArakoonClientConfig(clusterId, nodes)
        return Arakoon.ArakoonClient(config)
