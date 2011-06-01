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

"""
Arakoon Cluster Setup
"""



try:
    from pylabs.InitBase import q
except ImportError:
    from pymonkey.InitBase import q

import sys
import optparse

parser = optparse.OptionParser()
parser.add_option("-s", "--stop", dest="stop", action="store_true", default=False)
parser.add_option("-p", "--port", dest="port", type="int", default=15500)
parser.add_option("-c", "--cluster", dest="cluster", default="phpclient")
(options, args) = parser.parse_args()

if options.stop:
    clu = q.manage.arakoon.getCluster(options.cluster)
    if "%s_0"%(options.cluster) in clu.getStatus():
        clu.tearDown()
        print "Cluster tear down successfully!"
    else:
        print "Cluster is not running!"
else:
    clu = q.manage.arakoon.getCluster(options.cluster)
    if "%s_0"%(options.cluster) not in clu.getStatus():
        clu.setUp(1, options.port)
        clu.start()
        print "Arakoon Server started!"
    else:
        print "Cluster Already running!"

