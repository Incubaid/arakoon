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

import system_tests_common as Common
from nose.tools import *
from pymonkey import q
from arakoon.ArakoonExceptions import *
import logging


@Common.with_custom_setup(Common.setup_1_node, Common.basic_teardown)
def test_read_only():
   cluster = Common._getCluster()
   client = Common.get_client()
   v = 'XXX'
   client ['xxx']= v
   cluster.stop()
   cluster.setReadOnly()
   cluster.start()
   assert_raises(ArakoonException, client.set, 'xxx','yyy')
   xxx = client['xxx']
   assert_equals(xxx,v)
   
