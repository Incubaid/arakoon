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

from .. import system_tests_common as C

from nose.tools import *
from arakoon_1 import ArakoonExceptions as A1X
from arakoon_1 import ArakoonProtocol as A1P


CONFIG = C.CONFIG
import Compat as X

@C.with_custom_setup(C.setup_1_node, C.basic_teardown)
def test_hello():
   protocol_version = 1
   client = C.get_client(protocol_version)
   r = client.hello('sooky')
   X.logging.debug('r=%s', r)

@C.with_custom_setup(C.setup_1_node, C.basic_teardown)
def test_who_master():
   protocol_version = 1
   client = C.get_client(protocol_version)
   m = client.whoMaster()
   assert_equals(m,CONFIG.node_names[0])

@C.with_custom_setup(C.setup_1_node, C.basic_teardown)
def test_crud():
   protocol_version = 1
   client = C.get_client(protocol_version)
   key = 'x'
   value = 'X'
   client.set(key,value)
   v2 = client.get(key)
   assert_equals(value,v2)
   client.delete(key)
   e = client.exists(key)
   assert_equals(e,False)
   assert_raises(A1X.ArakoonNotFound,client.get,key)

@C.with_custom_setup(C.setup_1_node, C.basic_teardown)
def test_asserts():
   protocol_version = 1
   client = C.get_client(protocol_version)
   client.set('x','X')
   try:
      client.aSSert('x','X')
   except A1X.ArakoonException as ex:
      X.logging.error("not good: %s" % ex)
      assert_equals(True,False)

   X.logging.debug('second variation')
   assert_raises( A1X.ArakoonAssertionFailed, client.aSSert, 'x', None)

   X.logging.debug('third variation')
   ass = A1P.Assert('x','X')
   seq = A1P.Sequence()
   seq.addUpdate(ass)
   client.sequence(seq)


@C.with_custom_setup( C.setup_1_node, C.basic_teardown )
def test_prefix():
    C.prefix_scenario(1000, protocol_version = 1)

@C.with_custom_setup( C.setup_1_node, C.basic_teardown )
def test_test_and_set():
   client = C.get_client(protocol_version = 1)
   v2 = client.testAndSet('x',None,'X')
   assert_equals(v2, None)
   X.logging.debug("test_and_set None 'X' OK")
   v3 = client.testAndSet('x','X','X3')
   assert_equals(v3, 'X')
   v4 = client.testAndSet('x','X2','X4')
   assert_equals(v4, 'X3')


@C.with_custom_setup( C.setup_1_node, C.basic_teardown )
def test_statistics():
   client = C.get_client(protocol_version = 1)
   stats = client.statistics()
   X.logging.debug("stats = %s", stats)

@C.with_custom_setup(C.setup_1_node, C.basic_teardown)
def test_multiget():
   client = C.get_client(protocol_version = 1)
   client.set('x','X')
   client.set('y','Y')
   vs = client.multiGet(['x','y'])
   assert_equals(vs,['X','Y'])
   
