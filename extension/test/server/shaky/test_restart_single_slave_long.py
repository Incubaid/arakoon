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

from .. import system_tests_common as Common

@Common.with_custom_setup( Common.setup_3_nodes_forced_master_normal_slaves, Common.basic_teardown )
def test_restart_single_slave_long ():
    Common.restart_single_slave_scenario( 100, 10000, True )

@Common.with_custom_setup( Common.setup_3_nodes_forced_master, Common.basic_teardown )
def test_restart_single_slave_long_2 ():
    Common.restart_single_slave_scenario( 100, 10000, False )
