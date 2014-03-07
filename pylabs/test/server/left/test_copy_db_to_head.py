"""
This file is part of Arakoon, a distributed key-value store. Copyright
(C) 2014 Incubaid BVBA

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
from .. import system_tests_common as C
from nose.tools import *
import logging

@C.with_custom_setup(C.setup_2_nodes_forced_master_mini, C.basic_teardown)
def test_copy_db_to_head():
    # fill cluster until they have at least 5 tlogs
    C.iterate_n_times(5000, C.set_get_and_delete)

    slave = C.node_names[1]
    # n < 1 fails
    assert_raises( Exception, lambda: C.copyDbToHead(slave, 0))
    # fails on master
    assert_raises( Exception, lambda: C.copyDbToHead(Common.node_names[0], 2))

    C.copyDbToHead(slave, 1)

    C.stop_all()

    (home_dir, _, _, head_dir) = C.build_node_dir_names(slave)
    assert(len(X.listFilesInDir( home_dir, filter="*.tlog" )) == 1)
    assert(X.fileExists(head_dir + "/head.db"))
    a = C.get_i(slave, True)
    logging.info("slave_head_i='%s'", a)
    assert(a >= 5000)
