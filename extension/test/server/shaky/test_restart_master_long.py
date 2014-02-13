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
nFITNESS FOR A PARTICULAR PURPOSE.

See the GNU Affero General Public License for more details.
You should have received a copy of the
GNU Affero General Public License along with this program (file "COPYING").
If not, see <http://www.gnu.org/licenses/>.
"""

from .. import system_tests_common as Common
from arakoon.ArakoonExceptions import *
import arakoon
import logging
import time
import subprocess
from threading import Thread
from nose.tools import *

@Common.with_custom_setup( Common.default_setup, Common.basic_teardown )
def test_restart_master_long ():
    restart_iter_cnt = 10
    def write_loop ():
        Common.iterate_n_times( 100000,
                                Common.retrying_set_get_and_delete,
                                failure_max=2*restart_iter_cnt,
                                valid_exceptions=[ArakoonSockNotReadable,
                                                  ArakoonNotFound,
                                                  ArakoonGoingDown
                                                  ] )

    def range_query_loop1 ():
        Common.heavy_range_entries_scenario (200000,
                                             1500,
                                             1000)
    def range_query_loop2 ():
        Common.heavy_range_entries_scenario (210000,
                                             1500,
                                             1000)
    def range_query_loop3 ():
        Common.heavy_range_entries_scenario (220000,
                                             1500,
                                             1000)
    def range_query_loop4 ():
        Common.heavy_range_entries_scenario (230000,
                                             1500,
                                             1000)

    def restart_loop ():
        Common.delayed_master_restart_loop( restart_iter_cnt ,
                                            1.5 * Common.lease_duration )
    global test_failed
    test_failed = False
    Common.create_and_wait_for_thread_list( [restart_loop,
                                             write_loop,
                                             range_query_loop1,
                                             range_query_loop2,
                                             range_query_loop3,
                                             range_query_loop4] )

    cli = Common.get_client()
    time.sleep(2.0)
    key = "key"
    value = "value"
    cli.set(key, value)
    set_value = cli.get(key)
    assert_equals(  value, set_value ,
        "Key '%s' does not have expected value ('%s' iso '%s')" % (key, set_value, value) )

    Common.stop_all()
    Common.start_all()
    Common.stop_all()
    node_names = Common.node_names

    Common.assert_last_i_in_sync( node_names[0], node_names[1] )
    Common.assert_last_i_in_sync( node_names[2], node_names[1] )
    Common.compare_stores( node_names[0], node_names[1] )
    Common.compare_stores( node_names[2], node_names[1] )
    cli.dropConnections()
    logging.info("end of `test_restart_master_long`")
