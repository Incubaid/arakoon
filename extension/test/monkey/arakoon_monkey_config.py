'''
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
'''

from arakoon.ArakoonExceptions import *
from system_tests_common import *
from right.system_tests_anomaly import *
from quick.system_tests_basic import *

import arakoon

AM_WORK_LIST_MAX_ITEMS = 10
AM_MAX_WORKITEM_OPS = 100000


def dummy():
    pass 

monkey_catalogue = [
    ( set_get_and_delete, True ) ,
    ( prefix_scenario, False ) ,
    ( range_scenario, False ) ,
    ( range_entries_scenario, False ) ,
    #( tes_and_set_scenario, False )
    ]


network_error_regexes = [
    ArakoonSockReadNoBytes._msg,
    ArakoonSockNotReadable._msg,
    ArakoonSockRecvError._msg,
    ArakoonSockRecvClosed._msg,
    ArakoonSockSendError._msg 
    ]

restart_error_regexes = [
    ArakoonNoMaster._msg ,
    ArakoonNodeNotMaster._msg
    ]

restart_error_regexes.extend( network_error_regexes )

monkey_disruptions_catalogue = [
#    ( delayed_restart_1st_node, restart_error_regexes ) ,
#    ( delayed_restart_2nd_node, restart_error_regexes ) ,
#    ( delayed_restart_3rd_node, restart_error_regexes ) ,
#    ( delayed_restart_all_nodes, restart_error_regexes ) ,
#    ( iterate_block_unblock_single_slave, restart_error_regexes ) ,
#    ( iterate_block_unblock_both_slaves, restart_error_regexes ) ,
#    ( iterate_block_unblock_master, restart_error_regexes ) ,
    ( dummy, [] )
    ]

