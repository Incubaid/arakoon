"""
Copyright (2010-2014) INCUBAID BVBA

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from arakoon.ArakoonExceptions import *
import server.system_tests_common as C
#from right.system_tests_anomaly import *
#from quick.system_tests_basic import *

import arakoon

AM_WORK_LIST_MAX_ITEMS = 10
AM_MAX_WORKITEM_OPS = 100000


def dummy():
    pass

monkey_catalogue = [
    ( C.set_get_and_delete, True ) ,
    ( C.prefix_scenario, False ) ,
    ( C.range_scenario, False ) ,
    ( C.range_entries_scenario, False ) ,
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
