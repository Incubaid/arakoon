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



from .. import system_tests_common as Common
from arakoon.ArakoonExceptions import *
import arakoon
import logging
import time
import subprocess
from threading import Thread
from nose.tools import *
from Compat import X

@Common.with_custom_setup( Common.setup_3_nodes_mini, Common.basic_teardown )
def test_restart_master_long ():
    scale = 1
    restart_iter_cnt = 10 * scale
    factor = 100 * scale
    def write_loop ():
        Common.iterate_n_times( 100* factor,
                                Common.retrying_set_get_and_delete,
                                failure_max=2*restart_iter_cnt,
                                valid_exceptions=[X.arakoon_client.ArakoonSockNotReadable,
                                                  X.arakoon_client.ArakoonNotFound,
                                                  X.arakoon_client.ArakoonGoingDown
                                                  ] )

    valid_excs = [X.arakoon_client.ArakoonNotFound,
                  X.arakoon_client.ArakoonSocketException,
                  X.arakoon_client.ArakoonGoingDown,
                  X.arakoon_client.ArakoonNoMaster]
    def range_query_loop1 ():
        Common.heavy_range_entries_scenario (200 * factor,
                                             1500,
                                             1000,
                                             valid_exceptions=valid_excs)
    def range_query_loop2 ():
        Common.heavy_range_entries_scenario (210 * factor ,
                                             1500,
                                             1000,
                                             valid_exceptions=valid_excs)
    def range_query_loop3 ():
        Common.heavy_range_entries_scenario (220 * factor,
                                             1500,
                                             1000,
                                             valid_exceptions=valid_excs)
    def range_query_loop4 ():
        Common.heavy_range_entries_scenario (230* factor,
                                             1500,
                                             1000,
                                             valid_exceptions=valid_excs)

    def restart_loop ():
        Common.delayed_master_restart_loop( restart_iter_cnt ,
                                            1.5 * Common.lease_duration )
    global test_failed
    test_failed = False
    logging.info("set_get_deletes done, now heavy scenario")
    Common.create_and_wait_for_thread_list( [restart_loop,
                                             write_loop,
                                             range_query_loop1,
                                             range_query_loop2,
                                             range_query_loop3,
                                             range_query_loop4
    ] )

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
    Common.flush_stores()

    logging.info("going to inspect the running cluster")
    cluster = Common._getCluster()
    Common.inspect_cluster(cluster)
    logging.info("inspection succesful")

    Common.stop_all()
    node_names = Common.node_names

    # node1 and node2 can make progress after node0 was shut down so the assert below can fail
    # Common.assert_last_i_in_sync( node_names[0], node_names[1] )
    Common.assert_last_i_in_sync( node_names[2], node_names[1] )
    # node1 and node2 can make progress after node0 was shut down so the assert below can fail
    # Common.compare_stores( node_names[0], node_names[1] )
    Common.compare_stores( node_names[2], node_names[1] )
    cli.dropConnections()
    logging.info("end of `test_restart_master_long`")
