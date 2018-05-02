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



from Compat import X
from .. import system_tests_common as C
from nose.tools import *
import logging, time

@C.with_custom_setup(C.setup_2_nodes_forced_master_mini, C.basic_teardown)
def test_copy_db_to_head():
    # fill cluster until they have at least 10 tlogs
    C.iterate_n_times(5000, C.set_get_and_delete)

    slave = C.node_names[1]
    # n < 1 fails
    assert_raises( Exception, lambda: C.copyDbToHead(slave, 0))
    # fails on master
    assert_raises( Exception, lambda: C.copyDbToHead(C.node_names[0], 2))

    C.copyDbToHead(slave, 1)

    C.stop_all()

    (home_dir, _, tlf_dir, head_dir) = C.build_node_dir_names(slave)
    tlogs_count = len(X.listFilesInDir( home_dir, filter="*.tlog" ))
    tlf_count = len(X.listFilesInDir( tlf_dir, filter="*.tlf" ))
    assert(tlogs_count + tlf_count < 5)
    assert(X.fileExists(head_dir + "/head.db"))
    a = C.get_i(slave, True)
    logging.info("slave_head_i='%s'", a)
    assert(a >= 5000)

@C.with_custom_setup(C.setup_2_nodes_mini, C.basic_teardown)
def test_copy_db_to_head2():
    logging.info("test_copy_db_to_head")
    zero = C.node_names[0]
    one = C.node_names[1]
    n = 29876
    C.iterate_n_times(n, C.simple_set)
    logging.info("did %i sets, now copying db to head" % n)
    C.copyDbToHead(one,1)

    head_name = '%s/%s/head/head.db' %(C.data_base_dir, one)

    logging.info(head_name)
    assert_true(X.fileExists(head_name))
    C.stopOne(zero)
    C.wipe(zero)
    C.startOne(zero)
    cli = C.get_client()
    logging.info("cli class:%s", cli.__class__)
    assert_false(cli.expectProgressPossible())
    up2date = False
    counter = 0
    while not up2date and counter < 100:
        time.sleep(1.0)
        counter = counter + 1
        up2date = cli.expectProgressPossible()
    logging.info("catchup from 'collapsed' node finished")
