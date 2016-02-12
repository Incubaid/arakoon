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
from Compat import X
import arakoon
import time
import logging
from threading import Thread
from nose.tools import *

def _check_tlog_dirs(node, n):
    (home_dir, _, tlf_dir, head_dir) = Common.build_node_dir_names(node)
    
    tlogs = X.listFilesInDir( home_dir, filter="*.tlog" )
    tlxs  = X.listFilesInDir( tlf_dir,  filter="*.tlx" )
    logging.info("tlogs:%s", tlogs)
    logging.info("tlxs:%s", tlxs)
    print tlxs
    
    assert_equals(len(tlogs) + len(tlxs), n,
                  msg = "(%s + %s) should have %i file(s)" % (tlogs,tlxs,n))
    assert_true(X.fileExists(head_dir + "/head.db"))
    logging.info("tlog_dirs are as expected")

@Common.with_custom_setup(Common.setup_2_nodes_forced_master_mini, Common.basic_teardown)
def test_remote_collapse_witness_node():
    master = Common.node_names[0]
    witness = Common.node_names[1]
    n = 29876
    Common.iterate_n_times(n, Common.simple_set)
    logging.info("did %i sets, now going into collapse scenario" % n)
    Common.collapse(witness,1)
    logging.info("collapsing done")
    _check_tlog_dirs(witness,1)
    
    Common.stopOne(master)
    Common.wipe(master)
    Common.startOne(master)
    cli = Common.get_client()
    assert_false(cli.expectProgressPossible())
    up2date = False
    counter = 0
    while not up2date and counter < 100:
        time.sleep(1.0)
        counter = counter + 1
        up2date = cli.expectProgressPossible()
    logging.info("catchup from collapsed node finished")

@Common.with_custom_setup(Common.setup_2_nodes_mini, Common.basic_teardown)
def test_remote_collapse():
    zero = Common.node_names[0]
    one = Common.node_names[1]
    n = 29876
    Common.iterate_n_times(n, Common.simple_set)
    logging.info("did %i sets, now going into collapse scenario" % n)
    Common.collapse(zero,1)
    logging.info("collapsing done")
    _check_tlog_dirs(zero,1)
    
    Common.stopOne(one)
    Common.wipe(one)
    Common.startOne(one)
    cli = Common.get_client()
    assert_false(cli.expectProgressPossible())
    up2date = False
    counter = 0
    while not up2date and counter < 100:
        time.sleep(1.0)
        counter = counter + 1
        up2date = cli.expectProgressPossible()
    logging.info("catchup from collapsed node finished")
    

@Common.with_custom_setup(Common.setup_2_nodes_forced_master_mini, Common.basic_teardown)
def test_local_collapse_witness_node():
    master = Common.node_names[0]
    witness = Common.node_names[1]
    n = 29876
    Common.iterate_n_times(n, Common.simple_set)
    logging.info("did %i sets, now going into collapse scenario" % n)

    Common.local_collapse(witness,1)
    logging.info("collapsing done")
    
    
    Common.stopOne(master)
    Common.wipe(master)
    Common.startOne(master)
    cli = Common.get_client()
    assert_false(cli.expectProgressPossible())
    up2date = False
    counter = 0
    while not up2date and counter < 100:
        time.sleep(1.0)
        counter = counter + 1
        up2date = cli.expectProgressPossible()
    logging.info("catchup from collapsed node finished")

@Common.with_custom_setup(Common.setup_2_nodes_mini, Common.basic_teardown)
def test_local_collapse():
    print "starting test_local_collapse"
    
    zero = Common.node_names[0]
    one = Common.node_names[1]
    n = 29876
    Common.iterate_n_times(n, Common.simple_set)
    logging.info("did %i sets, now going into collapse scenario" % n)
    Common.local_collapse(zero,1)
    #
    logging.info("collapsing done")
    Common.stopOne(one)
    Common.wipe(one)
    Common.startOne(one)
    cli = Common.get_client()
    assert_false(cli.expectProgressPossible())
    up2date = False
    counter = 0
    while not up2date and counter < 100:
        time.sleep(1.0)
        counter = counter + 1
        up2date = cli.expectProgressPossible()
    logging.info("catchup from collapsed node finished")
