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


@Common.with_custom_setup(Common.setup_3_nodes_forced_master_slow_collapser, Common.basic_teardown)
def test_collapse_slowdown():
    """ Test wether collapse_slowdown really slows stuff down """
    slow_slave = Common.node_names[1]
    fast_slave = Common.node_names[2]
    n = 298765
    logging.info("going to do %i sets to fill tlogs", n)
    Common.iterate_n_times(n, Common.simple_set)
    logging.info("Did %i sets, now going into collapse scenario", n)

    t0 = time.time()
    logging.info('Collapsing slow slave')
    rc = Common.collapse(slow_slave, 1)
    assert rc == 0
    t1 = time.time()
    slow_duration = t1 - t0
    logging.info('slow collapse took %f', slow_duration)

    t0 = time.time()
    logging.info('Collapsing slow slave')
    rc = Common.collapse(fast_slave, 1)
    assert rc == 0
    t1 = time.time()
    normal_duration = t1 - t0
    logging.info('normal collapse took %f', normal_duration)

    assert 3 * normal_duration < slow_duration < 20 * normal_duration
