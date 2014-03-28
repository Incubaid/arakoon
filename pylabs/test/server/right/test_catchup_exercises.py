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


@Common.with_custom_setup(Common.setup_2_nodes_forced_master_mini, Common.basic_teardown)
def test_catchup_exercises():

    def do_one(n, max_wait):
        logging.info("do_one(%i,%f)", n, max_wait)
        Common.iterate_n_times(n, Common.simple_set)
        logging.info("sets done, stopping")
        Common.stop_all()
        logging.info("stopped all nodes")
        nn = Common.node_names[1]
        Common.wipe(nn)

        Common.start_all()
        logging.info("started all")

        counter = 0
        up2date = False
        cli = Common.get_client ()
        while not up2date and counter < max_wait :
            time.sleep( 1.0 )
            counter += 1
            up2date = cli.expectProgressPossible()
            logging.info("up2date=%s", up2date)

        cli.dropConnections()
        if counter >= max_wait :
            raise Exception ("Node did not catchup in a timely fashion")

    n = 2000
    w = 8 # 250/s should be more than enough even for dss driven vmachines
    for i in range(5):
        do_one(n,w)
        n = n * 2
        w = w * 2
