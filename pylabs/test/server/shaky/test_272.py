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
import logging
import time
import subprocess
from threading import Thread
from nose.tools import *


@Common.with_custom_setup(Common.setup_1_node, Common.basic_teardown)
def test_272():
    """
    test_272 : arakoon can go down during log rotation, but you need to have load to reproduce it (eta: 1110s)
    """
    node = Common.node_names[0]
    cluster = Common._getCluster()
    path = cluster._getConfigFileName() + ".cfg"
    logging.info('path=%s', path)
    bench = subprocess.Popen([Common.CONFIG.binary_full_path, '-config', path ,'--benchmark',
                              '-max_n', '100000'])
    time.sleep(10.0) # give it time to get up to speed
    rc = bench.returncode
    if rc != None:
        raise Exception ("benchmark should not have finished yet.")

    for i in range(100):
        Common.rotate_log(node, 1, False)
        time.sleep(0.2)
        # Note:
        # At this point, we expect one node to be running, as well as the
        # benchmark process. The `assert_running_nodes` procedure uses
        # 'pgrep -c' which counts the number of 'arakoon' processes running, so
        # we should pass *2* here, despite only a single node process should be
        # running.
        Common.assert_running_nodes(2)
        rc = bench.returncode
        if rc != None:
            raise Exception ("benchmark should not have stopped")

    Common.assert_running_nodes(2)
    logging.info("now wait for benchmark to finish")
    rc = bench.wait()
    if rc != 0:
        raise Exception("benchmark exited with rc = %s" % rc)
