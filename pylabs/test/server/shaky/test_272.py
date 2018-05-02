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
from Compat import X
import logging
import time
import subprocess
from threading import Thread
from nose.tools import *
import os
import signal

@Common.with_custom_setup(Common.setup_1_node, Common.basic_teardown)
def test_272():
    pass
    """
    test_272 : arakoon can go down during log rotation, but you need to have load to reproduce it 
    """
    node = Common.node_names[0]
    cluster = Common._getCluster()
    path = cluster._getConfigFileName() + ".cfg"
    logging.info('path=%s', path)

    f = open("./outputFile","wb")
    bench = subprocess.Popen([Common.CONFIG.binary_full_path,
                              '-config', path ,'--benchmark',
                              '-scenario','master, set, set_tx, get',
                              '-max_n', '60000'], stdout=f,stderr=f)

    time.sleep(10.0) # give it time to get up to speed
    rc = bench.returncode
    if rc <> None:
        raise Exception ("benchmark should not have finished yet.")


    cfg = Common.getConfig(node)
    log_dir = cfg['log_dir']
    log_file = '/'.join([log_dir, "%s.log" % node ])

    def target(i):
        fn = "%s.%03i" % (log_file,i)
        return fn

    for i in xrange(100):
        new_file = target(i)
        print "%s => %s" % (log_file, new_file)
        count = 0
        while not X.fileExists(log_file) and count < 10:
            print "%s does not exist" % log_file
            time.sleep(0.2)
            count += 1
            
        os.rename (log_file, new_file)
        Common.send_signal(node,signal.SIGUSR1)
        Common.assert_running_nodes(1)
        rc = bench.returncode
        if rc <> None:
            raise Exception ("benchmark should not have stopped")

    Common.assert_running_nodes(1)
    logging.info("now wait for benchmark to finish")
    rc = bench.wait()
    f.close()
    Common.assert_running_nodes(1)
    if rc <> 0:
        raise Exception("benchmark exited with rc = %s" % rc)

    # now check there are no holes.

    seqn = -1
    for i in xrange(100):
        fn = target(i)
        with open(fn,'r') as f:
            lines = f.readlines()
            for line in lines:
                try:
                    parts =  line.split(" - ")
                    s = parts[4]
                    seqn_next = int(s)
                    assert_equals(seqn_next, seqn + 1,
                                  msg = "%s:sequence numbers do not follow: %s;%s" % (fn, seqn , seqn_next))
                    seqn = seqn_next
                except Exception as ex:
                    logging.info("Error while parsing line %s" % line)
                    raise ex
    logging.info("last seqn:%i" % seqn)
