"""
"""

from .. import system_tests_common as Common
from arakoon.ArakoonExceptions import *
from Compat import X
from nose.tools import *
import logging
import time
import subprocess

@Common.with_custom_setup(Common.setup_3_nodes, Common.basic_teardown)
def test_issue_93():
    pass
    """
1. Startup 3 node arakoons
2. Put alot of data in it
3. Stop 1 node
4. Check master
5. wipe node 1
6. start node 1 (rebuild starts)
7. Check master (lost master here)
"""
    def launch_bench(n, output, ks = 20):
        cluster = Common._getCluster()
        path = cluster._getConfigFileName() + ".cfg"
        f = open(output, 'wb')
        bench = subprocess.Popen([Common.CONFIG.binary_full_path,
                                  '-config', path ,'--benchmark',
                                  '-scenario','master, set_tx',
                                  '-key_size', str(ks),
                                  '-tx_size', '200',
                                  '-value_size', '400',
                                  '-max_n', str(n)],
                                 stdout=f,
                                 stderr=f)
        return (bench, f)
    ## 2
    n = 1180 * 1000


    t0 = time.time()
    bench, f = launch_bench(n, './bench_output.txt')
    bench.wait()
    t1 = time.time()
    ## 3
    zero = Common.node_names[0]
    Common.stopOne(zero)

    ## 4
    cli = Common.get_client()
    cli.nop()

    master0 = cli.whoMaster()

    ## 5
    Common.wipe(zero)

    ## 6
    Common.startOne(zero)

    ## 7


    #### (while rebuilding, put new load.)
    bench2, f2 = launch_bench(n, './bench_output2.txt', ks = 21 ) # make it grow
    d = t1 - t0
    d2 = int(2 * d)
    for i in xrange(d2):
        master = cli.whoMaster()
        assert_true(master == master0)
        logging.debug("master:%s", master)
        time.sleep(1.0)
        cli.nop()
    bench2.wait()
    Common.assert_running_nodes(3)
