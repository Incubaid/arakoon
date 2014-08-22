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



import time
import shutil

from .. import system_tests_common as C
from nose.tools import *

from Compat import X

"""
@C.with_custom_setup(C.setup_3_nodes, C.basic_teardown)
def test_diverge ():
    C.iterate_n_times(100, C.simple_set)
    C.stop_all()

    C.remove_node(1)
    C.remove_node(2)
    C.regenerateClientConfig(C.cluster_id)
    C.start_all()
    C.iterate_n_times(5, C.simple_set, 100)
    C.stop_all()

    C.remove_node(0)
    C.add_node(1)
    C.add_node(2)
    C.regenerateClientConfig(C.cluster_id)
    C.start_all()
    C.iterate_n_times(10, C.simple_set)
    C.stop_all()

    C.add_node(0)
    C.regenerateClientConfig(C.cluster_id)
    C.start_all()
    time.sleep(3.0)
"""

@C.with_custom_setup(C.setup_2_nodes_forced_master ,C.basic_teardown)
def test_power_failure ():
    cluster = C._getCluster()
    logging.info("")
    C.iterate_n_times(50, C.simple_set)

    for i in range(2):
        node_id = C.node_names[i]
        C.stopOne(node_id)
        home = cluster.getNodeConfig(node_id)['home']
        backup = '/'.join([X.tmpDir, 'backup_' + node_id])
        shutil.copytree(home, backup)
        C.startOne(node_id)

    C.iterate_n_times(10, C.simple_set)
    C.stop_all()

    for i in range(2):
        node_id = C.node_names[i]
        home = cluster.getNodeConfig(node_id)['home']
        backup = '/'.join([X.tmpDir, 'backup_' + node_id])
        shutil.rmtree(home)
        shutil.move(backup, home)
        C.startOne(node_id)

    C.iterate_n_times(10, C.simple_set)
    C.startOne(C.node_names[2])
