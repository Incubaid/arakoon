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

def last_slave(master_id, slaves=None):
    if slaves is None:
        slaves = Common.node_names

    slaves = filter(lambda x: x!= master_id, slaves)
    return slaves [-1]


@Common.with_custom_setup(Common.setup_3_nodes, Common.basic_teardown)
def test_shaky_slave():
    cli = Common.get_client()
    master_id = cli.whoMaster()
    slave_id = last_slave(master_id, Common.node_names[:3])
    Common.stopOne(slave_id)
    print ("slave %s stopped" % slave_id)
    n = 200
    Common.iterate_n_times( n, Common.simple_set)
    cycles = 100
    for i in range(cycles):
        print ("starting cycle %i" % i)
        Common.startOne(slave_id)
        Common.iterate_n_times( n, Common.simple_set)
        Common.stopOne(slave_id)
        Common.iterate_n_times( n, Common.simple_set)
    print "phewy!"

@Common.with_custom_setup(Common.setup_3_nodes, Common.basic_teardown)
def test_fat_shaky_slave():
    cli = Common.get_client()
    master_id = cli.whoMaster()
    slave_id = last_slave(master_id, Common.node_names[:3])
    Common.stopOne(slave_id)
    print ("slave %s stopped" % slave_id)
    n = 200
    Common.iterate_n_times( n, Common.simple_set)
    cycles = 10
    for i in range(cycles):
        print ("starting cycle %i" % i)
        Common.startOne(slave_id)
        Common.iterate_n_times( n, Common.simple_set)
        Common.stopOne(slave_id)
        Common.iterate_n_times( n, Common.simple_set)
    print "phewy!"

@Common.with_custom_setup(Common.setup_3_nodes, Common.basic_teardown)
def test_shaky_cluster():
    n = 200
    names = Common.node_names
    def stop_all():
        for node_name in names:
            Common.stopOne(node_name)

    def start_all():
        for node_name in names:
            Common.startOne(node_name)

    for i in range(n):
        Common.restart_all()
        Common.assert_running_nodes(3)
