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

@Common.with_custom_setup( Common.setup_3_nodes_forced_master_normal_slaves, Common.basic_teardown )
def test_restart_single_slave_long ():
    Common.restart_single_slave_scenario( 100, 10000, True )

@Common.with_custom_setup( Common.setup_3_nodes_forced_master, Common.basic_teardown )
def test_restart_single_slave_long_2 ():
    Common.restart_single_slave_scenario( 100, 10000, False )
