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
from nose.tools import *
import os
import random

@Common.with_custom_setup (Common.setup_3_nodes_forced_master,
                           Common.basic_teardown)
def test_db_defrag_witness_node():
    assert_raises( Exception, Common.defragDb, Common.node_names[0])
    assert_raises( Exception, Common.defragDb, Common.node_names[1])

@Common.with_custom_setup (Common.setup_3_nodes_forced_master_normal_slaves,
                           Common.basic_teardown)
def test_db_defrag():
    """
    test_db_defrag : asserts the defrag call works, and actually shrinks the database (eta: 650s)
    """
    assert_raises( Exception, Common.defragDb, Common.node_names[0] )
    client = Common.get_client()
    a = 16807
    m = 2147483647
    seed = 1
    q = m / a
    r = m % a
    for i in xrange(100 * 1000):
        hi = seed / q
        lo = seed % q
        test = a * lo - r * hi
        if test > 0:
            seed = test
        else:
            seed = test + m
        key = seed
        key_s = "key_%09i" % key
        vs = random.randint(113, 257)
        v = "xxxxxxxxxx" * vs
        client.set(key_s,key_s)
        client.set(key_s,v)
        #print i, key_s, vs
        if i == 65537:
            seed = 1

    slave = Common.node_names[1]
    print slave
    db_file = Common.get_node_db_file( slave)
    start_size = os.path.getsize( db_file )
    print "start_size=", start_size
    Common.defragDb(Common.node_names[1]) 
    opt_size = os.path.getsize(db_file)
    template = "Size did not shrink (enough). Original: '%d'. Optimized: '%d'." 
    msg = template % (start_size, opt_size) 
    assert_true( opt_size < 0.9 * start_size, msg)
