"""
This file is part of Arakoon, a distributed key-value store. Copyright
(C) 2010 Incubaid BVBA

Licensees holding a valid Incubaid license may use this file in
accordance with Incubaid's Arakoon commercial license agreement. For
more information on how to enter into this agreement, please contact
Incubaid (contact details can be found on www.arakoon.org/licensing).

Alternatively, this file may be redistributed and/or modified under
the terms of the GNU Affero General Public License version 3, as
published by the Free Software Foundation. Under this license, this
file is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
nFITNESS FOR A PARTICULAR PURPOSE.

See the GNU Affero General Public License for more details.
You should have received a copy of the
GNU Affero General Public License along with this program (file "COPYING").
If not, see <http://www.gnu.org/licenses/>.
"""

from .. import system_tests_common as Common
import logging
from nose.tools import *
import time
import re


@Common.with_custom_setup(Common.setup_3_nodes, Common.basic_teardown)
def test_restart_propose_master():
    """
    test_restart_propose_master : asserts that upon restarting nodes, old master gets re-elected

    Test verifies that upon restarting nodes in the cluster, only the old master node will propose
    itself as master until the the first lease duration expires. Slave nodes should not propose
    becoming a master for the first lease period.
    """
    node_names = Common.node_names[:3]
    lease_duration = Common.lease_duration
    initial_master = Common.get_client().whoMaster()
    logging.info('initial master is %s' % initial_master)

    logging.info('stop all nodes')
    Common.stop_all()

    logging.info('rotating logs')
    for node_name in node_names:
        Common.rotate_log(node_name, max_logs_to_keep=1, compress_old_files=False)

    logging.info('starting up all nodes')
    Common.start_all()

    logging.info('sleeping for 1 lease_duration (%d)' % lease_duration)
    time.sleep(lease_duration)

    master = Common.get_client().whoMaster()
    logging.info('new master is %s' % master)
    assert_equals(initial_master, master, 'Master node has changed after restart from %s to %s.' %
                  (initial_master, master))

    for node_name in node_names:
        expected_pattern_found = False
        is_master_node = node_name == master

        # Participate in master elections n>0
        propose_master_pattern = '.* sending msg to [\w_]+: Prepare\(\d.*'
        # Do not participate in master elections n<0
        fake_prepare_pattern = '.* sending msg to [\w_]+: Prepare\(-\d.*'

        if node_name == initial_master:
            # For original master node, it should propose it self as master
            # sending the largest value for n in Prepare(n,v)
            expected_pattern = propose_master_pattern
            unexpected_pattern = fake_prepare_pattern
        else:
            # For slave nodes, it should not propose it self as master
            # sending a small value for n in Prepare(n,v) so that it does not win
            expected_pattern = fake_prepare_pattern
            unexpected_pattern = propose_master_pattern

        # Check the log file for each node to see if it will propose itself as master
        log_file = open(Common.get_node_log_file(node_name), 'r')
        lines_read = log_file.readlines()
        for line in lines_read:
            assert_false(re.match(unexpected_pattern, line), ('Found the unexpected log line %s '
                                                              'for node %s(master=%s)' %
                                                              (line, node_name, is_master_node)))
            if re.match(expected_pattern, line):
                expected_pattern_found = True

        assert_true(expected_pattern_found, ('Could not find the expected pattern %s for node %s('
                                             'master=%s)' % (expected_pattern, node_name,
                                                             is_master_node)))
