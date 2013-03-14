# This file is part of Arakoon, a distributed key-value store
#
# Copyright (C) 2013, Incubaid BVBA
#
# Licensees holding a valid Incubaid license may use this file in
# accordance with Incubaid's Arakoon commercial license agreement. For
# more information on how to enter into this agreement, please contact
# Incubaid (contact details can be found on www.arakoon.org/licensing).
#
# Alternatively, this file may be redistributed and/or modified under
# the terms of the GNU Affero General Public License version 3, as
# published by the Free Software Foundation. Under this license, this
# file is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# See the GNU Affero General Public License for more details. You should
# have received a copy of the # GNU Affero General Public License along
# with this program (file "COPYING").
#
# If not, see <http://www.gnu.org/licenses/>.

'''
Demonstration wrapper script for Arakoon node launching and error handling

The handlers are based on Amplidata's 'ARA002' operations procedures.

Note this script can not be used as-is!
'''

import os
import os.path
import sys
import logging
import subprocess

# Note: when using the Pylabs extension and its 'wrapper script' support, the
# required executable path & arguments are passed to the 'wrapper script' as
# arguments, so the script should pick these up, remove any unwanted flags (e.g.
# '-daemonize'), and use what's left, instead of these 
EXECUTABLE = os.path.join(os.getcwd(), 'arakoon.native')
ARGS = [EXECUTABLE,
        '--node', 'arakoon_0',
       ]


LOGGER = logging.getLogger(__name__)
HANDLERS = {}
DO_RESTART = object()

# Actions to resolve error scenarios
def move_db_aside():
    '''Move TC database aside'''

    LOGGER.info('Moving DB aside')
    raise NotImplementedError

def move_db_wal_aside():
    '''Move TC WAL aside'''

    LOGGER.info('Moving DB WAL aside')
    raise NotImplementedError

def move_tlogs_aside():
    '''Move tlogs aside'''

    LOGGER.info('Moving tlogs aside')
    raise NotImplementedError

def copy_tlogs_from_other_node():
    '''Copy tlogs from another node'''

    LOGGER.info('Copying tlogs from other node')
    raise NotImplementedError

def mark_tlog_valid():
    '''Mark a tlog as valid

    This should be done using the Arakoon binary and the '--mark-tlog' flag.
    '''

    LOGGER.info('Marking tlogs as valid')
    raise NotImplementedError


# Handlers for specific exit codes
def handle_success():
    '''Handler for successful exit'''

    LOGGER.info('Node process quit successfully')

    # Don't restart
    return False
HANDLERS[0] = handle_success

def handle_store_ahead_of_tlogs():
    '''Handler for 'store ahead of tlogs' errors'''

    LOGGER.error('Store ahead of tlogs, fixing')

    move_db_aside()
    move_db_wal_aside()

    # Restart process
    return True
HANDLERS[40] = handle_store_ahead_of_tlogs

def handle_store_counter_too_low():
    '''Handler for 'store counter too low' errors'''

    LOGGER.error('Store counter too low, fixing')

    move_db_aside()
    move_db_wal_aside()
    move_tlogs_aside()

    copy_tlogs_from_other_node()

    # Restart
    return True
HANDLERS[41] = handle_store_counter_too_low

def handle_tlc_not_properly_closed():
    '''Handler for 'tlc not properly closed' errors'''

    LOGGER.error('TLC not properly closed')

    mark_tlog_valid()

    # Restart
    return True
HANDLERS[42] = handle_tlc_not_properly_closed

def handle_invalid_tlog_dir():
    '''Handler for 'invalid tlog dir' errors'''

    LOGGER.error('Invalid tlog dir in configuration')

    # Can't handle this case automatically
    return False
HANDLERS[43] = handle_invalid_tlog_dir

def handle_invalid_home_dir():
    '''Handler for 'invalid home dir' errors'''

    LOGGER.error('Invalid home dir in configuration')

    # Can't handle this case automatically
    return False
HANDLERS[44] = handle_invalid_home_dir


# Process launcher and exit code handling
def launch(previous_res):
    '''Single iteration for launching a node process and handling its exit'''

    LOGGER.info('Launching node subprocess')
    proc = subprocess.Popen(
        args=ARGS,
        executable=ARGS[0],
        close_fds=True
    )

    #pylint: disable-msg=E1101
    LOGGER.debug('Subprocess running, pid %d', proc.pid)
    #pylint: enable-msg=E1101

    # Depending on the way this thing is started, a
    #
    # deamonize()
    #
    # might be required somewhere in here, but obviously only during the first
    # execution of this procedure!

    LOGGER.info('Waiting for subprocess to exit')
    ret = proc.wait() #pylint: disable-msg=E1101
    LOGGER.info('Subprocess quit, rc=%d', ret)

    if ret == previous_res:
        LOGGER.fatal('Got exit code %d twice in a row, giving up', ret)
        return ret

    handler = HANDLERS.get(ret, None)

    if handler is None:
        LOGGER.fatal('No handler for exit code %d', ret)
        do_restart = False
    else:
        LOGGER.info('Running handler for exit code %d', ret)
        do_restart = handler()

    if do_restart:
        LOGGER.info('Handler requested restart')
        # Note: we don't call 'launch' again here, since that'd introduce an
        # ever-growing stack. This hack pushed the tail-call back into the stack
        # frame above ours. Trampoline-style!
        return DO_RESTART

    return ret

# Process launch & restart loop
def main():
    '''Main entry point'''

    LOGGER.info('Starting Arakoon node launcher')

    res = DO_RESTART

    while res is DO_RESTART:
        res = launch(res)

    LOGGER.info('Not restarting, rc=%d', res)

    return res


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    res = main()

    sys.exit(res)
