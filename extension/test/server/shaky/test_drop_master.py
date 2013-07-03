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
FITNESS FOR A PARTICULAR PURPOSE.

See the GNU Affero General Public License for more details.
You should have received a copy of the
GNU Affero General Public License along with this program (file "COPYING").
If not, see <http://www.gnu.org/licenses/>.
"""

from .. import system_tests_common as Common
from arakoon.ArakoonExceptions import *
import arakoon
import logging
import time
import subprocess
from nose.tools import *
import os
import random
from threading import Thread, Condition


def drop_master(n):
    cli = Common.get_client()
    previousMaster = cli.whoMaster()
    for i in range(n):
        logging.info("starting iteration %i", i)
        Common.dropMaster(previousMaster)
        cli._masterId = None
        master = cli.whoMaster()
        assert_not_equals(master, previousMaster, "Master did not change after drop master request.")
        previousMaster = master
        logging.info("finished iteration %i", i)

@Common.with_custom_setup( Common.setup_3_nodes, Common.basic_teardown)
def test_drop_master():
    n = 10
    drop_master(n)
"""    drop_masters = lambda : drop_master(n)
    Common.create_and_wait_for_threads ( 1, 1, drop_masters, n*8*3 )"""


def _test_drop_master_with_load_(client):
    global busy, excs

    n = 10

    busy = True
    excs = []
    cv = Condition()

    t0 = Thread(target = lambda : client(0))
    t0.start()
    t1 = Thread(target = lambda : client(1))
    t1.start()
    t2 = Thread(target = lambda : client(2))
    t2.start()
    t3 = Thread(target = lambda : client(3))
    t3.start()

    def inner_drop_master():
        global busy
        try :
            drop_master(n)
            cv.acquire()
            busy = False
            cv.notify()
            cv.release()
        except Exception, ex:
            excs.append(ex)
            cv.acquire()
            busy = False
            cv.notify()
            cv.release()
            raise ex


    t_drop = Thread(target = inner_drop_master)
    t_drop.start()

    cv.acquire()
    cv.wait(400.0)

    assert_false( busy ) # test should be finished by now and is probably hanging
    if busy:
        busy = False
        time.sleep(10) # give the client threads some time to finish
        assert_false( True ) # test should be finished by now and is probably hanging

    if len(excs) <> 0:
        raise excs[0]

@Common.with_custom_setup( Common.setup_3_nodes, Common.basic_teardown)
def test_drop_master_with_load():
    def client(n):
        global excs, busy
        try :
            while busy:
                Common.iterate_n_times( 100, Common.simple_set, startSuffix = n * 100 )
        except Exception, ex:
            excs.append(ex)
            raise ex

    _test_drop_master_with_load_(client)


@Common.with_custom_setup( Common.setup_3_nodes, Common.basic_teardown)
def test_drop_master_with_load_and_verify():
    def client(n):
        global excs, busy
        try :
            while busy:
                Common.iterate_n_times( 100, Common.retrying_set_get_and_delete, startSuffix = n * 100 )
        except Exception, ex:
            excs.append(ex)
            raise ex

    _test_drop_master_with_load_(client)
