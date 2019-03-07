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

import sys

import copy
import re
import logging
import random
import smtplib
import traceback
import threading
import time
import os
import nose.tools as NT

from Compat import X
import server.system_tests_common as C
import arakoon_monkey_config as MConfig
from arakoon.ArakoonProtocol import ArakoonProtocol
from arakoon.ArakoonExceptions import *
MEM_MAX_KB = 1024 * 256
MAX_USED = 0
monkey_dies = False

random.seed(42)

def get_monkey_work_dir() :
    return '/'.join([X.tmpDir,"arakoon_monkey"])

def get_work_list_log_read ( iter_cnt ):
    return get_work_list_log ( iter_cnt, "r")

def get_work_list_log_write ( iter_cnt ):
    return get_work_list_log ( iter_cnt, "w")

def get_work_list_log ( iter_cnt, flag ):
    log_file_name = '/'.join([get_monkey_work_dir() , "arakoon_monkey_%012d.wlist" % iter_cnt] )
    file = open( log_file_name , flag )
    return file

def generate_work_list( iter_cnt ):

    global disruptive_f
    log = get_work_list_log_write( iter_cnt )

    admin_f = random.choice( MConfig.monkey_disruptions_catalogue )
    disruption = "%s\n" %  admin_f[0].func_name
    logging.info( "Disruption for this iteration is: %s" % disruption.strip() )
    log.write ( disruption )
    disruptive_f = admin_f[0]

    work_list_size = random.randint( 2, MConfig.AM_WORK_LIST_MAX_ITEMS)

    f_list = []

    for i in range(work_list_size):
        cat_entry =  random.choice( MConfig.monkey_catalogue )
        fun = cat_entry[0]
        iops = i * MConfig.AM_MAX_WORKITEM_OPS
        if ( cat_entry [1] == True ) :
            random_n = random.randint( 100, MConfig.AM_MAX_WORKITEM_OPS )
            work_item = "%s %d %d\n" % (fun.func_name, random_n, iops)
            logging.info( "Adding work item: %s" % work_item.strip() )
            log.write( work_item )
            tmp_f = C.generate_lambda( C.iterate_n_times, random_n, fun, iops )

        else :
            work_item = "%s %d\n" % (fun.func_name,
                                     iops)
            log.write ( work_item )
            logging.info( "Adding work item: %s" % work_item.strip())
            tmp_f = C.generate_lambda( fun, iops)

        f_list.append( C.generate_lambda( wrapper, tmp_f ) )
    log.close()
    return (disruptive_f, f_list )

def build_function_dict() :
    ret_val = dict()
    for item in MConfig.monkey_catalogue:
        ret_val[ item[0].func_name ] = item

    for item in MConfig.monkey_disruptions_catalogue:
        ret_val[ item[0].func_name ] = item

    return ret_val

f_dict = build_function_dict()
disruptive_f = MConfig.dummy

def wrapper( f ):
    try:
        f()
    except Exception,ex:
        valid_ex = False
        ex_msg = "%s" % ex
        regexes = f_dict[ disruptive_f.func_name ] [1]
        for regex in regexes:
            if re.match( regex, ex_msg ) :
                valid_ex = True
        if not valid_ex :
            global monkey_dies
            monkey_dies = True
            raise
        else :
            logging.fatal( "Wiping exception under the rug (%s: '%s')" ,ex.__class__.__name__, ex_msg )

def play_iteration( iteration ):
    global disruptive_f

    log = get_work_list_log_read( iteration )
    lines = log.readlines()


    thr_list = list()

    for line in lines:
        line = line.strip()
        parts = line.split( " " )
        parts_len = len(parts)
        if parts_len == 1 :
            # thr = create_and_start_thread( f_dict[ parts[0] ][0]  )
            thr = None
            disruptive_f = f_dict [ parts[0].strip() ]
        if parts_len == 2 :
            tmp_f = generate_lambda( f_dict[ parts[0] ][0], int(parts[1] ) )
            thr = create_and_start_thread( generate_lambda(wrapper, tmp_f) )
        if parts_len == 3 :
            tmp_f = generate_lambda( iterate_n_times, int(parts[1]), f_dict[parts[0]][0], int(parts[2]))
            thr = create_and_start_thread( generate_lambda( wrapper, tmp_f ))

        if thr is not None:
            thr_list.append( thr )

    def sleeper():
        logging.info("Going to sleep for 120s")
        time.sleep( 120.0 )
        logging.info("Slept for 120s")

    thr = create_and_start_thread( sleeper )
    thr_list.append(thr)
    disruptive_f ()

    for thr in thr_list:
        thr.join()

_start_time = time.time()
_sentence = float (os.environ.get('SENTENCE',60 * 60 * 4))

def done_time():
    t1 = time.time()
    return  (t1 - _start_time) > _sentence

def wait_for_it () :
    global monkey_dies
    def last_i(nn):
        node_name = C.node_names[nn]
        i_s = C.get_last_i_tlog2(node_name)
        i = int(i_s)
        return i

    def all_i_tlogs():
        return [last_i(nn) for nn in xrange(3)]

    def all_i_stats():
        client = C.get_client()
        stats = client.statistics()
        node_is = stats['node_is']
        all_i = node_is.values()
        return all_i

    def wait_for(method, ms, sleep, start_i):
        logging.info( "Work is done. Waiting for %s to get in sync",ms )
        all_i = method()
        mark = max (all_i)
        min_i = min(all_i)
        min_i = min(min_i, start_i)
        d = abs(mark - min_i)
        period = (float(d) / 500.0) + sleep * 2
        catchup = True
        timeout = False
        logging.info("mark=%i", mark)
        t0 = time.time()
        while catchup:
            time.sleep(sleep)
            all_i = method()
            lowest = min(all_i)
            logging.info("mark=%i; lowest=%i", mark, lowest)
            if lowest >= mark:
                catchup = False
            else:
                t1 = time.time()
                if t1 - t0 > period:
                    catchup = False
                    timeout = True

        if timeout:
            logging.info("took more than %fs failing", period)
            monkey_dies = True
            raise Exception("timeout")
        else:
            C.assert_running_nodes(3)
        return min_i

    min_i = 1000000000 # larger than anything we will encounter
    if not monkey_dies:
        min_i = wait_for(all_i_tlogs, "tlogs", 10.0, min_i)

    if not monkey_dies:
        wait_for(all_i_stats, "stats", 20.0, min_i)

def health_check() :

    logging.info( "Starting health check" )

    cli = C.get_client()
    encodedPing = ArakoonProtocol.encodePing( "me", C.cluster_id )

    global monkey_dies

    if ( monkey_dies ) :
        return

    # Make sure all processes are running
    C.assert_running_nodes( 3 )

    # Do a ping to all nodes
    for node in C.node_names :
        try :
            con = cli._sendMessage( node, encodedPing )
            reply = con.decodeStringResult()
            logging.info ( "Node %s is responsive: '%s'" , node, reply )
        except Exception, ex:
            monkey_dies = True
            logging.fatal( "Node %s is not responding: %s:'%s'", node, ex.__class__.__name__, ex )

    if ( monkey_dies ) :
        return

    key = "@@some_key@@"
    value = "@@some_value@@"

    # Perform a basic set get and delete to see the cluster can make progress
    cli.set( key, value )
    NT.assert_equals( cli.get( key ), value )
    cli.delete( key )
    NT.assert_raises( ArakoonNotFound, cli.get, key )

    # Give the nodes some time to sync up
    time.sleep(2.0)
    C.stop_all()
    logging.info("tlogs in sync?")
    # Make sure the tlogs are in sync
    C.assert_last_i_in_sync( C.node_names[0], C.node_names[1] )
    C.assert_last_i_in_sync( C.node_names[1], C.node_names[2] )
    # Make sure the stores are equal
    logging.info("stores equal?")
    C.compare_stores( C.node_names[0], C.node_names[1] )
    C.compare_stores( C.node_names[2], C.node_names[1] )


    cli._dropConnections()

    if not check_disk_space():
        logging.critical("SUCCES! Monkey filled the disk to its threshold")
        sys.exit(0)
    if done_time () :
        logging.critical("SUCCES! Monkey did his time ...")
        sys.exit(0)

    logging.info("Cluster is healthy!")

def check_disk_space():
    cmd = "df -h | awk ' { if ($6==\"/\") print $5 } ' | cut -d '%' -f 1"
    (exit,stdout,stderr) = q.system.process.run(cmd)
    if( exit != 0 ):
        raise Exception( "Could not determine free disk space" )
    stdout = stdout.strip()
    disk_free = int( stdout )
    free_threshold = 95
    if disk_free > free_threshold :
        return False
    logging.info( "Still under free disk space threshold. Used space: %d%% < %d%% " % (disk_free,free_threshold) )
    return True

def memory_monitor():
    global monkey_dies
    global MAX_USED
    while monkey_dies == False :
        for name in C.node_names:
            used = C.get_memory_usage( name )
            if used > MAX_USED:
                MAX_USED = used
                logging.info("MAX_USED:%i", MAX_USED)

            if used > MEM_MAX_KB:
                logging.critical( "!!!! %s uses more than %d kB of memory (%d) " % (name, MEM_MAX_KB, used))
                C.stop_all()
                monkey_dies = True
            else :
                logging.info( "Node %s under memory threshold (%d)" % (name, used) )
        time.sleep(10.0)

def make_monkey_run() :

    global monkey_dies

    C.data_base_dir = '/'.join([X.tmpDir,"/arakoon_monkey/"])

    t = threading.Thread( target=memory_monitor)
    t.start()

    C.stop_all()
    cluster = C._getCluster(C.cluster_id)
    cluster.tearDown()
    #setup_3_nodes_forced_master()
    C.setup_3_nodes( C.data_base_dir )
    monkey_dir = get_monkey_work_dir()
    if X.fileExists( monkey_dir ) :
        X.removeDirTree( monkey_dir )
    X.createDir( monkey_dir )
    iteration = 0
    C.start_all()
    time.sleep( 1.0 )
    while( True ) :
        iteration += 1
        logging.info( "Preparing iteration %d" % iteration )
        thr_list = list ()
        try:
            (disruption, f_list) = generate_work_list( iteration )
            logging.info( "Starting iteration %d" % iteration )
            thr_list = C.create_and_start_thread_list( f_list )

            disruption ()

            for thr in thr_list :
                thr.join(60.0 * 60.0)
                if thr.isAlive() :
                    logging.fatal( "Thread did not complete in a timely fashion.")
                    monkey_dies = True

            if not monkey_dies :
                wait_for_it ()

            if not monkey_dies :
                health_check ()

        except SystemExit, ex:
            if str(ex) == "0":
                sys.exit(0)
            else :
                logging.fatal( "Caught SystemExit => %s: %s" %(ex.__class__.__name__, ex) )
                tb = traceback.format_exc()
                logging.fatal( tb )
                for thr in thr_list :
                    thr.join()
                monkey_dies = True

        except Exception, ex:
            logging.fatal( "Caught fatal exception => %s: %s" %(ex.__class__.__name__, ex) )
            tb = traceback.format_exc()
            logging.fatal( tb )
            for thr in thr_list :
                thr.join()
            monkey_dies = True

        if monkey_dies :
            euthanize_this_monkey ()

        C.rotate_logs(5,False)

        C.stop_all()
        logging.info("stopped all, going to wipe")
        toWipe = C.node_names[random.randint(0,2)]
        logging.info("Wiping node %s" % toWipe)
        C.whipe(toWipe)
        logging.info("starting all")
        C.start_all()

        toCollapse = C.node_names[random.randint(0,2)]
        while toCollapse == toWipe:
            toCollapse = C.node_names[random.randint(0,2)]

        collapse_candidate_count = C.get_tlog_count (toCollapse ) -1
        if collapse_candidate_count > 0 :
            logging.info("Collapsing node %s" % toCollapse )
            if C.collapse(toCollapse, 1 ) != 0:
                logging.error( "Could not collapse tlog of node %s" % toCollapse )



def send_email(from_addr, to_addr_list, cc_addr_list,
              subject, message,
              login, password,
              smtpserver ):

    import smtplib

    header  = 'From: %s\n' % from_addr
    header += 'To: %s\n' % ','.join(to_addr_list)
    header += 'Cc: %s\n' % ','.join(cc_addr_list)
    header += 'Subject: %s\n\n' % subject
    message = header + message

    server = smtplib.SMTP ()
    server.connect(smtpserver)
    server.starttls()
    server.login(login,password)
    problems = server.sendmail(from_addr, to_addr_list, message)
    server.quit()
    return problems

def get_mail_escalation_cfg() :

    cfg_file = q.config.getInifile("arakoon_monkey")
    cfg_file_dict = cfg_file.getFileAsDict()

    if ( len( cfg_file_dict.keys() ) == 0) :
        raise Exception( "Escalation ini-file empty" )
    if not cfg_file_dict.has_key ( "email" ):
        raise Exception ( "Escalation ini-file does not have a 'email' section" )

    cfg = cfg_file_dict [ "email" ]

    required_keys = [
        "server",
        "port",
        "from",
        "to",
        "login",
        "password",
        "subject",
        "msg"
        ]

    missing_keys = []
    for key in required_keys :
        if not cfg.has_key( key ) :
            missing_keys.append( key )
    if len(missing_keys) != 0 :
        raise Exception( "Required key(s) missing in monkey escalation config file: %s" % missing_keys )

    split_to = []
    for to in cfg["to"].split(",") :
        to = to.strip()
        if len( to ) > 0 :
            split_to.append ( to )

    cfg["to"] = split_to

    return cfg

def escalate_dying_monkey() :

    cfg = get_mail_escalation_cfg()

    logging.info( "Sending mails to : %s" % cfg["to"] )

    problems = send_email( cfg["from"], cfg["to"], [] ,
                cfg["subject"], cfg["msg"], cfg["login"], cfg["password"] ,
                "%s:%d" % (cfg["server"], int(cfg["port"]) ) )

    if ( len(problems) != 0 ) :
        logging.fatal("Could not send mail to the following recipients: %s" % problems )


def euthanize_this_monkey() :
    logging.fatal( "This monkey is going to heaven" )
    try :
        escalate_dying_monkey()
    except Exception, ex:
        logging.fatal( "Could not escalate dead monkey => %s: '%s'" % (ex.__class__.__name__, ex) )
    C.stop_all()
    sys.exit( 255 )

if __name__ == "__main__" :
    print sys.path
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    make_monkey_run()
