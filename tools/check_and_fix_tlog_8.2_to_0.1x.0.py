"""
This script will detect faulty tlogs (the ones with a faulty 1st entry as a result of the tlog rotation bug). 

It starts of by dumping the tlogs and reading out the first entry. If the i-value of this entry is not an exact multiple of
100000, then we have found a faulty tlog.

In case a faulty tlog is found, the script proceeds with renaming all tlf files (by adding a .fix extension) and creating a new head.db (by copying over the main db).

After this script is run, the node MUST BE UPGRADED to 0.1x.0 as the older builds don't have a clue what head.db is and will only detect the missing tlogs.
After a successful upgrade, the .fix tlogs can be removed.
"""

from pymonkey import InitBase
from pymonkey import q
import time
import subprocess
import os
import string

dump_cmd = "/opt/qbase3/apps/arakoon/bin/arakoond --dump-tlog"

class FaultyTlogs():
    
    _msgF = "Fault tlog found (%s)"
    
    def __init__(self, msg):
        self._msg  = FaultyTlogs._msgF % msg

    def __str__ (self):
        return self._msg


def get_tlogs(tlog_dir):
    fns = os.listdir(tlog_dir)
    tlogs = list()
    for fn in fns:
        if fn.endswith('.tlc') or fn.endswith('.tlf'):
            tlogs.append(fn)
    return tlogs

def check_tlogs(tlog_dir):
    fns = os.listdir(tlog_dir)
    for fn in get_tlogs(tlog_dir):
        src = '%s/%s' % (tlog_dir, fn)
        cmd = "%s %s | head" % (dump_cmd,src)
        (exit,stdout,stderr) = q.system.process.run(cmd)
        line1 = stdout.split("\n") [0]
        if line1 == "":
            raise FaultyTlogs( stderr )
        istr = line1.split(":") [0] 
        if istr == "":
            raise FaultyTlogs( stderr )
        i1 = int( istr )
        if( i1 % 100000 ) != 0 :
            raise FaultyTlogs( "Wrong first value of tlog")

def rename_tlogs(tlog_dir):
    fns = os.listdir(tlog_dir)
    for fn in get_tlogs(tlog_dir):
        src = '%s/%s' % (tlog_dir, fn)
        dst = src + '.fix'
        cmd = "mv %s %s" % (src,dst)
        q.system.process.execute(cmd)

            
def fix_local(n, section):
    print "Checking node %s" % n
    home = section['home']
    tlog_dir = section.get('tlog_dir', home)
    db_name = '%s/%s.db' % (home, n)
    head_name = '%s/head.db' % tlog_dir
    
    try:
        check_tlogs(tlog_dir)
        print "  - Tlogs healthy"
    except FaultyTlogs as e:
        print "  - Found a faulty tlog. Attempting repair"
        running = q.cmdtools.arakoon.getStatusOne(n) == q.enumerators.AppStatusType.RUNNING
        q.cmdtools.arakoon.stopOne(n)
        print "  - Copying database to head.db (%s -> %s)" % (db_name, head_name)
        q.system.process.run('cp %s %s' % (db_name,head_name) )
        print "  - Obsoleting old tlogs"
        rename_tlogs(tlog_dir)
        if running:
            print "  - Restarting arakoon."
            q.cmdtools.arakoon.startOne(n)
        print "  - Done with repairing node %s" % n

def fix_it():
    local = q.config.arakoon.listLocalNodes()
    for n in local:
        cfg = q.config.arakoon.getNodeConfig(n)
        fix_local(n,cfg)
    print "All done: Success"

if __name__ == '__main__' :    
    fix_it()
