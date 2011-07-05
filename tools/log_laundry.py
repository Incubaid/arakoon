import datetime
import time
f = open('opt/qbase3/var/log/arakoon/node_00E081B42960.log.1','r')

def get_date(line):
    dt = datetime.datetime.strptime(line[:15], "%b %d %H:%M:%S")
    dt = dt.replace(year=2011)
    tt = dt.timetuple()
    f = time.mktime(tt)
    return f

def get_allocated(line):
    ei = line.find('KB',30)
    return line[33:ei]

def get_sets(line):
    si = line.find('n_sets:',30) + 7
    ei = line.find(',',si) 
    return int(line[si:ei])

def get_deletes(line):
    si = line.find('n_deletes:',30) + 10
    ei = line.find(',', si)
    return int(line[si:ei])

def get_gets(line):
    si = line.find('n_gets:',30) + 7
    ei = line.find(',', si)
    return int(line[si:ei])

def get_avg_set(line):
    si = line.find('avg_set_size:', 30) + 13
    ei = line.find(',',si)
    return float(line[si:ei])

def get_avg_get(line):
    si = line.find('avg_get_size:', 30) + 13
    ei = line.find(',', si)
    return float(line[si:ei])

pd = None
ps = None
n_sessions = 0
n_cs = 0 # concurrent sessions

for line in f:
    if line.find('NODE')>0:
        d = get_date(line)
        print "%s;%s" % (d,line[23:])
        n_sessions = 0
        n_cs = 0
    if line.find('allocated')>0:
        d = get_date(line)
        alloc = get_allocated(line)
        sets, avg_set, deletes,gets,avg_get = pt
        t = (d, '     ', sets, avg_set, 
             deletes, 
             gets, avg_get, 
             n_sessions, 
             n_cs,
             alloc)
        print "%s;%s;%i;%f;%i;%i;%f;%i;%i;%s" % t
    if line.find('starting session')>0:
        d = get_date(line)
        n_sessions = n_sessions+1
        n_cs = n_cs + 1
    if line.find('stats')>0:
        d = get_date(line)
        sets = get_sets(line)
        deletes = get_deletes(line)
        gets = get_gets(line)
        avg_s = get_avg_set(line)
        avg_g = get_avg_get(line)
        pt = (sets,avg_s, deletes,gets, avg_g)
    if line.find('exiting session')>0:
        n_cs = n_cs - 1
    pd = d




