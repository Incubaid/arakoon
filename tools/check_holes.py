import string
import subprocess
import os
import sys


if len(sys.argv) < 3:
    print "this script checks if compressed tlogs have holes"
    print "usage:"
    print "python %s <path_to_arakoon_exec> <path_to_tlog_dir>" % sys.argv[0]
    sys.exit(-1)

binary = sys.argv[1]
folder = sys.argv[2]
prev_i = -1
files = filter(lambda f: f.endswith(".tlf") or f.endswith(".tlc"), 
               os.listdir(folder))
files.sort()
n=4

for f in files:
    fn = "%s/%s" %(folder, f)
    p = subprocess.Popen([binary, '--dump-tlog', fn],stdout= subprocess.PIPE)
    line = p.stdout.readline()
    while line:
        #print line
        i_s = line[:string.index(line,':')]
        i = int(i_s)
        if i % 10000 == 0:
            #print i
            pass
        if i == prev_i + 1:
            prev_i = i
            line = p.stdout.readline()
        elif i == prev_i:
            pass
        else:
            print prev_i
            p.kill()
            raise Exception("%s has a hole" % f)
    print "%s: ok" % f
    rc = p.poll()
    if rc == 0:
        pass
    else:
        raise Exception()

