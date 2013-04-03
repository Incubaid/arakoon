import sys

def find_message(line):
    seq_b = line.find("SEQ: ")
    if seq_b >= 0:
        #print line
        src_b = seq_b + 5
        src_e = line.find(" => ")
        src = line[src_b:src_e]
        dest_b = src_e + 4
        dest_e = line.find(" : ", dest_b)
        dest = line[dest_b:dest_e]
        tag_b = dest_e + 3
        tag = line[tag_b:-1]
        time_b = line.find('debug:')+ 6
        time_s = line[time_b:seq_b-1]
        time_s = time_s.strip()
        time = float(time_s)
        #print "seq_b:%i time_b:%i=>%f" % (time_b,seq_b, time)
        msg = (time, src,dest,tag)

        return msg
    
    return None



def parse(fns):
    messages = []
    for fn in fns:
        with open(fn,'r') as f:
            for line in f.readlines():
                msg = find_message(line)
                if msg:
                    messages.append(msg)
    messages.sort()
    return messages



def main(fns):
    messages = parse(fns)

    print "msc{"
    #print "\tarcgradient = 8;"
    cluster = ['sturdy_2','sturdy_1', 'sturdy_0']
    print '\tn [label="network"],sturdy_2 [label = "sturdy_2"], sturdy_1 [label = "sturdy_1"], sturdy_0 [label = "sturdy_0"];'
    for m in messages:
        (time, src,dst,msg) = m
        print '\t %s => n [label = "%s t:%s"];' % (src,msg, dst)
        print '\t n => %s [label = "s:%s %s"];' % (dst,src, msg)

    #print '\tc=>s2 [label="DROP_MASTER"];'
    print "}"


if __name__ == '__main__':
    if len(sys.argv) < 2: 
        print "please supply the names of the log files to use"
        print """something like this:
    python gen_sequence.py *.log > diagram.msc
    mscgen -T svg -i diagram.msc -o diagram.svg 
"""
    else:
        fns = sys.argv[1:]
        main(fns)
