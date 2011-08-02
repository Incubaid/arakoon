import bz2
import struct
import binascii
import os
import time
import sys

def sn_from(buf, offset):
    r = struct.unpack_from("q",buf, offset)
    return r[0], offset + 8

def int32_from(buf, offset):
    r = struct.unpack_from("I", buf, offset)
    return r[0], offset + 4

def string_from(buf, offset):
    size,o2 = int32_from(buf, offset)
    too = o2 + size
    v = buf[o2:too]
    return v, too


def test(sn,prev):
    if sn == prev + 1 or sn == prev:
        pass
    else:
        raise Exception("%i <-> %i" % (sn,prev))

def do_entry(inflated, offset):
    t0 = time.time()
    sn,o2 = sn_from(inflated, offset)
    crc,o3 = int32_from(inflated,o2)
    cmd,o4 = string_from(inflated,o3)
    t1 = time.time()
    delay = t1 - t0
    time.sleep(delay)
    return (sn,crc,cmd), o4

def do_chunk(prev_i, chunk):
    t0 = time.time()
    inflated = bz2.decompress(chunk)
    t1 = time.time()
    delay = t1 - t0
    time.sleep(delay)
    too = len(inflated)
    offset = 0
    prev = prev_i
    while offset < too:
        #print "\t",binascii.hexlify(inflated[offset: offset+16])
        (sn,crc,cmd),o2 = do_entry(inflated, offset)
        test(sn,prev)
        #print sn
        offset = o2
        prev = sn
    #print prev_i,prev
    return prev

def do_tlc_chunk(prev, chunk):
    t0 = time.time()
    inflated = bz2.decompress(chunk)
    t1 = time.time()
    delay = t1 - t0
    time.sleep(delay)
    offset = 0
    too = len(inflated)
    while offset < too:
        (sn,crc,cmd), o2 = do_entry(inflated, offset)
        test(sn,prev)
        offset = o2
        prev = sn
    return prev
        
def do_tlf(first, canonical) :
    f = open(canonical,'rb')
    all = f.read()
    f.close()
    offset = 0
    too = len(all)
    while offset < too:
        last_i,o2 = sn_from(all,offset)  
        chunk, o3 = string_from(all, o2)
        new_f = do_chunk(first, chunk)
        assert last_i == new_f
        offset = o3
        first = new_f
    return first

def do_tlc(first, canonical):
    f = open(canonical,'rb')
    all = f.read()
    f.close()
    offset = 0
    too = len(all)
    while offset < too:
        n_entries,o2 = int32_from(all,offset)
        chunk,o3 = string_from(all,o2)
        new_f = do_tlc_chunk(first, chunk)
        offset = o3
        first = new_f
    return first


    

def do_dir(dn):
    fns = filter(lambda f: f.endswith(".tlf") or f.endswith(".tlc"), 
                 os.listdir(dn))
    def n_from(e): return int(e[:e.index('.')])
    def cmp(a,b): return n_from(a) - n_from(b)
    fns.sort(cmp)

    for fn in fns:
        canonical = "%s/%s" % (dn,fn)
        first = int(fn[:fn.index('.')]) * 100000
        if fn.endswith(".tlf"):
            last  = do_tlf(first, canonical)
        else:
            last  = do_tlc(first, canonical)

        assert first + 99999 == last 
        print fn, "ok"

#do_tlc(500000,'/tmp/010.tlc')
    
if __name__ == '__main__':
    if len(sys.argv) <2:
        print "python",sys.argv[0], "<path_to_tlog_dir>"
        sys.exit(-1)
    else:
        do_dir(sys.argv[1])

