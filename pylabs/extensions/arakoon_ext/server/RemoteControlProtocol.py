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

import socket
import logging
import struct


_COLLAPSE_TLOGS = 0x14
_SET_INTERVAL = 0x17
_DOWNLOAD_DB = 0x1b
_OPTIMIZE_DB = 0x25
_DEFRAG_DB = 0x26
_DROP_MASTER = 0x30
_FLUSH_STORE = 0x42
_COPY_DB_TO_HEAD = 0x44
_MAGIC   = 0xb1ff0000
_VERSION = 0x00000001

def _int_to(i):
    r = struct.pack("I", i)
    return r

def _int_from(buff,pos):
    r = struct.unpack_from("I",buff, pos)
    return r[0], pos + 4

def _int64_from(buff,pos):
    r = struct.unpack_from("Q",buff,pos)
    return r[0], pos + 8

def _string_to(s):
    size = len(s)
    r = struct.pack("I%ds" % size, size, s)
    return r

def _prologue(clusterId, sock):
    m  = _int_to(_MAGIC)
    m += _int_to(_VERSION)
    m += _string_to(clusterId)
    sock.sendall(m)

def _receive_all(sock,n):
    todo = n
    r = ""
    while todo:
        chunk = sock.recv(todo)
        if chunk == "" :
            raise RuntimeError("Not enough data on socket. Aborting...")
        todo -= len(chunk)
        r += chunk
    return r

def _receive_int(sock):
    sizes = _receive_all(sock,4)
    i,_  = _int_from(sizes,0)
    return i

def _receive_int64(sock):
    buf = _receive_all(sock, 8)
    i64,_ = _int64_from(buf,0)
    return i64

def _receive_string(sock):
    size = _receive_int(sock)
    s = _receive_all(sock,size)
    return s

def check_error_code(s):
    rc = _receive_int(s)
    if rc:
        msg   = _receive_string(s)
        raise Exception(rc, msg)

def make_socket(ip, port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sa = (ip, port)
    s.connect(sa)
    return s
