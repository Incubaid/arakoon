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

"""
Module implementing the Arakoon protocol
"""
from ArakoonExceptions import *
from ArakoonValidators import SignatureValidator
from NurseryRouting import RoutingInfo

import struct
import logging
import select
import cStringIO
import binascii

FILTER = ''.join([(len(repr(chr(x)))==3) and chr(x) or '.' for x in range(256)])

ARA_CFG_TRY_CNT = 1
ARA_CFG_CONN_TIMEOUT = 60
ARA_CFG_CONN_BACKOFF = 5
ARA_CFG_NO_MASTER_RETRY = 60

class ArakoonClientConfig :

    def __init__ (self, clusterId, nodes):
        """
        Constructor of an ArakoonClientConfig object

        The constructor takes one optional parameter 'nodes'.
        This is a dictionary containing info on the arakoon server nodes. It contains:
          - nodeids as keys
          - (hostname/ip, tcp port) tuples as value
        e.g. ::
            cfg = ArakoonClientConfig ('ricky',
                { "myFirstNode" : ( "127.0.0.1", 4000 ),
                  "mySecondNode" : ( "127.0.0.1", 5000 ) ,
                  "myThirdNode"  : ( "127.0.0.1", 6000 ) } )

        @type clusterId: string
        @param clusterId: name of the cluster
        @type nodes: dict
        @param nodes: A dictionary containing the locations for the server nodes

        """
        self._clusterId = clusterId
        self._nodes = nodes
    
    def __str__(self):
        r = "ArakoonClientConfig(%s,%s)" % (self._clusterId,
                                            str(self._nodes))
        return r

    @staticmethod
    def getNoMasterRetryPeriod() :
        """
        Retrieve the period messages to the master should be retried when a master re-election occurs
        
        This period is specified in seconds
        
        @rtype: integer
        @return: Returns the retry period in seconds
        """
        return ARA_CFG_NO_MASTER_RETRY
    
    def getNodeLocation(self, nodeId):
        """
        Retrieve location of the server node with give node identifier

        A location is a pair consisting of a hostname or ip address as first element.
        The second element of the pair is the tcp port

        @type nodeId: string
        @param nodeId: The node identifier whose location you are interested in

        @rtype: pair(string,int)
        @return: Returns a pair with the nodes hostname or ip and the tcp port, e.g. ("127.0.0.1", 4000)
        """
        return self._nodes[ nodeId ]


    def getTryCount (self):
        """
        Retrieve the number of attempts a message should be tried before giving up

        Can be controlled by changing the global variable L{ARA_CFG_TRY_CNT}

        @rtype: integer
        @return: Returns the max retry count.
        """
        return ARA_CFG_TRY_CNT


    def getNodes(self):
        """
        Retrieve the dictionary with node locations

        @rtype: dict
        @return: Returns a dictionary mapping the node identifiers (string) to its location ( pair<string,integer> )
        """
        return self._nodes


    @staticmethod
    def getConnectionTimeout():
        """
        Retrieve the tcp connection timeout

        Can be controlled by changing the global variable L{ARA_CFG_CONN_TIMEOUT}

        @rtype: integer
        @return: Returns the tcp connection timeout
        """
        return ARA_CFG_CONN_TIMEOUT

    @staticmethod
    def getBackoffInterval():
        """
        Retrieves the backoff interval.

        If an attempt to send a message to the server fails,
        the client will wait a random number of seconds. The maximum wait time is n*getBackoffInterVal()
        with n being the attempt counter.
        Can be controlled by changing the global variable L{ARA_CFG_CONN_BACKOFF}

        @rtype: integer
        @return: The maximum backoff interval
        """
        return ARA_CFG_CONN_BACKOFF

    def getClusterId(self):
        return self._clusterId

class ArakoonClientLogger :

    @staticmethod
    def logWarning( msg, *args ):
        logging.warning(msg, *args )

    @staticmethod
    def logError( msg, *args  ):
        logging.error( msg, *args  )

    @staticmethod
    def logCritical( msg, *args  ):
        logging.critical( msg, *args  )

    @staticmethod
    def logDebug ( msg, *args  ):
        logging.debug ( msg, *args  )


def dump(src, length=8):
    N = 0
    result = ''
    while src:
        s, src = src[:length], src[length:]
        hexa = ' '.join(["%02X"%ord(x) for x in s])
        s = s.translate(FILTER)
        result += "%04X:  %-*s   %s\n" % (N, length*3, hexa, s)
        N += length
    return result

# Define the size of an int in bytes
ARA_TYPE_INT64_SIZE = 8
ARA_TYPE_INT_SIZE = 4
ARA_TYPE_BOOL_SIZE = 1

# Magic used to mask each command
ARA_CMD_MAG    = 0xb1ff0000
ARA_CMD_VER    = 0x00000002
# Hello command
ARA_CMD_HEL    = 0x00000001 | ARA_CMD_MAG
# Who is master?
ARA_CMD_WHO    = 0x00000002 | ARA_CMD_MAG
# Existence of a value for a key
ARA_CMD_EXISTS = 0x00000007 | ARA_CMD_MAG
# Get a value
ARA_CMD_GET    = 0x00000008 | ARA_CMD_MAG
# Update a value
ARA_CMD_SET    = 0x00000009 | ARA_CMD_MAG
# Delete a key value pair
ARA_CMD_ASSERT = 0x00000016 | ARA_CMD_MAG
ARA_CMD_DEL    = 0x0000000a | ARA_CMD_MAG
# Get a range of keys
ARA_CMD_RAN    = 0x0000000b | ARA_CMD_MAG
# Get keys matching a prefix
ARA_CMD_PRE    = 0x0000000c | ARA_CMD_MAG
# Test and set a value
ARA_CMD_TAS    = 0x0000000d | ARA_CMD_MAG
# range entries
ARA_CMD_RAN_E  = 0x0000000f | ARA_CMD_MAG

#sequence
ARA_CMD_SEQ                      = 0x00000010 | ARA_CMD_MAG

ARA_CMD_MULTI_GET                = 0x00000011 | ARA_CMD_MAG

ARA_CMD_EXPECT_PROGRESS_POSSIBLE = 0x00000012 | ARA_CMD_MAG

ARA_CMD_STATISTICS               = 0x00000013 | ARA_CMD_MAG

ARA_CMD_USER_FUNCTION            = 0x00000015 | ARA_CMD_MAG

ARA_CMD_KEY_COUNT                = 0x0000001a | ARA_CMD_MAG

ARA_CMD_CONFIRM                  = 0x0000001c | ARA_CMD_MAG

ARA_CMD_GET_NURSERY_CFG          = 0x00000020 | ARA_CMD_MAG
ARA_CMD_REV_RAN_E                = 0x00000023 | ARA_CMD_MAG
ARA_CMD_SYNCED_SEQUENCE          = 0x00000024 | ARA_CMD_MAG
ARA_CMD_VERSION                  = 0x00000028 | ARA_CMD_MAG
# Arakoon error codes
# Success
ARA_ERR_SUCCESS = 0
# No entity
ARA_ERR_NO_ENT = 1
# Node is not the master
ARA_ERR_NOT_MASTER = 4
# not found
ARA_ERR_NOT_FOUND = 5
# wrong cluster
ARA_ERR_WRONG_CLUSTER = 6
ARA_ERR_ASSERTION_FAILED = 7
ARA_ERR_RANGE_ERROR = 9

NAMED_FIELD_TYPE_INT    = 1
NAMED_FIELD_TYPE_INT64  = 2
NAMED_FIELD_TYPE_FLOAT  = 3
NAMED_FIELD_TYPE_STRING = 4
NAMED_FIELD_TYPE_LIST   = 5





def _packInt ( toPack ):
    return struct.pack( "I", toPack )

def _vpackInt(toPack):
    c = toPack
    r = cStringIO.StringIO()
    go_on = True
    while go_on:
        if c == 0 :
            r . write('\x00')
            go_on = False
        elif c < 128:
            r . write(chr(c))
            go_on = False
        else:
            last = c & 0x7f 
            byte = last | 0x80
            r . write(chr(byte))
            c = c >> 7
    v = r.getvalue()
    return v

class PInput:
    def __init__(self, con):
        size = _recvSize(con)
        self._rest = _readExactNBytes(con, size)
        self._off = 0

    def input_vint(self):
        index = self._off
        go_on = True
        v = 0
        shift = 0
        while go_on:
            c = self._rest[index]
            index = index + 1
            cv = ord(c)
            last = cv & 0x7f
            v = v + (last << shift)
            shift = shift + 7
            if cv < 128:
                go_on = False
        self._off = index
        return v

    def input_bool(self):
        c = self._rest[self._off]
        self._off = self._off + 1
        if c == '1':
            return True
        elif c == '0':
            return False
        else:
            raise Exception("'%s' is not a bool" % c)
            
    def input_float(self):
        f = struct.unpack_from("d", self._rest, self._off)
        self._off = self._off + 8
        return f
    
    def input_string(self):
        size = self.input_vint()
        next = self._off + size
        s = self._rest[self._off:next]
        self._off = next
        return s

    def input_string_option(self):
        c = self._rest[self._off]
        self._off = self._off + 1
        if c == '1':
            return self.input_string()
        elif c == '0':
            return None
        else:
            raise

    def input_string_list(self):
        n = self.input_vint()
        r = [self.input_string() for i in xrange(n)]
        return r


    def input_named_field(self):
        type = self.input_vint()
        name = self.input_string()
        result = dict()
        
        if type == NAMED_FIELD_TYPE_INT:
            result[name] = self.input_vint()
        elif type == NAMED_FIELD_TYPE_INT64:
            result[name] = self.input_vint64()
        elif type == NAMED_FIELD_TYPE_FLOAT:
            result[name] = self.input_float()
        elif type == NAMED_FIELD_TYPE_STRING:
            result[name] = self.input_string()
        elif type == NAMED_FIELD_TYPE_LIST:
            length = self.input_vint()
            localDict = dict()
            for i in xrange(length):
                field = self.input_named_field()
                localDict.update( field )
                result[name] = localDict
        return result
    
    def input_statistics(self):
        r = self.input_named_field()
        stats = r['arakoon_stats']
        return stats
    
def _packString( toPack):
    toPackLength = len(toPack)
    return struct.pack("I%ds" % ( toPackLength), toPackLength, toPack )

def _vpackString( toPack ):
    toPackLength = len( toPack )
    r = _vpackInt(toPackLength) + toPack
    return r

def _packStringOption ( toPack = None ):
    if toPack is None:
        return _packBool ( 0 )
    else :
        return _packBool ( 1 ) + _packString (toPack)
        
def _packInt64 ( toPack ):
    return struct.pack( "q", toPack )

def _packSignedInt ( toPack ):
    return struct.pack( "i", toPack )

def _packBool ( toPack) :
    return struct.pack( "?", toPack)

def _vpackBool ( toPack) :
    if toPack:
        return '1'
    else :
        return '0'

def _vpackStringOption(toPack = None):
    if toPack is None:
        return _vpackBool(False)
    else:
        return _vpackBool(True) + _vpackString(toPack)

def _vpackIntOption(toPack):
    if toPack is None:
        return _vpackBool(False)
    else:
        return _vpackBool(True) + _vpackInt (toPack)

def sendPrologue(socket, clusterId):
    p  = _packInt(ARA_CMD_MAG)
    p += _packInt(ARA_CMD_VER)
    p += _packString(clusterId)
    socket.sendall(p)
    
def _readExactNBytes( con, n ):

    if not con._connected :
        raise ArakoonSockRecvClosed()
    bytesRemaining = n
    tmpResult = ""
    timeout = ArakoonClientConfig.getConnectionTimeout()

    while bytesRemaining > 0 :

        tripleList = select.select(  [con._socket] , [] , [] , timeout )

        if ( len ( tripleList [0]) != 0 ) :
            newChunk = ""
            try :
                newChunk =  tripleList [0][0].recv ( bytesRemaining)
            except Exception, ex:
                ArakoonClientLogger.logError ("Error while receiving from socket. %s: '%s'" % (ex.__class__.__name__, ex) )
                con._connected = False
                raise ArakoonSockRecvError()
                
            newChunkSize = len( newChunk )
            if newChunkSize == 0 :
                try: 
                    con._socket.close()
                except Exception, ex:
                    ArakoonClientLogger.logError( "Error while closing socket. %s: %s" % (ex.__class__.__name__,ex))
                con._connected = False
                raise ArakoonSockReadNoBytes ()
            tmpResult = tmpResult + newChunk
            bytesRemaining = bytesRemaining - newChunkSize

        else :
            try: 
                con._socket.close()
            except Exception, ex:
                ArakoonClientLogger.logError( "Error while closing socket. %s: %s" % (ex.__class__.__name__,ex))
            con._connected = False

            raise ArakoonSockNotReadable()

    return tmpResult

def _recvString ( con ):
    strLength = _recvInt( con )
    buf = _readExactNBytes( con, strLength)
    return struct.unpack( "%ds" % strLength, buf ) [0]

def _unpackInt(buf, offset):
    r=struct.unpack_from( "I", buf,offset)
    return r[0], offset + ARA_TYPE_INT_SIZE

def _unpackSignedInt(buf, offset):
    r=struct.unpack_from( "i", buf,offset)
    return r[0], offset + ARA_TYPE_INT_SIZE

def _unpackInt64(buf, offset):
    r= struct.unpack_from("q", buf, offset)
    return r[0], offset + 8

def _unpackString(buf, offset):
    size,o2 = _unpackInt(buf, offset)
    v = buf[o2:o2 + size]
    return v, o2+size
    
def _unpackStringList(buf, offset):
    size,offset = _unpackInt(buf, offset)
    retVal = []
    for i in range( size ) :
        x, offset = _unpackString(buf, offset)
        retVal.append(x)
    return retVal


    
    raise ArakoonException("Cannot decode named field %s. Invalid type: %d" % (name,type) )

def _recvSize ( con ):
    buf = _readExactNBytes ( con, ARA_TYPE_INT_SIZE )
    i,o2 = _unpackInt(buf,0)
    return i

def _recvInt64 ( con ):
    buf = _readExactNBytes( con, ARA_TYPE_INT64_SIZE )
    i,o2 = _unpackInt64(buf,0)
    return i

def _unpackBool(buf, offset):
    r = struct.unpack_from( "?", buf, offset) [0]
    return r, offset+1

def _recvBool ( con ):
    buf = _readExactNBytes( con, 1 )
    b, o2 = _unpackBool(buf,0)
    return b

def _unpackFloat(buf, offset):
    r = struct.unpack_from("d", buf, offset)
    return r[0], offset+8

def _recvFloat(buf):
    buf = _readExactNBytes(con, 8)
    f,o2 = _unpackFloat(buf,0)
    return f

def _recvStringOption ( con ):
    isSet = _recvBool( con )
    if( isSet ) :
        s = _recvString(con)
        return s
    else :
        return None


def _encodeRange(bKey,bInc,eKey,eInc, maxEntries, allowDirty):
    assert (maxEntries >= 0)
    retVal  = _vpackBool(allowDirty)
    retVal += _vpackStringOption( bKey ) + _vpackBool ( bInc )
    retVal += _vpackStringOption( eKey ) + _vpackBool (eInc) + _vpackIntOption (maxEntries)
    return  retVal        

class Update(object):
    pass
class Set(Update):
    def __init__(self,key,value):
        self._key = key
        self._value = value

    def write(self, fob):
        fob.write(_packInt(1))
        fob.write(_packString(self._key))
        fob.write(_packString(self._value))  

class Delete(Update):
    def __init__(self,key):
        self._key = key

    def write(self, fob):
        fob.write(_packInt(2))
        fob.write(_packString(self._key))

class Assert(Update):
    def __init__(self, key, vo):
        self._key = key
        self._vo = vo

    def write(self, fob):
        fob.write(_packInt(8))
        fob.write(_packString(self._key))
        fob.write(_packStringOption(self._vo))

class Sequence(Update):
    def __init__(self):
        self._updates = []

    def addUpdate(self,u):
        self._updates.append(u)

    @SignatureValidator( 'string', 'string' )
    def addSet(self, key,value):
        self._updates.append(Set(key,value))
    
    @SignatureValidator( 'string' )
    def addDelete(self, key):
        self._updates.append(Delete(key))

    def addAssert(self, key,vo):
        self._updates.append(Assert(key,vo))

    def write(self, fob):
        fob.write( _packInt(5))
        fob.write( _packInt(len(self._updates)))
        for update in self._updates:
            update.write(fob)
        
        
class ArakoonProtocol :

    @staticmethod
    def encodePing(clientId, clusterId ):
        r  = _packInt(ARA_CMD_HEL)
        r += _packString(clientId)
        r += _packString(clusterId)
        return r

    @staticmethod
    def encodeWhoMaster():
        return _packInt( ARA_CMD_WHO )

    @staticmethod
    def encodeExists(key, allowDirty):
        msg = _packInt(ARA_CMD_EXISTS)
        msg += _vpackBool(allowDirty)
        msg += _vpackString(key)
        return msg

    @staticmethod
    def encodeAssert(key, vo, allowDirty):
        msg = _packInt(ARA_CMD_ASSERT)
        msg += _vpackBool(allowDirty)
        msg += _vpackString(key)
        msg += _vpackStringOption(vo)
        return msg


    @staticmethod
    def encodeGet(key , allowDirty):
        msg = _packInt(ARA_CMD_GET)
        msg += _vpackBool(allowDirty)
        msg += _vpackString(key)
        return msg

    @staticmethod
    def encodeSet( key, value ):
        return _packInt( ARA_CMD_SET ) + _vpackString( key ) + _vpackString ( value )

    @staticmethod
    def encodeConfirm(key, value):
        return _packInt(ARA_CMD_CONFIRM) + _vpackString(key) + _vpackString(value)

    @staticmethod
    def encodeSequence(seq, sync):
        r = cStringIO.StringIO()
        seq.write(r)
        flattened = r.getvalue()
        r.close()
        cmd = ARA_CMD_SEQ
        if sync:
            cmd = ARA_CMD_SYNCED_SEQUENCE
        return _packInt(cmd) + _vpackString(flattened)     
        
    @staticmethod
    def encodeDelete( key ):
        return _packInt ( ARA_CMD_DEL ) + _vpackString ( key )

    @staticmethod
    def encodeRange( first, finc, last, linc, maxEntries , allowDirty):
        return _packInt( ARA_CMD_RAN ) + _encodeRange(first,finc,last,linc,maxEntries,allowDirty)


    @staticmethod
    def encodeRangeEntries(first, finc, last, linc, maxEntries, allowDirty):
        r = _packInt(ARA_CMD_RAN_E) + _encodeRange(first,finc,last,linc,maxEntries,allowDirty)
        return r

    @staticmethod
    def encodeReverseRangeEntries(first, finc, last, linc, maxEntries, allowDirty):
        r = _packInt(ARA_CMD_REV_RAN_E) + _encodeRange(first,finc,last,linc,maxEntries, allowDirty)
        return r

    @staticmethod
    def encodePrefixKeys( key, maxCnt, allowDirty ):
        retVal = _packInt( ARA_CMD_PRE) + _vpackBool(allowDirty)
        retVal += _vpackString(key)
        retVal += _vpackIntOption(maxCnt)
        return retVal

    @staticmethod
    def encodeTestAndSet( key, oldVal, newVal ):
        retVal = _packInt( ARA_CMD_TAS ) + _vpackString( key )
        retVal += _vpackStringOption( oldVal )
        retVal += _vpackStringOption( newVal )
        return retVal

    @staticmethod
    def encodeMultiGet(keys, allowDirty):
        retVal = _packInt(ARA_CMD_MULTI_GET) + _vpackBool(allowDirty)
        retVal += _vpackInt(len(keys))
        for key in keys:
            retVal += _vpackString(key)
        return retVal

    @staticmethod
    def encodeExpectProgressPossible():
        retVal = _packInt(ARA_CMD_EXPECT_PROGRESS_POSSIBLE)
        return retVal

    @staticmethod
    def encodeStatistics():
        retVal = _packInt(ARA_CMD_STATISTICS)
        return retVal

    @staticmethod
    def encodeGetVersion():
        retVal = _packInt(ARA_CMD_VERSION)
        return retVal

    @staticmethod
    def encodeGetKeyCount () :
        return _packInt(ARA_CMD_KEY_COUNT)
    
    @staticmethod 
    def encodeGetNurseryCfg ():
        return _packInt(ARA_CMD_GET_NURSERY_CFG)
    
    @staticmethod    
    def encodeUserFunction(name, argument):
        retVal = _packInt(ARA_CMD_USER_FUNCTION)
        retVal += _vpackString(name)
        retVal += _vpackStringOption(argument)
        return retVal

    @staticmethod
    def close(msgBuffer):
        total = _packInt(len(msgBuffer)) + msgBuffer
        return total


    @staticmethod
    def _evaluateErrorCode(input ):
        errorCode = input.input_vint ()
        if errorCode == ARA_ERR_SUCCESS :
            return
        else :
            errorMsg = input.input_string()

        if errorCode == ARA_ERR_NOT_FOUND:
            raise ArakoonNotFound(errorMsg)

        if errorCode == ARA_ERR_NOT_MASTER:
            raise ArakoonNodeNotMaster()
        if errorCode == ARA_ERR_ASSERTION_FAILED:
            raise ArakoonAssertionFailed(errorMsg)    
        if errorCode == ARA_ERR_RANGE_ERROR:
            raise NurseryRangeError(errorMsg) 
           
        if errorCode != ARA_ERR_SUCCESS:
            raise ArakoonException( "EC=%d. %s" % (errorCode, errorMsg) )

    @staticmethod
    def readAnswer(con):
        input = PInput(con)
        ArakoonProtocol._evaluateErrorCode(input)
        return input
        
    @staticmethod
    def decodeStringOptionResult ( con ):
        input = ArakoonProtocol.readAnswer(con)
        so = input.input_string_option()
        return so

    @staticmethod
    def decodeVoidResult( con ):
        input = ArakoonProtocol.readAnswer(con)
        return

    @staticmethod
    def decodeStringResult ( con ):
        input = ArakoonProtocol.readAnswer(con)
        s = input.input_string()
        return s

    @staticmethod
    def decodeVersionResult(con):
        input = ArakoonProtocol.readAnswer(con)
        major = input.input_vint()
        minor = input.input_vint()
        patch = input.input_vint()
        info  = input.input_string()
        return (major,minor,patch,info)
    
    @staticmethod
    def decodeStringListResult( con ):
        input = ArakoonProtocol.readAnswer(con)
        r = input.input_string_list()
        return r

    @staticmethod
    def decodeStringPairListResult(con):
        input = ArakoonProtocol.readAnswer(con)
        size = input.input_vint()
        r = []
        for i in xrange(size):
            k = input.input_string()
            v = input.input_string()
            p = (k,v)
            r.append(p)
        return r

    @staticmethod
    def decodeInt64Result( con ) :
        input = ArakoonProtocol.readAnswer(con)
        v = input.input_vint()
        return v

    @staticmethod
    def decodeBoolResult( con ):
        input = ArakoonProtocol.readAnswer(con)
        r = input.input_bool()
        return r




    @staticmethod
    def decodeNurseryCfgResult( con ):
        input = ArakoonProtocol.readAnswer(con)
        
        
        encoded = input.input_string()
        routing, offset = RoutingInfo.unpack(encoded, 0, _unpackBool, _unpackString)
        
        cfgCount = input.input_vint()
        resultCfgs = dict()
        for i in range(cfgCount) :
            clusterId = input.input_string()
            clusterSize = input.input_vint()
            cfg = dict()
            for j in range(clusterSize):
                nodeId = input.input_string ()
                ip = input.input_string ()
                port = input.input_vint()
                cfg[nodeId] = (ip,port)
            cliCfg = ArakoonClientConfig(clusterId, cfg)
            resultCfgs[clusterId] = cliCfg
        return (routing, resultCfgs)      
        

    @staticmethod
    def decodeStatistics(con):
        input = ArakoonProtocol.readAnswer(con)
        r = input.input_statistics()
        return r


