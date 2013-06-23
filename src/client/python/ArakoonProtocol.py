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
import types

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
          - ([ip], port) as values
        e.g. ::
            cfg = ArakoonClientConfig ('ricky',
                { "myFirstNode" : (["127.0.0.1"], 4000 ),
                  "mySecondNode" :(["127.0.0.1"], 5000 ),
                  "myThirdNode"  :(["127.0.0.1","10.0.0.1"], 6000 )] })

        @type clusterId: string
        @param clusterId: name of the cluster
        @type nodes: dict
        @param nodes: A dictionary containing the locations for the server nodes

        """
        self._clusterId = clusterId
        self._nodes = self._cleanUp(nodes)
    

    def _cleanUp(self, nodes):
        for k in nodes.keys():
            t = nodes[k]
            maybe_string = t[0]
            if type(maybe_string) == types.StringType:
                ip_list = maybe_string.split(',')
                port = t[1]
                nodes[k] = (ip_list, port)

        return nodes
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
    
    def getNodeLocations(self, nodeId):
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
        result += "%04X   %-*s   %s\n" % (N, length*3, hexa, s)
        N += length
    return result

# Define the size of an int in bytes
ARA_TYPE_INT64_SIZE = 8
ARA_TYPE_INT_SIZE = 4
ARA_TYPE_BOOL_SIZE = 1

# Magic used to mask each command
ARA_CMD_MAG    = 0xb1ff0000
ARA_CMD_VER    = 0x00000001
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
ARA_CMD_ASSERT       = 0x00000016 | ARA_CMD_MAG
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
ARA_CMD_DELETE_PREFIX            = 0x00000027 | ARA_CMD_MAG
ARA_CMD_VERSION                  = 0x00000028 | ARA_CMD_MAG
ARA_CMD_ASSERT_EXISTS            = 0x00000029 | ARA_CMD_MAG
ARA_CMD_MULTI_GET_OPTION         = 0x00000031 | ARA_CMD_MAG
ARA_CMD_CURRENT_STATE            = 0x00000032 | ARA_CMD_MAG

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
ARA_ERR_WRONG_CLUSTER       = 6
ARA_ERR_ASSERTION_FAILED    = 7
ARA_ERR_RANGE_ERROR         = 9
ARA_ERR_GOING_DOWN          = 16
ARA_ERR_ASSERTEXISTS_FAILED = 17
ARA_ERR_NOT_SUPPORTED       = 0x20

NAMED_FIELD_TYPE_INT    = 1
NAMED_FIELD_TYPE_INT64  = 2
NAMED_FIELD_TYPE_FLOAT  = 3
NAMED_FIELD_TYPE_STRING = 4
NAMED_FIELD_TYPE_LIST   = 5

def _packString( toPack ):
    toPackLength = len( toPack )
    return struct.pack("I%ds" % ( toPackLength), toPackLength, toPack )

def _packStringOption ( toPack = None ):
    if toPack is None:
        return _packBool ( 0 )
    else :
        return _packBool ( 1 ) + _packString (toPack)

def _packInt ( toPack ):
    return struct.pack( "I", toPack )

def _packInt64 ( toPack ):
    return struct.pack( "q", toPack )

def _packSignedInt ( toPack ):
    return struct.pack( "i", toPack )

def _packBool ( toPack) :
    return struct.pack( "?", toPack)

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
            msg = str(con._socketInfo)
            try: 
                con._socket.close()
            except Exception, ex:
                ArakoonClientLogger.logError( "Error while closing socket. %s: %s" % (ex.__class__.__name__,ex))
            con._connected = False
            raise ArakoonSockNotReadable(msg = msg)

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
    return retVal, offset

def _unpackNamedField(buf, offset):
    type, offset = _unpackInt(buf, offset)
    name, offset = _unpackString(buf, offset)
    result = dict()
    
    if type == NAMED_FIELD_TYPE_INT:
        result[name], offset = _unpackInt(buf,offset)
        return result, offset
    if type == NAMED_FIELD_TYPE_INT64:
        result[name], offset = _unpackInt64(buf,offset)
        return result, offset
    if type == NAMED_FIELD_TYPE_FLOAT:
        result[name], offset = _unpackFloat(buf,offset)
        return result, offset
    if type == NAMED_FIELD_TYPE_STRING:
        result[name], offset = _unpackString(buf,offset)
        return result, offset
    if type == NAMED_FIELD_TYPE_LIST:
        length, offset = _unpackInt(buf,offset)
        localDict = dict()
        for i in range(length):
            field, offset = _unpackNamedField(buf, offset)
            localDict.update( field )
        result[name] = localDict
        return result, offset
    
    raise ArakoonException("Cannot decode named field %s. Invalid type: %d" % (name,type) )

def _recvInt ( con ):
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
        return _recvString( con )
    else :
        return None


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

class AssertExists(Update):
    def __init__(self, key):
        self._key = key

    def write(self, fob):
        fob.write(_packInt(15))
        fob.write(_packString(self._key))

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

    def addAssertExists(self, key):
        self._updates.append(AssertExists(key))

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
    def encodeGetVersion():
        r = _packInt(ARA_CMD_VERSION)
        return r
    
    @staticmethod
    def encodeGetCurrentState():
        r = _packInt(ARA_CMD_CURRENT_STATE)
        return r

    @staticmethod
    def encodeWhoMaster():
        return _packInt( ARA_CMD_WHO )

    @staticmethod
    def encodeExists(key, allowDirty):
        msg = _packInt(ARA_CMD_EXISTS)
        msg += _packBool(allowDirty)
        msg += _packString(key)
        return msg

    @staticmethod
    def encodeAssert(key, vo, allowDirty):
        msg = _packInt(ARA_CMD_ASSERT)
        msg += _packBool(allowDirty)
        msg += _packString(key)
        msg += _packStringOption(vo)
        return msg

    @staticmethod
    def encodeAssertExists(key, allowDirty):
        msg = _packInt(ARA_CMD_ASSERT_EXISTS)
        msg += _packBool(allowDirty)
        msg += _packString(key)
        return msg

    @staticmethod
    def encodeGet(key , allowDirty):
        msg = _packInt(ARA_CMD_GET)
        msg += _packBool(allowDirty)
        msg += _packString(key)
        return msg

    @staticmethod
    def encodeSet( key, value ):
        return _packInt( ARA_CMD_SET ) + _packString( key ) + _packString ( value )

    @staticmethod
    def encodeConfirm(key, value):
        return _packInt(ARA_CMD_CONFIRM) + _packString(key) + _packString(value)

    @staticmethod
    def encodeSequence(seq, sync):
        r = cStringIO.StringIO()
        seq.write(r)
        flattened = r.getvalue()
        r.close()
        cmd = ARA_CMD_SEQ
        if sync:
            cmd = ARA_CMD_SYNCED_SEQUENCE
        return _packInt(cmd) + _packString(flattened)     
        
    @staticmethod
    def encodeDelete( key ):
        return _packInt ( ARA_CMD_DEL ) + _packString ( key )

    @staticmethod
    def encodeRange( bKey, bInc, eKey, eInc, maxCnt , allowDirty):
        retVal = _packInt( ARA_CMD_RAN ) + _packBool(allowDirty)
        retVal += _packStringOption( bKey ) + _packBool ( bInc )
        retVal += _packStringOption( eKey ) + _packBool (eInc) + _packSignedInt (maxCnt)
        return  retVal

    @staticmethod
    def encodeRangeEntries(first, finc, last, linc, maxEntries, allowDirty):
        r = _packInt(ARA_CMD_RAN_E) + _packBool(allowDirty)
        r += _packStringOption(first) + _packBool(finc)
        r += _packStringOption(last)  + _packBool(linc)
        r += _packSignedInt(maxEntries)
        return r

    @staticmethod
    def encodeReverseRangeEntries(first, finc, last, linc, maxEntries, allowDirty):
        r = _packInt(ARA_CMD_REV_RAN_E) + _packBool(allowDirty)
        r += _packStringOption(first) + _packBool(finc)
        r += _packStringOption(last) + _packBool(linc)
        r += _packSignedInt(maxEntries)
        return r

    @staticmethod
    def encodePrefixKeys( key, maxCnt, allowDirty ):
        retVal = _packInt( ARA_CMD_PRE) + _packBool(allowDirty)
        retVal += _packString( key )
        retVal += _packSignedInt( maxCnt )
        return retVal

    @staticmethod
    def encodeTestAndSet( key, oldVal, newVal ):
        retVal = _packInt( ARA_CMD_TAS ) + _packString( key )
        retVal += _packStringOption( oldVal )
        retVal += _packStringOption( newVal )
        return retVal

    @staticmethod
    def encodeMultiGet(keys, allowDirty):
        retVal = _packInt(ARA_CMD_MULTI_GET) + _packBool(allowDirty)
        retVal += _packInt(len(keys))
        for key in keys:
            retVal += _packString(key)
        return retVal

    @staticmethod
    def encodeMultiGetOption(keys,allowDirty):
        retVal = _packInt(ARA_CMD_MULTI_GET_OPTION) + _packBool(allowDirty)
        retVal += _packInt(len(keys))
        for key in keys:
            retVal += _packString(key)
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
    def encodeUserFunction(name, argument):
        retVal = _packInt(ARA_CMD_USER_FUNCTION)
        retVal += _packString(name)
        retVal += _packStringOption(argument)
        return retVal
    
    @staticmethod
    def encodeDeletePrefix(prefix):
        retVal =  _packInt(ARA_CMD_DELETE_PREFIX)
        retVal += _packString(prefix)
        return retVal
        
    @staticmethod
    def _evaluateErrorCode( con ):
        errorCode = _recvInt ( con )
        # """ ArakoonException( "Received invalid response from the server" )"""
        if errorCode == ARA_ERR_SUCCESS :
            return
        else :
            errorMsg = _recvString ( con )
        if errorCode == ARA_ERR_NOT_FOUND:
            raise ArakoonNotFound(errorMsg)
        if errorCode == ARA_ERR_NOT_MASTER:
            raise ArakoonNodeNotMaster()
        if errorCode == ARA_ERR_ASSERTION_FAILED:
            raise ArakoonAssertionFailed(errorMsg)
        if errorCode == ARA_ERR_ASSERTEXISTS_FAILED:
            raise ArakoonAssertExistsFailed(errorMsg)    
        if errorCode == ARA_ERR_RANGE_ERROR:
            raise NurseryRangeError(errorMsg) 
        if errorCode == ARA_ERR_GOING_DOWN:
            raise ArakoonGoingDown(errorMsg)
        if errorCode != ARA_ERR_SUCCESS:
            raise ArakoonException( "EC=%d. %s" % (errorCode, errorMsg) )

    @staticmethod
    def decodeInt64Result( con ) :
        ArakoonProtocol._evaluateErrorCode( con )
        return _recvInt64( con )

    @staticmethod
    def decodeIntResult(con):
        ArakoonProtocol._evaluateErrorCode(con)
        return _recvInt(con)
    
    @staticmethod
    def decodeVoidResult( con ):
        ArakoonProtocol._evaluateErrorCode( con )

    @staticmethod
    def decodeBoolResult( con ):
        ArakoonProtocol._evaluateErrorCode( con )
        return _recvBool( con )

    @staticmethod
    def decodeStringResult ( con ):
        ArakoonProtocol._evaluateErrorCode( con )
        return _recvString( con )

    @staticmethod
    def decodeStringOptionResult ( con ):
        ArakoonProtocol._evaluateErrorCode( con )
        return _recvStringOption( con )

    @staticmethod
    def decodeStringListResult( con ):

        ArakoonProtocol._evaluateErrorCode( con )
        retVal = []

        arraySize = _recvInt( con )

        for i in range( arraySize ) :
            retVal[:0] = [ _recvString( con ) ]
        return retVal

    @staticmethod
    def decodeStringOptionListResult(con):
        ArakoonProtocol._evaluateErrorCode(con)
        retVal = []
        arraySize = _recvInt(con)
        for i in range(arraySize):
            s = _recvStringOption(con)
            retVal.append(s)
        return retVal


    @staticmethod
    def decodeNurseryCfgResult( con ):
        ArakoonProtocol._evaluateErrorCode(con)
        
        offset = 0
        encoded = _recvString( con )
        routing, offset = RoutingInfo.unpack(encoded, offset, _unpackBool, _unpackString)
        cfgCount, offset = _unpackInt(encoded, offset)
        resultCfgs = {}
        for i in range(cfgCount) :
            clusterId, offset = _unpackString(encoded, offset)
            clusterSize, offset = _unpackInt(encoded, offset)
            cfg = dict()
            for j in range(clusterSize):
                nodeId, offset = _unpackString(encoded, offset)
                ips, offset = _unpackStringList(encoded, offset)
                port, offset = _unpackInt(encoded, offset)
                cfg[nodeId] = (ips,port)
            cliCfg = ArakoonClientConfig(clusterId, cfg)
            resultCfgs[clusterId] = cliCfg
        return (routing, resultCfgs)      
        

    @staticmethod
    def decodeStringPairListResult(con):
        ArakoonProtocol._evaluateErrorCode(con)
        result = []

        size = _recvInt( con )

        for i in range(size):
            k = _recvString ( con )
            v = _recvString ( con )
            result [:0] = [(k, v)]

        return result

    @staticmethod
    def decodeStatistics(con):
        ArakoonProtocol._evaluateErrorCode(con)
        
        buffer = _recvString(con)
        result, offset = _unpackNamedField(buffer,0)
        return result['arakoon_stats']

    @staticmethod
    def decodeVersionResult(con):
        ArakoonProtocol._evaluateErrorCode(con)
        major = _recvInt(con)
        minor = _recvInt(con)
        patch = _recvInt(con)
        info  = _recvString(con)
        return (major,minor, patch, info)
    
    @staticmethod
    def encodeGetKeyCount () :
        return _packInt(ARA_CMD_KEY_COUNT)
    
    @staticmethod 
    def encodeGetNurseryCfg ():
        return _packInt(ARA_CMD_GET_NURSERY_CFG) 
