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

import struct
import logging
import select
import cStringIO

FILTER = ''.join([(len(repr(chr(x)))==3) and chr(x) or '.' for x in range(256)])

ARA_CFG_TRY_CNT = 1
ARA_CFG_CONN_TIMEOUT = 60
ARA_CFG_CONN_BACKOFF = 5
ARA_CFG_NO_MASTER_RETRY = 60

class ArakoonClientConfig :

    def __init__ (self, nodes=None):
        """
        Constructor of an ArakoonClientConfig object

        The constructor takes one optional parameter 'nodes'.
        This is a dictionary containing info on the arakoon server nodes. It contains:
          - nodeids as keys
          - (hostname/ip, tcp port) tuples as value
        e.g. ::
            cfg = ArakoonClientConfig ( { "myFirstNode" : ( "127.0.0.1", 4000 ),
                    "mySecondNode" : ( "127.0.0.1", 5000 ) ,
                    "myThirdNode"  : ( "127.0.0.1", 6000 ) } )
        Defaults to a single node running on localhost:4000

        @type nodes: dict
        @param nodes: A dictionary containing the locations for the server nodes

        """
        if nodes is None:
            self._nodes = { "arakoon_0" : ( "127.0.0.1", 4000 ) }

        else :
            self._nodes = nodes

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
ARA_TYPE_INT_SIZE = 4
ARA_TYPE_BOOL_SIZE = 1

# Magic used to mask each command
ARA_CMD_MAG = 0xb1ff0000

# Hello command
ARA_CMD_HEL = 0x00000001 | ARA_CMD_MAG
# Who is master?
ARA_CMD_WHO = 0x00000002 | ARA_CMD_MAG
# Existence of a value for a key
ARA_CMD_EXISTS = 0x07    | ARA_CMD_MAG
# Get a value
ARA_CMD_GET = 0x00000008 | ARA_CMD_MAG
# Update a value
ARA_CMD_SET = 0x00000009 | ARA_CMD_MAG
# Delete a key value pair
ARA_CMD_DEL = 0x0000000a | ARA_CMD_MAG
# Get a range of keys
ARA_CMD_RAN = 0x0000000b | ARA_CMD_MAG
# Get keys matching a prefix
ARA_CMD_PRE = 0x0000000c | ARA_CMD_MAG
# Test and set a value
ARA_CMD_TAS = 0x0000000d | ARA_CMD_MAG
# range entries
ARA_CMD_RAN_E = 0x0000000f | ARA_CMD_MAG

#sequence
ARA_CMD_SEQ                      = 0x00000010 | ARA_CMD_MAG

ARA_CMD_MULTI_GET                = 0x00000011 | ARA_CMD_MAG

ARA_CMD_EXPECT_PROGRESS_POSSIBLE = 0x00000012 | ARA_CMD_MAG

ARA_CMD_STATISTICS               = 0x00000013 | ARA_CMD_MAG

# Arakoon error codes
# Success
ARA_ERR_SUCCESS = 0
# No entity
ARA_ERR_NO_ENT = 1
# Node is not the master
ARA_ERR_NOT_MASTER = 4
# not found
ARA_ERR_NOT_FOUND = 5


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

def _packSignedInt ( toPack ):
    return struct.pack( "i", toPack )

def _packBool ( toPack) :
    return struct.pack( "?", toPack)

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

def _recvInt ( con ):
    buf = _readExactNBytes ( con, ARA_TYPE_INT_SIZE )
    i,o2 = _unpackInt(buf,0)
    return i

def _recvBool ( con ):
    buf = _readExactNBytes( con, 1 )
    return struct.unpack( "?", buf ) [0]

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

    def write(self, fob):
        fob.write( _packInt(5))
        fob.write( _packInt(len(self._updates)))
        for update in self._updates:
            update.write(fob)
        
        
class ArakoonProtocol :

    @staticmethod
    def encodeHello(clientId, clusterId ):
        r  = _packInt(ARA_CMD_HEL)
        r += _packString(clientId)
        r += _packString(clusterId)
        return r

    @staticmethod
    def encodeWhoMaster():
        return _packInt( ARA_CMD_WHO )

    @staticmethod
    def encodeExists(key):
        return _packInt(ARA_CMD_EXISTS) + _packString(key)

    @staticmethod
    def encodeGet( key ):
        return _packInt( ARA_CMD_GET ) + _packString(key)

    @staticmethod
    def encodeSet( key, value ):
        return _packInt( ARA_CMD_SET ) + _packString( key ) + _packString ( value )

    @staticmethod
    def encodeSequence(seq):
        r = cStringIO.StringIO()
        seq.write(r)
        flattened = r.getvalue()
        r.close()
        return _packInt(ARA_CMD_SEQ) + _packString(flattened)
        
        

    @staticmethod
    def encodeDelete( key ):
        return _packInt ( ARA_CMD_DEL ) + _packString ( key )

    @staticmethod
    def encodeRange( bKey, bInc, eKey, eInc, maxCnt ):
        retVal = _packInt( ARA_CMD_RAN ) + _packStringOption( bKey ) + _packBool ( bInc )
        retVal += _packStringOption( eKey ) + _packBool (eInc) + _packSignedInt (maxCnt)
        return  retVal

    @staticmethod
    def encodeRangeEntries(first, finc, last, linc, maxEntries):
        r = _packInt(ARA_CMD_RAN_E)
        r += _packStringOption(first) + _packBool(finc)
        r += _packStringOption(last)  + _packBool(linc)
        r += _packSignedInt(maxEntries)
        return r

    @staticmethod
    def encodePrefixKeys( key, maxCnt ):
        retVal = _packInt( ARA_CMD_PRE) + _packString( key )
        retVal += _packSignedInt( maxCnt )
        return retVal

    @staticmethod
    def encodeTestAndSet( key, oldVal, newVal ):
        retVal = _packInt( ARA_CMD_TAS ) + _packString( key )
        retVal += _packStringOption( oldVal )
        retVal += _packStringOption( newVal )
        return retVal

    @staticmethod
    def encodeMultiGet(keys):
        retVal = _packInt(ARA_CMD_MULTI_GET)
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
        
        if errorCode != ARA_ERR_SUCCESS:
            raise ArakoonException( "EC=%d. %s" % (errorCode, errorMsg) )

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
        result = {}
        buffer = _recvString(con)
        #struct.unpack("ddddd...", buffer) would have been better...
        start,o2        = _unpackFloat(buffer,0)
        last, o3        = _unpackFloat(buffer,o2)
        avg_set_size,o4 = _unpackFloat(buffer,o3)
        avg_get_size,o5 = _unpackFloat(buffer,o4)
        n_sets,o6       = _unpackInt(buffer,o5)
        n_gets,o7       = _unpackInt(buffer,o6)
        n_deletes,o8    = _unpackInt(buffer,o7)
        n_multigets,o9  = _unpackInt(buffer,o8)
        n_sequences,o10 = _unpackInt(buffer,o9)
        result['start'] = start
        result['last'] = last
        result['avg_set_size'] = avg_set_size
        result['avg_get_size'] = avg_get_size
        result['n_sets'] = n_sets
        result['n_gets'] = n_gets
        result['n_deletes'] = n_deletes
        result['n_multigets'] = n_multigets
        result['n_sequences'] = n_sequences
        return result
