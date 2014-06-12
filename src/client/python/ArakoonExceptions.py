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




class ArakoonException (Exception) :
    _msg = None

    def __init__ (self, msg = "" ):
        if ( self._msg is not None and msg == "" ):
            msg = self._msg
        Exception.__init__ (self, msg)

class ArakoonInvalidConfig( ArakoonException, RuntimeError ):
    _msg = "Invalid configuration"

    def __init__ (self, additionalInfo="" ):
        if additionalInfo != "" :
            self._msg = "%s: %s" % (self._msg, additionalInfo)


class ArakoonNotFound( ArakoonException, KeyError ):
    _msg = "Key not found"

class ArakoonUnknownNode (ArakoonException):
    _msgF = "Unknown node identifier: %s"

    def __init__ (self, nodeId):
        self._msg = ArakoonUnknownNode._msgF % nodeId
        ArakoonException.__init__( self, self._msg )

class ArakoonNodeNotLocal( ArakoonException ):
    _msgF = "%s is not a local node"

    def __init__ (self, node):
        self._msg = ArakoonNodeNotLocal._msgF % ( node )
        ArakoonException.__init__( self, self._msg )

class ArakoonNotConnected( ArakoonException ):
    _msgF = "No connection available to node at %s on port %s"

    def __init__ (self, t):
        ips = t[0]
        port = t[1]
        self._msg = ArakoonNotConnected._msgF % ( ips, port )
        ArakoonException.__init__( self, self._msg )

class ArakoonNoMaster( ArakoonException ):
    _msg = "Could not determine the Arakoon master node"

class ArakoonNoMasterResult( ArakoonException ):
    _msg = "Master could not be contacted."

class ArakoonNodeNotMaster( ArakoonException ):
    _msg = "Cannot perform operation on non-master node"

class ArakoonNodeNoLongerMaster( ArakoonException ):
    _msg = "Operation might or might not be performed on node which is no longer the master"

class ArakoonAssertionFailed(ArakoonException):
    _msg = "Assert did not yield expected result"
    def __init__(self,msg):
        ArakoonException.__init__(self,msg)

class ArakoonAssertExistsFailed(ArakoonException):
    _msg = "AssertExists did not yield expected result"
    def __init__(self,msg):
        ArakoonException.__init__(self,msg)

class ArakoonBadInput( ArakoonException ):
    _msg = "Bad input for arakoon operation"

class ArakoonInconsistentRead( ArakoonException ):
    _msg = "Node could not satisfy the specified consistency"

class ArakoonUserfunctionFailure( ArakoonException ):
    _msg = "User function failed in an unexpected way"

class ArakoonGoingDown(ArakoonException):
    _msg = "Server is going down"

class ArakoonSocketException ( ArakoonException ):
    pass

class ArakoonNotSupportedException(ArakoonException):
    pass

class ArakoonSockReadNoBytes( ArakoonSocketException ):
    _msg = "Could not read a single byte from the socket. Aborting."

class ArakoonSockNotReadable( ArakoonSocketException ):
    _msg = "Socket is not readable. Aborting."

class ArakoonSockRecvError ( ArakoonSocketException ):
    _msg = "Error while receiving data from socket"

class ArakoonSockRecvClosed( ArakoonSocketException ):
    _msg = "Cannot receive on a not-connected socket"

class ArakoonSockSendError( ArakoonSocketException ):
    _msg = "Error while sending data on socket"

class ArakoonInvalidArguments( ArakoonException, TypeError ):
    _msgF = "Invalid argument(s) for %s: %s"

    def __init__ (self, fun_name, invalid_args):
        error_string = "%s=%s" % (invalid_args[0][0],invalid_args[0][1])
        if len(invalid_args) > 1 :
            error_string = reduce( lambda s1,s2: "%s, %s=%s" % (s1,s2[0],s2[1]) , invalid_args[1:], error_string )
        self._msg = ArakoonInvalidArguments._msgF % ( fun_name, error_string)
        ArakoonException.__init__(self, self._msg )

class NurseryException( ArakoonException ):
    pass

class NurseryRangeError( NurseryException ):
    pass

class NurseryInvalidConfig(NurseryException):
    pass
