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

class ArakoonAssertionFailed(ArakoonException):    
    _msg = "Assert did not yield expected result"
    def __init__(self,msg):
        ArakoonException.__init__(self,msg)

class ArakoonAssertExistsFailed(ArakoonException):    
    _msg = "AssertExists did not yield expected result"
    def __init__(self,msg):
        ArakoonException.__init__(self,msg)

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
