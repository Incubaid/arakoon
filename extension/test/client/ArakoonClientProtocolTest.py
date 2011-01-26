'''
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
'''


#from pymonkey import q
from nose.tools import *

from arakoon import *

UT_ENCODED_GET_REQUEST = "0800EDFE030000006B6579"
UT_ENCODED_SET_REQUEST = "0900EDFE030000006B65790500000076616C7565"
UT_ENCODED_HELLO_REQUEST = "0100EDFE0800000068656C6C6F4D7367"
UT_ENCODED_WHO_MASTER_REQUEST = "0200EDFE"
UT_ENCODED_DELETE_REQUEST = "0A00EDFE030000006B6579"
UT_ENCODED_RANGE_REQUEST = "0B00EDFE0105000000737461727401010400000073746F700070000000"
UT_ENCODED_PREFIX_KEYS_REQUEST = "0C00EDFE0600000070726566697870000000"
UT_ENCODED_TEST_AND_SET_REQUEST = "0D00EDFE030000006B657901060000006F6C6456616C060000006E657756616C"


UT_ENCODED_STRING_RESPONSE = "000000000500000076616C7565"
UT_ENCODED_VOID_RESPONSE = "00000000"
UT_ENCODED_ERROR_RESPONSE = "19000000050000004572726F72"


def _binStringToHex ( binString ):
    return "".join( ["%02X"%ord(x) for x in binString ] )

def _hexStringToBinary( hexString ):
    i = 0
    result = ""
    while  i < len(hexString)  :
        intVal = int( hexString[i:i+2] , 16 )
        result = result + struct.pack( "B", intVal )
        i = i + 2
    return result



def test_encode_get_request():
    encodedGetReq = _binStringToHex( ArakoonProtocol.encodeGet( "key")  )
    assert_equals( encodedGetReq, UT_ENCODED_GET_REQUEST )

def test_encode_set_request():
    encodedSetReq = _binStringToHex( ArakoonProtocol.encodeSet( "key", "value")  )
    assert_equals( encodedSetReq, UT_ENCODED_SET_REQUEST )

def test_encode_hello_request () :
    encodedHelloReq = _binStringToHex ( ArakoonProtocol.encodeHello( "helloMsg" ) )
    assert_equals( encodedHelloReq, UT_ENCODED_HELLO_REQUEST)

def test_encode_who_master_request() :
    encodedWhoMasterReq = _binStringToHex ( ArakoonProtocol.encodeWhoMaster() )
    assert_equals( encodedWhoMasterReq, UT_ENCODED_WHO_MASTER_REQUEST )

def test_encode_delete():
    encodedDeleteReq = _binStringToHex ( ArakoonProtocol.encodeDelete( "key" ) )
    assert_equals( encodedDeleteReq, UT_ENCODED_DELETE_REQUEST )

def test_encode_range():
    encodedRangeReq = _binStringToHex( ArakoonProtocol.encodeRange ( "start", True, "stop", False, 112 ) )
    assert_equals( encodedRangeReq, UT_ENCODED_RANGE_REQUEST )

def test_encode_prefix_keys() :
    encodedPrefixKeysReq = _binStringToHex( ArakoonProtocol.encodePrefixKeys( "prefix", 112) )
    assert_equals( encodedPrefixKeysReq, UT_ENCODED_PREFIX_KEYS_REQUEST )

def test_encode_test_and_set ():
    encodedTestAndSetReq = _binStringToHex( ArakoonProtocol.encodeTestAndSet( "key", "oldVal", "newVal" ) )
    assert_equals( encodedTestAndSetReq, UT_ENCODED_TEST_AND_SET_REQUEST )

def test_decode_string_response():
    value = ArakoonProtocol.decodeStringResult( _hexStringToBinary( UT_ENCODED_STRING_RESPONSE ) )
    assert_equals( value, "value" )

def test_decode_void_response ():
    ArakoonProtocol.decodeVoidResult( _hexStringToBinary( UT_ENCODED_VOID_RESPONSE ) )

def test_decode_error_response() :
    assert_raises( ArakoonException, ArakoonProtocol.decodeVoidResult, ( _hexStringToBinary( UT_ENCODED_ERROR_RESPONSE ) ) )

if __name__ == "__main__" :

    test_encode_get_request ()
    test_encode_set_request ()
    test_encode_hello_request()
    test_encode_who_master_request()
    test_encode_delete()
    test_encode_range()
    test_encode_prefix_keys()
    test_encode_test_and_set()

    test_decode_void_response()
    test_decode_string_response()
    test_decode_error_response()


