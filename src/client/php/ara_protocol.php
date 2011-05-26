<?php

require_once 'ara_def.php';


function packString($toPack){
    return pack("Ia*", strlen($toPack), $toPack );
}

function packStringOption ($toPack=null){
    if ($toPack == null){
        return packBool(0);
    }
    else{
        return packBool(1).packString($toPack);
    }
}

function packInt($toPack){
    return pack("I", $toPack);
}

function packSignedInt($toPack){
    return pack("i", $toPack);
}

function packBool($toPack){
    /*Note: Used char to pack BOOL as it is not available, meanwhile it occupies a single byte*/
    return pack("c", $toPack);
}

function sendPrologue($socket, $clusterId){
    $p = packInt(ARA_CMD_MAG);
    $p .= packInt(ARA_CMD_VER);
    $p .= packString($clusterId);
    socket_write($socket, $p);
}

function readExactNBytes($con, $n ){
    if (!$con->isConnected()){
        $msg = __FUNCTION__." Socket is not connected";
        throw new Exception($msg);
    }
    $bytesRemaining = $n;
    $tmpResult = "";
    $timeout = ArakoonClientConfig::getConnectionTimeout();

    while ($bytesRemaining > 0){
        $a = array($con->socket);
        $null = null;
        $stat = socket_select( $a, $null, $null, $timeout );
        if ($stat !== FALSE){
            $newChunk = "";
            $newChunk = socket_read($con->socket, $bytesRemaining);
            $newChunkSize = strlen($newChunk);
            if ($newChunkSize == 0){
                $con->close();           
                return FALSE;
            }
            $tmpResult = $tmpResult . $newChunk;
            $bytesRemaining -= $newChunkSize;
        }
        else{
            $con->close();
            $msg = __FUNCTION__ . " Failed to Select $stat";
            throw new Exception($msg);
        }
    }

    return $tmpResult;

}

function recvString($con){
    $strLength = recvInt($con);
    $buf = readExactNBytes($con, $strLength);
    $ret = unpack("a*str", $buf);
    return $ret['str'];
}

function unpackInt($buf, $offset){
    if($offset > 0){
        $r = unpack("c{$offset}char/Iint", $buf);
    }else{
        $r = unpack("Iint", $buf);
    }
    return array($r['int'], $offset + ARA_TYPE_INT_SIZE);
}

function unpackInt64($buf, $offset){
    $msg = __FUNCTION__ . " Not Implemented";
    throw new Exception($msg);
}
    
function unpackString($buf, $offset){
    list($size,$o2) = unpackInt($buf, $offset);
    $v = substr($buf, $o2, o2+$size);
    return array($v, $o2+$size);    
}
    
function recvInt($con){
    $buf = readExactNBytes($con, ARA_TYPE_INT_SIZE);
    
    list($i,$o2) = unpackInt($buf,0);
    return $i;
}
    
function recvBool ($con){
    $buf = readExactNBytes($con, ARA_TYPE_BOOL_SIZE);
    $bool = unpack("cbool", $buf);
    return $bool['bool'];
}
    
function unpackFloat($buf, $offset){
    if($offset > 0){
        $r = unpack("c{$offset}char/dfloat", $buf);
    }else{
        $r = unpack("dfloat", $buf);
    }    
    
    return array($r['float'], $offset+8);
}
    
function recvFloat($con){
    $buf = readExactNBytes($con, 8);
    list($f,$o2)= unpackFloat($buf,0);
    return $f;
}
    
function recvStringOption($con){
    $isSet = recvBool($con);
    if($isSet){
        return recvString($con);
    }
    
    return null;
}


class Set
{
 
    private $key;
    private $value;

    public function __construct($key, $value) {
        $this->key = $key;
        $this->value = $value;
    }
    
    function write($fob){
        $fob->write(packInt(1));
        $fob->write(packString($this->key));
        $fob->write(packString($this->value));
    }
    
}

class Delete
{
    private $key;
    public function __construct($key){
        $this->key = $key;
    }
    
    function write($fob){
        $fob->write(packInt(2));
        $fob->write(packString($this->key));
    }
    
}
    
class Sequence 
{
    private $updates;
    
    public function  __contruct(){
        $this->updates = array();
    }
    
    function addUpdate($u){
        $this->updates[] = $u;
    }
    
    function addSet($key, $value){
        $this->updates[] = new Set($key,$value);
    }
    
    function addDelete($key){
        $this->updates[] = new Delete($key);
    }

    function write($fob){
        $fob->write(packInt(5));
        $fob->write(packInt(count($this->updates)));
        foreach ($this->updates as $update) {
            $update->write($fob);
        }
    }
    
}


class StringIO
{
    private $value = "";
    public function __construct(){
    }

    public function write($val){
        $this->value .= $val;
    }

    public function getValue(){
        return $this->value;
    }

    public function close(){
        $this->value = "";
    }
}


class ArakoonProtocol
{
    public function __construct() {
        ;
    }
    
    static function encodePing($clientId, $clusterId ){
        $r  = packInt(ARA_CMD_HEL);
        $r .= packString($clientId);
        $r .= packString($clusterId);
        return $r;
    }

    static function encodeWhoMaster(){
        return packInt(ARA_CMD_WHO);
    }

    static function encodeExists($key, $allowDirty){
        $msg = packInt(ARA_CMD_EXISTS);
        $msg .= packBool($allowDirty);
        $msg .= packString($key);
        return $msg;
    }

    static function encodeGet($key , $allowDirty){
        $msg = packInt(ARA_CMD_GET);
        $msg .= packBool($allowDirty);
        $msg .= packString($key);
        return $msg;
    }

    static function encodeSet($key, $value ){
        return packInt( ARA_CMD_SET ) . packString( $key ) . packString ( $value );
    }

    static function encodeSequence($seq){
        $r = new StringIO();
        $seq->write($r);
        $flattened = $r->getValue();
        $r->close();
        return packInt(ARA_CMD_SEQ) . packString($flattened);
    }
        
    static function encodeDelete($key){
        return packInt(ARA_CMD_DEL) . packString($key);
    }

    static function encodeRange($bKey, $bInc, $eKey, $eInc, $maxCnt , $allowDirty){
        $retVal = packInt(ARA_CMD_RAN) . packBool($allowDirty);
        $retVal .= packStringOption($bKey) . packBool($bInc);
        $retVal .= packStringOption($eKey) . packBool($eInc) . packSignedInt($maxCnt);
        return  $retVal;
    }

    static function encodeRangeEntries($first, $finc, $last, $linc, $maxEntries, $allowDirty){
        $r = packInt(ARA_CMD_RAN_E) . packBool($allowDirty);
        $r .= packStringOption($first) . packBool($finc);
        $r .= packStringOption($last)  . packBool($linc);
        $r .= packSignedInt($maxEntries);
        return $r;
    }

    static function encodePrefixKeys($key, $maxCnt, $allowDirty ){
        $retVal = packInt(ARA_CMD_PRE) . packBool($allowDirty);
        $retVal .= packString($key);
        $retVal .= packSignedInt($maxCnt);
        return $retVal;
    }

    static function encodeTestAndSet($key, $oldVal, $newVal ){
        $retVal = packInt(ARA_CMD_TAS) . packString($key);
        $retVal .= packStringOption($oldVal);
        $retVal .= packStringOption($newVal);
        return $retVal;
    }

    static function encodeMultiGet($keys, $allowDirty){
        $retVal = packInt(ARA_CMD_MULTI_GET) . packBool($allowDirty);
        $retVal .= packInt(count($keys));
        foreach($keys as $key){
            $retVal .= packString($key);
        }
        return $retVal;
    }

    static function encodeExpectProgressPossible(){
        $retVal = packInt(ARA_CMD_EXPECT_PROGRESS_POSSIBLE);
        return $retVal;
    }

    static function encodeStatistics(){
        $retVal = packInt(ARA_CMD_STATISTICS);
        return $retVal;
    }
    
    static function decodeVoidResult($con){
        ArakoonProtocol::evaluateErrorCode($con);
    }
    
    static function decodeBoolResult($con){
        ArakoonProtocol::evaluateErrorCode($con);
        return recvBool($con);
    }
    
    static function decodeStringResult($con){
        ArakoonProtocol::evaluateErrorCode($con);
        return recvString($con);
    }
    
    static function decodeStringOptionResult($con){
        ArakoonProtocol::evaluateErrorCode($con);
        return recvStringOption($con);
    }

    static function decodeStringListResult($con){
        ArakoonProtocol::evaluateErrorCode($con);
        $retVal = array();

        $arraySize = recvInt($con);

        for($i=0; $i < $arraySize; $i++){
            array_unshift($retVal, recvString($con));
        }
        return $retVal;
    }
    
    static function decodeStringPairListResult($con){
        ArakoonProtocol::evaluateErrorCode($con);
        $result = array();
        $size = recvInt($con);

        for($i=0; $i < $size; $i++){
            $k = recvString($con);
            $v = recvString($con);
            array_unshift($result, array($k, $v));
        }

        return $result;
    }

    static function decodeStatistics($con){
        ArakoonProtocol::evaluateErrorCode($con);
        $result = array();
        $buffer = recvString($con);
        #struct.unpack("ddddd...", buffer) would have been better...
        list($start,$o2)        = unpackFloat($buffer,0);
        list($last, $o3)        = unpackFloat($buffer, $o2);
        list($avg_set_size,$o4) = unpackFloat($buffer, $o3);
        list($avg_get_size,$o5) = unpackFloat($buffer, $o4);
        list($n_sets,$o6)       = unpackInt($buffer, $o5);
        list($n_gets,$o7)       = unpackInt($buffer, $o6);
        list($n_deletes,$o8)    = unpackInt($buffer, $o7);
        list($n_multigets,$o9)  = unpackInt($buffer, $o8);
        list($n_sequences,$o10) = unpackInt($buffer, $o9);
        list($n_entries, $o11)  = unpackInt($buffer, $o10);
        $node_is = array();
        $offset = $o11;
        for ($j=0; $j < $n_entries; $j++){
            list($name, $offset) = unpackString($buffer, $offset);
            list($i,    $offset) = unpackInt64($buffer,  $offset);
            $node_is[$name]=$i;
        }
        $result['start'] = $start;
        $result['last'] = $last;
        $result['avg_set_size'] = $avg_set_size;
        $result['avg_get_size'] = $avg_get_size;
        $result['n_sets'] = $n_sets;
        $result['n_gets'] = $n_gets;
        $result['n_deletes'] = $n_deletes;
        $result['n_multigets'] = $n_multigets;
        $result['n_sequences'] = $n_sequences;
        $result['node_is'] = $node_is;
        return $result;
    }
    
    static function evaluateErrorCode($con){
        $errorCode = recvInt($con);
        if ($errorCode == ARA_ERR_SUCCESS)
            return;
        else
            $errorMsg = recvString($con);

        if ($errorCode == ARA_ERR_NOT_FOUND){
            $msg = __FUNCTION__ . " Error: " . ARA_ERR_NOT_FOUND . " Message: $errorMsg";
            throw new Exception($msg);
        }
        if ($errorCode == ARA_ERR_NOT_MASTER){
            $msg = __FUNCTION__ . " Error: " . ARA_ERR_NOT_FOUND . " Message: $errorMsg";
            throw new Exception($msg);
        }
        if (errorCode != ARA_ERR_SUCCESS){
            $msg = __FUNCTION__ . " Error: $errorCode";
            throw new Exception($msg);
        }
    }
    
}

?>