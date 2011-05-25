<?php

include_once 'ara_protocol.php';

class ArakoonClientConnection
{

    private $clusterId =0;
    private $nodeLocation = array();
    private $connected = False;
    public $socket = null;
    
    
    /*
     * @param nodeLocation : Array holding "ip" and "port"
     * @param clusterId : string holding cluster id
     * @rtype: void
    */
    function __construct($nodeLocation, $clusterId) {
        $this->clusterId = $clusterId;
        $this->nodeLocation = $nodeLocation;
        $this->connected = False;
        $this->socket = null;
        $this->reconnect();
    }

    private function reconnect(){
        
        $this->close();

        $this->socket = socket_create(AF_INET, SOCK_STREAM, 0);
        $err = socket_connect($this->socket, $this->nodeLocation['ip'], $this->nodeLocation['port']);
        if (!$err){
            return FALSE;
        }
        sendPrologue($this->socket, $this->clusterId);
        return $this->connected = True;
    }

    function send($msg){
        if(!$this->connected){
            if(!$this->reconnect()){
                return FALSE;
            }
        }
        socket_write($this->socket, $msg);      
    }
    
    function isConnected()
    {
        return $this->connected;
    }
    
    function close(){
        if(!$this->connected){
            return TRUE;
        }
        socket_close($this->socket);
        $this->connected = FALSE;
    }
    
    
    function decodeStringResult(){
        return ArakoonProtocol::decodeStringResult($this);
    }

    function decodeBoolResult(){
        return ArakoonProtocol::decodeBoolResult($this);
    }

    function decodeVoidResult(){
        ArakoonProtocol::decodeVoidResult($this);
    }

    function decodeStringOptionResult(){
        return ArakoonProtocol::decodeStringOptionResult($this);
    }

    function decodeStringListResult(){
        return ArakoonProtocol::decodeStringListResult($this);
    }

    function decodeStringPairListResult(){
        return ArakoonProtocol::decodeStringPairListResult($this);
    }

    function decodeStatistics(){
        return ArakoonProtocol::decodeStatistics($this);
    }
    
}    
    
?>
