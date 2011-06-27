<?php
/*
 * This file is part of Arakoon, a distributed key-value store. 
 * Copyright (C) 2010 Incubaid BVBA
 * Licensees holding a valid Incubaid license may use this file in
 * accordance with Incubaid's Arakoon commercial license agreement. For
 * more information on how to enter into this agreement, please contact
 * Incubaid (contact details can be found on http://www.arakoon.org/licensing).
 * 
 * Alternatively, this file may be redistributed and/or modified under
 * the terms of the GNU Affero General Public License version 3, as
 * published by the Free Software Foundation. Under this license, this
 * file is distributed in the hope that it will be useful, but WITHOUT 
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or 
 * FITNESS FOR A PARTICULAR PURPOSE.
 * 
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the 
 * GNU Affero General Public License along with this program (file "COPYING").
 * If not, see <http://www.gnu.org/licenses/>.
*/

/*
* @copyright Copyright (C) 2010 Incubaid BVBA
*/


require_once 'logging.php';
require_once 'ara_protocol.php';

class ArakoonClientConnection
{

    private $clusterId =0;
    private $nodeLocation = array();
    private $connected = False;
    public $socket = null;
    
    
    /*
     * @param array $nodeLocation : Array holding "ip" and "port"
     * @param string $clusterId : string holding cluster id
     * 
     * @return void
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
        try{
            Logging::trace("Trying to establish connection with node with ip {$this->nodeLocation['ip']}", __FILE__, __FUNCTION__, __LINE__);
            $err = socket_connect($this->socket, $this->nodeLocation['ip'], $this->nodeLocation['client_port']);
            if (!$err){
                Logging::fatal("Cannot connect to node with ip {$this->nodeLocation['ip']}", __FILE__, __FUNCTION__, __LINE__);
                return FALSE;
            }            
        }catch(Exception $ex){
            Logging::fatal("Cannot connect to node with ip {$this->nodeLocation['ip']}", __FILE__, __FUNCTION__, __LINE__);
            throw new Exception("Cannot connect to node with ip {$this->nodeLocation['ip']}, Exception $ex");
        }
        sendPrologue($this->socket, $this->clusterId);
        return $this->connected = True;
    }

    function send($msg){
        Logging::trace("Enter", __FILE__, __FUNCTION__, __LINE__);
        if(!$this->connected){
            if(!$this->reconnect()){
                $msg = "Send Connection failed!";
                Logging::error($msg, __FILE__, __FUNCTION__, __LINE__);                
                throw new Exception($msg);
            }
        }
        return socket_write($this->socket, $msg);
    }
    
    function isConnected()
    {
        return $this->connected;
    }
    
    function close(){
        if(!$this->connected){
            return;
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
