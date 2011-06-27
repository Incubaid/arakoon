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
require_once 'ara_connection.php';
require_once 'ara_config.php';
require_once 'ara_protocol.php';
require_once 'ara_def.php';


/*
 * Arakoon client class
*/
class Arakoon
{   
    private $masterId = null;
    private $config = null;
    private $connections = array();
    private $allowDirty = false;
    private $dirtyReadNode = 0;
    
    /*Constructor of an Arakoon client object.
     * It takes one optional paramater 'config'.
     * This parameter contains info on the arakoon server nodes.
     * See the constructor of L{ArakoonClientConfig} for more details.
     * 
     * @param ArakoonClientConfig config: The L{ArakoonClientConfig} object to be used by the client. Defaults to None in which
     * case a default L{ArakoonClientConfig} object will be created.
    */
    public function __construct($config = NULL) {
        if ($config == NULL)
        {
            $config = new ArakoonClientConfig();
        }

        $this->config = $config;
        $nodes = $this->config->getNodes();
        if (count($nodes) == 0){
            throw new Exception('Error, Node list empty!');
        }
    }
    
    /*
     * Get configuration from file in QBASE
     * 
     * @param string $clusterId used to get the corresponding config file in QBASE
     * 
     * @return Arakoon new Arakoon object
     */
    
    static function getClient($clusterId){
        
        if(file_exists(ARA_CFG_QBASE5_PATH)){
            $cfgPath = ARA_CFG_QBASE5_PATH;
        }
        elseif(file_exists(ARA_CFG_QBASE3_PATH)){
            $cfgPath = ARA_CFG_QBASE3_PATH;
        }
        else{
            throw new Exception("Error: Couldn't find a configuration file for cluster: $clusterId");
        }
        
        $arakoonclusters = parse_ini_file($cfgPath, true);
        if (!array_key_exists($clusterId, $arakoonclusters)){
            throw Exception("No such client configured for cluster [{$clusterId}].");
        }
        $clustersection = $arakoonclusters[$clusterId];
        if (!array_key_exists("path", $clustersection)){
            throw Exception("No such client configured for cluster [{$clusterId}].");
        }
        $path = $clustersection["path"];
        $arakoonclient = parse_ini_file($path. "/{$clusterId}_client.cfg", true);
        if( !array_key_exists("global", $arakoonclient)){
            throw Exception("No such client configured for cluster [{$clusterId}].");
        }
        $nodes = split(",", $arakoonclient["global"]["cluster"]);
        $nodes_array = array();
        foreach($nodes as $node){
            $node = trim($node);
            $ip = $arakoonclient[$node]["ip"];
            $port = $arakoonclient[$node]["client_port"];
            $nodes_array[$node] = array("ip"=> $ip, "port"=> $port);
        }
        $cfg = new ArakoonClientConfig($clusterId, $nodes_array);
        return new Arakoon($cfg);
    }
    
    function allowDirtyReads(){
        $this->allowDirty = TRUE;
    }
    
    function disallowDirtyReads(){
        $this->allowDirty = FALSE;
    }

    /*
     * Set the node that will be used for dirty read operations
     * 
     * @param string node : the node identifier
     * 
     * @return void
    */
    function setDirtyReadNode($node){
        if (!in_array($node, array_keys($this->config->getNodes()))){
                throw new Exception ("Unkown Node: $node");
        }
        $this->dirtyReadNode = $node;
    }

    /*
     * Retrieve the node that will be used for dirty read operations
     * 
     * @return string :the node identifier
    */
    function getDirtyReadNode(){
        return $this->dirtyReadNode;
    }

    
    /*
     * send a hello message to the node with your id and the cluster id.
     * Will return the server node identifier and the version of arakoon it is running
     * 
     * @param string $clientId: string
     * @param string $clusterId : must match the cluster_id of the node
     * 
     * @return string :The master identifier and its version in a single string
    */
    function hello ($clientId, $clusterId='arakoon'){
        Logging::trace("Enter", __FILE__, __FUNCTION__, __LINE__);
        $encoded = ArakoonProtocol::encodePing($clientId, $clusterId);
        $conn = $this->sendToMaster($encoded);
        return $conn->decodeStringResult();
    }
    
    /*
     * Checks if certain Key exists in the Arakoon store
     * 
     * @param string $key : key
     * 
     * @return bool :True if there is a value for that key, False otherwise    
     */
    function exists($key){
        Logging::trace("Enter", __FILE__, __FUNCTION__, __LINE__);
        $msg = ArakoonProtocol::encodeExists($key, $this->allowDirty);
        if ($this->allowDirty){
            $conn = $this->sendMessage($this->dirtyReadNode, $msg);
        }
        else{
            $conn = $this->sendToMaster($msg);
        }
        return $conn->decodeBoolResult();
    }
    
        
    /*
     * Retrieve a single value from the store.
     * Retrieve the value associated with the given key
     * 
     * @param string $key : The key whose value you are interested in
     * 
     * @return string :The value associated with the given key
     */    
    function get($key){ 
        Logging::trace("Enter", __FILE__, __FUNCTION__, __LINE__);
        $msg = ArakoonProtocol::encodeGet($key, $this->allowDirty);
        if ($this->allowDirty){
            $conn = $this->sendMessage($this->dirtyReadNode, $msg);
        }
        else{
            $conn = $this->sendToMaster($msg);
        }
        return $conn->decodeStringResult();
    }
    

    /*
     * Retrieve the values for the keys in the given list.
     * 
     * @param string array $key: string list
     * 
     * @return string array :the values associated with the respective keys
     */
    function multiGet($keys){
        Logging::trace("Enter", __FILE__, __FUNCTION__, __LINE__);
        $msg = ArakoonProtocol::encodeMultiGet($keys, $this->allowDirty);
        if ($this->allowDirty){
            $conn = $this->sendMessage($this->dirtyReadNode, $msg);
        }
        else{
            $conn = $this->sendToMaster($msg);
        }
        $result = $conn->decodeStringListResult();
        return $result;
    }
    

    /*
     * Update the value associated with the given key.
     * If the key does not yet have a value associated with it, a new key value pair will be created.
     * If the key does have a value associated with it, it is overwritten.
     * For conditional value updates see L{testAndSet}
     * 
     * @param string $key: The key whose associated value you want to update
     * @param string $value: The value you want to store with the associated key
     * 
     * @return void
     * 
     */
    function set($key, $value){
        Logging::trace("Enter", __FILE__, __FUNCTION__, __LINE__);
        $conn = $this->sendToMaster(ArakoonProtocol::encodeSet($key, $value));
        $conn->decodeVoidResult();
    }
    
    /*
     * Try to execute a sequence of updates.
     * 
     * It's all-or-nothing: either all updates succeed, or they all fail.
     * 
     * @param Sequence seq: Sequence of operations
     * 
     */
    function sequence($seq){
        Logging::trace("Enter", __FILE__, __FUNCTION__, __LINE__);
        $encoded = ArakoonProtocol::encodeSequence($seq);
        $conn = $this->sendToMaster($encoded);
        $conn->decodeVoidResult();
    }
    
    /*
     * Remove a key-value pair from the store.
     * 
     * @param string $key: Remove this key and its associated value from the store
     * 
     * @return void 
     */
    function delete($key){
        Logging::trace("Enter", __FILE__, __FUNCTION__, __LINE__);
        $conn = $this->sendToMaster(ArakoonProtocol::encodeDelete($key));
        $conn->decodeVoidResult();
    }
    
    /*
     * Perform a range query on the store, retrieving the set of matching keys
     * Retrieve a set of keys that lexographically fall between the beginKey and the endKey
     * You can specify whether the beginKey and endKey need to be included in the result set
     * Additionaly you can limit the size of the result set to maxElements. Default is to return all matching keys.
     * 
     * @param string $beginKey: Lower boundary of the requested range
     * @param bool $beginKeyIncluded: Indicates if the lower boundary should be part of the result set
     * @param string $endKey: Upper boundary of the requested range
     * @param bool $endKeyIncluded: Indicates if the upper boundary should be part of the result set
     * @param int $maxElements: The maximum number of keys to return. Negative means no maximum, all matches will be returned. Defaults to -1.
     * 
     * @return array :Returns a list containing all matching keys
     * 
    */
    function range($beginKey, $beginKeyIncluded, $endKey, $endKeyIncluded, $maxElements=-1 ){
        Logging::trace("Enter", __FILE__, __FUNCTION__, __LINE__);
        $msg = ArakoonProtocol::encodeRange($beginKey, $beginKeyIncluded, $endKey,
                                           $endKeyIncluded, $maxElements, $this->allowDirty);
        if ($this->allowDirty){
            $conn = $this->sendMessage($this->dirtyReadNode, $msg);
        }
        else{
            $conn = $this->sendToMaster($msg);
        }
        return $conn->decodeStringListResult();
    }
    

    /*
     * Perform a range query on the store, retrieving the set of matching key-value pairs
     * Retrieve a set of keys that lexographically fall between the beginKey and the endKey
     * You can specify whether the beginKey and endKey need to be included in the result set
     * Additionaly you can limit the size of the result set to maxElements. Default is to return all matching keys.
     * 
     * @param string $beginKey: Lower boundary of the requested range
     * @param bool $beginKeyIncluded: Indicates if the lower boundary should be part of the result set
     * @param string $endKey: Upper boundary of the requested range
     * @param bool $endKeyIncluded: Indicates if the upper boundary should be part of the result set
     * @param int $maxElements: The maximum number of key-value pairs to return. Negative means no maximum, all matches will be returned. Defaults to -1.
     * 
     * @return array $result :Returns a list containing all matching key-value pairs
     */    
    function range_entries($beginKey, $beginKeyIncluded, $endKey, $endKeyIncluded, $maxElements=-1){
        Logging::trace("Enter", __FILE__, __FUNCTION__, __LINE__);
        $msg = ArakoonProtocol::encodeRangeEntries($beginKey,
                                                 $beginKeyIncluded,
                                                 $endKey,
                                                 $endKeyIncluded,
                                                 $maxElements,
                                                 $this->allowDirty);
        if ($this->allowDirty){
            $conn = $this->sendMessage($this->dirtyReadNode, $msg);
        }
        else{
            $conn = $this->sendToMaster($msg);
        }
        $result = $conn->decodeStringPairListResult();
        return $result;
    }
    

    /*
     * Retrieve a set of keys that match with the provided prefix.
     * You can indicate whether the prefix should be included in the result set if there is a key that matches exactly
     * Additionaly you can limit the size of the result set to maxElements
     * 
     * @param string $keyPrefix: The prefix that will be used when pattern matching the keys in the store
     * @param int $maxElements: The maximum number of keys to return. Negative means no maximum, all matches will be returned. Defaults to -1.
     * 
     * @return array Returns a list of keys matching the provided prefix
     */
    function prefix($keyPrefix , $maxElements=-1){
        Logging::trace("Enter", __FILE__, __FUNCTION__, __LINE__);
        $msg = ArakoonProtocol::encodePrefixKeys($keyPrefix, $maxElements, $this->allowDirty);
        if ($this->allowDirty){
            $conn = $this->sendMessage($this->dirtyReadNode, $msg);
        }
        else{
            $conn = $this->sendToMaster($msg);
        }
        return $conn->decodeStringListResult();
    }
    
    
    function whoMaster(){
        Logging::trace("Enter", __FILE__, __FUNCTION__, __LINE__);
        $this->determineMaster();
        return $this->masterId;
    }
    
    function expectProgressPossible(){
        Logging::trace("Enter", __FILE__, __FUNCTION__, __LINE__);
        $msg = ArakoonProtocol::encodeExpectProgressPossible();
        try{
            $conn = $this->sendToMaster($msg);
            return $conn->decodeBoolResult();
        }
        catch (Exception $ex){
            Logging::error("Received exception $ex", __FILE__, __FUNCTION__, __LINE__);
            return FALSE;
        }
    }
    
    /*
     * Conditionaly update the value associcated with the provided key.
     * The value associated with key will be updated to newValue if the current value in the store equals oldValue
     * If the current value is different from oldValue, this is a no-op.
     * Returns the value that was associated with key in the store prior to this operation. This way you can check if the update was executed or not.
     * 
     * @param string $key: The key whose value you want to updated
     * @param string $oldValue: The expected current value associated with the key.
     * @param string $newValue: The desired new value to be stored.
     * 
     * @return string :The value that was associated with the key prior to this operation
    */
    function testAndSet($key, $oldValue, $newValue){
        Logging::trace("Enter", __FILE__, __FUNCTION__, __LINE__);
        $msg = ArakoonProtocol::encodeTestAndSet($key, $oldValue, $newValue);
        $conn = $this->sendToMaster($msg);
        return $conn->decodeStringOptionResult();
    }
    
    
    private function determineMaster(){
        Logging::trace("Enter", __FILE__, __FUNCTION__, __LINE__);
        $nodeIds = array();

        if ($this->masterId == null){
            # Prepare to ask random nodes who is master
            $nodes = $this->config->getNodes();
            $nodeIds = array_keys($nodes);
            shuffle($nodeIds);

            foreach($nodeIds as $node){
                try{
                    $this->masterId = $this->getMasterIdFromNode($node);
                    $tmpMaster = $this->masterId;

                    try{
                        if($this->masterId != null){
                            if ($this->masterId != $node && ! $this->validateMasterId($this->masterId)){
                                $this->masterId = null;
                            }
                            else{
                                break;
                            }
                        }
                        else{
                            Logging::warning("Node '$node' does not know who the master is", __FILE__, __FUNCTION__, __LINE__);
                        }
                    }
                    catch (Exception $ex){    
                        Logging::warning("Could not validate master on node '$tmpMaster'", __FILE__, __FUNCTION__, __LINE__);
                        Logging::debug("Exception: $ex", __FILE__, __FUNCTION__, __LINE__);
                        $this->masterId = null;
                    }
                            
                }    
                catch (Exception $ex){
                    //Exceptions will occur when nodes are down, simply ignore and try the next node
                    Logging::warning("Could not query node '$node' to see who is master", __FILE__, __FUNCTION__, __LINE__);
                    Logging::debug("Exception: $ex", __FILE__, __FUNCTION__, __LINE__);                    
                }
            }
                
        }
        if ($this->masterId == null){
            Logging::fatal("Could not determine master.", __FILE__, __FUNCTION__, __LINE__);
            throw new Exception('Error, Could not determine master!');
        }
        Logging::trace("Leave", __FILE__, __FUNCTION__, __LINE__);

    }
    
    private function sendToMaster($msg){
        Logging::trace("Enter", __FILE__, __FUNCTION__, __LINE__);
        $this->determineMaster();
        Logging::trace("Sending to master!", __FILE__, __FUNCTION__, __LINE__);
        $retVal = $this->sendMessage($this->masterId, $msg );
        if ($retVal == null){
            Logging::error("Send To Master Failed!", __FILE__, __FUNCTION__, __LINE__);
            throw new Exception('Error, sendToMaster Failed');
        }
        return $retVal;
    }
    
    private function validateMasterId($masterId){
        Logging::trace("Enter", __FILE__, __FUNCTION__, __LINE__);
        if ($masterId == null){
            return False;
        }
        $otherMasterId = $this->getMasterIdFromNode($masterId);
        return $masterId == $otherMasterId;
   
    }
    
    private function getMasterIdFromNode($nodeId){
        Logging::trace("Enter", __FILE__, __FUNCTION__, __LINE__);
        $conn = $this->sendMessage( $nodeId , ArakoonProtocol::encodeWhoMaster() );
        $masterId = $conn->decodeStringOptionResult();
        Logging::trace("Leave with $masterId", __FILE__, __FUNCTION__, __LINE__);
        return $masterId;
    }
    
    private  function sendMessage($nodeId, $msgBuffer, $tryCount=-1){
        Logging::trace("Enter", __FILE__, __FUNCTION__, __LINE__);
        $result = null;

        if ($tryCount == -1){
            $tryCount = $this->config->getTryCount();
        }
        
        for ($i=0; $i < $tryCount; $i++){

            if ($i > 0){
                $maxSleep = $i * ArakoonClientConfig::getBackoffInterval();
                $this->sleep(rand(0, $maxSleep));
            }   
                try{
                    $connection = $this->getConnection($nodeId);
                    $connection->send($msgBuffer);
                    $result = $connection;
                    break;
                } 
                catch ( Exception $ex){
                    Logging::warning("Attempt $i to exchange message with node $nodeId failed with error ($ex).", __FILE__, __FUNCTION__, __LINE__);
                    //Get rid of the connection in case of an exception
                    try{
                        $this->connections[$nodeId]->close();
                        unset($this->connections[$nodeId]);
                        $this->masterId = null;
                    }
                    catch (Exception $ex){
                        Logging::warning("Couldnt close connection with node: $nodeId", __FILE__, __FUNCTION__, __LINE__);                        
                    }
                }
        }
        if ($result == null){
            // If result is None, this means that all retries failed.
            Logging::fatal("Error, All Retries Failed to Send Message", __FILE__, __FUNCTION__, __LINE__);
            throw new Exception('Error, All Retries Failed');
        }
        Logging::trace("Leave", __FILE__, __FUNCTION__, __LINE__);
        return $result;
    }
    
    private function getConnection($nodeId){
        Logging::trace("Enter", __FILE__, __FUNCTION__, __LINE__);
        $connection = null;
        if (array_key_exists($nodeId, $this->connections)){
            $connection = $this->connections[$nodeId];
        }
        if ($connection == null){
            try{
                $nodeLocation = $this->config->getNodeLocation($nodeId);
                $clusterId = $this->config->getClusterId();
                Logging::trace("Creating new connection with $nodeId", __FILE__, __FUNCTION__, __LINE__);
                $connection = new ArakoonClientConnection($nodeLocation , $clusterId);
                Logging::trace("Connection created with $nodeId", __FILE__, __FUNCTION__, __LINE__);                
                $this->connections[$nodeId] = $connection;
                
            }catch(Exception $ex){
                Logging::error("Cannot create new connection. Exception: $ex", __FILE__, __FUNCTION__, __LINE__);
            }
        }
        Logging::trace("Leave", __FILE__, __FUNCTION__, __LINE__);
        return $connection;
    }
    
    private function dropConnections(){
        Logging::trace("Enter", __FILE__, __FUNCTION__, __LINE__);
        $keysToRemove = array_keys($this->connections);
        
        foreach($keysToRemove as $key){
            try {
                $this->connections[$key]->close();
                unset ($this->connections[$key]);                
            }
            catch(Exception $ex){
                Logging::error("Cannot close connection with node: {$this->connections[$key]}", __FILE__, __FUNCTION__, __LINE__);
            }
        }
    }
    
}
?>
