<?php

include_once 'ara_connection.php';
include_once 'ara_config.php';
include_once 'ara_protocol.php';
include_once 'ara_def.php';

class Arakoon
{
    
    private $masterId = null;
    private $config = null;
    private $connections = array();
    private $allowDirty = false;
    private $dirtyReadNode = 0;
    
    /*
        Constructor of an Arakoon client object.

        It takes one optional paramater 'config'.
        This parameter contains info on the arakoon server nodes.
        See the constructor of L{ArakoonClientConfig} for more details.

        @type config: L{ArakoonClientConfig}
        @param config: The L{ArakoonClientConfig} object to be used by the client. Defaults to None in which
        case a default L{ArakoonClientConfig} object will be created.
    */
    public function __construct($config) {
        if ($config == null){
            $config = new ArakoonClientConfig();
        }

        $this->config = $config;
        $nodeList = array_keys($this->config->getNodes());
        if (count($nodeList) == 0){
            throw new Exception('Error, Node list empty!');
        }
        $this->dirtyReadNode = $nodeList[array_rand($nodeList)];
    }
    
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
     * @type node : string
     * @param node : the node identifier
     * @rtype: void
    */
    function setDirtyReadNode($node){
        if (!in_array($node, array_keys($this->config->getNodes())))
                throw new Exception ("Unkown Node: $node");
        $this->dirtyReadNode = $node;
    }

    /*
     * Retrieve the node that will be used for dirty read operations
     * @rtype: string
     * @return : the node identifier
    */
    function getDirtyReadNode(){
        return $this->dirtyReadNode;
    }

    
    /*
     * send a hello message to the node with your id and the cluster id.
     * Will return the server node identifier and the version of arakoon it is running
     * @type clientId  : string
     * @type clusterId : string
     * @param clusterId : must match the cluster_id of the node
     * @rtype: string
     * @return: The master identifier and its version in a single string
    */
    function hello ($clientId, $clusterId='arakoon'){
        $encoded = ArakoonProtocol::encodePing($clientId,$clusterId);
        $conn = $this->sendToMaster($encoded);
        return $conn->decodeStringResult();
    }
    
    /*
     * @type key : string
     * @param key : key
     * @return : True if there is a value for that key, False otherwise    
     */
    function exists($key){
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
     * @type key: string
     * @param key: The key whose value you are interested in
     * @rtype: string
     * @return: The value associated with the given key
     */    
    function get($key){ 
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
     * @type key: string list
     * @rtype: string list
     * @return: the values associated with the respective keys
     */
    function multiGet($keys){
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
     * @type key: string
     * @type value: string
     * @param key: The key whose associated value you want to update
     * @param value: The value you want to store with the associated key
     * 
     * @rtype: void
     * 
     */
    function set($key, $value){
        $conn = $this->sendToMaster(ArakoonProtocol::encodeSet($key, $value));
        $conn->decodeVoidResult();
    }
    
    /*
     * Try to execute a sequence of updates.
     * 
     * It's all-or-nothing: either all updates succeed, or they all fail.
     * @type seq: Sequence
     * 
     */
    function sequence($seq){
        $encoded = ArakoonProtocol::encodeSequence($seq);
        $conn = $this->sendToMaster($encoded);
        $conn->decodeVoidResult();
    }
    
    /*
     * Remove a key-value pair from the store.
     * @type key: string
     * @param key: Remove this key and its associated value from the store
     * @rtype: void    
     * 
     */
    function delete($key){
        $conn = $this->sendToMaster(ArakoonProtocol::encodeDelete($key));
        $conn->decodeVoidResult();
    }
    
    /*
        Perform a range query on the store, retrieving the set of matching keys

        Retrieve a set of keys that lexographically fall between the beginKey and the endKey
        You can specify whether the beginKey and endKey need to be included in the result set
        Additionaly you can limit the size of the result set to maxElements. Default is to return all matching keys.

        @type beginKey: string option
        @type beginKeyIncluded: boolean
        @type endKey :string option
        @type endKeyIncluded: boolean
        @type maxElements: integer
        @param beginKey: Lower boundary of the requested range
        @param beginKeyIncluded: Indicates if the lower boundary should be part of the result set
        @param endKey: Upper boundary of the requested range
        @param endKeyIncluded: Indicates if the upper boundary should be part of the result set
        @param maxElements: The maximum number of keys to return. Negative means no maximum, all matches will be returned. Defaults to -1.

        @rtype: list of strings
        @return: Returns a list containing all matching keys
    
    */
    function range($beginKey, $beginKeyIncluded, $endKey, $endKeyIncluded, $maxElements=-1 ){
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
        Perform a range query on the store, retrieving the set of matching key-value pairs

        Retrieve a set of keys that lexographically fall between the beginKey and the endKey
        You can specify whether the beginKey and endKey need to be included in the result set
        Additionaly you can limit the size of the result set to maxElements. Default is to return all matching keys.

        @type beginKey: string option
        @type beginKeyIncluded: boolean
        @type endKey :string option
        @type endKeyIncluded: boolean
        @type maxElements: integer
        @param beginKey: Lower boundary of the requested range
        @param beginKeyIncluded: Indicates if the lower boundary should be part of the result set
        @param endKey: Upper boundary of the requested range
        @param endKeyIncluded: Indicates if the upper boundary should be part of the result set
        @param maxElements: The maximum number of key-value pairs to return. Negative means no maximum, all matches will be returned. Defaults to -1.

        @rtype: list of strings
        @return: Returns a list containing all matching key-value pairs

     */    
    function range_entries($beginKey, $beginKeyIncluded, $endKey, $endKeyIncluded, $maxElements=-1){
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
        Retrieve a set of keys that match with the provided prefix.

        You can indicate whether the prefix should be included in the result set if there is a key that matches exactly
        Additionaly you can limit the size of the result set to maxElements

        @type keyPrefix: string
        @type maxElements: integer
        @param keyPrefix: The prefix that will be used when pattern matching the keys in the store
        @param maxElements: The maximum number of keys to return. Negative means no maximum, all matches will be returned. Defaults to -1.

        @rtype: list of strings
        @return: Returns a list of keys matching the provided prefix

     */
    function prefix($keyPrefix , $maxElements=-1){
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
        $this->determineMaster();
        return $this->masterId;
    }
    
    function expectProgressPossible(){
        $msg = ArakoonProtocol::encodeExpectProgressPossible();
        try{
            $conn = $this->sendToMaster($msg);
            return $conn->decodeBoolResult();
        }
        catch (Exception $ex){
            return FALSE;
        }
    }
    
    /*
        Conditionaly update the value associcated with the provided key.
        The value associated with key will be updated to newValue if the current value in the store equals oldValue
        If the current value is different from oldValue, this is a no-op.
        Returns the value that was associated with key in the store prior to this operation. This way you can check if the update was executed or not.

        @type key: string
        @type oldValue: string option
        @type newValue: string
        @param key: The key whose value you want to updated
        @param oldValue: The expected current value associated with the key.
        @param newValue: The desired new value to be stored.

        @rtype: string
        @return: The value that was associated with the key prior to this operation
    */
    function testAndSet($key, $oldValue, $newValue){
        $msg = ArakoonProtocol::encodeTestAndSet($key, $oldValue, $newValue);
        $conn = $this->sendToMaster($msg);
        return $conn->decodeStringOptionResult();
    }
    
    
    private function determineMaster(){
        $nodeIds = array();

        if ($this->masterId == null){
            # Prepare to ask random nodes who is master
            $nodeIds = array_keys($this->config->getNodes());
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
                            //ArakoonClientLogger::logWarning( "Node '%s' does not know who the master is", $node );
                        }
                    }
                    catch (Exception $ex){    
                        //ArakoonClientLogger::logWarning( "Could not validate master on node '%s'", $tmpMaster );
                        //ArakoonClientLogger::logDebug( "%s" % (ex));
                        $this->masterId = null;
                    }
                            
                }    
                catch (Exception $ex){
                    //Exceptions will occur when nodes are down, simply ignore and try the next node
                    //ArakoonClientLogger.logWarning( "Could not query node '%s' to see who is master", node )
                    //ArakoonClientLogger.logDebug( "%s: %s" % (ex.__class__.__name__, ex))
                }
            }
                
        }
        if ($this->masterId == null){
            //ArakoonClientLogger::logError( "Could not determine master."  );
            throw new Exception('Error, Could not determine master!');
        }


    }
    
    private function sendToMaster($msg){
        $this->determineMaster();
        $retVal = $this->sendMessage($this->masterId, $msg );
        if ($retVal == null){
            throw new Exception('Error, sendToMaster Failed');
        }
        return $retVal;
    }
    
    private function validateMasterId($masterId){
        if ($masterId == null)
            return False;
        $otherMasterId = $this->getMasterIdFromNode($masterId);
        return $masterId == $otherMasterId;
   
    }
    
    private function getMasterIdFromNode($nodeId){
        $conn = $this->sendMessage( $nodeId , ArakoonProtocol::encodeWhoMaster() );
        $masterId = $conn->decodeStringOptionResult();
        return $masterId;
    }
    
    private  function sendMessage($nodeId, $msgBuffer, $tryCount=-1){
        $result = null;

        if ($tryCount == -1)
            $tryCount = $this->config->getTryCount();

        for ($i=0; $i < $tryCount; $i++){

            if ($i > 0){
                $maxSleep = $i * ArakoonClientConfig::getBackoffInterval();
                $this->sleep(rand(0, $maxSleep) );
            }   
                try{
                    $connection = $this->getConnection( $nodeId );
                    $connection->send($msgBuffer);
                    // Message sent correctly, return client connection so result can be read
                    $result = $connection;
                    break;
                } 
                catch ( Exception $ex){
                    $fmt = "Attempt %d to exchange message with node %s failed with error (%s).";
                    //ArakoonClientLogger::logWarning($fmt , $i, $nodeId, $ex);
                    //Get rid of the connection in case of an exception
                    try{
                        $this->connections[$nodeId]->close();
                        unset($this->connections[$nodeId]);
                        $this->masterId = null;
                    }
                    catch (Exception $ex){
                        throw new Exception("Couldnt close connection with node: $nodeId");
                    }
                }
        }
        if ($result == null){
            // If result is None, this means that all retries failed.
            // Re-raise the last exception to escalate the problem
            throw new Exception('Error, All Retries Failed');
        }

        return $result;

    }
    
    private function getConnection($nodeId){
        $connection = null;
        if (array_key_exists($nodeId, $this->connections)){
            $connection = $this->connections[$nodeId];
        }
        if ($connection == null){
            $nodeLocation = $this->config->getNodeLocation($nodeId);
            $clusterId = $this->config->getClusterId();
            $connection = new ArakoonClientConnection($nodeLocation , $clusterId);
            $this->connections[$nodeId] = $connection;
        }
        return $connection;   
    }
    
    private function dropConnections(){
        $keysToRemove = array_keys($this->connections);
        
        foreach($keysToRemove as $key){
            try {
                $this->connections[$key]->close();
                unset ($this->connections[$key]);                
            }
            catch(Exception $ex){
                /*Log Error*/
            }
        }
    }
    
}
?>
