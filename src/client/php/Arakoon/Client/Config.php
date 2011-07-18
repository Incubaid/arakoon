<?php
/**
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

require_once 'Node.php';
require_once 'Exception.php';

/**
 * Arakoon_Client_Config
 *
 * @category   	Arakoon
 * @package    	Arakoon_Client
 * @copyright 	Copyright (C) 2010 Incubaid BVBA
 * @license    	http://www.arakoon.org/licensing
 */
class Arakoon_Client_Config
{ 
	const DEFAULT_CLUSTER_ID 			= 'arakoon';
	const SEND_MESSAGE_TRY_COUNT		= 1;			// amount of attempts before a message should be tried before giving up on
	const CONNECTION_TIMEOUT 			= 60;			// tcp connection timeout
	const CONNECTION_BACKOFF_INTERVAL 	= 5;			// max interval in seconds a client should wait untill attempting to send another message to the server
	const NO_MASTER_RETRY_PERIOD 		= 60;	   		// period in seconds in which messages can be sent to re-elect a master (no master scenario)
	const MAX_KEY_BYTE_SIZE				= 8388608;		// key maximum byte size (8mb)
	const MAX_VALUE_BYTE_SIZE			= 8388608;		// value maximum byte size (8mb)
	
	const GLOBAL_KEY 		= 'global';
	const CLUSTER_KEY 		= 'cluster';
	const CLUSTER_ID_KEY	= 'cluster_id';
	const IP_KEY 			= 'ip';
	const CLIENT_PORT_KEY	= 'client_port';
	const HOME_KEY			= 'home';
		
    private $_clusterId;
    private $_nodes;

    /**
     * Constructor of an Arakoon_Client_Config object.
     * 
     * @param string	$clusterId	Arakoon client cluster identifier
     * @param array 	$nodes		array containing Arakoon client nodes, defaults to empty array
     */
    public function __construct($clusterId, $nodes=array())
    {
        $this->_clusterId = $clusterId;
        $this->_nodes = $nodes;
    }
    
    /**
     * Creates an Arakoon client configuration from a file.
     * 
     *  @param 	string 					$filePath path to the Arakoon client configuration ini file
     *  @return Arakoon_Client_Config	Arakoon client configuration
     *  @throws Arakoon_Client_Config 				when fhe file extention of the file isn't supported
     */
	public static function createFromFile($filePath)
	{
		$extention = substr(strrchr($filePath, '.'), 1);
		
		if ($extention == "ini")
		{
			return Arakoon_Client_Config::CreateFromIniFile($filePath);			
		}
		else
		{
			throw new Arakoon_Client_Exception("Config file extention not supported!");	
		}
	}
	
	/**
     * Creates an Arakoon client configuration from an ini file.
     * 
     *  @param 	string 					$filePath path to the Arakoon client configuration ini file
     *  @return Arakoon_Client_Config	an Arakoon client configuration
     */
	private static function createFromIniFile($filePath)
	{
		$configArray = parse_ini_file($filePath, true);		
		Arakoon_Client_Config::validate($configArray);
        return Arakoon_Client_Config::parse($configArray);
	}

	/**
	 * Parses an Arakoon client configuration array to a Arakoon_Client_Config instance.
	 * 
	 * @param 	array 					$configArray array containing an Arakoon client configuration
     * @return 	Arakoon_Client_Config	Arakoon client configuration
     */
	private static function parse(array $configArray)
	{        
		$clusterId = $configArray[self::GLOBAL_KEY][self::CLUSTER_ID_KEY];		
		$nodeIds = split(',', $configArray[self::GLOBAL_KEY][self::CLUSTER_KEY]);	
		$nodes = array();
			
		foreach($nodeIds as $nodeId)
		{
			$nodeId = trim($nodeId);
			$ip = $configArray[$nodeId][self::IP_KEY];
			$clientPort = $configArray[$nodeId][self::CLIENT_PORT_KEY];
			$home = $configArray[$nodeId][self::HOME_KEY];
			$nodes[] = new Arakoon_Client_Node($nodeId, $ip, $clientPort, $home);
        }
        
        return new Arakoon_Client_Config($clusterId, $nodes);
	}	
	
	/**
     * Validates an Arakoon client configuration array.
     * 
     * @throws Arakoon_Client_Config when a required part of the Arakoon client configuration array is undefined
     */
	private static function validate(array $config)
	{	
        if (!array_key_exists(self::GLOBAL_KEY, $config))
        {
            throw new Arakoon_Client_Config('Global section undefined');
        }
        
        if (!array_key_exists(self::CLUSTER_KEY, $config[self::GLOBAL_KEY]))
        {
            throw new Arakoon_Client_Config('Cluster undefined!');
        }	
        
		if (!array_key_exists(self::CLUSTER_ID_KEY, $config[self::GLOBAL_KEY]))
        {
            throw new Arakoon_Client_Config('Cluster identifier undefined!');
        }        
        
        $nodeNames = split(',', $config[self::GLOBAL_KEY][self::CLUSTER_KEY]);        
		foreach($nodeNames as $nodeName)
		{
            $nodeName = trim($nodeName);
            
			if (array_key_exists($nodeName, $config))
	        {
	            Arakoon_Client_Config::validateNode($config[$nodeName]);
	        }
	        else
	        {
	        	throw new Arakoon_Client_Config("Node ($nodeName) section undefined");
	        }
        }
	}
	
	/**
     * Validates an Arakoon node configuration array.
     * 
     * @throws Arakoon_Client_Config when a required part of the Arakoon node configuration array is undefined
     */
	private static function validateNode(array $node)
	{			
		if (!array_key_exists(self::IP_KEY, $node))
        {
            throw new Arakoon_Client_Config('Node ip undefined!');
        }
		
        if (!array_key_exists(self::CLIENT_PORT_KEY, $node))
        {
            throw new Arakoon_Client_Config('Node client port undefined!');
        }
        
		if (!array_key_exists(self::HOME_KEY, $node))
        {
            throw new Arakoon_Client_Config('Node home undefined!');
        }
	}
    
    /**
     * Gets the Arakoon client configuration its cluster identifier.
     * 
     * @return string cluster identifier
     */
	public function getClusterId()
    {
        return $this->_clusterId;
    }
    
	/**
     * Gets a node from the Arakoon client configuration with the given node identifier.
     * 
     * @param 	string				$nodeId node identifier
     * @return 	Arakoon_Client_Node Arakoon client node
     */
	public function getNode($nodeId)
    {        
    	$resultNode = NULL;  
    	  	
    	foreach ($this->_nodes as $node)
    	{
    		if ($node->getId() == $nodeId)
    		{
    			$resultNode = $node;
    			break;
    		}
    	} 
    	   	
    	return $resultNode;
    }
    
    /**
     * Gets the Arakoon client configuration its nodes.
     * 
     * @return array Arakoon nodes
     */
	public function getNodes()
    {
        return $this->_nodes;
    }
    
    /**
     * Checks if the Arakoon client configuration contains a node with the given node identifier.
     * 
     * @param	string 	$nodeId node identifier that needs to be checked
     * @return 	boolean TRUE if the config contains a node with the given node identifier, FALSE otherwise
     */
    public function nodeExists($nodeId)
    {
    	$exists = FALSE;
    	    	  	
    	foreach ($this->_nodes as $node)
    	{
    		if ($node->getId() == $nodeId)
    		{
    			$exists = TRUE;
    			break;
    		}
    	} 
    	   	
    	return $exists;
    }
    
	/**
     * Checks if the Arakoon client configuration contains a node with the given node identifier.
     * If not an exception is thrown.
     * 
     * @param	string 	$nodeId node identifier that needs to be checked
     * @return	void
     * @throws	Arakoon_Client_Exception when given node identifier doesn't exist
     */
    public function assertNodeExists($nodeId)
    {
    	$exists = $this->nodeExists($nodeId);
    	
    	if (!$exists)
    	{
			$message = "No node exists with the given node identifier ($nodeId)";
    		Arakoon_Client_Logger::logWarning($message, __FILE__, __FUNCTION__, __LINE__);
			throw new Arakoon_Client_Exception($message);    		
    	}
    }
}
?>