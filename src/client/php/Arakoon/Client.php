<?php
/**
 * This file is part of Arakoon, a distributed key-value store.*
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

require_once 'Client/Logger.php';
require_once 'Client/Config.php';
require_once 'Client/NodeConnection.php';
require_once 'Client/Protocol.php';
require_once 'Client/Operation.php';
require_once 'Client/Operation/Sequence.php';
require_once 'Client/Operation/Set.php';
require_once 'Client/Operation/Delete.php';

/**
 * This class represents the Arakoon_Client class.
 * 
 * @category   	Arakoon
 * @package    	Arakoon_Client
 * @copyright 	Copyright (C) 2010 Incubaid BVBA
 * @license    	http://www.arakoon.org/licensing
 */
class Arakoon_Client
{
	private $_config;
	private $_connections;
	private $_allowDirtyReads;
	private $_dirtyReadNodeId;
	private $_masterId;

	/**
	 * Constructor of an Arakoon_Client object.
	 *
	 * @param 	ArakoonClientConfig $config contains info on the nodes and defaults to NULL in wich case a default ArakoonClientConfig 
	 * @return 	void
	 */
	public function __construct(Arakoon_Client_Config $config=NULL)
	{	
		if ($config == NULL)
		{
			$config = new Arakoon_Client_Config();			
		}
		
		$nodes = $config->getNodes();		
		if (count($nodes) == 0)
		{
			throw new Exception('Config doesn\'t contain any nodes');
		}
		
		$this->_config = $config;
		$this->_connections = array();		
		$this->_allowDirtyReadsReads = FALSE;
		$this->_dirtyReadNodeId = array_rand(array_keys($nodes));
		$this->_masterId = NULL;
	}
	
	/**
	 * Allows the use of dirty reads.
	 *
	 * @return void
	 */
	public function allowDirtyReads()
	{
		$this->_allowDirtyReads = TRUE;
	}

	/**
	 * Disalows the use of dirty reads.
	 *
	 * @return void
	 */
	public function disallowDirtyReads()
	{
		$this->_allowDirtyReads = FALSE;
	}
	
	/**
	 * Gets the configuration object assigned to the Arakoon client.
	 * 
	 * @return Arakoon_Client_Config
	 */
	public function getConfig()
	{
		return $this->_config;
	}

	/**
	 * Gets the node used for dirty read operations its identifier.
	 *
	 * @return string dirty read node its identifier
	 */
	public function getDirtyReadNode()
	{
		return $this->_dirtyReadNodeId;
	}
	
	/**
	 * Sets the node for dirty read operations using the given node identifier.
	 *
	 * @param	string $nodeId dirty read node its identifier
	 * @return	void
	 */
	public function setDirtyReadNode($nodeId)
	{		
		if ($this->_config->nodeExists($nodeId))
		{
			$this->_dirtyReadNodeId = $nodeId;			
		}
		else
		{
			throw new Exception("Unkown node identifier ($nodeId)");
		}		
	}
	
	/**
	 * @todo document
	 */
	public function expectProgressPossible()
	{
		$result = FALSE;		
		$message = Arakoon_Client_Protocol::encodeExpectProgressPossible();
		
		try
		{
			$connection = $this->sendMessageToMaster($message);
			$result = Arakoon_Client_Protocol::decodeBoolResult($connection);
		}
		catch (Exception $ex)
		{
			Arakoon_Client_Protocol::logError("Received exception $ex", __FILE__, __FUNCTION__, __LINE__);
		}
		
		return $result;
	}
	
	/**
	 * Deletes a key-value pair using the given key
	 *
	 * @param string $key : key of the key-value pair that needs to be removed
	 *
	 * @return void
	 */
	public function delete($key)
	{
		$deleteOperation = new Arakoon_Client_Operation_Delete($key);
		$message = $deleteOperation->encode();
		$connection = $this->sendMessageToMaster($message);
		Arakoon_Client_Protocol::decodeVoidResult($connection);
	}
	
	/**
	 * Checks if a key-value pair exists using it's given key
	 *
	 * @param 	string 	$key	key-value pair key
	 * @return 	bool			true if the key-value pair exists, false otherwise
	 */
	public function exists($key)
	{
		$message = Arakoon_Client_Protocol::encodeExists($key, $this->_allowDirtyReads);		
		$connection = $this->sendReadMessage($message);
		$result = Arakoon_Client_Protocol::decodeBoolResult($connection);
		
		return $result; 
	}
	
	/**
	 * Retrieves a key-value pair it's value using the given key.
	 * 
	 * @param 	string 		$key key whose value you are interested in
	 * @return 	string 		the value associated with the given key
	 * @throws 	Exception	when the given key doesn't exists
	 */
	public function get($key)
	{
		$message = Arakoon_Client_Protocol::encodeGet($key, $this->_allowDirtyReads);		
		$connection = $this->sendReadMessage($message);
		$result = Arakoon_Client_Protocol::decodeStringResult($connection);
		
		return $result; 		
	}
	
	/**
	 * Sends a hello message to the master node with a given client id and cluster id
	 * and returns the master node id and the arakoon version
	 *
	 * @param 	string	$clientId 	client id
	 * @param 	string	$clusterId 	cluster id
	 * @return 	string	master id and arakoon version
	 */
	public function hello($clientId, $clusterId=ArakoonClientConfig::DEFAULT_CLUSTER_ID)
	{
		$message = Arakoon_Client_Protocol::encodeHello($clientId, $clusterId);
		$connection = $this->sendMessageToMaster($message);
		$result = Arakoon_Client_Protocol::decodeStringResult($connection);
		
		return $result;
	}
	
	
	/**
	 * Retrieves the values for the keys in the given array
	 * If one or more of the keys doesn't exist an empty list will be returned
	 * 
	 * @param 	array $key		array containing the keys wich values needs to be retrieved
	 * @return 	array $values	the values associated with the respective keys
	 */
	public function multiGet($keys)
	{
		$message = Arakoon_Client_Protocol::encodeMultiGet($keys, $this->_allowDirtyReads);
		$connection = $this->sendReadMessage($message);
		$result = Arakoon_Client_Protocol::decodeStringListResult($connection);
		
		return $result;
	}
	
	/**
	 * Retrieves a set of keys that match the provided prefix
	 * You can indicate whether the prefix should be included in the result set if there is a key that matches exactly
	 * Additionaly you can limit the size of the result set to maxElements
	 *
	 * @param 	string 	$keyPrefix		The prefix that will be used when pattern matching the keys in the store
	 * @param 	int 	$maxElements	The maximum number of keys to return. Negative means no maximum, all matches will be returned. Defaults to -1.
	 * @return 	array 	$result			Returns a list of keys matching the provided prefix
	 */
	public function prefix($keyPrefix , $maxElements = -1)
	{
		$message = Arakoon_Client_Protocol::encodePrefix($keyPrefix, $maxElements, $this->_allowDirtyReads);
		$connection = $this->sendReadMessage($message);
		$result = Arakoon_Client_Protocol::decodeStringListResult($connection); 
		
		return $result;
	}
	
	/**
	 * Performs a range query on the store, retrieving the set of matching keys
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
	public function range($beginKey, $includeBeginKey, $endKey, $includeEndKey, $maxElements = -1)
	{
		$message = Arakoon_Client_Protocol::encodeRange($beginKey, $includeBeginKey, $endKey, $includeEndKey, $maxElements, $this->_allowDirtyReads);		
		$connection = $this->sendReadMessage($message);	
		$result = Arakoon_Client_Protocol::decodeStringListResult($connection);
		
		return $result;
	}
	
	/**
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
	public function rangeEntries($beginKey, $includeBeginKey, $endKey, $includeEndKey, $maxElements = -1)
	{
		$message = Arakoon_Client_Protocol::encodeRangeEntries($beginKey, $includeBeginKey, $endKey, $includeEndKey, $maxElements, $this->_allowDirtyReads);		
		$connection = $this->sendReadMessage($message);	
		$result = Arakoon_Client_Protocol::decodeStringPairListResult($connection);
		
		return $result;
	}
	
	/**
	 * Executes a sequence of operations using the "all or nothing" concept
	 * meaning if one operation should fails all the other will too
	 * 
	 * @param Sequence $sequence : Sequence of operations
	 *
	 * @return void
	 */
	public function sequence(Arakoon_Client_Operation_Sequence $sequence)
	{
		$message = $sequence->encode();
		$connection = $this->sendMessageToMaster($message);
		Arakoon_Client_Protocol::decodeVoidResult($connection);
	}
	
	/**
	 * Updates the value associated with the given key
	 * If the key doesn't exists, a new key value pair will be created
	 * If the key exists, it's associated value will be overwritten
	 *
	 * @param string $key	: key whose associated value needs to be updated
	 * @param string $value	: value to store with the given key
	 *
	 * @return void
	 */
	public function set($key, $value)
	{
		if (!isset($key))
		{
			throw new Exception('Key invalid');
		}
		
		if (!isset($value))
		{
			throw new Exception('Value invalid');
		}
		
		$setOperation = new Arakoon_Client_Operation_Set($key, $value);		
		$message = $setOperation->encode();
		$connection = $this->sendMessageToMaster($message);
		Arakoon_Client_Protocol::decodeVoidResult($connection);
	}	
	
	/**
	 * @todo document
	 */
	public function statistics()
	{
		$message = Arakoon_Client_Protocol::encodeStatistics();
		$connection = $this->sendMessageToMaster($message);
		$result = Arakoon_Client_Protocol::decodeStatisticsResult($connection);
		
		return $result;
	}
	
	/**
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
	public function testAndSet($key, $oldValue, $newValue)
	{
		Arakoon_Client_Operation::validateKey($key);
		Arakoon_Client_Operation::validateValueSize($oldValue);
		Arakoon_Client_Operation::validateValueSize($newValue);
		
		$message = Arakoon_Client_Protocol::encodeTestAndSet($key, $oldValue, $newValue);
		$connection = $this->sendMessageToMaster($message);
		$result = Arakoon_Client_Protocol::decodeStringOptionResult($connection);
		
		return $result;
	}
	
	/**
	 * Gets the master node its identifier..
	 *
	 * @return string master node its identifier
	 */
	public function whoMaster()
	{
		return $this->getMasterId();
	}

	/**
	 * Gets the master identifier from the given node.
	 *
	 * @param string	$nodeId		: node identifier of the node to ask who the master is
	 * @return string 	$masterId	: master identifier
	 */
	private function getMasterIdFromNode($nodeId)
	{
		$message = Arakoon_Client_Protocol::encodeWhoMaster();		
		$connection = $this->sendMessageToNode($nodeId, $message);		
		return Arakoon_Client_Protocol::decodeStringOptionResult($connection);
	}
	
	/**
	 * Checks if a given node identifier is equal to the master identifier.
	 *
	 * @param 	string 	$nodeId identifier of the node to connect to
	 * @return 	bool 	true if the given node identifier equals the master identifier, false otherwise
	 */
	private function isMaster($nodeId)
	{
		if (!$this->_config->nodeExists($nodeId))
		{
			Arakoon_Client_Logger::logWarning("Unknown node identifier ($nodeId)", __FILE__, __FUNCTION__, __LINE__);
			throw new Exception("Unkown node identifier ($nodeId)");			
		}
		
		$masterId = $this->getMasterIdFromNode($nodeId);
		
		return $nodeId == $masterId;		 
	}
	
	/**
	 * @todo document
	 */
	private function getMasterId()
	{
		if (isset($this->_masterId))
		{
			return $this->_masterId;				
		}
		
		$nodes = $this->_config->getNodes();
		shuffle($nodes);
		
		while (count($nodes) > 0)
		{
			$randomNode = array_pop($nodes);
			$randomNodeId = $randomNode->getId();			
			$possibleMasterId = $this->getMasterIdFromNode($randomNodeId);
			
			if ($this->_config->nodeExists($possibleMasterId))
			{
				if (($randomNodeId == $possibleMasterId) || $this->isMaster($possibleMasterId))
				{
					$this->_masterId = $possibleMasterId;
					break;
				}
				else
				{
					Arakoon_Client_Logger::logWarning("Node ($randomNodeId) does not know who master is", __FILE__, __FUNCTION__, __LINE__);
				}
				
				// remove possible master id to reduce unnecessary calls
				for ($i = 0; $i < count($shuffledNodes); $i++)
				{
					if ($shuffledNodes[$i]->getId() == $possibleMasterId)
					{
						array_slice($shuffledNodes, $i, 1);
					}
				}
			}
			else
			{
				Arakoon_Client_Logger::logWarning("Unknown node identifier ($possibleMasterId)", __FILE__, __FUNCTION__, __LINE__);
			}		
		}
		
		if ($this->_masterId == NULL)
		{
			Arakoon_Client_Logger::logFatal("Could not determine master", __FILE__, __FUNCTION__, __LINE__);
			throw new Exception('Could not determine master');
		}	

		return $this->_masterId;
	}

	/**
	 * @todo document
	 */
	private function sendReadMessage($message)
	{
		if ($this->_allowDirtyReads)
		{
			return $this->sendMessageToNode($this->_dirtyReadNodeId, $message);
		}
		else
		{
			return $this->sendMessageToMaster($message);
		}
	}
	
	private function sendMessageToMaster($message)
	{
		$masterId = $this->getMasterId();
		return $this->sendMessageToNode($masterId, $message);
	}
	
	/**
	 * Retrieves the connection to node using the given node id
	 *
	 * @param string $nodeId 						: id of the node to connect to
	 * @param string $message 						: message that needs to be send to the node
	 *
	 * @return ArakoonClientConnection $connection 	: node connection
	 */
	private function sendMessageToNode($nodeId, $message, $tryCount=Arakoon_Client_Config::MESSAGE_TRY_COUNT)
	{
		$connection = NULL;
		$succes = FALSE;
		
		for ($i = 0; $i < $tryCount; $i++)
		{
			if ($i > 0)
			{
				sleep(rand(0, ArakoonClientConfig::CONNECTION_BACKOFF_INTERVAL));
			}
			
			try
			{
				$connection = $this->getNodeConnection($nodeId);				
				$succes = $connection->sendMessage($message);
				
				if ($succes)
				{
					break;
				}				
			}
			catch (Exception $exception)
			{
				Arakoon_Client_Logger::logDebug('Exception: $exception', __FILE__, __FUNCTION__, __LINE__);
				Arakoon_Client_Logger::logWarning("Attempt $i to send a message to a node ($nodeId) failed (exception: $exception).", __FILE__, __FUNCTION__, __LINE__);				
				$this->dropNodeConnection($nodeId);
				$this->_masterId = NULL;
			}
		}
		
		if (!$succes)
		{
			Arakoon_Client_Logger::logFatal("All connection retries failed", __FILE__, __FUNCTION__, __LINE__);						
			throw new Exception('All connection retries failed');
		}
		
		return $connection;
	}

	/**
	 * Retrieves the connection to node using the given node id
	 *
	 * @param string nodeId 						: node to connect to it's id
	 *
	 * @return ArakoonClientConnection $connection 	: node connection
	 */
	private function getNodeConnection($nodeId)
	{
		if (!$this->_config->nodeExists($nodeId))
		{
			$message = "Unknown node identifier ($nodeId)";
			Arakoon_Client_Logger::logWarning($message, __FILE__, __FUNCTION__, __LINE__);
			throw new Exception($message);			
		}
				
		if (!array_key_exists($nodeId, $this->_connections))
		{
			$this->connectToNode($nodeId);
		}
		
		return $this->_connections[$nodeId];
	}
	
	/**
	 * Creates a new connection to a node using the given node id
	 *
	 * @param string nodeId 						: node to connect to it's id
	 *
	 * @return ArakoonClientConnection $connection 	: node connection
	 */
	private function connectToNode($nodeId)
	{
		$node = $this->_config->getNode($nodeId);
		$connection = new Arakoon_Client_NodeConnection($node->getIp(), $node->getClientPort(), TRUE);
		$this->_connections[$nodeId] = $connection;
		
		$prologueMsg = Arakoon_Client_Protocol::EncodePrologue($this->_config->getClusterId());			
		$connection->sendMessage($prologueMsg);
	}
	
	/**
	 * @todo document
	 */
	private function dropNodeConnection($nodeId)
	{
		$this->_connections[$nodeId]->close();
		unset($this->_connections[$nodeId]);
	}
	
	/**
	 * @todo document
	 */
	private function dropAllNodeConnections()
	{	
		foreach($this->_connections as $nodeId->$connection)
		{
			dropNodeConnection($nodeId);
		}
	}
}
?>