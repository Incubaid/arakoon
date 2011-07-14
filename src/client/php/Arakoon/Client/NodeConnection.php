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

require_once 'Logger.php';

/**
 * This class represents the Arakoon_Client_NodeConnection class.
 *
 * @category   	Arakoon
 * @package    	Arakoon_Client
 * @subpackage	Operation
 * @copyright 	Copyright (C) 2010 Incubaid BVBA
 * @license    	http://www.arakoon.org/licensing
 */

class Arakoon_Client_NodeConnection
{
	private $_ip;
	private $_port;
	private $_socket;

	/**
	 * Constructor of an Arakoon_Client_NodeConnection object.
	 *
	 * @param 	array 	$node		array holding node info (ip, port)
	 * @param 	string 	$clusterId	string holding cluster id     *
	 * @return 	void
	 */
	public function __construct($ip, $port, $autoConnect=FALSE)
	{
		$this->_ip = $ip;
		$this->_port = $port;
		$this->_socket = NULL;
		
		if ($autoConnect)
		{
			$this->connect();
		}
	}

	/**
	 * @todo document
	 */
	public function connect()
	{				
		if ($this->isConnected())
		{
			return;
		}

		$succes = FALSE;
		$this->_socket = socket_create(AF_INET, SOCK_STREAM, 0);

		try
		{
			Arakoon_Client_Logger::logTrace("Connecting to $this->_ip on port $this->_port", __FILE__, __FUNCTION__, __LINE__);

			$connected = socket_connect($this->_socket, $this->_ip, $this->_port);
			 
			if (!$connected)
			{
				$socketError = socket_last_error($this->_socket);
				throw new Exception($socketError);
			}
			
			Arakoon_Client_Logger::logTrace("Connected to $this->_ip on port $this->_port", __FILE__, __FUNCTION__, __LINE__);
		}
		catch(Exception $exception)
		{
			$fatalMsg = "Cannot connect to ip $this->_ip on port $this->_port";							
			Arakoon_Client_Logger::logFatal($fatalMsg, __FILE__, __FUNCTION__, __LINE__);
			Arakoon_Client_Logger::logDebug('Exception: $exception', __FILE__, __FUNCTION__, __LINE__);				 
			throw new Exception($fatalMsg);
		}	
		return $succes;
	}

	public function getSocket()
	{
		return $this->_socket;
	}
	/**
	 * @todo document
	 */
	private function reconnect()
	{
		$this->disconnect();
		return $this->connect();
	}

	/**
	 * @todo document
	 */
	function isConnected()
	{
		return !empty($this->_socket);
	}

	/**
	 * @todo document
	 */
	public function disconnect()
	{
		if($this->isConnected())
		{
			try
			{
				Arakoon_Client_Logger::logTrace("Disconnecting from $this->_ip on port $this->_port", __FILE__, __FUNCTION__, __LINE__);
				
				socket_close($this->_socket);
				$this->_socket = NULL;
				
				Arakoon_Client_Logger::logTrace("Disconnected from $this->_ip on port $this->_port", __FILE__, __FUNCTION__, __LINE__);
			}
			catch (Exception $exception)
			{
				Arakoon_Client_Logger::logWarning("Encountered problems while disconnecting from $this->_ip on port $this->_port", __FILE__, __FUNCTION__, __LINE__);
				Arakoon_Client_Logger::logDebug('Exception: $exception', __FILE__, __FUNCTION__, __LINE__);			
			}
		}
	}
	 	
	/**
	 * @todo document
	 */
	public function sendMessage($message)
	{
		Arakoon_Client_Logger::logTrace("Enter", __FILE__, __FUNCTION__, __LINE__);
		
		if (!$this->isConnected())
		{
			$this->reconnect();
		}
				
		$succes = TRUE;
		$byteCount = socket_write($this->_socket, $message);
		
		if ($byteCount === FALSE)
		{
			$succes = FALSE;
		}
		
		Arakoon_Client_Logger::logTrace('Leave', __FILE__, __FUNCTION__, __LINE__);
		
		return $succes;
	}
	
	/**
	 * @todo document
	 */
	public function readBytes($byteCount)
	{
    	Arakoon_Client_Logger::logTrace("Enter", __FILE__, __FUNCTION__, __LINE__);    
    	
    	if (!$this->isConnected())
    	{
    		$fatalMsg = "Socket not connected to ip $this->_ip on port $this->_port";
    		Arakoon_Client_Logger::logFatal($fatalMsg, __FILE__, __FUNCTION__, __LINE__);								 
        	throw new Exception($fatalMsg);
    	}
    	
    	$bytesRemaining = $byteCount;
    	$result = '';
    	
    	while ($bytesRemaining > 0)
    	{
        	Arakoon_Client_Logger::logTrace('Trying to socket select', __FILE__, __FUNCTION__, __LINE__);
        	
        	$sockets = array($this->_socket);
        	$null = null; // socket_select needs reference arguments
        	$changedSocketCount = socket_select($sockets, $null, $null, Arakoon_Client_Config::CONNECTION_TIMEOUT);
        	
        	Arakoon_Client_Logger::logTrace("Socket select returned $changedSocketCount", __FILE__, __FUNCTION__, __LINE__);
        	
        	if ($changedSocketCount === FALSE)
        	{
        		$socketError = socket_last_error($this->_socket);
	            $this->disconnect();            
	            throw new Exception($socketError);	            
        	}
        	else if ($changedSocketCount > 0)
        	{
            	$chunk = socket_read($this->_socket, $bytesRemaining);
            	$chunkByteSize = strlen($chunk);

            	Arakoon_Client_Logger::logTrace("Socket read returned $chunkByteSize bytes after requesting $bytesRemaining bytes", __FILE__, __FUNCTION__, __LINE__);
            	
            	if ($chunkByteSize == 0)
            	{                	
                	$this->disconnect();           
                	return FALSE;
            	}
            	$result .= $chunk;
            	$bytesRemaining -= $chunkByteSize;
	        }
    	}
    	    
	    Arakoon_Client_Logger::logTrace('Leave', __FILE__, __FUNCTION__, __LINE__);
	    
	    return $result;
	}
}
?>
