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
require_once 'Exception.php';

/**
 * Arakoon_Client_NodeConnection
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
	private $_autoConnect;

	/**
	 * Constructor of an Arakoon_Client_NodeConnection object.
	 *
	 * @param 	array 	$node		array holding node info (ip, port)
	 * @param 	string 	$clusterId	string holding cluster id     *
	 * @return 	void
	 */
	public function __construct($node, $autoConnect=TRUE)
	{
		$this->_ip = $node->getIp();
		$this->_port = $node->getClientPort();
		$this->_socket = NULL;
		$this->_autoConnect = $autoConnect;
		
		if ($this->_autoConnect)
		{
			$this->connect();
		}
	}

	/**
	 * @todo document
	 */
	public function connect()
	{				
		Arakoon_Client_Logger::logTrace('Enter', __FILE__, __FUNCTION__, __LINE__);
		
		if ($this->isConnected())
		{
			return;
		}
		
		$timeOutOption = array(
    		'sec' => Arakoon_Client_Config::CONNECTION_TIMEOUT,	// seconds
    		'usec' => 0											// microseconds		
    	);
    	
		$this->_socket = socket_create(AF_INET, SOCK_STREAM, 0);    	
		socket_set_option($this->_socket, SOL_SOCKET, SO_RCVTIMEO, $timeOutOption);
		socket_set_option($this->_socket, SOL_SOCKET, SO_SNDTIMEO, $timeOutOption);		
		
		try
		{
			Arakoon_Client_Logger::logTrace('Connecting to ' . $this->_ip . ' on port ' . $this->_port, __FILE__, __FUNCTION__, __LINE__);

			$connected = socket_connect($this->_socket, $this->_ip, $this->_port);
			 
			if (!$connected)
			{				
				throw new Arakoon_Client_Exception();
			}
			
			Arakoon_Client_Logger::logTrace('Connected ' . $this->_ip . ' on port ' . $this->_port, __FILE__, __FUNCTION__, __LINE__);
		}
		catch(Exception $exception)
		{
			$lastSocketError = socket_last_error($this->_socket);			
			Arakoon_Client_Logger::logDebug('Last socket socket error: ' . $lastSocketError, __FILE__, __FUNCTION__, __LINE__);
			
			$fatalMsg = 'Cannot connect to ip ' . $this->_ip . ' on port ' . $this->_port . 'due a socket error (error: ' . $error . ')';										
			Arakoon_Client_Logger::logFatal($fatalMsg, __FILE__, __FUNCTION__, __LINE__);
							 
			throw new Arakoon_Client_Exception($fatalMsg);
		}	
		
		Arakoon_Client_Logger::logTrace('Leave', __FILE__, __FUNCTION__, __LINE__);
	}

	/**
	 * @todo document
	 */
	public function getSocket()
	{
		return $this->_socket;
	}
	
	/**
	 * @todo document
	 */
	private function reconnect()
	{
		Arakoon_Client_Logger::logTrace('Enter', __FILE__, __FUNCTION__, __LINE__);
		
		$this->disconnect();
		$this->connect();
		
		Arakoon_Client_Logger::logTrace('Leave', __FILE__, __FUNCTION__, __LINE__);
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
				Arakoon_Client_Logger::logTrace('Disconnecting from ' . $this->_ip . ' on port ' . $this->_port, __FILE__, __FUNCTION__, __LINE__);
				
				socket_close($this->_socket);
				$this->_socket = NULL;
				
				Arakoon_Client_Logger::logTrace('Disconnected from ' . $this->_ip . ' on port ' . $this->_port, __FILE__, __FUNCTION__, __LINE__);
			}
			catch (Exception $exception)
			{
				Arakoon_Client_Logger::logWarning('Could not disconnect from ' . $this->_ip . ' on port ' . $this->_port, __FILE__, __FUNCTION__, __LINE__);
				Arakoon_Client_Logger::logDebug('Exception: ' . $exception, __FILE__, __FUNCTION__, __LINE__);			
			}
		}
		else
		{
			Arakoon_Client_Logger::logTrace('Already disconnected from ' . $this->_ip . ' on port ' . $this->_port, __FILE__, __FUNCTION__, __LINE__);
		}
	}
	 	
	/**
	 * @todo document
	 */
	public function writeBytes($buffer)
	{
		Arakoon_Client_Logger::logTrace('Enter', __FILE__, __FUNCTION__, __LINE__);
		
		if ($this->_autoConnect && !$this->isConnected())
		{
			$this->reconnect();
		}
		
		$bufferByteCount = strlen($buffer);
		
		Arakoon_Client_Logger::logTrace('Trying to socket write ' . $bufferByteCount . ' bytes to ' . $this->_ip . ' on port ' . $this->_port, __FILE__, __FUNCTION__, __LINE__);
		
		$writtenByteCount = socket_write($this->_socket, $buffer);
		
		if ($writtenByteCount === FALSE)
		{
			$timedOut = stream_get_meta_data($this->_socket, 'timed_out');
			$warningMsg = NULL;
			
    		if ($timedOut)
    		{
    			$warningMsg = 'Socket write to ' . $this->_ip . ' on port ' . $this->_port . 'timed out after ' . Arakoon_Client_Config::CONNECTION_TIMEOUT . ' seconds';
    		}
    		else
    		{	    			
    			$warningMsg = 'Could not socket write ' . $bufferByteCount . ' bytes to ' . $this->_ip . ' on port ' . $this->_port;
    		}
	    		
    		$this->disconnect();
	    		
	    	Arakoon_Client_Logger::logWarning($warningMsg, __FILE__, __FUNCTION__, __LINE__);
			throw new Arakoon_Client_Exception($warningMessage);
		}
		else
		{
			Arakoon_Client_Logger::logTrace('Successfully wrote ' . $writtenByteCount . ' bytes to ' . $this->_ip . ' on port ' . $this->_port, __FILE__, __FUNCTION__, __LINE__);
		}
		
		Arakoon_Client_Logger::logTrace('Leave', __FILE__, __FUNCTION__, __LINE__);
	}
	
	/**
	 * @todo document
	 */
	public function readBytes($byteCount)
	{
    	Arakoon_Client_Logger::logTrace('Enter', __FILE__, __FUNCTION__, __LINE__);    
    	
		if ($this->_autoConnect && !$this->isConnected())
		{
			$this->reconnect();
		}
    	
    	$bytesRemaining = $byteCount;
    	$result = '';
    	
    	while ($bytesRemaining > 0)
	    {   	
        	Arakoon_Client_Logger::logTrace('Trying to socket read ' . $bytesRemaining . ' bytes', __FILE__, __FUNCTION__, __LINE__);	
        	
            $chunk = socket_read($this->_socket, $bytesRemaining);
	        $chunkByteSize = strlen($chunk);

	        Arakoon_Client_Logger::logTrace('Socket read returned ' . $chunkByteSize . ' bytes after requesting ' . $bytesRemaining . ' bytes', __FILE__, __FUNCTION__, __LINE__);
	            	
	        if ($chunkByteSize == 0)
	        {            
	        	$timeOutErrorCode = 110;    	
	        	$errorCode = socket_last_error($this->_socket);
	        	$errorMsg = socket_strerror($errorCode);
	        			
				Arakoon_Client_Logger::logDebug('Last socket socket error:' . $lastSocketError, __FILE__, __FUNCTION__, __LINE__);
				$warningMsg = NULL;
				
	    		if ($errorCode == $timeOutErrorCode)
	    		{
	    			$warningMsg = 'Socket read from ' . $this->_ip . ' on port ' . $this->_port. ' timed out after ' . Arakoon_Client_Config::CONNECTION_TIMEOUT . ' seconds';
	    		}
	    		else
	    		{	    			
	    			$warningMsg = 'Could not socket read bytes from ' . $this->_ip . ' on port ' . $this->_port;		
	    		}
	    		
	    		$this->disconnect();
	    		
	    		Arakoon_Client_Logger::logWarning($warningMsg, __FILE__, __FUNCTION__, __LINE__);
	    		throw new Arakoon_Client_Exception($warningMsg);
	        }
	            	
	        $result .= $chunk;
	        $bytesRemaining -= $chunkByteSize;
	    }
    	
	    Arakoon_Client_Logger::logTrace('Leave', __FILE__, __FUNCTION__, __LINE__);
	    
	    return $result;
	}
}
?>
