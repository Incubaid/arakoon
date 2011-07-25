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

require_once "AssertFailedException.php";
require_once "Exception.php";
require_once "Logger.php";
require_once "NotFoundException.php";
require_once "NotMasterException.php";
require_once "WrongClusterException.php";

/**
 * Arakoon_Client_Protocol
 *
 * @category   	Arakoon
 * @package    	Arakoon_Client
 * @copyright 	Copyright (C) 2010 Incubaid BVBA
 * @license    	http://www.arakoon.org/licensing
 */
class Arakoon_Client_Protocol
{
	/**
	 * Operation codes
	 */
	const OP_CODE_ASSERT					= 0x00000016;
	const OP_CODE_DELETE					= 0x0000000a;
	const OP_CODE_EXISTS					= 0x07;
	const OP_CODE_EXPECT_PROGRESS_POSSIBLE	= 0x00000012;
	const OP_CODE_GET						= 0x00000008;
	const OP_CODE_HELLO						= 0x00000001;
	const OP_CODE_GET_KEY_COUNT				= 0x0000001a;
	const OP_CODE_MAGIC 					= 0xb1ff0000;
	const OP_CODE_MULTI_GET					= 0x00000011;
	const OP_CODE_PREFIX					= 0x0000000c;
	const OP_CODE_RANGE						= 0x0000000b;
	const OP_CODE_RANGE_ENTRIES				= 0x0000000f;
	const OP_CODE_SEQUENCE					= 0x00000010;
	const OP_CODE_SET						= 0x00000009;
	const OP_CODE_STATISTICS				= 0x00000013;
	const OP_CODE_TEST_AND_SET				= 0x0000000d;
	const OP_CODE_USER_FUNCTION				= 0x00000015;
	const OP_CODE_VERSION					= 0x00000001;
	const OP_CODE_WHO_MASTER 				= 0x00000002;
	
	/**
	 * Sequenced operation codes
	 */
	const OP_CODE_SEQUENCED_ASSERT		= 8;
	const OP_CODE_SEQUENCED_DELETE		= 2;
	const OP_CODE_SEQUENCED_SET			= 1;
	const OP_CODE_SEQUENCED_SEQUENCE	= 5;
	
	/**
	 * Result codes
	 */
	const RESULT_CODE_ASSERT_FAILED	= 7;
    const RESULT_CODE_SUCCES 		= 0;
    const RESULT_CODE_NOT_MASTER 	= 4;
    const RESULT_CODE_NOT_FOUND 	= 5;
    const RESULT_CODE_WRONG_CLUSTER	= 6;
    
	/**
	 * Type sizes
	 */
    const TYPE_SIZE_BOOL = 1;
    const TYPE_SIZE_FLOAT = 8;
    const TYPE_SIZE_INT_32 = 4;
    const TYPE_SIZE_INT_64 = 8;    

	/**
     * Constructor of an Arakoon_Client_Protocol object.
     * The constructor is private, because the class only contains static functions.
     * 
     * @return void
     */
    private function __construct()
    {
    	
    }
    
    
    /**
	 * ============================================================================================================
	 * Pack functions
	 * ====================================================================================================
	 */
    
	/**
     * @todo document
     */
	public static function packBool($data)
	{
		$buffer = pack("c", $data); // char format is used because there is no boolean format available
    	return $buffer;
	}
	
    /**
     * @todo document
     */
	public static function packInt($data)
	{
	    $buffer =  pack("I", $data);
	    return $buffer;
	}
	
	/**
     * @todo document
     */
	public static function packSignedInt($data)
	{
	    $buffer =  pack("i", $data);
	    return $buffer;
	}
	
	/**
     * @todo document
     */
	public static function packString($data)
	{
    	$buffer =  pack("Ia*", strlen($data), $data);
    	return $buffer;
	}
	
	/**
     * @todo document
     */
	public static function packStringOption($data)
	{
		$buffer = "";
		
		if (empty($data))
		{
	        $buffer .= self::packBool(0);
	    }
	    else
	    {
	        $buffer .= self::packBool(1);
	        $buffer .= self::packString($data);
	    }
	    
	    return $buffer;
	}
	
	
	/**
	 * ====================================================================================================
	 * Unpack functions
	 * ====================================================================================================
	 */
	
	/**
     * @todo document
     */
	public static function unpackBool($buffer)
	{
		$data = unpack("cbool", $buffer);
		return $data["bool"];
	}
	
	/**
     * Unpacks a string.
     * 
     * @param 	$buffer
     * @param 	$offset
     * @return 	array
     */
	public static function unpackEntireString($buffer)
	{
	    $data = unpack("a*string", $buffer);
		return $data["string"];
	}
	
	/**
     * @todo document
     */
	public static function unpackFloat($buffer, $offset)
	{
	    if($offset > 0)
	    {
	        $data = unpack("c{$offset}char/dfloat", $buffer);
	    }
	    else
	    {
	        $data = unpack("dfloat", $buffer);
	    }    
	    
	    return array($data["float"], $offset + self::TYPE_SIZE_FLOAT);
	}

	
	/**
     * Unpacks an integer.
     * 
     * @param	$data signed integer that needs to be packed
     * @return 	packed data
     */
	public static function unpackInt($buffer, $offset)
	{
	    if($offset > 0)
	    {
	    	$data = unpack("c{$offset}char/Iint", $buffer);
	    }
	    else
	    {
	        $data = unpack("Iint", $buffer);
	    }
	    
	    return array($data["int"], $offset + self::TYPE_SIZE_INT_32);
	}
	
	/**
     * @todo document
     */
	public static function unpackInt64($buffer, $offset)
	{
		if (PHP_INT_SIZE == 4)
		{
			$fatalMsg = "Cannot unpack 64 bit integer in a 32 environment";
			Arakoon_Client_Logger::logFatal($fatalMsg, __FILE__, __FUNCTION__, __LINE__);
			
			throw new Arakoon_Client_Exception($fatalMsg);	
		}
		
		$buffer = substr($buffer, $offset, self::TYPE_SIZE_INT_64);
		$data = unpack("Iint", $buffer);

		return array($data["int"], $offset + self::TYPE_SIZE_INT_64);
	}
		
	/**
     * Unpacks a string.
     * 
     * @param 	$buffer
     * @param 	$offset
     * @return 	array
     */
	public static function unpackString($buffer, $offset)
	{
	    list($size, $offset2) = self::unpackInt($buffer, $offset);
	    $string = substr($buffer, $offset2, $size);
	    return array($string, $offset2 + $size);    
	}
	
	
	/**
	 * ====================================================================================================
	 * Read functions
	 * ====================================================================================================
	 */
	
	/**
     * @todo document
     */
	private static function readBool($connection)
	{
    	$buffer = $connection->readBytes(self::TYPE_SIZE_BOOL);
    	return self::unpackBool($buffer);
	}
	
	/**
     * @todo document
     */
	private static function readFloat($connection)
	{
    	$buffer = $connection->readBytes(self::TYPE_SIZE_FLOAT);    
	    list($float, $offset) = self::unpackFloat($buffer, 0);
	    return $float;
	}
	
	/**
     * @todo document
     */
	private static function readInt($connection)
	{
    	$buffer = $connection->readBytes(self::TYPE_SIZE_INT_32);    
	    list($integer, $offset) = self::unpackInt($buffer, 0);
	    return $integer;
	}
	
	/**
     * @todo document
     */
	private static function readInt64($connection)
	{
    	$buffer = $connection->readBytes(self::TYPE_SIZE_INT_64);    
	    list($integer, $offset) = self::unpackInt($buffer, 0);
	    return $integer;
	}	
		
	/**
     * @todo document
     */
	private static function readString($connection)
	{
	    $byteCount = self::readInt($connection);
	    $buffer = $connection->readBytes($byteCount);
	    return $buffer;
	}

	/**
     * @todo document
     */
	private static function readStringOption($connection)
	{
	    $isSet = self::readBool($connection);
	    $string = NULL;
	    
	    if($isSet)
	    {
	        $string = self::readString($connection);
	    }
	    
	    return $string;
	}
	
	
	/**
	 * ====================================================================================================
	 * Encode functions
	 * ====================================================================================================
	 */
	
	/**
     * @todo document
     */
	static function encodeDelete($key)
	{
        $buffer = self::packInt(self::OP_CODE_DELETE | self::OP_CODE_MAGIC);
        $buffer .= self::packString($key);
        
        return $buffer;
    }
    
    /**
     * @todo document
     */
	public static function encodeExpectProgressPossible()
	{
        $buffer = self::packInt(self::OP_CODE_EXPECT_PROGRESS_POSSIBLE | self::OP_CODE_MAGIC);
        
        return $buffer;
    }
    
	/**
     * @todo document
     */
	public static function encodeExists($key, $allowDirtyRead)
	{
        $buffer = self::packInt(self::OP_CODE_EXISTS | self::OP_CODE_MAGIC);
        $buffer .= self::packBool($allowDirtyRead);
        $buffer .= self::packString($key);
        
        return $buffer;
    }    
        
	/**
     * @todo document
     */
	public static function encodeGet($key, $allowDirtyRead)
	{
		$buffer = self::packInt(self::OP_CODE_GET | self::OP_CODE_MAGIC);
        $buffer .= self::packBool($allowDirtyRead);
        $buffer .= self::packString($key);
        
        return $buffer;
	}
	
	/**
     * @todo document
     */
	public static function encodeHello($clientId, $clusterId)
	{
        $buffer  = self::packInt(self::OP_CODE_HELLO | self::OP_CODE_MAGIC);
        $buffer .= self::packString($clientId);
        $buffer .= self::packString($clusterId);
        
        return $buffer;
    }
    
	/**
     * @todo document
     */
	public static function encodeGetKeyCount()
	{
        $buffer  = self::packInt(self::OP_CODE_GET_KEY_COUNT | self::OP_CODE_MAGIC);
        
        return $buffer;
    }
	
	/**
     * @todo document
     */
	public static function encodeMultiGet($keys, $allowDirtyRead)
	{
		$buffer = self::packInt(self::OP_CODE_MULTI_GET | self::OP_CODE_MAGIC);
        $buffer .= self::packBool($allowDirtyRead);
        $buffer .= self::packInt(count($keys));
        
        foreach ($keys as $key)
        {
            $buffer .= self::packString($key);
        }
        
        return $buffer;
    }
    
    /**
     * @todo document
     */
	static function encodePrefix($key, $maxElements, $allowDirtyRead)
	{
        $buffer = self::packInt(self::OP_CODE_PREFIX | self::OP_CODE_MAGIC);
        $buffer .= self::packBool($allowDirtyRead);
        $buffer .= self::packString($key);
        $buffer .= self::packSignedInt($maxElements);
        
        return $buffer;
    }
    
	/**
     * @todo document
     */
	public static function EncodePrologue($clusterId)
	{
    	$buffer = self::packInt(self::OP_CODE_MAGIC);
    	$buffer .= self::packInt(self::OP_CODE_VERSION);
    	$buffer .= self::packString($clusterId);
    	
    	return $buffer;
	}
	
	/**
     * @todo document
     */
	public static function encodeRange($beginKey, $includeBeginKey, $endKey, $includeEndKey, $maxElements , $allowDirtyRead)
	{
        $buffer = self::packInt(self::OP_CODE_RANGE | self::OP_CODE_MAGIC);
        $buffer .= self::packBool($allowDirtyRead);
        $buffer .= self::packStringOption($beginKey);
        $buffer .= self::packBool($includeBeginKey);
        $buffer .= self::packStringOption($endKey);
        $buffer .= self::packBool($includeEndKey);
        $buffer .= self::packSignedInt($maxElements);
        
        return  $buffer;
    }

    /**
     * @todo document
     */
    public static function encodeRangeEntries($beginKey, $includeBeginKey, $endKey, $includeEndKey, $maxElements , $allowDirtyRead)
    {
        $buffer = self::packInt(self::OP_CODE_RANGE_ENTRIES | self::OP_CODE_MAGIC);
        $buffer .= self::packBool($allowDirtyRead);
        $buffer .= self::packStringOption($beginKey);
        $buffer .= self::packBool($includeBeginKey);
        $buffer .= self::packStringOption($endKey);
        $buffer .= self::packBool($includeEndKey);
        $buffer .= self::packSignedInt($maxElements);
        
        return  $buffer;
    }
        
	/**
     * @todo document
     */
	public static function encodeSet($key, $value)
	{
        $buffer = self::packInt(self::OP_CODE_SET | self::OP_CODE_MAGIC);
        $buffer .= self::packString($key);
        $buffer .= self::packString($value);
        
        return $buffer;
    }    

    /**
     * @todo document
     */
    public static function encodeStatistics()
    {
        $buffer = self::packInt(self::OP_CODE_STATISTICS | self::OP_CODE_MAGIC);
        
        return $buffer;
    }
    
    /**
     * @todo document
     */
	public static function encodeTestAndSet($key, $oldValue, $newValue)
	{
        $buffer = self::packInt(self::OP_CODE_TEST_AND_SET | self::OP_CODE_MAGIC);
        $buffer .= self::packString($key);
        $buffer .= self::packStringOption($oldValue);
        $buffer .= self::packStringOption($newValue);
        
        return $buffer;
    }
    
	/**
     * @todo document
     */
    public static function encodeWhoMaster()
    {
        $buffer = self::packInt(self::OP_CODE_WHO_MASTER | self::OP_CODE_MAGIC);
        
        return $buffer; 
    }
    
    
    /**
	 * ====================================================================================================
	 * Decode functions
	 * ====================================================================================================
	 */
    
	/**
     * @todo document
     */
	public static function decodeBoolResult($connection)
	{
    	self::evaluateResultCode($connection);
    	$bool = self::readBool($connection);
        
    	return $bool;
	}
    
	/**
     * @todo document
     */
	public static function decodeInt64Result($connection)
	{
    	self::evaluateResultCode($connection);
    	$int64 = self::readInt64($connection);
        
    	return $int64;
	}	
	
	/**
     * @todo document
     */
	public static function decodeStatisticsResult($connection)
	{
        self::evaluateResultCode($connection);
        $statistics = array();
        $buffer = self::readString($connection);
        
        $offset0 = 0;
        list($start, $offset1)   		= self::unpackFloat($buffer, $offset0);
        list($last, $offset2)        	= self::unpackFloat($buffer, $offset1);
        list($avg_set_size, $offset3) 	= self::unpackFloat($buffer, $offset2);
        list($avg_get_size, $offset4) 	= self::unpackFloat($buffer, $offset3);
        list($n_sets, $offset5)       	= self::unpackInt($buffer, $offset4);
        list($n_gets, $offset6)      	= self::unpackInt($buffer, $offset5);
        list($n_deletes, $offset7)    	= self::unpackInt($buffer, $offset6);
        list($n_multigets, $offset8)  	= self::unpackInt($buffer, $offset7);
        list($n_sequences, $offset9) 	= self::unpackInt($buffer, $offset8);
        list($n_entries, $offset10)  	= self::unpackInt($buffer, $offset9);
        
        $node_is = array();
        $cycleOffset = $offset10;
        for ($i = 0; $i < $n_entries; $i++)
        {
            list($name, $tempOffset1) = self::unpackString($buffer, $cycleOffset);
            list($integer, $tempOffset2) = self::unpackInt64($buffer, $tempOffset1);
            $node_is[$name] = $integer;
            
            $cycleOffset = $tempOffset2;
        }
        
        $statistics["start"] = $start;
        $statistics["last"] = $last;
        $statistics["avg_set_size"] = $avg_set_size;
        $statistics["avg_get_size"] = $avg_get_size;
        $statistics["n_sets"] = $n_sets;
        $statistics["n_gets"] = $n_gets;
        $statistics["n_deletes"] = $n_deletes;
        $statistics["n_multigets"] = $n_multigets;
        $statistics["n_sequences"] = $n_sequences;
        $statistics["node_is"] = $node_is;

        return $statistics;
    }
    
    /**
     * @todo document
     */
	public static function decodeStringResult($connection)
	{
        self::evaluateResultCode($connection);
        $string = self::readString($connection);
        
        return $string; 
    }
    
	public static function decodeStringListResult($connection)
	{
        self::evaluateResultCode($connection);        
        $list = array();
        $listLength = self::readInt($connection);

        for($i = 0; $i < $listLength; $i++)
        {
            array_unshift($list, self::readString($connection));
        }
        
        return $list;
    }
    
	/**
     * @todo document
     */
	public static function decodeStringOptionResult($connection)
	{
        self::evaluateResultCode($connection);
        $stringOption = self::readStringOption($connection);
        
        return $stringOption; 
    }
    
    /**
     * @todo document
     */
	static function decodeStringPairListResult($connection)
	{
        self::evaluateResultCode($connection);
        $list = array();
        $listLength = self::readInt($connection);

        for($i = 0; $i < $listLength; $i++)
        {
            $key = self::readString($connection);
            $value = self::readString($connection);
            array_unshift($list, array($key, $value));
        }

        return $list;
    }
    
	/**
     * @todo document
     */
	public static function decodeVoidResult($connection)
	{
        self::evaluateResultCode($connection);
    }
    
    
   /**
	 * ====================================================================================================
	 * Other functions
	 * ====================================================================================================
	 */
         
    /**
     * @todo document
     */
    private static function evaluateResultCode($connection)
    {
        $resultCode = self::readInt($connection);
                
        if ($resultCode != self::RESULT_CODE_SUCCES)
        {
        	$resultMsg = self::readString($connection);
        	$exceptionMsg = "An error occured (code: $resultCode) while executing an Arakoon operation (message: $resultMsg)";
        	$exception = NULL;
        	
	        switch($resultCode)
	        {	        		
	        	case self::RESULT_CODE_NOT_FOUND:
	        		$exception = new Arakoon_Client_NotFoundException($exceptionMsg);
	        		break;
	            	            	
	        	case self::RESULT_CODE_NOT_MASTER:
	        		$exception = new Arakoon_Client_NotMasterException($exceptionMsg);
	        		break;
	        		
        		case self::RESULT_CODE_WRONG_CLUSTER:
	        		$exception = new Arakoon_Client_WrongClusterException($exceptionMsg);
	        		break;
	        		
        		case self::RESULT_CODE_ASSERT_FAILED:
        			$exception = new Arakoon_Client_AssertFailedException($exceptionMsg);

	        	default:
	        		$exception = new Arakoon_Client_Exception();
	        		break;	            
	        }
	        
	        Arakoon_Client_Logger::logError($exceptionMsg, __FILE__, __FUNCTION__, __LINE__);
	        throw $exception;    
        }
    }
}
?>