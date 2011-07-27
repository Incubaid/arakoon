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

require_once "Logger.php";
require_once "Config.php";

/**
 * Arakoon_Client_Operation
 * 
 * @category   	Arakoon
 * @package    	Arakoon_Client
 * @copyright 	Copyright (C) 2010 Incubaid BVBA
 * @license    	http://www.arakoon.org/licensing
 */
abstract class Arakoon_Client_Operation
{
    
    /**
     * @todo document
     */
    abstract public function encode($sequenced = FALSE);
    
    /**
     * @todo document
     */
	public static function validateKey($key)
    {
    	self::validateKeySize($key);
    	
    	if (!isset($key))
		{
			throw new Arakoon_Client_Exception("Key invalid");
		}
    }
    
	/**
     * @todo document
     */
	public static function validateKeys(array $keys)
    {
    	foreach ($keys as $key)
    	{
    		self::validateKey($key);
    	}    	
    }
        
    /**
     * @todo document
     */
    public static function validateKeySize($key)
    {
    	if (mb_strlen($key) > Arakoon_Client_Config::MAX_KEY_BYTE_SIZE)
		{
			throw new Arakoon_Client_Exception("Key exceeds max size (8mb)");
		}
    }
    
	/**
     * @todo document
     */
    public static function validateKeyPrefix($key)
    {
    	if (!isset($key))
		{
			throw new Arakoon_Client_Exception("Key prefix invalid");
		}
    }
    
	/**
     * @todo document
     */
	public static function validateValue($value)
    {
    	self::validateValueSize($value);
    	
    	if (!isset($value))
		{
			throw new Arakoon_Client_Exception("Value invalid");
		}
    }
    
	/**
     * @todo document
     */
    public static function validateValueSize($value)
    {
    	if (mb_strlen($value) > Arakoon_Client_Config::MAX_KEY_BYTE_SIZE)
		{
			throw new Arakoon_Client_Exception("Key exceeds max size (8mb)");
		}
    }
}
?>