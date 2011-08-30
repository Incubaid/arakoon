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

/**
 * Arakoon_Client_Operation_Assert
 * 
 * @category   	Arakoon
 * @package    	Arakoon_Client
 * @subpackage	Operation
 * @copyright 	Copyright (C) 2010 Incubaid BVBA
 * @license    	http://www.arakoon.org/licensing
 */
class Arakoon_Client_Operation_Assert extends Arakoon_Client_Operation
{
    private $_key;
    private $_expectedValue;
    private $_allowDirtyRead;
    
    /**
     * @todo document
     * 
     * @param $key 				string
     * @param $expectedValue 	string	can be none
     */
    public function __construct($key, $expectedValue, $allowDirtyRead = FALSE)
    {
    	self::validateKey($key);
		
        $this->_key = $key;   
        $this->_expectedValue = $expectedValue;
        $this->_allowDirtyRead = $allowDirtyRead;
    }
    
    /**
     * @todo document
     */
    public function encode($sequenced = FALSE)
    {
    	$buffer = "";
    	
    	if ($sequenced)
    	{
    		$buffer .= Arakoon_Client_Protocol::packInt(Arakoon_Client_Protocol::OP_CODE_SEQUENCED_ASSERT);
    	}
    	else
    	{
    		$buffer .= Arakoon_Client_Protocol::packInt(Arakoon_Client_Protocol::OP_CODE_ASSERT | Arakoon_Client_Protocol::OP_CODE_MAGIC);
    		$buffer .= Arakoon_Client_Protocol::packBool($this->_allowDirtyRead);	
    	}
    	
    	
        $buffer .= Arakoon_Client_Protocol::packString($this->_key);
        $buffer .= Arakoon_Client_Protocol::packStringOption($this->_expectedValue);
        
        return $buffer;
    }
}
?>