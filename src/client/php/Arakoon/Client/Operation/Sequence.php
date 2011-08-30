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

require_once "Assert.php";
require_once "Set.php";
require_once "Delete.php";

/**
 * Arakoon_Client_Operation_Sequence
 * 
 * @category   	Arakoon
 * @package    	Arakoon_Client
 * @subpackage	Operation
 * @copyright 	Copyright (C) 2010 Incubaid BVBA
 * @license    	http://www.arakoon.org/licensing
 */
class Arakoon_Client_Operation_Sequence extends Arakoon_Client_Operation
{
    private $_operations;
    
    /**
     * @todo document
     */
    public function  __contruct()
    {
        $this->_operations = array();
    }
    
    /**
     * @todo document
     */
    private function addOperation($operation)
    {
        $this->_operations[] = $operation;
    }
    
	/**
     * @todo document
     */
    public function addAssertOperation($key, $expectedValue)
    {
    	$assertOperation = new Arakoon_Client_Operation_Assert($key, $expectedValue);
    	$this->addOperation($assertOperation);
    }
    
	/**
     * @todo document
     */
	public function addDeleteOperation($key)
    {
    	$deleteOperation = new Arakoon_Client_Operation_Delete($key);
    	$this->addOperation($deleteOperation);
    }
    
    /**
     * @todo document
     */
	public function addSequenceOperation($sequenceOperation)
    {
    	$this->addOperation($sequenceOperation);
    }
    
    /**
     * @todo document
     */
    public function addSetOperation($key, $value)
    {
    	$setOperation = new Arakoon_Client_Operation_Set($key, $value);
    	$this->addOperation($setOperation);
    }        
    
	/**
     * @todo document
     */
    public function encode($sequenced = FALSE)
    {
    	$buffer = "";
    	
    	if ($sequenced)
    	{
    		$buffer .= Arakoon_Client_Protocol::packInt(Arakoon_Client_Protocol::OP_CODE_SEQUENCED_SEQUENCE);
    		$buffer .= Arakoon_Client_Protocol::packInt(count($this->_operations));
    			
    		foreach ($this->_operations as $operation)
    		{
            	$buffer .= $operation->encode(TRUE);
	        }
    	}
    	else
    	{
    		$buffer = Arakoon_Client_Protocol::packInt(Arakoon_Client_Protocol::OP_CODE_SEQUENCE | Arakoon_Client_Protocol::OP_CODE_MAGIC);
    		$buffer .= Arakoon_Client_Protocol::packString($this->encode(TRUE));
    	}
    	
        return $buffer;
    }
}
?>