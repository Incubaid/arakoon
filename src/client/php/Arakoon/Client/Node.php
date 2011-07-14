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

/**
 * This class represents the Arakoon_Client_Config_Node class.
 *
 * @category   	Arakoon
 * @package    	Arakoon_Client
 * @subpackage	Config
 * @copyright 	Copyright (C) 2010 Incubaid BVBA
 * @license    	http://www.arakoon.org/licensing
 */
class Arakoon_Client_Node
{
	private $_id;
    private $_ip;
    private $_clientPort;
    private $_home;

    /**
     * Constructor of an Arakoon_Client_Node object.
     * 
     * @param string 	$ip
     * @param integer	$port
     */
    public function __construct($id, $ip, $clientPort, $home)
    {
    	$this->_id = $id;
        $this->_ip = $ip;
        $this->_clientPort = $clientPort;
        $this->_home = $home;
    }
	    
	/**
     * Gets the node its identifier.
     * 
     * @return string node identifier
     */
    public function getId()
    {
        return $this->_id;
    }
    
    /**
     * Gets the node its ip address.
     * 
     * @return string node ip address
     */
    public function getIp()
    {
        return $this->_ip;
    }
    
    /**
     * Gets the node its client port.
     * 
     * @return integer node client port
     */
	public function getClientPort()
    {
        return $this->_clientPort;
    } 

    /**
     * Gets the node its home directory.
     * 
     * @return string node home directory
     */
	public function getHome()
    {
        return $this->_home;
    }   
}
?>