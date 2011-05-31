<?php
/*
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

/*
* @copyright Copyright (C) 2010 Incubaid BVBA
*/


require_once 'ara_def.php';
require_once 'logging.php';

class ArakoonClientConfig
{
    private $clusterId = null;
    private $nodes = null;

    /*
     * Constructor of an ArakoonClientConfig object
     * 
     * The constructor takes one optional parameter 'nodes'.
     * This is a dictionary containing info on the arakoon server nodes. It contains:
     *      - nodeids as keys
     *      - (hostname/ip, tcp port) tuples as value
     *@example :
     *      $nodes = array(
     *          'sampleapp_0' => array('ip'=>'127.0.0.1', 'port' => 20100)
     *      );
     * 
     * @param string clusterId: name of the cluster
     * @param array $nodes: An array containing the locations for the server nodes
    */
    public function __construct($clusterId, $nodes) {
       $this->clusterId = $clusterId;
       $this->nodes = $nodes;
    }

    /*   
     * Retrieve the period messages to the master should be retried when a master re-election occurs
     * This period is specified in seconds
     *  
     * @return int :Returns the retry period in seconds
    */
    static function getNoMasterRetryPeriod(){
        return ARA_CFG_NO_MASTER_RETRY;
    }

    /*
     * Retrieve the tcp connection timeout
     * Can be controlled by changing the global variable L{ARA_CFG_CONN_TIMEOUT}
     * 
     * @return int :Returns the tcp connection timeout
    */
    static function getConnectionTimeout(){
        return ARA_CFG_CONN_TIMEOUT;
    }

    /*
     * Retrieves the backoff interval.
     * If an attempt to send a message to the server fails,
     * the client will wait a random number of seconds. The maximum wait time is n*getBackoffInterVal()
     * with n being the attempt counter.
     * Can be controlled by changing the global variable L{ARA_CFG_CONN_BACKOFF}
     * 
     * @return int :The maximum backoff interval
    */
   
    static function getBackoffInterval(){
        return ARA_CFG_CONN_BACKOFF;
    }
    
    /*
     * Retrieve location of the server node with give node identifier
     * A location is a pair consisting of a hostname or ip address as first element.
     * The second element of the pair is the tcp port
     * 
     * @param string $nodeId: The node identifier whose location you are interested in
     * 
     * @return array :Returns a pair with the nodes hostname or ip and the tcp port, e.g. ("127.0.0.1", 4000)
    */ 
    function getNodeLocation($nodeId){
        if (!key_exists($nodeId, $this->nodes)){
            Logging::error("Invalid node: $nodeId", __FILE__, __FUNCTION__, __LINE__);
            throw new Exception("Invalid node: $nodeId");
        }
        return $this->nodes[$nodeId];   
    }

    /*
     * Retrieve the number of attempts a message should be tried before giving up
     * 
     * Can be controlled by changing the global variable L{ARA_CFG_TRY_CNT}
     * 
     * @return int :Returns the max retry count.
    */
    function getTryCount (){
        return ARA_CFG_TRY_CNT;
    }

    /*
     * Retrieve the dictionary with node locations
     * 
     * @return array :Returns aan array mapping the node identifiers (string) to its location
    */
    function getNodes(){
        return $this->nodes;
    }
        
    function getClusterId(){
        return $this->clusterId;
    }

}

?>
