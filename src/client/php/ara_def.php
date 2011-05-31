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



/*
 * Default cfg files in PYLABS
 */
define("ARA_CFG_QBASE3_PATH","/opt/qbase3/cfg/qconfig/arakoonclients.cfg");
define("ARA_CFG_QBASE5_PATH","/opt/qbase5/cfg/qconfig/arakoonclients.cfg");


/*
 * Configuration constants
 */
define("ARA_CFG_TRY_CNT", 1);
define("ARA_CFG_CONN_TIMEOUT", 60);
define("ARA_CFG_CONN_BACKOFF", 5);
define("ARA_CFG_NO_MASTER_RETRY", 60);


/*
 * Protocol Constants
 */
define("ARA_TYPE_INT_SIZE", 4);
define("ARA_TYPE_BOOL_SIZE", 1);

// Magic used to mask each command
define("ARA_CMD_MAG", 0xb1ff0000);
define("ARA_CMD_VER", 0x00000001);
// Hello command
define("ARA_CMD_HEL", 0x00000001 | ARA_CMD_MAG);

// Who is master?
define("ARA_CMD_WHO", 0x00000002 | ARA_CMD_MAG);
// Existence of a value for a key
define("ARA_CMD_EXISTS", 0x07 | ARA_CMD_MAG);
// Get a value
define("ARA_CMD_GET", 0x00000008 | ARA_CMD_MAG);
// Update a value
define("ARA_CMD_SET", 0x00000009 | ARA_CMD_MAG);
// Delete a key value pair
define("ARA_CMD_DEL", 0x0000000a | ARA_CMD_MAG);
// Get a range of keys
define("ARA_CMD_RAN", 0x0000000b | ARA_CMD_MAG);
// Get keys matching a prefix
define("ARA_CMD_PRE", 0x0000000c | ARA_CMD_MAG);
// Test and set a value
define("ARA_CMD_TAS", 0x0000000d | ARA_CMD_MAG);
// range entries
define("ARA_CMD_RAN_E", 0x0000000f | ARA_CMD_MAG);

//sequence
define("ARA_CMD_SEQ", 0x00000010 | ARA_CMD_MAG);
define("ARA_CMD_MULTI_GET", 0x00000011 | ARA_CMD_MAG);
define("ARA_CMD_EXPECT_PROGRESS_POSSIBLE", 0x00000012 | ARA_CMD_MAG);
define("ARA_CMD_STATISTICS", 0x00000013 | ARA_CMD_MAG);

/*
 * Arakoon error codes
 */
//Success
define("ARA_ERR_SUCCESS", 0);
//No entity
define("ARA_ERR_NO_ENT", 1);
//Node is not the master
define("ARA_ERR_NOT_MASTER", 4);
//not found
define("ARA_ERR_NOT_FOUND", 5);
//wrong cluster
define("ARA_ERR_WRONG_CLUSTER", 6);

?>
