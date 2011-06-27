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
 * @copyright Copyright (C) 2010 Incubaid BVBA
 */

/**
 * ArakoonTestClient class (singleton)
 */

require_once '../arakoon.php';

class ArakoonTestEnvironment
{
	private static $_instance = NULL;
	
	private $_client;
	private $_clientConfig;		
	
	private function __construct()
	{

	}

    static public function getInstance()
    {
        if (self::$_instance == NULL)
        {
        	self::$_instance = new ArakoonTestEnvironment();
        }
        
        return self::$_instance;
    }
    
	public function getClient()
    {
    	return $this->_client;
    }
    
	public function getClientConfig()
    {
    	return $this->_clientConfig;
    }
    
	public function setup($config, $arakoonExeCmd, $configFilePath)
	{
		$this->_clientConfig = $config;
		$this->_client = new Arakoon($this->_clientConfig);
		
		foreach ($this->_clientConfig->getNodes() as $nodeId => $node)
		{
			if (!file_exists($node['home']))
			{
				mkdir($node['home']);
			}
			
			shell_exec($arakoonExeCmd . ' -config ' . $configFilePath . ' -daemonize --node ' . $nodeId);
		}
		
		// sleep 1 second to ensure Arakoon nodes are up
		sleep(1);
	}
	
	public function tearDown()
	{
		foreach ($this->_clientConfig->getNodes() as $nodeId => $node)
		{
			if (file_exists($node['home']))
			{
				$this->recursiveRemoveDir($node['home']);
			}
		}
			
		shell_exec('killall arakoon');
	}
	
	private function recursiveRemoveDir($dir)
	{
		$removeSucces = TRUE;
	
		if(substr($dir,-1) == '/')
		{
			$dir = substr($dir, 0, -1);
		}
	
		if (file_exists($dir) && is_dir($dir) || is_readable($dir))
		{
			$dirHandle = opendir($dir);
				
			while ($contents = readdir($dirHandle))
			{
				if($contents != '.' && $contents != '..')
				{
					$path = $dir . "/" . $contents;
						
					if(is_dir($path))
					{
						$this->recursiveRemoveDir($path);
					}
					else
					{
						unlink($path);
					}
				}
			}
			rmdir($dir);
		}
		else
		{
			$removeSucces = FALSE;
		}
	}
}

?>