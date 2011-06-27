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

require_once 'ara_config.php';

class ArakoonClientConfigFactory
{
	public static function CreateFromFile($filePath)
	{
		$extention = substr(strrchr($filePath, '.'), 1);
		
		if ($extention == "ini"){
			return ArakoonClientConfigFactory::CreateFromIniFile($filePath);			
		}
		else
		{
			//throw new Exception("Error: Config file extention not supported!");	
		}
	}
	
	private static function CreateFromIniFile($filePath)
	{
		$config = parse_ini_file($filePath, true);		
		ArakoonClientConfigFactory::ValidateClientConfig($config);
        
		$clusterId = $config['global']['cluster_id'];
		$nodes = array();
		
		$nodeNames = split(',', $config['global']['cluster']);		
		foreach($nodeNames as $nodeName)
		{
			$nodeName = trim($nodeName);
			$nodes[$nodeName] = $config[$nodeName];
        }
        
        return new ArakoonClientConfig($clusterId, $nodes);
	}	
	
	private static function ValidateClientConfig(array $config)
	{		
        if (!array_key_exists('global', $config))
        {
            throw new Exception('Global section undefined');
        }
		
        if (!array_key_exists('cluster', $config['global']))
        {
            throw new Exception('Cluster undefined!');
        }	
        
		if (!array_key_exists('cluster_id', $config['global']))
        {
            throw new Exception('Cluster_id undefined!');
        }        
        
        $nodeNames = split(',', $config["global"]["cluster"]);
        
		foreach($nodeNames as $nodeName){
            $nodeName = trim($nodeName);
            
			if (array_key_exists($nodeName, $config))
	        {
	            ArakoonClientConfigFactory::ValidateClientNodeConfig($config[$nodeName]);
	        }
	        else
	        {
	        	throw new Exception('Node section undefined');
	        }
        }
	}
	
	private static function ValidateClientNodeConfig($nodeConfig)
	{			
		if (!array_key_exists('ip', $nodeConfig))
        {
            throw new Exception('Node ip undefined!');
        }
		
        if (!array_key_exists('client_port', $nodeConfig))
        {
            throw new Exception('Node client port undefined!');
        }
        
		if (!array_key_exists('messaging_port', $nodeConfig))
        {
            throw new Exception('Node message port undefined!');
        }
        
		if (!array_key_exists('home', $nodeConfig))
        {
            throw new Exception('Node home undefined!');
        }
	}
	
}

?>