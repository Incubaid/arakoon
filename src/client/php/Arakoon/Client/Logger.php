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
 * Arakoon_Client_LoggerLevel
 *
 * @category   	Arakoon
 * @package    	Arakoon_Client
 * @copyright 	Copyright (C) 2010 Incubaid BVBA
 * @license    	http://www.arakoon.org/licensing
 */
class Arakoon_Client_LoggerLevel
{
	const TRACE = 0;
    const DEBUG = 1;
    const INFO = 2;
    const WARNING = 3;
    const ERROR = 4;
    const FATAL = 5;
    
	/**
     * Arakoon_Client_LoggerLevel constructor.
     * 
     * @return void
     */
	private function __construct()
	{
	
    }
    
    /**
     * Gets the name of a given log level.
     * 
     * @return string $name log level name
     */
    public static function getName($level)
    {              
    	$name = "";    
        switch($level)
        {
            case self::TRACE:
                $name = "TRACE";
                break;
                
            case self::DEBUG:
                $name = "DEBUG";
                break;
                
            case self::INFO:
                $name = "INFO";
                break;
                
            case self::WARNING:
                $name = "WARNING";
                break;
                
            case self::ERROR:
                $name = "ERROR";
                break;
                
            case self::FATAL:
                $name = "FATAL";
                break;               
                 
            default:
                $name = "UNDEFINED";
        }        
        return $name;
    }  
}

/**
 * This class represents the Arakoon_Client_Logger class.
 *
 * @category   	Arakoon
 * @package    	Arakoon_Client
 * @subpackage	Operation
 * @copyright 	Copyright (C) 2010 Incubaid BVBA
 * @license    	http://www.arakoon.org/licensing
 */
class Arakoon_Client_Logger
{
    const DEFAULT_LOG_LEVEL = Arakoon_Client_LoggerLevel::INFO;    
    const LOG_LINE_DATE_FORMAT  = 'Y-m-d G:i:s';

    private static $_instance = NULL;
    
    private $_enabled;
    private $_logFileDestination;
    private $_level;    	
	
    /**
     * Arakoon_Client_Logger Constructor.
     * 
     * @return void
     */
	private function __construct()
	{
		$this->_enabled = TRUE;
		$this->_level = self::DEFAULT_LOG_LEVEL;
    }
    
    /**
     * Singleton enforcer.
     * 
     * @return Arakoon_Client_Logger
     */
	static public function getInstance()
    {
        if (empty(self::$_instance))
        {
        	self::$_instance = new self();
        }
        
        return self::$_instance;
    }
    
    /**
     * Setups the logger using the given arguments.
     * 
     * @param string 	$logFileLocation	location of the log file
     * @param string 	$level				log level, defaults to info
     * @param boolean 	$cleanLogFile		cleans the log file if true, defaults to false
     */
    public static function setup($logFileLocation, $level = self::DEFAULT_LOG_LEVEL, $cleanLogFile = FALSE)
    {
    	self::setLogFileLocation($logFileLocation);
    	self::setLevel($level);    	
    	if ($cleanLogFile)
    	{
    		self::cleanLogFile($logFileLocation);
    	}
    }
    
    /**
     * Enables the logger.
     * 
     * @return void
     */
    public static function enable()
    {
    	self::getInstance()->_enabled = TRUE;
    }
    
    /**
     * Disables the logger.
     * 
     * @return void
     */
	public static function disable()
    {
    	self::getInstance()->_enabled = FALSE;
    }
    
    /**
     * Gets the logger it's log file location.
     * 
     * @return string
     */
    public static function getLogFileLocation()
    {
    	return self::getInstance()->_logFileDestination;
    }
    
    /**
     * Sets the logger it's log file location.
     * 
     * @param 	string $location log file path
     * @return 	void
     */
    public static function setLogFileLocation($location)
    {    	    	
        if (file_exists($location))
        {
            if (!is_writable($location))
            {
                throw new Exception('Log file not writable!');
            }
        }
        else
        {
        	$touchSucces = touch($location);
        	if (!$touchSucces)
        	{
        		throw new Exception('Could not create log file!');        		
        	}
        }
        self::getInstance()->_logFileDestination = $location;
    }
    
    /**
     * Gets the logger its threshold level.
     * 
     * @return string	$level
     */
	public static function getLevel()
    {
    	return self::getInstance()->_level;
    }
    
    /**
     * Sets the logger its threshold level.
     * 
     * @param 	string $level
     * @return	void
     */
	public static function setLevel($level)
    {
    	if ($level >= 0 && $level <= 5)
    	{
    		self::getInstance()->_level = $level;
    	}
    	else
    	{
         	throw new Exception("Invalid log level ($level)");
       	}
    }
    
    /**
     * Cleans up the logger its log file.
     * 
     * @return void
     */
    public static function cleanLogFile()
    {
    	$logger = self::getInstance();
    	    	
    	if (empty($logger->_logFileDestination))
    	{
    		throw new Exception("Log file path not set!");
    	}
    	
    	try
    	{
    		fopen($logger->_logFileDestination, 'w');
    	}
    	catch(Exception $exception)
    	{
    		throw new Exception("Could not clean log file (exception: $exception)");
    	}
    }

    /**
     * Logs a trace message.
     * 
     * @param string $message 	log message, defaults to an empty string
     * @param string $file		file where the logger is called from, defaults to an empty string
     * @param string $function	function where the logger is called from, defaults to an empty string
     * @param string $line		line where the logger is called from, defaults to an empty string
     * @return void
     */
    public static function logTrace($message, $file='', $function='', $line='')
    {
        return Arakoon_Client_Logger::log(Arakoon_Client_LoggerLevel::TRACE, $message, $file, $function, $line);
    }
    
    /**
     * Logs a info message.
     * 
     * @param string $message 	log message, defaults to an empty string
     * @param string $file		file where the logger is called from, defaults to an empty string
     * @param string $function	function where the logger is called from, defaults to an empty string
     * @param string $line		line where the logger is called from, defaults to an empty string
     * @return void
     */
    public static function logInfo($message, $file='', $function='', $line='')
    {
        return Arakoon_Client_Logger::log(Arakoon_Client_LoggerLevel::INFO, $message, $file, $function, $line);
    }

    /**
     * Logs a debug message.
     * 
     * @param string $message 	log message, defaults to an empty string
     * @param string $file		file where the logger is called from, defaults to an empty string
     * @param string $function	function where the logger is called from, defaults to an empty string
     * @param string $line		line where the logger is called from, defaults to an empty string
     * @return void
     */
    public static function logDebug($message, $file='', $function='', $line='')
    {
        return Arakoon_Client_Logger::log(Arakoon_Client_LoggerLevel::DEBUG, $message, $file, $function, $line);
    }

    /**
     * Logs a warning message.
     * 
     * @param string $message 	log message, defaults to an empty string
     * @param string $file		file where the logger is called from, defaults to an empty string
     * @param string $function	function where the logger is called from, defaults to an empty string
     * @param string $line		line where the logger is called from, defaults to an empty string
     * @return void
     */
    public static function logWarning($message, $file='', $function='', $line='')
    {
        return Arakoon_Client_Logger::log(Arakoon_Client_LoggerLevel::WARNING, $message, $file, $function, $line);
    }

    /**
     * Logs a error message.
     * 
     * @param string $message 	log message, defaults to an empty string
     * @param string $file		file where the logger is called from, defaults to an empty string
     * @param string $function	function where the logger is called from, defaults to an empty string
     * @param string $line		line where the logger is called from, defaults to an empty string
     * @return void
     */
    public static function logError($message, $file='', $function='', $line='')
    {
        return Arakoon_Client_Logger::log(Arakoon_Client_LoggerLevel::ERROR, $message, $file, $function, $line);
    }

    /**
     * Logs a fatal message.
     * 
     * @param string $message 	log message, defaults to an empty string
     * @param string $file		file where the logger is called from, defaults to an empty string
     * @param string $function	function where the logger is called from, defaults to an empty string
     * @param string $line		line where the logger is called from, defaults to an empty string
     * @return void
     */
    public static function logFatal($message, $file='', $function='', $line='')
    {
      	return Arakoon_Client_Logger::log(Arakoon_Client_LoggerLevel::FATAL, $message, $file, $function, $line);
    }
	
    /**
     * Logs a message.
     * 
     * @param string $level 	log level
     * @param string $message 	log message, defaults to an empty string
     * @param string $file		file where the logger is called from, defaults to an empty string
     * @param string $function	function where the logger is called from, defaults to an empty string
     * @param string $line		line where the logger is called from, defaults to an empty string
     * @return void
     */
    private static function log($level, $message, $file='', $function='', $line='')
    {
    	$logger = self::getInstance();
    	
    	if (empty($logger->_logFileDestination))
        {
        	return;		
        }
            	
		$time = date(self::LOG_LINE_DATE_FORMAT);
		$name = Arakoon_Client_LoggerLevel::getName($level);
		$fileBase = (!empty($file)) ? basename($file) : '';
		
		$line = "$time | $name [$fileBase] | $function | $line] $message \n";
    	
        if ($logger->_level <= $level && $logger->_enabled)
        {
        	try
        	{
        		error_log($line, 3 , $logger->_logFileDestination);
        	}
        	catch (Exception $exception)
        	{
        		error_log('Arakoon client logger error', 0);
        	}
        }
 
    }      
}
?>