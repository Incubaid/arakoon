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

class Logging
{

    const TRACE = 0;
    const DEBUG = 1;
    const INFO  = 2;
    const WARN  = 3;
    const ERROR = 4;
    const FATAL = 5;
    const OFF   = 6;


    const LOG_OK = 0;
    const LOG_ERR = 1;
    const LOG_ERR_OPEN_FAILED = 2;

    static private $logErr = Logging::LOG_ERR;
    static private $dateFormat  = "Y-m-d G:i:s";
    static private $logFile;
    static private $priority = Logging::INFO;

    static function setup($filePath, $priority, $clean=false)
    {
        if ($priority == Logging::OFF) {
            return;
        }

        self::$logFile = $filePath;
        self::$priority = $priority;

        if (file_exists(self::$logFile))
        {
            if (!is_writable(self::$logFile))
            {
                return Logging::LOG_ERR_OPEN_FAILED;
            }
            if ($clean){
                unlink($filePath);
            }
        }

        $fileHandle = fopen($filePath , "a");
        if ($fileHandle){
            fclose($fileHandle);
        }
        
        return self::$logErr = Logging::LOG_OK;        
    }

    static function trace($log, $FILE='', $FUNCTION='', $LINE='')
    {
        $fileBase = ($FILE != "")?basename($FILE):"";
        return Logging::writeLog("[$fileBase| $FUNCTION | $LINE] $log" , Logging::TRACE);
    }

    
    static function info($log, $FILE='', $FUNCTION='', $LINE='')
    {
        $fileBase = ($FILE != "")?basename($FILE):"";
        return Logging::writeLog("[$fileBase| $FUNCTION | $LINE] $log" , Logging::INFO);
    }

    static function debug($log, $FILE='', $FUNCTION='', $LINE='')
    {
        $fileBase = ($FILE != "")?basename($FILE):"";
        return Logging::writeLog("[$fileBase | $FUNCTION | $LINE] $log" , Logging::DEBUG);
    }

    static function warning($log, $FILE='', $FUNCTION='', $LINE='')
    {
        $fileBase = ($FILE != "")?basename($FILE):"";        
        return Logging::writeLog("[$fileBase | $FUNCTION | $LINE] $log" , Logging::WARN);
    }

    static function error($log, $FILE='', $FUNCTION='', $LINE='')
    {
        $fileBase = ($FILE != "")?basename($FILE):"";
        return Logging::writeLog("[$fileBase | $FUNCTION | $LINE] $log" , Logging::ERROR);
    }

    static function fatal($log, $FILE='', $FUNCTION='', $LINE='')
    {
        $fileBase = ($FILE != "")?basename($FILE):"";
        return Logging::writeLog("[$fileBase | $FUNCTION | $LINE] $log" , Logging::FATAL);
    }

    static private function writeLog($line, $priority)
    {
        if (self::$priority <= $priority)
        {
            $status = Logging::getTime($priority);
            if ( self::$logErr == Logging::LOG_OK && self::$priority != Logging::OFF )
            {
                if(error_log("$status $line \n", 3 , self::$logFile) === false){
                    return Logging::LOG_ERR;
                }
            }
        }
        return Logging::LOG_OK;
    }

    static private function getTime($level)
    {
        $time = date(self::$dateFormat);
        
        switch($level)
        {
            case Logging::INFO:
                return "$time | INFO | ";
            case Logging::WARN:
                return "$time | WARN | ";
            case Logging::DEBUG:
                return "$time | DEBUG | ";
            case Logging::ERROR:
                return "$time | ERROR | ";
            case Logging::FATAL:
                return "$time | FATAL | ";
            case Logging::TRACE:
                return "$time | TRACE | ";                
            default:
                return "$time | LOG | ";
        }
    }
}

?>