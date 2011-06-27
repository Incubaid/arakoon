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

require_once'arakoon_test_environment.php';

/**
 * ArakoonDefaultTestCase class
 */
abstract class ArakoonDefaultTestCase extends TestCase
{	
	private $_data;
	
    public function __construct($name, $description, $id)
    {
        $testEnvironment = ArakoonTestEnvironment::getInstance();
        
        $this->_data = array();
        $this->_data['arakoonClient'] = $testEnvironment->getClient();
		$this->_data['arakoonClientConfig'] = $testEnvironment->getClientConfig();
		
		parent::__construct($name, $description, $id);
    }

    public function SetUp()
    {
    	
    }
	
	public function Run()
    {
    	
    }
    
  	public function TearDown()
  	{
  		
  	}
  	
	public function &__get($name)
	{
        if (!array_key_exists($name, $this->_data))
        {
        	$this->_data[$name] = $name .'_'. $this->getGuid();
        }
        
        return $this->_data[$name];
    }
    
	public function __set($name, $value)
	{
        $this->_data[$name] = $value;
    }
    
	public function getGuid()
	{
	    if (function_exists('com_create_guid') === true)
	    {
	        return trim(com_create_guid(),'{}');
	    }
	
	    return sprintf('%04X%04X-%04X-%04X-%04X-%04X%04X%04X', mt_rand(0, 65535), mt_rand(0, 65535), mt_rand(0, 65535), mt_rand(16384, 20479), mt_rand(32768, 49151), mt_rand(0, 65535), mt_rand(0, 65535), mt_rand(0, 65535));
	}
}

/**
 * SetTestCase class
 */
class SetTestCase extends ArakoonDefaultTestCase
{
    public function __construct()
    {
        parent::__construct('set test','sets a key-value pair and checks if it exists','set');
    }

    public function Run()
    {         	
    	$this->arakoonClient->set($this->key, $this->value);
    	    	
    	$existsResult = $this->arakoonClient->exists($this->key);
        $this->AssertEquals($existsResult, 1,'Set test failed because the key doesn\'t exist!');
    }        
}

/**
 * SetMaxKeySizeTestCase class
 */
class SetMaxKeySizeTestCase extends ArakoonDefaultTestCase
{
	const MAX_KEY_BYTE_SIZE = 8388608; // 8 * 1024 * 1024 = 8mb
	
    public function __construct()
    {
        parent::__construct('set max key size test','sets a key-value pair using a key with the max key size (8mb)','set-max-key-size');
    }
    
    public function Setup()
    {    	
    	$this->key = str_repeat('0', self::MAX_KEY_BYTE_SIZE);  	
    }

    public function Run()
    {   
    	$this->arakoonClient->set($this->key, $this->value);    	
    	
    	$existsResult = $this->arakoonClient->exists($this->key);    	
    	$this->AssertEquals($existsResult, 1,'Set test failed because the key doesn\'t exists while it should!');
    }        
}

/**
 * SetMaxKeySizeExceededTestCase class
 */
class SetMaxKeySizeExceededTestCase extends ArakoonDefaultTestCase
{
	const MAX_KEY_BYTE_SIZE = 8388608; // 8 * 1024 * 1024 = 8mb
	
    public function __construct()
    {
        parent::__construct('set max key size exceeded test','sets a key-value pair using a key that exceeds the max key size (8mb)','set-max-key-size-exceeded');
    }
    
    public function Setup()
    {    	
    	$this->key = str_repeat('0', self::MAX_KEY_BYTE_SIZE + 1);  	
    }

    public function Run()
    {    
    	$this->arakoonClient->set($this->key, $this->value);    
    		
    	$existsResult = $this->arakoonClient->exists($this->key);    	
    	$this->AssertEquals($existsResult, 0,'Set max key size exceeded test failed because the key exists while it shouldn\'t!');
    }        
}

/**
 * GetExistingKeyTestCase class
 */
class GetExistingKeyTestCase extends ArakoonDefaultTestCase
{	
    public function __construct()
    {
        parent::__construct('get existing key test','gets an existing value using an existing key','get-existing-key');
    }

    public function Setup()
    {
    	$this->arakoonClient->set($this->key, $this->value);
    }
    public function Run()
    {        
        $getResult = $this->arakoonClient->get($this->key);
        $this->AssertEquals($getResult, $this->value,'Get using existing key test failed because the returned value differs the expected value!');
    }        
}

/**
 * GetNonExistingKeyTestCase class
 */
class GetNonExistingKeyTestCase extends ArakoonDefaultTestCase
{	
    public function __construct()
    {
        parent::__construct('get non existing key test','gets a value using a non-existing key','get-non-existing-key');
    }

    public function Run()
    {        
    	try
    	{
        	$getResult = $this->arakoonClient->get($this->key);
        	$this->Fail('Get using non-existing key test failed because no exception was thrown!');
    	}
    	catch (Exception $exception)
    	{
    		$this->Pass('Get using non-existing key test succeeded because an exception was thrown.');
    	}
    }        
}

/**
 * DeleteExistingKeyTestCase class
 */
class DeleteExistingKeyTestCase extends ArakoonDefaultTestCase
{
    public function __construct()
    {
        parent::__construct('delete existing key test','deletes a key-value pair using an existing key','delete-existing-key');
    }

    public function SetUp()
    {    	
        $this->arakoonClient->set($this->key, $this->value);
    }

    public function Run()
    {
        $this->arakoonClient->delete($this->key);
        
        $keyExists = $this->arakoonClient->exists($this->key);
        $this->AssertEquals($keyExists, 0,'Delete using existing key test failed because the key still exists!');
    }
}

/**
 * DeleteNonExistingKeyTestCase class
 */
class DeleteNonExistingKeyTestCase extends ArakoonDefaultTestCase
{
    public function __construct()
    {
        parent::__construct('delete non existing key test','deletes a key-value pair using a non-existing key','delete-non-existing-key');
    }

    public function Run()
    {
    	try
    	{
        	$this->arakoonClient->delete($this->key);
        	$this->Fail('Delete using non-existing key test failed because no exception was thrown!');
    	}
    	catch (Exception $exception)
    	{
    		$this->Pass('Delete using non-existing key test succeeded because an exception was thrown.');
    	}
    }
}

/**
 * SequenceValidTestCase class
 */
class SequenceValidTestCase extends ArakoonDefaultTestCase
{	
	public function __construct()
    {
        parent::__construct('sequence valid test','executes a sequence containing valid updates (set - delete)','sequence-valid');
    }

	public function SetUp()
    {
		$this->arakoonClient->set($this->key1, $this->value1);		
    }
    
    public function Run()
    {
    	$sequence = new Sequence();
    	$sequence->addDelete($this->key1);                
        $sequence->addSet($this->key2, $this->value2);
                
        $this->arakoonClient->sequence($sequence);        
        
        $exists1Result = $this->arakoonClient->exists($this->key1);        
        $this->AssertEquals($exists1Result, 0,'Sequence valid updates test failed because the deleted key-value pair still exists!');
        
        $exists2Result = $this->arakoonClient->exists($this->key2);
        $this->AssertEquals($exists2Result, 1,'Sequence valid updates test failed because the previously set key-value pair doesnt\'t exist!');
    }    
}

/**
 * SequenceInvalidTestCase class
 */
class SequenceInvalidTestCase extends ArakoonDefaultTestCase
{	
	public function __construct()
    {
        parent::__construct('sequence invalid test','executes a sequence containing invalid updates (set - delete)','sequence-invalid');
    }
    
    public function Run()
    {
    	$sequence = new Sequence();                
        $sequence->addDelete($this->key1);
    	$sequence->addSet($this->key2, $this->value2);  
                    
    	try
    	{
        	$this->arakoonClient->sequence($sequence);	
        	$this->Fail('Sequence invalid updates test failed because no exception was thrown!');
    	}
    	catch (Exception $exception)
    	{
    		$this->Pass('Sequence invalid updates test succeeded because an exception was thrown!');
    	}
    }    
}

/**
 * ExistsExistingKeyTestCase class
 */
class ExistsExistingKeyTestCase extends ArakoonDefaultTestCase
{	
	public function __construct()
    {
        parent::__construct('exists existing key test','determines if a existing key exists','exist-existing-key');
    }

	public function SetUp()
    {
		$this->arakoonClient->set($this->key, $this->value);
    }
    
    public function Run()
    {
        $keyExists = $this->arakoonClient->exists($this->key);
        $this->AssertEquals($keyExists, 1,'Exists on existing key test failed because the key doesn\'t exists while it should!');
    }    
}

/**
 * ExistsNonExistingKeyTestCase class
 */
class ExistsNonExistingKeyTestCase extends ArakoonDefaultTestCase
{	
	public function __construct()
    {
        parent::__construct('exists non existing key test','determines if a non-existing key exists','exists-non-existing-key');
    }
    
    public function Run()
    {
        $keyExists = $this->arakoonClient->exists($this->key);
        $this->AssertEquals($keyExists, 0,'Exists on non existing key test failed because the key exists while it shouldn\'t!');
    }    
}

/**
 * RangeExistingKeysTestCase class
 */
class RangeExistingKeysTestCase extends ArakoonDefaultTestCase
{
	const MAX_RANGE_ELEMENTS = 5;
	
	public function __construct()
    {
        parent::__construct('range existing keys test','gets a range of keys using existing keys','range-existing-keys');
    }

    public function SetUp()
    {
    	$this->keys = array();
    	$sequence = new Sequence();                

    	foreach(range('a','z') as $letter)
    	{
    		$key = $this->prefix .'_'. $letter .'_key';
        	$value = $this->prefix .'_'. $letter .'_value';
        	
        	array_push($this->keys, $key);
        	$sequence->addSet($key, $value);
    	}

    	$this->arakoonClient->sequence($sequence);        
    }

    public function Run()
    {
    	$keyCount = count($this->keys);
    	$errorMsg ='Range existing keys test failed!';
    	
    	$range1Result = $this->arakoonClient->range($this->keys[0], TRUE, $this->keys[$keyCount - 1], TRUE);        
        $this->AssertEquals(count($range1Result), $keyCount, $errorMsg);
        
        $range2Result = $this->arakoonClient->range($this->keys[0], TRUE, $this->keys[$keyCount - 1], FALSE);        
        $this->AssertEquals(count($range2Result), $keyCount - 1, $errorMsg);
        
        $range3Result = $this->arakoonClient->range($this->keys[0], FALSE, $this->keys[$keyCount - 1], TRUE);        
        $this->AssertEquals(count($range3Result), $keyCount - 1, $errorMsg);
        
        $range4Result = $this->arakoonClient->range($this->keys[0], FALSE, $this->keys[$keyCount - 1], FALSE);        
        $this->AssertEquals(count($range4Result), $keyCount - 2, $errorMsg);
        
        $range5Result = $this->arakoonClient->range($this->keys[0], TRUE, $this->keys[$keyCount - 1], TRUE, self::MAX_RANGE_ELEMENTS);        
        $this->AssertEquals(count($range5Result), self::MAX_RANGE_ELEMENTS, $errorMsg);        
    }  
}

/**
 * RangeNonExistingKeysTestCase class
 */
class RangeNonExistingKeysTestCase extends ArakoonDefaultTestCase
{
	const MAX_RANGE_ELEMENTS = 5;
	
	public function __construct()
    {
        parent::__construct('range non existing keys test','gets a range of keys using non-existing keys','range-non-existing-keys');
    }

    public function SetUp()
    {
    	$this->keys = array();               

    	foreach(range('a','z') as $letter)
    	{
    		$key = $this->prefix .'_'. $letter .'_key';
        	
        	array_push($this->keys, $key);
    	}       
    }

    public function Run()
    {
    	$keyCount = count($this->keys);
    	$errorMsg ='Range non existing keys test failed!';
    	
    	$range1Result = $this->arakoonClient->range($this->keys[0], TRUE, $this->keys[$keyCount - 1], TRUE);        
        $this->AssertEquals(count($range1Result), 0, $errorMsg);
        
        $range2Result = $this->arakoonClient->range($this->keys[0], TRUE, $this->keys[$keyCount - 1], FALSE);        
        $this->AssertEquals(count($range2Result), 0, $errorMsg);
        
        $range3Result = $this->arakoonClient->range($this->keys[0], FALSE, $this->keys[$keyCount - 1], TRUE);        
        $this->AssertEquals(count($range3Result), 0, $errorMsg);
        
        $range4Result = $this->arakoonClient->range($this->keys[0], FALSE, $this->keys[$keyCount - 1], FALSE);        
        $this->AssertEquals(count($range4Result), 0, $errorMsg);
        
        $range5Result = $this->arakoonClient->range($this->keys[0], TRUE, $this->keys[$keyCount - 1], TRUE, self::MAX_RANGE_ELEMENTS);        
        $this->AssertEquals(count($range5Result), 0, $errorMsg);        
    }  
}

/**
 * RangeEntriesExistingKeysTestCase class
 */
class RangeEntriesExistingKeysTestCase extends ArakoonDefaultTestCase
{
	const MAX_RANGE_ELEMENTS = 5;
	
	public function __construct()
    {
        parent::__construct('range entries existing keys test','gets a range of key-value pairs using existing keys','range-entries-existing-keys');
    }

    public function SetUp()
    {
    	$this->keys = array();
    	$sequence = new Sequence();                

    	foreach(range('a','z') as $letter)
    	{
    		$key = $this->prefix .'_'. $letter .'_key';
        	$value = $this->prefix .'_'. $letter .'_value';
        	
        	array_push($this->keys, $key);
        	$sequence->addSet($key, $value);
    	}

    	$this->arakoonClient->sequence($sequence);        
    }

    public function Run()
    {
    	$keyCount = count($this->keys);
    	$errorMsg ='Range entries existing keys test failed!';
    	
    	$range1Result = $this->arakoonClient->range($this->keys[0], TRUE, $this->keys[$keyCount - 1], TRUE);        
        $this->AssertEquals(count($range1Result), $keyCount, $errorMsg);
        
        $range2Result = $this->arakoonClient->range($this->keys[0], TRUE, $this->keys[$keyCount - 1], FALSE);        
        $this->AssertEquals(count($range2Result), $keyCount - 1, $errorMsg);
        
        $range3Result = $this->arakoonClient->range($this->keys[0], FALSE, $this->keys[$keyCount - 1], TRUE);        
        $this->AssertEquals(count($range3Result), $keyCount - 1, $errorMsg);
        
        $range4Result = $this->arakoonClient->range($this->keys[0], FALSE, $this->keys[$keyCount - 1], FALSE);        
        $this->AssertEquals(count($range4Result), $keyCount - 2, $errorMsg);
        
        $range5Result = $this->arakoonClient->range($this->keys[0], TRUE, $this->keys[$keyCount - 1], TRUE, self::MAX_RANGE_ELEMENTS);        
        $this->AssertEquals(count($range5Result), self::MAX_RANGE_ELEMENTS, $errorMsg);        
    }  
}

/**
 * RangeEntriesNonExistingKeysTestCase class
 */
class RangeEntriesNonExistingKeysTestCase extends ArakoonDefaultTestCase
{
	const MAX_RANGE_ELEMENTS = 5;
	
	public function __construct()
    {
        parent::__construct('range entries non existing keys test','gets a range of key-value pairs using non-existing keys','range-entries-non-existing-keys');
    }

    public function SetUp()
    {
    	$this->keys = array();               

    	foreach(range('a','z') as $letter)
    	{
    		$key = $this->prefix .'_'. $letter .'_key';
        	
        	array_push($this->keys, $key);
    	} 
    }

    public function Run()
    {
    	$keyCount = count($this->keys);
    	$errorMsg ='Range entries non existing keys test failed!';
    	
    	$range1Result = $this->arakoonClient->range($this->keys[0], TRUE, $this->keys[$keyCount - 1], TRUE);        
        $this->AssertEquals(count($range1Result), 0, $errorMsg);
        
        $range2Result = $this->arakoonClient->range($this->keys[0], TRUE, $this->keys[$keyCount - 1], FALSE);        
        $this->AssertEquals(count($range2Result), 0, $errorMsg);
        
        $range3Result = $this->arakoonClient->range($this->keys[0], FALSE, $this->keys[$keyCount - 1], TRUE);        
        $this->AssertEquals(count($range3Result), 0, $errorMsg);
        
        $range4Result = $this->arakoonClient->range($this->keys[0], FALSE, $this->keys[$keyCount - 1], FALSE);        
        $this->AssertEquals(count($range4Result), 0, $errorMsg);
        
        $range5Result = $this->arakoonClient->range($this->keys[0], TRUE, $this->keys[$keyCount - 1], TRUE, self::MAX_RANGE_ELEMENTS);        
        $this->AssertEquals(count($range5Result), 0, $errorMsg);        
    }  
}

/**
 * TestAndSetExistingKeyTestCase class
 */
class TestAndSetExistingKeyTestCase extends ArakoonDefaultTestCase
{
    public function __construct()
    {
        parent::__construct('test and set existing key test','tests a key-value pair using an existing key','test-and-set-existing-key');
    }

    public function SetUp()
    {
        $this->arakoonClient->set($this->key1, $this->value1);
    }

    public function Run()
    {
        $result = $this->arakoonClient->testAndSet($this->key1, $this->value1, $this->value2);        
        $this->AssertEquals($result, $this->value1,'Test and set test failed because the result differs the expect result(previous value)!');
    }
}

/**
 * TestAndSetNonExistingKeyTestCase class
 */
class TestAndSetNonExistingKeyTestCase extends ArakoonDefaultTestCase
{
    public function __construct()
    {
        parent::__construct('test and set non existing key test','tests a key-value pair using an non-existing key','test-and-set-non-existing-key');
    }

    public function Run()
    {
        $result = $this->arakoonClient->testAndSet($this->key1, $this->value1, $this->value2);        
        $this->AssertEquals($result, NULL,'Test and set test failed because the result differs the expect result(none)!');
    }
}

/**
 * MultiGetExistingKeysTestCase class
 */
class MultiGetExistingKeysTestCase extends ArakoonDefaultTestCase
{
	const KEY_VALUE_PAIR_COUNT = 5;
	
    public function __construct()
    {
        parent::__construct('multi get existing keys test','gets a collections of values using existing keys','multi-get-existing-keys');
    }

    public function SetUp()
    {
        $this->keys = array();
    	$sequence = new Sequence();                
           
        for ($i = 0; $i < self::KEY_VALUE_PAIR_COUNT; $i++)
        {
        	$key = "key$i";
        	$value = "value$i";
        	$this->keys[] = $this->$key;
        	$sequence->addSet($this->$key, $this->$value);
        }
        
        $this->arakoonClient->sequence($sequence);      
    }

    public function Run()
    {
    	// check if result is in right order
        $multiGetResult = $this->arakoonClient->multiGet($this->keys);
        $this->AssertEquals(count($multiGetResult), self::KEY_VALUE_PAIR_COUNT,'Multi get existing keys test failed because the result count differs the expected result count('. self::KEY_VALUE_PAIR_COUNT .')!');
    }
}

/**
 * MultiGetNonExistingKeysTestCase class
 */
class MultiGetNonExistingKeysTestCase extends ArakoonDefaultTestCase
{
	const KEY_VALUE_PAIR_COUNT = 5;
	
    public function __construct()
    {
        parent::__construct('multi get non existing keys test','gets a collection of values using non-existing keys','multi-get-non-existing-keys');
    }

    public function SetUp()
    {
        $this->keys = array();
    	$sequence = new Sequence();                
           
        for ($i = 0; $i < self::KEY_VALUE_PAIR_COUNT; $i++)
        {
        	$key = "key$i";
        	$this->keys[] = $this->$key;
        }   
    }

    public function Run()
    {
    	try
    	{
        	$multiGetResult = $this->arakoonClient->multiGet($this->keys);
        	$this->Fail('Multi get using non-existing key test failed because no exception was thrown!');
    	}
    	catch (Exception $exception)
    	{
    		$this->Pass('Multi get using non-existing key test succeeded because an exception was thrown.');    		
    	}
    }
}

/**
 * PrefixValidTestCase class
 */
class PrefixValidTestCase extends ArakoonDefaultTestCase
{
	const KEY_VALUE_PAIR_COUNT = 5;
	
    public function __construct()
    {
        parent::__construct('valid prefix test','gets a collection of keys beginning with a valid prefix','valid-prefix');
    }

    public function SetUp()
    {
    	$sequence = new Sequence();                
           
        for ($i = 0; $i < self::KEY_VALUE_PAIR_COUNT; $i++)
        {
        	$key = $this->prefix . "_key_$i";
        	$sequence->addSet($key,'value');
        }
        
        $this->arakoonClient->sequence($sequence);
    }

    public function Run()
    {        
        $prefixResult = $this->arakoonClient->prefix($this->prefix);
        $this->AssertEquals(count($prefixResult) , self::KEY_VALUE_PAIR_COUNT,'Valid prefix test failed because the result count differs the expected result count('. self::KEY_VALUE_PAIR_COUNT .')!');
    }
}

/**
 * PrefixInvalidTestCase class
 */
class PrefixInvalidTestCase extends ArakoonDefaultTestCase
{
    public function __construct()
    {
        parent::__construct('invalid prefix test','gets a collection of keys beginning with an invalid prefix','invalid-prefix');
    }

    public function Run()
    {        
        $prefixResult = $this->arakoonClient->prefix($this->prefix);
        $this->AssertEquals(count($prefixResult) , 0,'Invalid prefix test failed because the result count differs the expected result count(0)!');
    }
}

/**
 * ExpectProgressPossibleTestCase class
 */
class ExpectProgressPossibleTestCase extends ArakoonDefaultTestCase
{
    public function __construct()
    {
        parent::__construct('expect progress possible test','determines if progress is possible','expect-progress-possible');
    }

    public function Run()
    {
        // TODO EXPAND TEST
        $result = $this->arakoonClient->expectProgressPossible();
        $this->AssertEquals($result, 1,'Expect progress possible failed because the result differs the expected result(1)!');
    }
}

/**
 * HelloTestCase class
 */
class HelloTestCase extends ArakoonDefaultTestCase
{
    public function __construct()
    {
        parent::__construct('hello test','identifies client to the server','hello');
    }
    
    public function Run()
    {
        $helloResult = $this->arakoonClient->hello("clientId", $this->arakoonClientConfig->getClusterId());
        $this->AssertNotEquals(strlen($helloResult), 0,'Hello test failed because no version string was returned!');
    }
}

/**
 * WhoMasterTestCase class
 */
class WhoMasterTestCase extends ArakoonDefaultTestCase
{
    public function __construct()
    {
        parent::__construct('who master test','requests the master node id','who-master');
    }
    
    public function Run()
    {
        $master = $this->arakoonClient->whoMaster();
        $this->AssertNotEquals(strlen($master), 0,'Who master test failed because no master node id was returned!');
    }
}

?>