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

require_once '../Arakoon/Client/Config.php';
require_once 'php_unit_test_framework/php_unit_test.php';
require_once 'ArakoonTestEnvironment.php';

/**
 * ArakoonDefaultTestCase
 */
abstract class ArakoonDefaultTestCase extends TestCase
{
	private $_data;

	public function __construct($name, $description, $id)
	{
		$testEnvironment = ArakoonTestEnvironment::getInstance();
		$arakoonClient = $testEnvironment->getClient();
		$this->_data = array();
		$this->_data['arakoonClient'] = $arakoonClient;
		$this->_data['arakoonClientConfig'] = $arakoonClient->getConfig();


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
			$this->_data[$name] = $name .'_'. $this->createGuid();
		}
		return $this->_data[$name];
	}

	public function __set($name, $value)
	{
		$this->_data[$name] = $value;
	}

	public function createGuid()
	{
		if (function_exists('com_create_guid') === true)
		{
			return trim(com_create_guid(), '{}');
		}
		return sprintf('%04X%04X-%04X-%04X-%04X-%04X%04X%04X', mt_rand(0, 65535), mt_rand(0, 65535), mt_rand(0, 65535), mt_rand(16384, 20479), mt_rand(32768, 49151), mt_rand(0, 65535), mt_rand(0, 65535), mt_rand(0, 65535));
	}
}

/**
 * SetTestCase
 */
class SetTestCase extends ArakoonDefaultTestCase
{
	public function __construct()
	{
		parent::__construct('set test',
							'sets a key-value pair and checks if its key exists',
							'set');
	}

	public function Run()
	{
		$this->arakoonClient->set($this->key, $this->value);
		$existsResult = $this->arakoonClient->exists($this->key);
		$this->AssertEquals($existsResult, 1, 'key of previously set key-value pair doesn\'t exist');
	}
}

/**
 * SetNoneKeyTestCase
 */
class SetNoneKeyTestCase extends ArakoonDefaultTestCase
{
	public function __construct()
	{
		parent::__construct('set none key test',
							'sets a key-value pair using a none key and checks if an exception is thrown',
							'set-none-key');
	}

	public function Run()
	{				
		try
		{
			$this->arakoonClient->set(NULL, $this->value);
			$this->Fail('no exception was thrown when it should have');
		}
		catch (Exception $exception)
		{
			$this->Pass('exception was thrown');
		}		
	}
}

/**
 * SetNoneValueTestCase
 */
class SetNoneValueTestCase extends ArakoonDefaultTestCase
{
	public function __construct()
	{
		parent::__construct('set none value test',
							'sets a key-value pair using a none value and checks if an exception is thrown',
							'set-none-value');
	}

	public function Run()
	{
		try
		{
			$this->arakoonClient->set($this->key, NULL);
			$this->Fail('no exception was thrown when it should have');
		}
		catch (Exception $exception)
		{
			$this->Pass('exception was thrown');
		}
	}
}

/**
 * SetMaxKeySizeTestCase
 */
class SetMaxKeySizeTestCase extends ArakoonDefaultTestCase
{
	public function __construct()
	{
		parent::__construct('set max key size test',
							'sets a key-value pair using a key that has the max key size (8mb) and checks if its key exists',
							'set-max-key-size');
	}

	public function Setup()
	{
		$this->key = str_repeat('0', Arakoon_Client_Config::MAX_KEY_BYTE_SIZE);
	}

	public function Run()
	{
		$this->arakoonClient->set($this->key, $this->value);		 
		$existsResult = $this->arakoonClient->exists($this->key);
		$this->AssertEquals($existsResult, 1, 'key of previously set key-value pair doesn\'t exist');
	}
}

/**
 * SetMaxKeySizeExceededTestCase
 */
class SetMaxKeySizeExceededTestCase extends ArakoonDefaultTestCase
{
	public function __construct()
	{
		parent::__construct('set max key size exceeded test',
							'sets a key-value pair using a key that exceeds the max key size (8mb) and checks if its key exists',
							'set-max-key-size-exceeded');
	}

	public function Setup()
	{
		$this->key = str_repeat('0', Arakoon_Client_Config::MAX_KEY_BYTE_SIZE + 1);
	}

	public function Run()
	{
		try
		{
			$this->arakoonClient->set($this->key, $this->value);
			$this->Fail('no exception was thrown when it should have');
		}
		catch (Exception $exception)
		{
			$this->Pass('exception was thrown');
		}
	}
}

/**
 * GetExistingKeyTestCase
 */
class GetExistingKeyTestCase extends ArakoonDefaultTestCase
{
	public function __construct()
	{
		parent::__construct('get existing key test',
							'gets the value of an existing key-value pair using its key and checks if it\'s correct',
							'get-existing-key');
	}

	public function Setup()
	{
		$this->arakoonClient->set($this->key, $this->value);
	}
	public function Run()
	{
		$getResult = $this->arakoonClient->get($this->key);
		$this->AssertEquals($getResult, $this->value, 'result value differs expected value');
	}
}

/**
 * GetNonExistingKeyTestCase
 */
class GetNonExistingKeyTestCase extends ArakoonDefaultTestCase
{
	public function __construct()
	{
		parent::__construct('get non existing key test',
							'gets the value of a non-existing key-value pair using its key and checks if an exception is thrown',
							'get-non-existing-key');
	}

	public function Run()
	{
		try
		{
			$getResult = $this->arakoonClient->get($this->key);
			$this->Fail('no exception was thrown when it should have');
		}
		catch (Exception $exception)
		{
			$this->Pass('exception was thrown');
		}
	}
}

/**
 * GetNoneKeyTestCase
 */
class GetNoneKeyTestCase extends ArakoonDefaultTestCase
{
	public function __construct()
	{
		parent::__construct('get none key test',
							'gets the value of a non-existing key-value pair using a none key and checks if an exception is thrown',
							'get-none-key');
	}

	public function Run()
	{
		try
		{
			$getResult = $this->arakoonClient->get(NULL);
			$this->Fail('no exception was thrown when it should have');
		}
		catch (Exception $exception)
		{
			$this->Pass('exception was thrown');
		}
	}
}

/**
 * DeleteExistingKeyTestCase
 */
class DeleteExistingKeyTestCase extends ArakoonDefaultTestCase
{
	public function __construct()
	{
		parent::__construct('delete existing key test',
							'deletes an existing key-value pair using its key and checks if it still exists',
							'delete-existing-key');
	}

	public function SetUp()
	{
		$this->arakoonClient->set($this->key, $this->value);
	}

	public function Run()
	{
		$this->arakoonClient->delete($this->key);

		$keyExists = $this->arakoonClient->exists($this->key);
		$this->AssertEquals($keyExists, 0, 'key of previously set key-value pair still exists');
	}
}

/**
 * DeleteNonExistingKeyTestCase
 */
class DeleteNonExistingKeyTestCase extends ArakoonDefaultTestCase
{
	public function __construct()
	{
		parent::__construct('delete non existing key test',
							'deletes a non-existing key-value pair using a non-existing key and checks if an exception is thrown',
							'delete-non-existing-key');
	}

	public function Run()
	{
		try
		{
			$this->arakoonClient->delete($this->key);
			$this->Fail('no exception was thrown when it should have');
		}
		catch (Exception $exception)
		{
			$this->Pass('exception was thrown');
		}
	}
}

/**
 * DeleteNoneKeyTestCase
 */
class DeleteNoneKeyTestCase extends ArakoonDefaultTestCase
{
	public function __construct()
	{
		parent::__construct('delete none key test',
							'deletes a non-existing key-value pair using a none key and checks if an exception is thrown',
							'delete-none-key');
	}

	public function Run()
	{
		try
		{
			$this->arakoonClient->delete(NULL);
			$this->Fail('no exception was thrown when it should have');
		}
		catch (Exception $exception)
		{
			$this->Pass('exception was thrown');
		}
	}
}

/**
 * SequenceValidTestCase
 */
class SequenceValidTestCase extends ArakoonDefaultTestCase
{
	public function __construct()
	{
		parent::__construct('sequence valid test',
							'executes a sequence containing all possible valid operations and check if all operations are executed',
							'sequence-valid');
	}

	public function SetUp()
	{
		$this->arakoonClient->set($this->key2, $this->value2);
		$this->arakoonClient->set($this->key4, $this->value4);
	}

	public function Run()
	{
		$subSequence = new Arakoon_Client_Operation_Sequence();
		$subSequence->addSetOperation($this->key1, $this->value1);
		$subSequence->addDeleteOperation($this->key2);
		$subSequence->addAssertOperation($this->key1, $this->value1);	
		
		$sequence = new Arakoon_Client_Operation_Sequence();
		$sequence->addSetOperation($this->key3, $this->value3);
		$sequence->addDeleteOperation($this->key4);
		$sequence->addAssertOperation($this->key3, $this->value3);
		$sequence->addSequenceOperation($subSequence);
		
		$this->arakoonClient->sequence($sequence);

		$exists1Result = $this->arakoonClient->exists($this->key1);
		$this->AssertEquals($exists1Result, 1, 'key of previously set key-value pair doesnt\'t exist');

		$exists2Result = $this->arakoonClient->exists($this->key2);
		$this->AssertEquals($exists2Result, 0, 'key of deleted key-value pair still exists');

		$exists1Result = $this->arakoonClient->exists($this->key3);
		$this->AssertEquals($exists1Result, 1, 'key of previously set key-value pair doesnt\'t exist');

		$exists2Result = $this->arakoonClient->exists($this->key4);
		$this->AssertEquals($exists2Result, 0, 'key of deleted key-value pair still exists');	
	}
}

/**
 * SequenceInvalidSetTestCase
 */
class SequenceInvalidSetTestCase extends ArakoonDefaultTestCase
{
	public function __construct()
	{
		parent::__construct('sequence invalid set test',
							'executes a sequence containing an invalid set operation and checks an if exception is thrown',
							'sequence-invalid-set');
	}

	public function Run()
	{		
		$sequence = new Arakoon_Client_Operation_Sequence();
				
		try
		{			
			$sequence->addSetOperation($this->key1, NULL);
			$this->arakoonClient->sequence($sequence);
			$this->Fail('no exception was thrown when it should have');
		}
		catch (Exception $exception)
		{
			$this->Pass('exception was thrown');
		}
	}
}

/**
 * SequenceInvalidSetTestCase
 */
class SequenceInvalidDeleteTestCase extends ArakoonDefaultTestCase
{
	public function __construct()
	{
		parent::__construct('sequence invalid delete test',
							'executes a sequence containing an invalid delete operation and checks an if exception is thrown',
							'sequence-invalid-delete');
	}

	public function Run()
	{	
		$sequence = new Arakoon_Client_Operation_Sequence();
		
		try
		{
			$sequence->addDeleteOperation($this->key);
		
			$this->arakoonClient->sequence($sequence);
			$this->Fail('no exception was thrown when it should have');
		}
		catch (Exception $exception)
		{			
			$this->Pass('an exception was thrown');
		}
	}
}

/**
 * SequenceInvalidAssertTestCase
 */
class SequenceInvalidAssertTestCase extends ArakoonDefaultTestCase
{
	public function __construct()
	{
		parent::__construct('sequence invalid assert test',
							'executes a sequence containing an invalid assert operation and checks if an exception is thrown',
							'sequence-invalid-assert');
	}

	public function Run()
	{	
		$sequence = new Arakoon_Client_Operation_Sequence();
		
		try
		{
			$sequence->addAssertOperation($this->key, $this->value);
		
			$this->arakoonClient->sequence($sequence);
			$this->Fail('no exception was thrown when it should have');
		}
		catch (Exception $exception)
		{			
			$this->Pass('an exception was thrown');
		}
	}
}

/**
 * ExistsExistingKeyTestCase
 */
class ExistsExistingKeyTestCase extends ArakoonDefaultTestCase
{
	public function __construct()
	{
		parent::__construct('exists existing key test',
							'checks if a existing key exists',
							'exist-existing-key');
	}

	public function SetUp()
	{
		$this->arakoonClient->set($this->key, $this->value);
	}

	public function Run()
	{
		$keyExists = $this->arakoonClient->exists($this->key);
		$this->AssertEquals($keyExists, 1, 'key doesn\'t exists while it should');
	}
}

/**
 * ExistsNonExistingKeyTestCase
 */
class ExistsNonExistingKeyTestCase extends ArakoonDefaultTestCase
{
	public function __construct()
	{
		parent::__construct('exists non existing key test',
							'checks if a non-existing key exists',
							'exists-non-existing-key');
	}

	public function Run()
	{
		$keyExists = $this->arakoonClient->exists($this->key);
		$this->AssertEquals($keyExists, 0, 'key exists while it shouldn\'t');
	}
}

/**
 * ExistsNoneKeyTestCase
 */
class ExistsNoneKeyTestCase extends ArakoonDefaultTestCase
{
	public function __construct()
	{
		parent::__construct('exists none key test',
							'checks if a none key exists and checks if an exception is thrown',
							'exists-none-key');
	}

	public function Run()
	{
		try
		{
			$getResult = $this->arakoonClient->exists(NULL);
			$this->Fail('no exception was thrown when it should have');
		}
		catch (Exception $exception)
		{
			$this->Pass('exception was thrown');
		}
	}
}

/**
 * ArakoonRangeDefaultTestCase
 */
class ArakoonRangeDefaultTestCase extends ArakoonDefaultTestCase
{
	const MAX_RANGE_ELEMENTS = 5;
	const RANGE_BEGIN_INDEX = 5;
	const RANGE_END_INDEX = 15;
	
	public function SetUp()
	{
		// create key-value pairs
		$this->pairs = array();		
		foreach(range('a', 'z') as $letter)
		{
			array_push($this->pairs, array(
				'key' => $this->prefix .'_'. $letter .'_key',
				'value' => $this->prefix .'_'. $letter .'_value'
			));
		}
		
		// store a shuffled copy of the pairs 
		$this->shuffledPairs = $this->pairs;
		shuffle($this->shuffledPairs);

		// sequence the shuffled pairs
		$sequence = new Arakoon_Client_Operation_Sequence();		
		for($i = 0; $i < count($this->shuffledPairs); $i++)
		{
			$sequence->addSetOperation($this->shuffledPairs[$i]['key'], $this->shuffledPairs[$i]['value']);
		}		
		$this->arakoonClient->sequence($sequence);
	}
}

/**
 * RangeExistingKeysNoBordersTestCase
 */
class RangeExistingKeysNoBordersTestCase extends ArakoonRangeDefaultTestCase
{
	public function __construct()
	{
		parent::__construct('range existing keys no borders test',
							'gets a range of keys using existing keys without the borders and checks if the key count and order are correct ',
							'range-existing-keys-no-borders');
	}

	public function Run()
	{	 
		$beginKey = $this->pairs[self::RANGE_BEGIN_INDEX]['key'];
		$endKey = $this->pairs[self::RANGE_END_INDEX]['key'];

		$rangeResult = $this->arakoonClient->range($beginKey, FALSE, $endKey, FALSE);

		// check range count without max
		$count = count($rangeResult);
		$expectedCount = self::RANGE_END_INDEX - self::RANGE_BEGIN_INDEX - 1;
		$this->AssertEquals($count, $expectedCount, 'result count (' . $count . ') differs the expected result count ('. $expectedCount .')');
	
		// check range order
		for ($i = 0; $i < $count; $i++)
		{
			if ($rangeResult[$i] != $this->pairs[$i + self::RANGE_BEGIN_INDEX + 1]['key'])
			{
				$this->Fail('values not in right order');
				break;	
			}		
		}
	}
}

/**
 * RangeExistingKeysBeginBorderTestCase
 */
class RangeExistingKeysBeginBorderTestCase extends ArakoonRangeDefaultTestCase
{
	public function __construct()
	{
		parent::__construct('range existing keys begin border test',
							'gets a range of keys using existing keys including the begin border and checks if the key count and order are correct ',
							'range-existing-keys-begin-border');
	}

	public function Run()
	{	 
		$beginKey = $this->pairs[self::RANGE_BEGIN_INDEX]['key'];
		$endKey = $this->pairs[self::RANGE_END_INDEX]['key'];

		$rangeResult = $this->arakoonClient->range($beginKey, TRUE, $endKey, FALSE);

		// check range count without max
		$count = count($rangeResult);
		$expectedCount = self::RANGE_END_INDEX - (self::RANGE_BEGIN_INDEX - 1) - 1;
		$this->AssertEquals($count, $expectedCount, 'result count (' . $count . ') differs the expected result count ('. $expectedCount .')');
	
		// check range order
		for ($i = 0; $i < $count; $i++)
		{
			if ($rangeResult[$i] != $this->pairs[$i + self::RANGE_BEGIN_INDEX]['key'])
			{
				$this->Fail('values not in right order');
				break;	
			}		
		}
	}
}

/**
 * RangeExistingKeysEndBorderTestCase
 */
class RangeExistingKeysEndBorderTestCase extends ArakoonRangeDefaultTestCase
{
	public function __construct()
	{
		parent::__construct('range existing keys end border test',
							'gets a range of keys using existing keys including the end border and checks if the key count and order are correct ',
							'range-existing-keys-end-border');
	}

	public function Run()
	{	 
		$beginKey = $this->pairs[self::RANGE_BEGIN_INDEX]['key'];
		$endKey = $this->pairs[self::RANGE_END_INDEX]['key'];

		$rangeResult = $this->arakoonClient->range($beginKey, FALSE, $endKey, TRUE);

		// check range count without max
		$count = count($rangeResult);
		$expectedCount = (self::RANGE_END_INDEX + 1) - self::RANGE_BEGIN_INDEX - 1;
		$this->AssertEquals($count, $expectedCount, 'result count (' . $count . ') differs the expected result count ('. $expectedCount .')');
	
		// check range order
		for ($i = 0; $i < $count; $i++)
		{
			if ($rangeResult[$i] != $this->pairs[$i + self::RANGE_BEGIN_INDEX + 1]['key'])
			{
				$this->Fail('values not in right order');
				break;	
			}		
		}
	}
}

/**
 * RangeExistingKeysBothBordersTestCase
 */
class RangeExistingKeysBothBordersTestCase extends ArakoonRangeDefaultTestCase
{
	public function __construct()
	{
		parent::__construct('range existing keys both borders test',
							'gets a range of keys using existing keys including both borders and checks if the key count and order are correct ',
							'range-existing-keys-both-borders');
	}

	public function Run()
	{	 
		$beginKey = $this->pairs[self::RANGE_BEGIN_INDEX]['key'];
		$endKey = $this->pairs[self::RANGE_END_INDEX]['key'];

		$rangeResult = $this->arakoonClient->range($beginKey, TRUE, $endKey, TRUE);

		// check range count without max
		$count = count($rangeResult);
		$expectedCount = (self::RANGE_END_INDEX + 1) - (self::RANGE_BEGIN_INDEX - 1) - 1;
		$this->AssertEquals($count, $expectedCount, 'result count (' . $count . ') differs the expected result count ('. $expectedCount .')');
	
		// check range order
		for ($i = 0; $i < $count; $i++)
		{
			if ($rangeResult[$i] != $this->pairs[$i + self::RANGE_BEGIN_INDEX]['key'])
			{
				$this->Fail('values not in right order');
				break;	
			}		
		}
	}
}

/**
 * RangeExistingKeysMaxTestCase
 */
class RangeExistingKeysMaxTestCase extends ArakoonRangeDefaultTestCase
{
	public function __construct()
	{
		parent::__construct('range existing keys max test',
							'gets a range of keys using existing keys limiting the result to a given maximum and checks if the key count and order are correct ',
							'range-existing-keys-max');
	}

	public function Run()
	{
		$beginKey = $this->pairs[self::RANGE_BEGIN_INDEX]['key'];
		$endKey = $this->pairs[self::RANGE_END_INDEX]['key'];
		
		$rangeResult = $this->arakoonClient->range($beginKey, FALSE, $endKey, FALSE, self::MAX_RANGE_ELEMENTS);
		$count = count($rangeResult);
		$this->AssertEquals($count, self::MAX_RANGE_ELEMENTS, 'result count (' . $count . ') differs the expected result count (' . self::MAX_RANGE_ELEMENTS .')');
	}
}

/**
 * RangeNonExistingKeysTestCase
 */
class RangeNonExistingKeysTestCase extends ArakoonDefaultTestCase
{
	public function __construct()
	{
		parent::__construct('range non existing keys test',
							'gets a range of keys using non-existing keys and checks if the key count is correct',
							'range-non-existing-keys');
	}

	public function Run()
	{
		$rangeResult = $this->arakoonClient->range($this->beginKey, FALSE, $this->endKey, FALSE);
		$count = count($rangeResult);
		$expectedCount = 0;
		$this->AssertEquals($count, $expectedCount, 'result count (' . $count . ') differs the expected result count (' . $expectedCount . ')');
	}
}

/**
 * RangeNoneKeysTestCase
 */
class RangeNoneKeysTestCase extends ArakoonDefaultTestCase
{
	public function __construct()
	{
		parent::__construct('range none keys test',
							'gets a range of keys using none keys and checks if the key count is correct',
							'range-none-keys');
	}

	public function Run()
	{
		try
		{
			$rangeResult = $this->arakoonClient->range(NULL, FALSE, NULL, FALSE);
			$this->Fail('no exception was thrown when it should have');
		}
		catch (Exception $exception)
		{
			$this->Pass('exception was thrown');
		}
	}
}

/**
 * RangeEntriesExistingKeysNoBordersTestCase
 */
class RangeEntriesExistingKeysNoBordersTestCase extends ArakoonRangeDefaultTestCase
{
	public function __construct()
	{
		parent::__construct('range entries existing keys no borders test',
							'gets a range of key-value pairs using existing keys without the borders and checks if the key count and order are correct',
							'range-entries-existing-keys-no-borders');
	}

	public function Run()
	{
		$beginKey = $this->pairs[self::RANGE_BEGIN_INDEX]['key'];
		$endKey = $this->pairs[self::RANGE_END_INDEX]['key'];

		$rangeResult = $this->arakoonClient->rangeEntries($beginKey, FALSE, $endKey, FALSE);
		$count = count($rangeResult);
		$expectedCount = self::RANGE_END_INDEX - self::RANGE_BEGIN_INDEX - 1;
		$this->AssertEquals($count, $expectedCount, 'result count (' . $count . ') differs the expected result count ('. $expectedCount .')');
	
		// check range order
		for ($i = 0; $i < $count; $i++)
		{
			if (($rangeResult[$i][0] != $this->pairs[$i + self::RANGE_BEGIN_INDEX + 1]['key']) || ($rangeResult[$i][1] != $this->pairs[$i + self::RANGE_BEGIN_INDEX + 1]['value']))
			{
				$this->Fail('key-values not in right order');
				break;	
			}		
		}
	}
}

/**
 * RangeEntriesExistingKeysBeginBorderTestCase
 */
class RangeEntriesExistingKeysBeginBorderTestCase extends ArakoonRangeDefaultTestCase
{
	public function __construct()
	{
		parent::__construct('range entries existing keys begin border test',
							'gets a range of key-value pairs using existing keys including the begin border and checks if the key count and order are correct',
							'range-entries-existing-keys-begin-border');
	}

	public function Run()
	{
		$beginKey = $this->pairs[self::RANGE_BEGIN_INDEX]['key'];
		$endKey = $this->pairs[self::RANGE_END_INDEX]['key'];

		$rangeResult = $this->arakoonClient->rangeEntries($beginKey, TRUE, $endKey, FALSE);
		$count = count($rangeResult);
		$expectedCount = self::RANGE_END_INDEX - (self::RANGE_BEGIN_INDEX - 1) - 1;
		$this->AssertEquals($count, $expectedCount, 'result count (' . $count . ') differs the expected result count ('. $expectedCount .')');
	
		// check range order
		for ($i = 0; $i < $count; $i++)
		{
			if (($rangeResult[$i][0] != $this->pairs[$i + self::RANGE_BEGIN_INDEX]['key']) || ($rangeResult[$i][1] != $this->pairs[$i + self::RANGE_BEGIN_INDEX]['value']))
			{
				$this->Fail('key-values not in right order');
				break;	
			}		
		}
	}
}

/**
 * RangeEntriesExistingKeysEndBorderTestCase
 */
class RangeEntriesExistingKeysEndBorderTestCase extends ArakoonRangeDefaultTestCase
{
	public function __construct()
	{
		parent::__construct('range entries existing keys end border test',
							'gets a range of key-value pairs using existing keys including the end border and checks if the key count and order are correct',
							'range-entries-existing-keys-end-border');
	}

	public function Run()
	{
		$beginKey = $this->pairs[self::RANGE_BEGIN_INDEX]['key'];
		$endKey = $this->pairs[self::RANGE_END_INDEX]['key'];

		$rangeResult = $this->arakoonClient->rangeEntries($beginKey, FALSE, $endKey, TRUE);
		$count = count($rangeResult);
		$expectedCount = (self::RANGE_END_INDEX + 1) - self::RANGE_BEGIN_INDEX - 1;
		$this->AssertEquals($count, $expectedCount, 'result count (' . $count . ') differs the expected result count ('. $expectedCount .')');
	
		// check range order
		for ($i = 0; $i < $count; $i++)
		{
			if (($rangeResult[$i][0] != $this->pairs[$i + self::RANGE_BEGIN_INDEX + 1]['key']) || ($rangeResult[$i][1] != $this->pairs[$i + self::RANGE_BEGIN_INDEX + 1]['value']))
			{
				$this->Fail('key-values not in right order');
				break;	
			}		
		}
	}
}

/**
 * RangeEntriesExistingKeysBothBordersTestCase
 */
class RangeEntriesExistingKeysBothBordersTestCase extends ArakoonRangeDefaultTestCase
{
	public function __construct()
	{
		parent::__construct('range entries existing keys both borders test',
							'gets a range of key-value pairs using existing keys including both borders and checks if the key count and order are correct',
							'range-entries-existing-keys-both-borders');
	}

	public function Run()
	{
		$beginKey = $this->pairs[self::RANGE_BEGIN_INDEX]['key'];
		$endKey = $this->pairs[self::RANGE_END_INDEX]['key'];

		$rangeResult = $this->arakoonClient->rangeEntries($beginKey, TRUE, $endKey, TRUE);
		$count = count($rangeResult);
		$expectedCount = (self::RANGE_END_INDEX + 1) - (self::RANGE_BEGIN_INDEX - 1) - 1;
		$this->AssertEquals($count, $expectedCount, 'result count (' . $count . ') differs the expected result count ('. $expectedCount .')');
	
		// check range order
		for ($i = 0; $i < $count; $i++)
		{
			if (($rangeResult[$i][0] != $this->pairs[$i + self::RANGE_BEGIN_INDEX]['key']) || ($rangeResult[$i][1] != $this->pairs[$i + self::RANGE_BEGIN_INDEX]['value']))
			{
				$this->Fail('key-values not in right order');
				break;	
			}		
		}
	}
}

/**
 * RangeEntriesExistingKeysMaxTestCase
 */
class RangeEntriesExistingKeysMaxTestCase extends ArakoonRangeDefaultTestCase
{
	public function __construct()
	{
		parent::__construct('range entries existing keys max test',
							'gets a range of key-value pairs using existing keys limiting the result to a given maximum and checks if the key count and order are correct',
							'range-entries-existing-keys-max');
	}

	public function Run()
	{
		$beginKey = $this->pairs[self::RANGE_BEGIN_INDEX]['key'];
		$endKey = $this->pairs[self::RANGE_END_INDEX]['key'];
		
		$rangeResult = $this->arakoonClient->rangeEntries($beginKey, FALSE, $endKey, FALSE, self::MAX_RANGE_ELEMENTS);
		$count = count($rangeResult);
		$this->AssertEquals($count, self::MAX_RANGE_ELEMENTS, 'result count (' . $count . ') differs the expected result count (' . self::MAX_RANGE_ELEMENTS .')');
	}
}

/**
 * RangeEntriesNonExistingKeysTestCase
 */
class RangeEntriesNonExistingKeysTestCase extends ArakoonDefaultTestCase
{
	public function __construct()
	{
		parent::__construct('range entries non existing keys test',
							'gets a range of key-value pairs using non-existing keys and checks if the key count is correct',
							'range-entries-non-existing-keys');
	}
	
	public function Run()
	{
		$rangeResult = $this->arakoonClient->rangeEntries($this->beginKey, FALSE, $this->endKey, FALSE);
		$rangeCount = count($rangeResult);
		$expectedCount = 0;
		$this->AssertEquals($rangeCount, $expectedCount, 'result count differs the expected result count (' . $expectedCount . ')');
	}
}

/**
 * RangeEntriesNoneKeysTestCase
 */
class RangeEntriesNoneKeysTestCase extends ArakoonDefaultTestCase
{
	public function __construct()
	{
		parent::__construct('range entries none keys test',
							'gets a range of key-value pairs using none keys and checks if the key count is correct',
							'range-entries-none-keys');
	}
	
	public function Run()
	{
		try
		{
			$rangeResult = $this->arakoonClient->rangeEntries(NULL, FALSE, NULL, FALSE);
			$this->Fail('no exception was thrown when it should have');
		}
		catch (Exception $exception)
		{
			$this->Pass('exception was thrown');
		}
	}
}

/**
 * TestAndSetExistingKeyExpectedValueNewValueTestCase
 */
class TestAndSetExistingKeyExpectedValueNewValueTestCase extends ArakoonDefaultTestCase
{
	public function __construct()
	{
		parent::__construct('test and set existing key expected value new value test',
							'tests and sets an existing key-value pair using its key, an expected value and an new value and checks if the key-value pair is updated',
							'test-and-set-existing-key-expected-value-new-value');
	}

	public function SetUp()
	{
		$this->arakoonClient->set($this->key1, $this->value1);
	}

	public function Run()
	{
		$originalValue = $this->arakoonClient->testAndSet($this->key1, $this->value1, $this->value2);
		$updatedValue = $this->arakoonClient->get($this->key1);
		$this->AssertEquals($updatedValue, $this->value2, 'value not updated');
	}
}

/**
 * TestAndSetExistingKeyExpectedValueNoneValueTestCase
 */
class TestAndSetExistingKeyExpectedValueNoneValueTestCase extends ArakoonDefaultTestCase
{
	public function __construct()
	{
		parent::__construct('test and set existing key expected value none value test',
							'tests and sets an existing key-value pair using its key, an expected value and a none value and checks if the key value pair is removed',
							'test-and-set-existing-key-expected-value-none-value');
	}

	public function SetUp()
	{
		$this->arakoonClient->set($this->key1, $this->value1);
	}

	public function Run()
	{
		$originalValue = $this->arakoonClient->testAndSet($this->key1, $this->value1, NULL);
		$exists = $this->arakoonClient->exists($this->key1);
		$this->AssertEquals($exists, 0, 'key-value pair not removed');
	}
}

/**
 * TestAndSetExistingKeyUnexpectedValueTestCase
 */
class TestAndSetExistingKeyUnexpectedValueTestCase extends ArakoonDefaultTestCase
{
	public function __construct()
	{
		parent::__construct('test and set existing key unexpected value test',
							'tests and sets an existing key-value pair using its key and an unexpected value and check if the key-value pair isn\'t updated',
							'test-and-set-existing-key-unexpected-value');
	}

	public function SetUp()
	{
		$this->arakoonClient->set($this->key1, $this->value1);
	}

	public function Run()
	{
		$originalValue = $this->arakoonClient->testAndSet($this->key1, $this->value2, $this->value3);
		$updatedValue = $this->arakoonClient->get($this->key1);
		$this->AssertEquals($originalValue, $updatedValue,  'value updated when it shouldn\'t');
	}
}

/**
 * TestAndSetNonExistingKeyExpectedValueNewValueTestCase
 */
class TestAndSetNonExistingKeyExpectedValueNewValueTestCase extends ArakoonDefaultTestCase
{
	public function __construct()
	{
		parent::__construct('test and set non existing key expected value new value test',
							'tests and sets an non-existing key-value pair using a non-existing key, an expected value and a new value and checks if none is returned',
							'test-and-set-non-existing-key-expected-value-new-value');
	}

	public function Run()
	{
		$originalValue = $this->arakoonClient->testAndSet($this->key1, $this->value1, $this->value2);
		$this->AssertEquals($originalValue, NULL,  'original value (' . $originalValue . ') returned when none should be returned');
	}
}

/**
 * TestAndSetNonExistingKeyNoneValueNewValueTestCase
 */
class TestAndSetNonExistingKeyNoneValueNewValueTestCase extends ArakoonDefaultTestCase
{
	public function __construct()
	{
		parent::__construct('test and set non existing key none value new value test',
							'tests and sets an non-existing key-value pair using a non-existing key, a none value and a new value and check if the key-value pair is created',
							'test-and-set-non-existing-key-none-value-new-value');
	}

	public function Run()
	{
		$originalValue = $this->arakoonClient->testAndSet($this->key1, NULL, $this->value1);
		$exists = $this->arakoonClient->exists($this->key1);
		$this->AssertEquals($exists, 1, 'key-value pair doesn\'t exists while it should');
	}
}

/**
 * TestAndSetNoneKeyTestCase
 */
class TestAndSetNoneKeyTestCase extends ArakoonDefaultTestCase
{
	public function __construct()
	{
		parent::__construct('test and set none key test',
							'tests and sets using a none key and checks if an exception is thrown',
							'test-and-set-none-key');
	}

	public function Run()
	{		
		try
		{
			$result = $this->arakoonClient->testAndSet(NULL, $this->value1, $this->value2);
			$this->Fail('no exception was thrown while it should have');
		}
		catch (Exception $exception)
		{
			$this->Pass('an exception was thrown');
		}
	}
}

/**
 * MultiGetExistingKeysTestCase
 */
class MultiGetExistingKeysTestCase extends ArakoonDefaultTestCase
{
	const KEY_VALUE_PAIR_COUNT = 5;

	public function __construct()
	{
		parent::__construct('multi get existing keys test',
							'gets a list of values using multiple existing keys and checks if a list is returned containing all the expected values in the right order',
							'multi-get-existing-keys');
	}

	public function SetUp()
	{
		$this->keys = array();
		$this->values = array();
		$sequence = new Arakoon_Client_Operation_Sequence();		 
		
		for ($i = 0; $i < self::KEY_VALUE_PAIR_COUNT; $i++)
		{
			$key = $this->key . $i;
			$value = $this->value . $i;
			array_push($this->keys, $key);
			array_push($this->values, $value);
			$sequence->addSetOperation($key, $value);
		}
				
		$this->arakoonClient->sequence($sequence);
	}

	public function Run()
	{
		$multiGetResult = $this->arakoonClient->multiGet($this->keys);
		$this->AssertEquals(count($multiGetResult), self::KEY_VALUE_PAIR_COUNT, 'result count differs the expected result count ('. self::KEY_VALUE_PAIR_COUNT .')');
		
		for ($i = 0; $i < self::KEY_VALUE_PAIR_COUNT; $i++)
		{
			if ($multiGetResult[$i] != $this->values[$i])
			{
				$this->Fail('values not in right order');
				break;	
			}		
		}
	}
}

/**
 * MultiGetNonExistingKeysTestCase
 */
class MultiGetNonExistingKeysTestCase extends ArakoonDefaultTestCase
{
	const KEY_VALUE_PAIR_COUNT = 5;

	public function __construct()
	{
		parent::__construct('multi get non existing keys test',
							'gets a list of values using non-existing keys and checks if an exception is thrown',
							'multi-get-non-existing-keys');
	}

	public function SetUp()
	{
		$this->keys = array();
		$sequence = new Arakoon_Client_Operation_Sequence();		 
		for ($i = 0; $i < self::KEY_VALUE_PAIR_COUNT; $i++)
		{
			$key = 'key' . $i;
			$this->keys[] = $this->$key;
		}
	}

	public function Run()
	{
		try
		{
			$multiGetResult = $this->arakoonClient->multiGet($this->keys);
			$this->Fail('no exception was thrown while it should have');
		}
		catch (Exception $exception)
		{
			$this->Pass('an exception was thrown');
		}
	}
}

/**
 * MultiGetNoneKeysTestCase
 */
class MultiGetNoneKeysTestCase extends ArakoonDefaultTestCase
{
	const KEY_VALUE_PAIR_COUNT = 5;

	public function __construct()
	{
		parent::__construct('multi get none keys test',
							'gets a list of values using none keys and checks if an exception is thrown',
							'multi-get-none-keys');
	}

	public function SetUp()
	{
		$this->keys = array();
		$this->keys[] = NULL;
	}

	public function Run()
	{
		try
		{
			$multiGetResult = $this->arakoonClient->multiGet($this->keys);
			$this->Fail('no exception was thrown while it should have');
		}
		catch (Exception $exception)
		{
			$this->Pass('an exception was thrown');
		}
	}
}

/**
 * MultiGetEmptyTestCase
 */
class MultiGetEmptyTestCase extends ArakoonDefaultTestCase
{
	const KEY_VALUE_PAIR_COUNT = 5;

	public function __construct()
	{
		parent::__construct('multi get empty',
							'gets a list of values using none keys and checks if an exception is thrown',
							'multi-get-empty');
	}

	public function SetUp()
	{
		$this->keys = array();
	}

	public function Run()
	{
		$multiGetResult = $this->arakoonClient->multiGet($this->keys);
		$count = count($multiGetResult);
		$expectedCount = 0;
		$this->AssertEquals($count, $expectedCount, 'result count (' . $count . ') differs the expected result count (' . $expectedCount . ')');
	}
}

/**
 * PrefixExistingTestCase
 */
class PrefixExistingTestCase extends ArakoonDefaultTestCase
{
	const KEY_VALUE_PAIR_COUNT = 5;

	public function __construct()
	{
		parent::__construct('prefix existing test',
							'gets a list of keys beginning with an existing prefix and checks if all expected values are returned in the right order ',
							'prefix-existing');
	}

	public function SetUp()
	{
		$this->keys = array();
		$sequence = new Arakoon_Client_Operation_Sequence();	
			 
		for ($i = 0; $i < self::KEY_VALUE_PAIR_COUNT; $i++)
		{			
			$key = $this->prefix . '_key_' . $i;
			$value = $this->prefix . '_value_' . $i;
			array_push($this->keys, $key);
			$sequence->addSetOperation($key, $value);
		}
		
		$this->arakoonClient->sequence($sequence);
	}

	public function Run()
	{
		$prefixResult = $this->arakoonClient->prefix($this->prefix);		
		$this->AssertEquals(count($prefixResult) , self::KEY_VALUE_PAIR_COUNT, 'result count differs the expected result count ('. self::KEY_VALUE_PAIR_COUNT .')');
		
		for ($i = 0; $i < self::KEY_VALUE_PAIR_COUNT; $i++)
		{
			if ($prefixResult[$i] != $this->keys[$i])
			{
				$this->Fail('values not in right order');
				break;	
			}		
		}		
	}
}

/**
 * PrefixNonExistingTestCase
 */
class PrefixNonExistingTestCase extends ArakoonDefaultTestCase
{
	public function __construct()
	{
		parent::__construct('prefix non-existing test',
							'gets a list of keys beginning with a non-existing prefix and checks if an empty list is returned ',
							'prefix-non-existing');
	}

	public function Run()
	{
		$prefixResult = $this->arakoonClient->prefix($this->prefix);
		$this->AssertEquals(count($prefixResult) , 0, 'result count differs the expected result count (0)');
	}
}

/**
 * PrefixNoneTestCase
 */
class PrefixNoneTestCase extends ArakoonDefaultTestCase
{
	public function __construct()
	{
		parent::__construct('prefix none test',
							'gets a list of keys beginning with a none prefix and checks if an exception is thrown',
							'prefix-none');
	}

	public function Run()
	{
		try
		{
			$prefixResult = $this->arakoonClient->prefix(NULL);
			$this->Fail('no exception was thrown when it should have');
		}
		catch (Exception $exception)
		{
			$this->Pass('exception was thrown');
		}
	}
}

/**
 * ExpectProgressPossibleTestCase
 */
class ExpectProgressPossibleTestCase extends ArakoonDefaultTestCase
{
	public function __construct()
	{
		parent::__construct('expect progress possible test',
							'determines if progress is possible',
							'expect-progress-possible');
	}

	public function Run()
	{
		$result = $this->arakoonClient->expectProgressPossible();
		$this->AssertEquals($result, 1, 'negative result returned');
	}
}

/**
 * HelloTestCase
 */
class HelloTestCase extends ArakoonDefaultTestCase
{
	public function __construct()
	{
		parent::__construct('hello test',
							'identifies client to the server and checks if a version string is returned',
							'hello');
	}

	public function Run()
	{
		$helloResult = $this->arakoonClient->hello('clientId', $this->arakoonClientConfig->getClusterId());
		$this->AssertNotEquals(strlen($helloResult), 0, 'no version string returned');
	}
}

/**
 * WhoMasterTestCase
 */
class WhoMasterTestCase extends ArakoonDefaultTestCase
{
	public function __construct()
	{
		parent::__construct('who master test',
							'requests the master node id and checks if the master node its id is is returned',
							'who-master');
	}

	public function Run()
	{
		$master = $this->arakoonClient->whoMaster();
		$this->AssertNotEquals(strlen($master), 0, 'no master node id returned');
	}
}

/**
 * WhoMasterTestCase
 */
class StatisticsTestCase extends ArakoonDefaultTestCase
{
	public function __construct()
	{
		parent::__construct('get statistics',
							'gets statistics check if they are returned',
							'get-statistics');
	}

	public function Run()
	{
		$statistics = $this->arakoonClient->statistics();
		$this->AssertNotEquals(count($statistics), 0, 'no statistics returned');
	}
}

/**
 * GetKeyCountTestCase
 */
class GetKeyCountTestCase extends ArakoonDefaultTestCase
{
	public function __construct()
	{
		parent::__construct('get key count test',
							'gets key count an check if it is returned',
							'get-key-count');
	}

	public function Run()
	{
		$keyCount = $this->arakoonClient->getKeyCount();
		$this->AssertNotEquals($keyCount, 0, 'key count equals 0');
	}
}

/**
 * AssertExistingKeyExpectedValueTestCase
 */
class AssertExistingKeyExpectedValueTestCase extends ArakoonDefaultTestCase
{
	public function __construct()
	{
		parent::__construct('assert existing key expected value test',
							'asserts using an existing key and an expected value and checks if it\'s a success',
							'assert-existing-key-expected-value');
	}

	public function SetUp()
	{
		$this->arakoonClient->set($this->key1, $this->value1);
	}
	
	public function Run()
	{
		try
		{
			$this->arakoonClient->assert($this->key1, $this->value1);
			$this->Pass('no exception was thrown');
		}
		catch (Exception $exception)
		{
			$this->Fail('an exception was thrown while it shouldn\'t have');
		}
	}
}

/**
 * AssertExistingKeyNotExpectedValueTestCase
 */
class AssertExistingKeyNotExpectedValueTestCase extends ArakoonDefaultTestCase
{
	public function __construct()
	{
		parent::__construct('assert existing key not expected value test',
							'asserts using an existing key and a not expected value and checks if an exception is thrown',
							'assert-existing-key-not-expected-value');
	}

	public function SetUp()
	{
		$this->arakoonClient->set($this->key1, $this->value1);
	}
	
	public function Run()
	{
		try
		{
			$this->arakoonClient->assert($this->key1, $this->value2);
			$this->Fail('no exception was thrown while it should have');
		}
		catch (Exception $exception)
		{
			$this->Pass('an exception was thrown');
		}
	}
}

/**
 * AssertNonExistingKeyExpectedValueTestCase
 */
class AssertNonExistingKeyTestCase extends ArakoonDefaultTestCase
{
	public function __construct()
	{
		parent::__construct('assert non-existing key expected value test',
							'asserts using a non-existing key and checks if an exception is thrown ',
							'assert-non-existing-key-expected-value');
	}
	
	public function Run()
	{
		try
		{
			$this->arakoonClient->assert($this->key1, $this->value1);
			$this->Fail('no exception was thrown while it should have');
		}
		catch (Exception $exception)
		{
			$this->Pass('an exception was thrown');
		}
	}
}

/**
 * AssertNoneKeyTestCase
 */
class AssertNoneKeyTestCase extends ArakoonDefaultTestCase
{
	public function __construct()
	{
		parent::__construct('assert none key expected value test',
							'asserts using a none key and checks if an exception is thrown',
							'assert-none-key-expected-value');
	}
	
	public function Run()
	{
		try
		{
			$this->arakoonClient->assert(NULL, $this->value1);
			$this->Fail('no exception was thrown while it should have');
		}
		catch (Exception $exception)
		{
			$this->Pass('an exception was thrown');
		}
	}
}

/**
 * SetWhileTestCase class
 */
class SetWhileOneDownTestCase extends ArakoonDefaultTestCase
{
    	public function __construct()
	{
		parent::__construct('set while test',
							'sets a key-value pair and checks if it\'s key exists',
							'set while one node down');
	}

	public function Run()
	{	
                $testEnvironment = ArakoonTestEnvironment::getInstance();
                $testEnvironment->killOneNode();
                $this->arakoonClient->set($this->key, $this->value);
		$existsResult = $this->arakoonClient->exists($this->key);
		$this->AssertEquals($existsResult, 1, 'key of previously set key-value pair doesn\'t exist');
	}
}

?>
