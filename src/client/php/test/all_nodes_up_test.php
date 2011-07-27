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
 * test script
 *
 * @category   	Arakoon
 * @copyright 	Copyright (C) 2010 Incubaid BVBA
 * @license    	http://www.arakoon.org/licensing
 */ 
require_once '../Arakoon/Client/Config.php';
require_once '../Arakoon/Client/Logger.php';
require_once 'php_unit_test_framework/php_unit_test.php';
require_once 'php_unit_test_framework_extention/xml_hudson_test_runner.php';
require_once 'ArakoonTestCase.php';
require_once 'ArakoonTestEnvironment.php';

$arguments = $_SERVER['argv'];
$currentDir = dirname(realpath(__FILE__));
$configFilePath = $currentDir . '/config.ini';
$arakoonExeCmd;

/**
 * check existance of config file when provided
 */
if (array_key_exists(1, $arguments) && file_exists($arguments[1]))
{
	$configFilePath = $arguments[1];
}

/**
 * determine Arakoon execute command
 */
if (strlen(shell_exec('which arakoon')) > 0)
{
	$arakoonExeCmd = 'arakoon';
}
else if (file_exists($currentDir . '/arakoon'))
{
	$arakoonExeCmd = $currentDir . '/arakoon';
}
else
{
	throw new Exception('No Arakoon available!');
}

/**
 * setup Arakoon client test environment & logger
 */
$config = Arakoon_Client_Config::CreateFromFile($configFilePath);
$testEnvironment = ArakoonTestEnvironment::getInstance();
$testEnvironment->setup($config, $arakoonExeCmd, $configFilePath);
Arakoon_Client_Logger::setup('log.txt', Arakoon_Client_LoggerLevel::TRACE, TRUE);

/**
 * setup test
 */

$testSuite = new TestSuite();

$testSuite->AddTest('SetTestCase');
$testSuite->AddTest('SetNoneKeyTestCase');
$testSuite->AddTest('SetNoneValueTestCase');
$testSuite->AddTest('SetMaxKeySizeTestCase');
$testSuite->AddTest('SetMaxKeySizeExceededTestCase');

$testSuite->AddTest('GetExistingKeyTestCase');
$testSuite->AddTest('GetNonExistingKeyTestCase');
$testSuite->AddTest('GetNoneKeyTestCase');

$testSuite->AddTest('DeleteExistingKeyTestCase');
$testSuite->AddTest('DeleteNonExistingKeyTestCase');
$testSuite->AddTest('DeleteNoneKeyTestCase');

$testSuite->AddTest('SequenceValidTestCase');
$testSuite->AddTest('SequenceInvalidSetTestCase');
$testSuite->AddTest('SequenceInvalidDeleteTestCase');
$testSuite->AddTest('SequenceInvalidAssertTestCase');

$testSuite->AddTest('ExistsExistingKeyTestCase');
$testSuite->AddTest('ExistsNonExistingKeyTestCase');
$testSuite->AddTest('ExistsNoneKeyTestCase');

$testSuite->AddTest('RangeExistingKeysNoBordersTestCase');
$testSuite->AddTest('RangeExistingKeysBeginBorderTestCase');
$testSuite->AddTest('RangeExistingKeysEndBorderTestCase');
$testSuite->AddTest('RangeExistingKeysBothBordersTestCase');
$testSuite->AddTest('RangeExistingKeysMaxTestCase');
$testSuite->AddTest('RangeNonExistingKeysTestCase');
$testSuite->AddTest('RangeNoneKeysTestCase');

$testSuite->AddTest('RangeEntriesExistingKeysNoBordersTestCase');
$testSuite->AddTest('RangeEntriesExistingKeysBeginBorderTestCase');
$testSuite->AddTest('RangeEntriesExistingKeysEndBorderTestCase');
$testSuite->AddTest('RangeEntriesExistingKeysBothBordersTestCase');
$testSuite->AddTest('RangeEntriesExistingKeysMaxTestCase');
$testSuite->AddTest('RangeEntriesNonExistingKeysTestCase');
$testSuite->AddTest('RangeEntriesNoneKeysTestCase');

$testSuite->AddTest('TestAndSetExistingKeyExpectedValueNewValueTestCase');
$testSuite->AddTest('TestAndSetExistingKeyExpectedValueNoneValueTestCase');
$testSuite->AddTest('TestAndSetExistingKeyUnexpectedValueTestCase');
$testSuite->AddTest('TestAndSetNonExistingKeyExpectedValueNewValueTestCase');
$testSuite->AddTest('TestAndSetNonExistingKeyNoneValueNewValueTestCase');
$testSuite->AddTest('TestAndSetNoneKeyTestCase');

$testSuite->AddTest('MultiGetExistingKeysTestCase');
$testSuite->AddTest('MultiGetNonExistingKeysTestCase');
$testSuite->AddTest('MultiGetNoneKeysTestCase');
$testSuite->AddTest('MultiGetEmptyTestCase');

$testSuite->AddTest('PrefixExistingTestCase');
$testSuite->AddTest('PrefixNonExistingTestCase');
$testSuite->AddTest('PrefixNoneTestCase');

$testSuite->AddTest('AssertExistingKeyExpectedValueTestCase');
$testSuite->AddTest('AssertExistingKeyNotExpectedValueTestCase');
$testSuite->AddTest('AssertNonExistingKeyTestCase');
$testSuite->AddTest('AssertNoneKeyTestCase');

$testSuite->AddTest('ExpectProgressPossibleTestCase');
$testSuite->AddTest('HelloTestCase');
$testSuite->AddTest('WhoMasterTestCase');
$testSuite->AddTest('StatisticsTestCase');
$testSuite->AddTest('GetKeyCountTestCase');

/**
 * run test
 */
$testRunner = new XmlHudsonTestRunner('arakoon-client-unit-tests');
$testRunner->Run($testSuite, 'all_nodes_up_test_report');

/**
 * teardown Arakoon
 */ 
$testEnvironment->tearDown();
?>
