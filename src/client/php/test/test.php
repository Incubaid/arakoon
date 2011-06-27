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

require_once '../arakoon_client_config_factory.php';
require_once 'php_unit_test_framework/php_unit_test.php';
require_once 'xml_hudson_test_runner.php';
require_once 'arakoon_test_case.php';
require_once 'arakoon_test_environment.php';

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
 * setup Arakoon
 */
$config = ArakoonClientConfigFactory::CreateFromFile($configFilePath);
$testEnvironment = ArakoonTestEnvironment::getInstance();
$testEnvironment->setup($config, $arakoonExeCmd, $configFilePath);

/**
 * setup test
 */
$testSuite = new TestSuite();
$testSuite->AddTest('SetTestCase');
$testSuite->AddTest('SetMaxKeySizeTestCase');
$testSuite->AddTest('SetMaxKeySizeExceededTestCase');
$testSuite->AddTest('GetExistingKeyTestCase');
$testSuite->AddTest('GetNonExistingKeyTestCase');
$testSuite->AddTest('DeleteExistingKeyTestCase');
$testSuite->AddTest('DeleteNonExistingKeyTestCase');
$testSuite->AddTest('ExistsExistingKeyTestCase');
$testSuite->AddTest('ExistsNonExistingKeyTestCase');
$testSuite->AddTest('RangeExistingKeysTestCase');
$testSuite->AddTest('RangeNonExistingKeysTestCase');
$testSuite->AddTest('RangeEntriesExistingKeysTestCase');
$testSuite->AddTest('RangeEntriesNonExistingKeysTestCase');
$testSuite->AddTest('SequenceValidTestCase');
$testSuite->AddTest('SequenceInvalidTestCase');
$testSuite->AddTest('TestAndSetExistingKeyTestCase');
$testSuite->AddTest('TestAndSetNonExistingKeyTestCase');
$testSuite->AddTest('MultiGetExistingKeysTestCase');
$testSuite->AddTest('MultiGetNonExistingKeysTestCase');
$testSuite->AddTest('PrefixValidTestCase');
$testSuite->AddTest('PrefixInvalidTestCase');
$testSuite->AddTest('ExpectProgressPossibleTestCase');
$testSuite->AddTest('HelloTestCase');
$testSuite->AddTest('WhoMasterTestCase');

/**
 * run test
 */
$testRunner = new XmlHudsonTestRunner('arakoon-client-unit-tests');
$testRunner->Run($testSuite, 'unit_test_report');

/**
 * teardown Arakoon
 */ 
$testEnvironment->tearDown();

?>