<?php
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
$testEnvironment->stopMaster();
Arakoon_Client_Logger::setup('/tmp/log.txt', Arakoon_Client_LoggerLevel::TRACE, TRUE);

/**
 * setup test
 */
$testSuite = new TestSuite();

$testSuite->AddTest('SetMasterFailoverTestCase');
$testSuite->AddTest('ExpectProgressPossibleMasterFailoverTestCase');

/**
 * run test
 */
$testRunner = new XmlHudsonTestRunner('arakoon-client-master-failover-unit-tests');
$testRunner->Run($testSuite, 'master_failover_test_report');

/**
 * teardown Arakoon
 */
$testEnvironment->tearDown();
?>
