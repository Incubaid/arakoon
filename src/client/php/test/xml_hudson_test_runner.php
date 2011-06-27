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

require_once 'php_unit_test_framework/php_unit_test.php';

/**
 * XmlHudsonTestRunner class
 */
class XmlHudsonTestRunner extends TestRunner
{
	private $_name;
	
	/**
	 * Constructor of an xml Hudson test runner.
	 * 
     * @param string $nane
     * 
     * @return void
     */
	public function __construct($name)
    {
    	$this->_name = $name;
    	
    	parent::__construct();
    }
	    
	/**
	 * Builds an xml report
	 *
	 * @return string
	 */
	public function Report()
	{
		return $this->generateTestSuiteXml();
	}
	
	/**
	 * Generates the test suite xml
	 *
	 * @return string
	 */
	private function generateTestSuiteXml()
	{
		$testCount = $this->GetNumberOfTestCaseResults(); 
		$testPassedCount = 0;
		
		for ($i = 0; $i < $testCount; ++$i)
		{
			if($this->GetTestCaseResult($i)->TestPassed())
			{
				$testPassedCount++;
			}
		}
		
		$testSuiteXmlPieces = array();
		array_push($testSuiteXmlPieces, '<?xml version="1.0" encoding="UTF-8"?>');
		array_push($testSuiteXmlPieces, '<testsuite  name="'. $this->_name . '" tests="' . $testCount . '" failures="' . ($testCount - $testPassedCount)  . '">');
		array_push($testSuiteXmlPieces, $this->generateTestCaseXml());
		array_push($testSuiteXmlPieces, '</testsuite>');
		
		return join($testSuiteXmlPieces);
	}

	/**
	 * Generates the test case xml
	 *
	 * @return string
	 */
	private function generateTestCaseXml()
	{		
		$testCaseXmlPieces = array();
		$testCount = $this->GetNumberOfTestCaseResults();
		
		for ($i = 0; $i < $testCount; ++$i)
		{
			$testCaseResult = $this->GetTestCaseResult($i);

			array_push($testCaseXmlPieces, '<testcase id="');
			array_push($testCaseXmlPieces, $testCaseResult->GetId());
			array_push($testCaseXmlPieces, '" classname="');
			array_push($testCaseXmlPieces, $testCaseResult->GetName());
			array_push($testCaseXmlPieces, '" name="');
			array_push($testCaseXmlPieces, $testCaseResult->GetName());
			
			if($testCaseResult->TestPassed())
			{
				array_push($testCaseXmlPieces, '" />');
			}
			else
			{
				array_push($testCaseXmlPieces, '">');
				
				for ($i = 0; $i < $testCaseResult->GetNumberOfEvents(); $i++)
				{
					$event = $testCaseResult->GetEvent($i);
					
					if ($event->GetType() == EventType::FAIL())
					{
						array_push($testCaseXmlPieces, '<failure>' . $event->GetMessage() . '</failure>');
					}
				}
				
				array_push($testCaseXmlPieces, '</testcase>');
			}
		}

		return join($testCaseXmlPieces);
	}

	/**
	 * Runs all the test cases in the $suite, with the option of storing the results in a file.
	 *
	 * @param TestSuite $suite 	The suite of tests to run.
	 * @param string $fileName	Filename to use for the report.
	 * 							# null 				- <em>stdout</em> is used
	 * 							# '' 				- A temporary filename is used, with the filename written to stdout
	 * 							# any other string 	- The given filename is used.
	 * @param string $extention	Extention to use for the report file. Defaults to 'xml'.
	 *
	 * @return integer 			Run result flag.
	 * 							# 0		All tests passed
	 * 							# -1	One or more tests failed
	 * 							# -2	Unable to open file
	 * 							# -3	Unable to write to file.
	 */
	public function Run(TestSuite &$suite, $filename = null, $extension = 'xml')
	{
		return parent::Run($suite, $filename, $extension);
	}
}