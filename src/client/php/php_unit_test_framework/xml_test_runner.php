<?php
//////////////////////////////////////////////////////////
///
/// $Author: edheal $
/// $Date: 2011-04-05 13:41:43 +0100 (Tue, 05 Apr 2011) $
/// $Id: xml_test_runner.php 9 2011-04-05 12:41:43Z edheal $
///
/// \file
/// \brief Runs the test and produces XML output.
///
/// \details
/// 
/// This test runner performs the tests and generates XML output.
///
/// \section License
///
/// A PHP Unit testing framework
///
/// Copyright (C) 2011 Ed Heal (ed.heal@yahoo.co.uk)
/// 
/// This program is free software: you can redistribute it and/or modify
/// it under the terms of the GNU General Public License as published by
/// the Free Software Foundation, either version 3 of the License, or
/// (at your option) any later version.
/// 
/// This program is distributed in the hope that it will be useful,
/// but WITHOUT ANY WARRANTY; without even the implied warranty of
/// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
/// GNU General Public License for more details.
/// 
/// You should have received a copy of the GNU General Public License
/// along with this program.  If not, see <http://www.gnu.org/licenses/>.
///
//////////////////////////////////////////////////////////

require_once 'php_unit_test.php';

//////////////////////////////////////////////////////////
/// \brief Test runner to generate XML reports.
///
/// This test runner generates the report in XML.
///
/// If you want to apply a style sheet to the output, subclass 
/// XMLTestRunnerWithStyleSheet. The following two classes:
/// - TextTestRunner;
/// - XHTMLTestRunner
/// uses this technique to generate XHTML and text (ASCII) output.
///
/// \todo Create a schema (XSD) for future developers.
//////////////////////////////////////////////////////////
class XMLTestRunner extends TestRunner
{
  //////////////////////////////////////////////////////////
  /// Generates the report in XML.
  //////////////////////////////////////////////////////////
  public function Report()
  {
    $report = '<?xml version="1.0" standalone="yes"?' . '>';
    $report .= "\n<testresults>\n";
    $end = $this->GetNumberOfTestCaseResults();
    for ($loop = 0; $loop < $end; ++$loop)
    {
      $testCaseResult = $this->GetTestCaseResult($loop);
      $report .= $this->ReportTestCase($testCaseResult);
    }
    $report .= "</testresults>\n";
    return $report;
  }
  
  private function ReportEvent(Event &$event)
  {
    $report = '        <type>' .
              htmlentities($event->GetTypeAsString()) .
              "</type>\n";
    $report .= '        <time>' .
               htmlentities($event->GetTime()) .
               "</time>\n";
    $type = $event->GetType();
    if (EventType::START_SETUP() == $type ||
        EventType::END_SETUP() == $type ||
        EventType::START_RUN() == $type ||
        EventType::END_RUN() == $type ||
        EventType::START_TEAR_DOWN() == $type ||
        EventType::END_TEAR_DOWN() == $type)
    {
      return $report;
    }
    if (EventType::USER_MSG() != $type ||
        EventType::PASS_MSG() != $type ||
        EventType::FAIL_MSG() != $type)
    {
      $report .= '        <reason>' .
                 htmlentities($event->GetReason()) .
                 "</reason>\n";
    }
    if (EventType::SYS_MSG() != $type)
    {
      $report .= '        <message>' .
              htmlentities($event->GetMessage()) .
              "</message>\n";
    }
    if (EventType::SYS_MSG() != $type &&
        EventType::USER_MSG() != $type &&
        EventType::FAIL_MSG() != $type &&
        EventType::PASS_MSG() != $type &&
        EventType::EXCEPTION_THROWN() != $type)
    {
      $report .= "        <actual>\n" .
                 '          <type>' . htmlentities($event->GetActualType()) . "</type>\n" .
                 '          <value>'.
                 htmlentities($event->GetActualValue()) .
                 "</value></actual>\n";
      $report .= "        <comparison>\n" .
                 '          <type>' . htmlentities($event->GetComparisonType()) . "</type>\n" .
                 '          <value>'.
                 htmlentities($event->GetComparisonValue()) .
                 "</value></comparison>\n";
    }
    if (EventType::SYS_MSG() != $type)
    {
      $report .= '        <file>' .
                 htmlentities($event->GetFile()) .
                 "</file>\n";
      $report .= '        <line>' .
                 htmlentities($event->GetLine()) .
                 "</line>\n";
    }
    return $report;
  }
  
  private function ReportTestCase(&$testCaseResult)
  {
    $report = "  <testcase>\n";
    $report .= '    <name>' .
               htmlentities($testCaseResult->GetName()) .
               "</name>\n";
    $report .= '    <id>' .
               htmlentities($testCaseResult->GetID()) .
               "</id>\n";
    $report .= '    <description>' .
               htmlentities($testCaseResult->GetDescription()) .
               "</description>\n";
    $report .= '    <passed>' .
               ($testCaseResult->TestPassed() ? 'Yes' : 'No') .
               "</passed>\n";
    $report .= "    <listOfEvents>\n";
    $end = $testCaseResult->GetNumberOfEvents();
    for ($loop = 0; $loop < $end; ++$loop)
    {
      $report .= "      <event>\n";
      $event = $testCaseResult->GetEvent($loop);
      $report .= $this->ReportEvent($event);
      $report .= "      </event>\n";
    }
    $report .= "    </listOfEvents>\n";
    $report .= "  </testcase>\n";

    return $report;
  }
  
  //////////////////////////////////////////////////////
  /// Runs all the test cases in the $suite, with the 
  /// option of storing the results in a file.
  /// \param[in] TestSuite &$suite The suite of tests to run.
  /// \param[in] string $filename Filename is either:
  ///                             - null - <em>stdout</em> is used;
  ///                             - '' - A temporary filename is used, with
  ///                               the filename written to <em>stdout</em>;
  ///                             - any other string - The given filename is used.
  /// \param[in] string $extension The extension to use for the file. Defaults to 'xml'
  /// \return Returns one of the following values:
  ///         - 0  - All tests passed;
  ///         - -1 - One or more tests failed;
  ///         - -2 - Unable to open file;
  ///         - -3 - Unable to write to file.
  //////////////////////////////////////////////////////
  public function Run(TestSuite &$suite,
                      $filename = null,
                      $extension = 'xml')
  {
    return parent::Run($suite, $filename, $extension);
  }
}
?>
