<?php
//////////////////////////////////////////////////////////
///
/// $Author: edheal $
/// $Date: 2011-04-05 13:41:43 +0100 (Tue, 05 Apr 2011) $
/// $Id: text_test_runner.php 9 2011-04-05 12:41:43Z edheal $
///
/// \file
/// \brief Runs the test and produces ASCII output.
///
/// \details
/// 
/// This test runner performs the tests and generates ASCII output.
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

require_once 'xml_test_runner_with_style_sheet.php';

//////////////////////////////////////////////////////////
/// \brief This test runner generates the report in ASCII.
///
/// The class defines a XSLT style sheet and then
/// uses XMLTestRunnerWithStyleSheet to generate
/// the XML report and apply the style sheet.
//////////////////////////////////////////////////////////
class TextTestRunner extends XMLTestRunnerWithStyleSheet
{
  //////////////////////////////////////////////////////////
  /// \brief Sets up the style sheet.
  ///
  /// This constructs the style sheet to create the ASCII
  /// report.
  //////////////////////////////////////////////////////////
  public function __construct()
  {
    parent::__construct();
    $this->styleSheet .= <<<END_OF_STYLE_SHEET
<xsl:output method="text" />
<xsl:template match="/testresults">
<xsl:text>
******************************************************************************
*                                                                            *
*                                TEST REPORT                                 *
*                                                                            *
******************************************************************************

+----------------------------------------------------------------------------+
|                                   SUMMARY                                  |
+----------------------------------------------------------------------------+
</xsl:text>
<xsl:for-each select="testcase">
<xsl:text>

Name: </xsl:text><xsl:value-of select="name"/>
<xsl:text>
Test ID: </xsl:text><xsl:value-of select="id"/>
<xsl:text>
Description:
</xsl:text>
<xsl:value-of select="description"/>
<xsl:text>
Test Passed: </xsl:text>
<xsl:if test="passed='Yes'">Passed</xsl:if>
<xsl:if test="passed='No'">Failed</xsl:if> 
</xsl:for-each>
<xsl:text>

+----------------------------------------------------------------------------+
|                                   DETAILS                                  |
+----------------------------------------------------------------------------+

</xsl:text>
<xsl:for-each select="testcase">
<xsl:text>
Name: </xsl:text>
<xsl:value-of select="name"/>
<xsl:text>
Test ID: </xsl:text>
<xsl:value-of select="id"/>
<xsl:text>
Test Passed: </xsl:text>
<xsl:if test="passed='Yes'"><xsl:text>Passed</xsl:text></xsl:if>
<xsl:if test="passed='No'"><xsl:text>Failed</xsl:text></xsl:if>

<xsl:text>
                                  TEST EVENTS
                                  -----------

</xsl:text>
<xsl:for-each select="listOfEvents/event">
<xsl:text>
</xsl:text>

<xsl:value-of select="time" />
<xsl:text>
</xsl:text>

<xsl:choose>

<xsl:when test="type='PASS'">
<xsl:text>Test step success
Reason: </xsl:text>
<xsl:value-of select="reason" />
<xsl:text>
Message: </xsl:text>
<xsl:value-of select="message" />
<xsl:text>
Actual value type:
</xsl:text>
<xsl:value-of select="actual/type" />
<xsl:text>
Actual value:
</xsl:text>
<xsl:value-of select="actual/value" />
<xsl:text>
Comparison value type:
</xsl:text>
<xsl:value-of select="comparison/type" />
<xsl:text>
Comparison value:
</xsl:text>
<xsl:value-of select="comparison/value" />
<xsl:text>
File: </xsl:text>
<xsl:value-of select="file" />
<xsl:text> at line </xsl:text>
<xsl:value-of select="line" />
<xsl:text>
</xsl:text>
</xsl:when>

<xsl:when test="type='PASS_MSG'">
<xsl:text>Test step success
Message: </xsl:text>
<xsl:value-of select="message" />
<xsl:text>
File: </xsl:text>
<xsl:value-of select="file" />
<xsl:text> at line </xsl:text>
<xsl:value-of select="line" />
<xsl:text>
</xsl:text>
</xsl:when>

<xsl:when test="type='FAIL'">
<xsl:text>Test step failed
Reason: </xsl:text>
<xsl:value-of select="reason" />
<xsl:text>
Message: </xsl:text>
<xsl:value-of select="message" />
<xsl:text>
Actual value type:
</xsl:text>
<xsl:value-of select="actual/type" />
<xsl:text>
Actual value:
</xsl:text>
<xsl:value-of select="actual/value" />
<xsl:text>
Comparison value type:
</xsl:text>
<xsl:value-of select="comparison/type" />
<xsl:text>
Comparison value:
</xsl:text>
<xsl:value-of select="comparison/value" />
<xsl:text>
File: </xsl:text>
<xsl:value-of select="file" />
<xsl:text> at line </xsl:text>
<xsl:value-of select="line" />
<xsl:text>
</xsl:text>
</xsl:when>

<xsl:when test="type='FAIL_MSG'">
<xsl:text>Test step failed
Message: </xsl:text>
<xsl:value-of select="message" />
<xsl:text>
File: </xsl:text>
<xsl:value-of select="file" />
<xsl:text> at line </xsl:text>
<xsl:value-of select="line" />
<xsl:text>
</xsl:text>
</xsl:when>

<xsl:when test="type='ERROR'">
<xsl:text>Test step in error
Reason: </xsl:text>
<xsl:value-of select="reason" />
<xsl:text>
Message: </xsl:text>
<xsl:value-of select="message" />
<xsl:text>
Actual value type:
</xsl:text>
<xsl:value-of select="actual/type" />
<xsl:text>
Actual value:
</xsl:text>
<xsl:value-of select="actual/value" />
<xsl:text>
Comparison value type:
</xsl:text>
<xsl:value-of select="comparison/type" />
<xsl:text>
Comparison value:
</xsl:text>
<xsl:value-of select="comparison/value" />
<xsl:text>
File: </xsl:text>
<xsl:value-of select="file" />
<xsl:text>at line </xsl:text>
<xsl:value-of select="line" />
<xsl:text>
</xsl:text>
</xsl:when>

<xsl:when test="type='USER_MSG'">
<xsl:text>Message: </xsl:text>
<xsl:value-of select="message" />
<xsl:text>
File: </xsl:text>
<xsl:value-of select="file" />
<xsl:text> at line </xsl:text>
<xsl:value-of select="line" />
<xsl:text>
</xsl:text>
</xsl:when>

<xsl:when test="type='SYS_MSG'">
<xsl:value-of select="reason" /><xsl:text>
</xsl:text>
</xsl:when>

<xsl:when test="type='EXCEPTION_THROWN'">
<xsl:text>Exception occured at: </xsl:text>
<xsl:value-of select="message" />
<xsl:text>
Exceptions details: </xsl:text>
<xsl:value-of select="reason" />
<xsl:text>
File: </xsl:text>
<xsl:value-of select="file" />
<xsl:text> at line </xsl:text>
<xsl:value-of select="line" />
<xsl:text>
</xsl:text>
</xsl:when>

<xsl:when test="type='START_SETUP'">
<xsl:text>Set up phase started.
</xsl:text>
</xsl:when>

<xsl:when test="type='END_SETUP'">
<xsl:text>Set up phase finished.
</xsl:text>
</xsl:when>
<xsl:when test="type='START_RUN'">
<xsl:text>Run phase started.
</xsl:text>
</xsl:when>

<xsl:when test="type='END_RUN'">
<xsl:text>Run phase finished.
</xsl:text>
</xsl:when>

<xsl:when test="type='START_TEAR_DOWN'">
<xsl:text>Tear down phase started.
</xsl:text>
</xsl:when>

<xsl:when test="type='END_TEAR_DOWN'">
<xsl:text>Tear down phase finished.
</xsl:text>
</xsl:when>

</xsl:choose>

</xsl:for-each>
<xsl:text>
==============================================================================
</xsl:text>

</xsl:for-each>
</xsl:template>
</xsl:stylesheet>  
END_OF_STYLE_SHEET;

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
  /// \param[in] string $extension The extension to use for the file. Defaults to 'txt'
  /// \return Returns one of the following values:
  ///         - 0  - All tests passed;
  ///         - -1 - One or more tests failed;
  ///         - -2 - Unable to open file;
  ///         - -3 - Unable to write to file.
  //////////////////////////////////////////////////////
  public function Run(TestSuite &$suite,
                      $filename = null,
                      $extension = 'txt')
  {
    return parent::Run($suite, $filename, $extension);
  }
}
?>