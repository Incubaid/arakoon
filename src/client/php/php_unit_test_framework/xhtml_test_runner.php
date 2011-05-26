<?php
//////////////////////////////////////////////////////////
///
/// $Author: edheal $
/// $Date: 2011-04-05 17:57:18 +0100 (Tue, 05 Apr 2011) $
/// $Id: xhtml_test_runner.php 12 2011-04-05 16:57:18Z edheal $
///
/// \file
/// \brief Runs the test and produces XHTML output.
///
/// \details
/// 
/// This test runner performs the tests and generates XHTML output.
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
/// \brief This test runner generates the report in XHTML.
///
/// The class defines a XSLT style sheet and then
/// uses XMLTestRunnerWithStyleSheet to generate
/// the XML report and apply the style sheet.
//////////////////////////////////////////////////////////
class XHTMLTestRunner extends XMLTestRunnerWithStyleSheet
{
  //////////////////////////////////////////////////////////
  /// Sets up the style sheet to convert XML to XHTML
  //////////////////////////////////////////////////////////
  public function __construct()
  {
    parent::__construct();
    $this->styleSheet .= <<<END_OF_STYLE_SHEET
<xsl:output method="xml" indent="yes"/>
<xsl:template match="/testresults">
<xsl:text disable-output-escaping="yes">
&lt;!DOCTYPE html PUBLIC &quot;-//W3C//DTD XHTML 1.1//EN&quot;
                  &quot;http://www.w3.org/TR/xhtml11/DTD/xhtml11.dtd&quot;&gt;
</xsl:text>      
<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en">
  <head>
    <meta http-equiv="content-type" content="application/xhtml+xml; charset=US-ASCII" />
    <title>Test Case Report</title>
    <style type="text/css">
/* http://meyerweb.com/eric/tools/css/reset/ */
/* v1.0 | 20080212 */

html, body, div, span, applet, object, iframe,
h1, h2, h3, h4, h5, h6, p, blockquote, pre,
a, abbr, acronym, address, big, cite, code,
del, dfn, em, font, img, ins, kbd, q, s, samp,
small, strike, strong, sub, sup, tt, var,
b, u, i, center,
dl, dt, dd, ol, ul, li,
fieldset, form, label, legend,
table, caption, tbody, tfoot, thead, tr, th, td {
  margin: 0;
  padding: 0;
  border: 0;
  outline: 0;
  font-size: 100%;
  vertical-align: baseline;
  background: transparent;
}
body {
  line-height: 1;
}

/* tables still need 'cellspacing="0"' in the markup */
table {
  border-collapse: collapse;
  border-spacing: 0;
}

/* END OF THE RESET BIT */

/* Stylesheet for the page */
html
{
  background-color: #ECFAFF;
}

body
{
  margin: 10px auto 10px auto;
  width: 980px;
}

h1
{
  margin-bottom: 5px;
  background-color: #BAEEA8;
  color: #BA21E0;
  font: bold 24px "Comic Sans MS", cursive;
  text-align: center;
}

h2
{
  font: 18px Impact, Charcoal, sans-serif;
  color: Black;
  margin-bottom: 12px;
  margin-top: 12px;
}

h3
{
  font: 14px "Arial Black", Gadget, sans-serif;
  color: #BA21E0;
  margin-top: 12px;
  margin-bottom: 12px;
  padding-top: 2px;
  padding-bottom: 2px;
}

a
{
  text-decoration: none;
  color: inherit;
}

tr
{
  color: Black;
  border: solid 1px black
}

td, th
{
  border: solid 1px black;
  padding: 2px 2px 2px 2px;
  font: 12px Arial, Helvetica, sans-serif;
}

th
{
  font-weight: bold;
  border-bottom-width: 2px;
  background-color: #A8E4FF;
  color: Black;
  border: solid 1px black
}

p
{
  font: 12px Arial, Helvetica, sans-serif;
  margin-top: 10px;
  margin-bottom: 10px;
}

ul
{
  margin-left: 15px;
  margin-top: 12px;
  font: 12px Arial, Helvetica, sans-serif;
  list-style: none;
}

strong
{
  font-weight: bold;
}

.pass
{
  background-color: #008000;
  color: White;
}

.passHeading
{
  background-color: #008000;
  color: White;
  padding-left: 6px;
  padding-top: 2px;
  padding-bottom: 2px;
}

.fail
{
  background-color: #900000;
  color: White;
}

.failHeading
{
  background-color: #900000;
  color: White;
  padding-left: 6px;
  padding-top: 2px;
  padding-bottom: 2px;
}

.footnote
{
  vertical-align: super;
  color: Black;
}

.centre
{
  text-align: center;
}

.detailsTable
{
  width: 980px;
}

.dateAndTimeColumn
{
  width: 180px;
}

.eventColumn
{
  width: 70px;
  text-align: center;
}

.reasonColumn
{
  width: 170px;
}

.messageColumn
{
  width: 170px;
}

.blankMessageColumn
{
  width: 170px;
  text-align: center;
}

.detailsHeadingColumn
{
  width: 105px;
  color: Yellow;
}

.testCaseIDColumn
{
  width: 120px;
}

.nameColumn
{
  width: 200px;
}

.descriptionColumn
{
  width: 400px;
}

.hasPassedColumn
{
  width: 60px;

}
.testCaseIDColumn
{
  width: 120px;
}

.nameColumn
{
  width: 200px;
}

.descriptionColumn
{
  width: 400px;
}

.hasPassedColumn
{
  width: 60px;
  text-align: center;
}    </style>
  </head>
  <body>
    <h1>Test Case Report</h1>
    <h2>Summary</h2>
    <table cellspacing="0" title="Summary of tests" summary="A list of tests carried out and if they passed or failed">
      <thead>
        <tr>
          <th class="testCaseIDColumn">Test case ID</th>
          <th class="nameColumn">Name</th>
          <th class="descriptionColumn">Description</th>
          <th class="hasPassedColumn">Passed?</th>
        </tr>
      </thead>
      <tbody>
        <xsl:for-each select="testcase">
          <xsl:if test="passed = 'Yes'">
            <tr class="pass">
              <td class="testCaseIDColumn">
                <a href="#{generate-id(id)}">
                  <xsl:value-of select="id"/>
                </a>
              </td>
              <td class="nameColumn"><xsl:value-of select="name"/></td>
              <td class="descriptionColumn"><xsl:value-of select="description"/></td>
              <td class="hasPassedColumn"><xsl:value-of select="passed"/></td>
            </tr>
          </xsl:if>
          <xsl:if test="passed != 'Yes'">
            <tr class="fail">
              <td class="testCaseIDColumn">
                <a href="#{generate-id(id)}">
                  <xsl:value-of select="id"/>
                </a>
              </td>
              <td class="nameColumn"><xsl:value-of select="name"/></td>
              <td class="descriptionColumn"><xsl:value-of select="description"/></td>
              <td class="hasPassedColumn"><xsl:value-of select="passed"/></td>
            </tr>
          </xsl:if>
        </xsl:for-each>
      </tbody>
    </table>
    
    <h2>Details</h2>
    <xsl:for-each select="testcase">
      <xsl:if test="passed = 'Yes'">
        <h3 class="passHeading">
          <a id="{generate-id(id)}">
            <xsl:value-of select="id"/> - <xsl:value-of select="name"/>
          </a>
        </h3>
      </xsl:if>
      <xsl:if test="passed != 'Yes'">
        <h3 class="failHeading">
          <a id="{generate-id(id)}">
            <xsl:value-of select="id"/> - <xsl:value-of select="name"/>
          </a>
        </h3>
      </xsl:if>
 
      <p><xsl:value-of select="description"/></p>
      
      <table cellspacing="0" class="detailsTable" title="Test events" summary="A list of events that occurred during the test">
        <thead>
          <tr>
            <th class="dateAndTimeColumn">Date &amp; Time</th>
            <th class="eventColumn">Event<a href="#eventsKey" class="footnote">1</a></th>
            <th class="reasonColumn">Reason</th>
            <th class="messageColumn">Message</th>
            <th class="detailsColumn" colspan="2">Details</th>
          </tr>
        </thead>
        <tbody>
          <xsl:for-each select="listOfEvents/event">
            <xsl:choose>
              <xsl:when test="type = 'PASS'">
                <tr class="pass">
                  <td rowspan="6" class="dateAndTimeColumn"><xsl:value-of select="time"/></td>
                  <td rowspan="6" class="eventColumn">PASS</td>
                  <td rowspan="6" class="reasonColumn"><xsl:value-of select="reason"/></td>
                  <td rowspan="6" class="messageColumn"><xsl:value-of select="message"/></td>
                  <td class="detailsHeadingColumn">File:</td>
                  <td><xsl:value-of select="file"/></td>
                </tr>
                <tr class="pass">
                  <td class="detailsHeadingColumn">Line:</td>
                  <td><xsl:value-of select="line"/></td>
                </tr>
                <tr class="pass">
                  <td class="detailsHeadingColumn">Actual Type:</td>
                  <td><pre><xsl:value-of select="actual/type"/></pre></td>
                </tr>
                <tr class="pass">
                  <td class="detailsHeadingColumn">Actual:</td>
                  <td><pre><xsl:value-of select="actual/value"/></pre></td>
                </tr>
                <tr class="pass">
                  <td class="detailsHeadingColumn">Comparison Type:</td>
                  <td><pre><xsl:value-of select="comparison/type"/></pre></td>
                </tr>
                <tr class="pass">
                  <td class="detailsHeadingColumn">Comparison:</td>
                  <td><pre><xsl:value-of select="comparison/value"/></pre></td>
                </tr>
              </xsl:when>
              <xsl:when test="type = 'PASS_MSG'">
                <tr class="pass">
                  <td rowspan="2" class="dateAndTimeColumn"><xsl:value-of select="time"/></td>
                  <td rowspan="2" class="eventColumn">PASS_MSG</td>
                  <td rowspan="2" class="reasonColumn">Users pass message</td>
                  <td rowspan="2" class="messageColumn"><xsl:value-of select="message"/></td>
                  <td class="detailsHeadingColumn">File:</td>
                  <td><xsl:value-of select="file"/></td>
                </tr>
                <tr class="pass">
                  <td class="detailsHeadingColumn">Line:</td>
                  <td><xsl:value-of select="line"/></td>
                </tr>
              </xsl:when>
              <xsl:when test="type = 'FAIL'">
                <tr class="fail">
                  <td rowspan="6" class="dateAndTimeColumn"><xsl:value-of select="time"/></td>
                  <td rowspan="6" class="eventColumn">FAIL</td>
                  <td rowspan="6" class="reasonColumn"><xsl:value-of select="reason"/></td>
                  <td rowspan="6" class="messageColumn"><xsl:value-of select="message"/></td>
                  <td class="detailsHeadingColumn">File:</td>
                  <td><xsl:value-of select="file"/></td>
                </tr>
                <tr class="fail">
                  <td class="detailsHeadingColumn">Line:</td>
                  <td><xsl:value-of select="line"/></td>
                </tr>
                <tr class="fail">
                  <td class="detailsHeadingColumn">Actual Type:</td>
                  <td><pre><xsl:value-of select="actual/type"/></pre></td>
                </tr>
                <tr class="fail">
                  <td class="detailsHeadingColumn">Actual:</td>
                  <td><pre><xsl:value-of select="actual/value"/></pre></td>
                </tr>
                <tr class="fail">
                  <td class="detailsHeadingColumn">Comparison Type:</td>
                  <td><pre><xsl:value-of select="comparison/type"/></pre></td>
                </tr>
                <tr class="fail">
                  <td class="detailsHeadingColumn">Comparison:</td>
                  <td><pre><xsl:value-of select="comparison/value"/></pre></td>
                </tr>
              </xsl:when>
              <xsl:when test="type = 'FAIL_MSG'">
                <tr class="fail">
                  <td rowspan="2" class="dateAndTimeColumn"><xsl:value-of select="time"/></td>
                  <td rowspan="2" class="eventColumn">FAIL_MSG</td>
                  <td rowspan="2" class="reasonColumn">Users fail message</td>
                  <td rowspan="2" class="messageColumn"><xsl:value-of select="message"/></td>
                  <td class="detailsHeadingColumn">File:</td>
                  <td><xsl:value-of select="file"/></td>
                </tr>
                <tr class="fail">
                  <td class="detailsHeadingColumn">Line:</td>
                  <td><xsl:value-of select="line"/></td>
                </tr>
              </xsl:when>
              <xsl:when test="type = 'ERROR'">
                <tr class="fail">
                  <td rowspan="6" class="dateAndTimeColumn"><xsl:value-of select="time"/></td>
                  <td rowspan="6" class="eventColumn">ERROR</td>
                  <td rowspan="6" class="reasonColumn"><xsl:value-of select="reason"/></td>
                  <td rowspan="6" class="messageColumn"><xsl:value-of select="message"/></td>
                  <td class="detailsHeadingColumn">File:</td>
                  <td><xsl:value-of select="file"/></td>
                </tr>
                <tr class="fail">
                  <td class="detailsHeadingColumn">Line:</td>
                  <td><xsl:value-of select="line"/></td>
                </tr>
                <tr class="fail">
                  <td class="detailsHeadingColumn">Actual Type:</td>
                  <td><pre><xsl:value-of select="actual/type"/></pre></td>
                </tr>
                <tr class="fail">
                  <td class="detailsHeadingColumn">Actual:</td>
                  <td><pre><xsl:value-of select="actual/value"/></pre></td>
                </tr>
                <tr class="fail">
                  <td class="detailsHeadingColumn">Comparison Type:</td>
                  <td><pre><xsl:value-of select="comparison/type"/></pre></td>
                </tr>
                <tr class="fail">
                  <td class="detailsHeadingColumn">Comparison:</td>
                  <td><pre><xsl:value-of select="comparison/value"/></pre></td>
                </tr>
              </xsl:when>
              <xsl:when test="type = 'USER_MSG'">
                <tr>
                  <td class="dateAndTimeColumn"><xsl:value-of select="time"/></td>
                  <td class="eventColumn">USER MSG</td>
                  <td class="reasonColumn">User message</td>
                  <td class="messageColumn"><xsl:value-of select="message"/></td>
                  <td colspan="2" class="centre">-</td>
                </tr>
              </xsl:when>
              <xsl:when test="type = 'SYS_MSG'">
                <tr>
                  <td class="dateAndTimeColumn"><xsl:value-of select="time"/></td>
                  <td class="eventColumn">SYS MSG</td>
                  <td class="reasonColumn"><xsl:value-of select="reason"/></td>
                  <td class="blankMessageColumn">-</td>
                  <td colspan="2" class="centre">-</td>
                </tr>
              </xsl:when>
              <xsl:when test="type = 'EXCEPTION_THROWN'">
                <tr class="fail">
                  <td rowspan="2" class="dateAndTimeColumn"><xsl:value-of select="time"/></td>
                  <td rowspan="2" class="eventColumn">EXCEPTION</td>
                  <td rowspan="2" class="reasonColumn"><xsl:value-of select="reason"/></td>
                  <td rowspan="2" class="messageColumn"><xsl:value-of select="message"/></td>
                  <td class="detailsHeadingColumn">File:</td>
                  <td><xsl:value-of select="file"/></td>
                </tr>
                <tr class="fail">
                  <td class="detailsHeadingColumn">Line:</td>
                  <td><xsl:value-of select="line"/></td>
                </tr>
              </xsl:when>
              <xsl:when test="type = 'START_SETUP'">
                <tr>
                  <td class="dateAndTimeColumn"><xsl:value-of select="time"/></td>
                  <td class="eventColumn">TS</td>
                  <td class="reasonColumn">'SetUp' started</td>
                  <td class="blankMessageColumn">-</td>
                  <td colspan="2" class="centre">-</td>
                </tr>
              </xsl:when>
              <xsl:when test="type = 'END_SETUP'">
                <tr>
                  <td class="dateAndTimeColumn"><xsl:value-of select="time"/></td>
                  <td class="eventColumn">TS</td>
                  <td class="reasonColumn">'SetUp' finished</td>
                  <td class="blankMessageColumn">-</td>
                  <td colspan="2" class="centre">-</td>
                </tr>
              </xsl:when>
              <xsl:when test="type = 'START_RUN'">
                <tr>
                  <td class="dateAndTimeColumn"><xsl:value-of select="time"/></td>
                  <td class="eventColumn">TS</td>
                  <td class="reasonColumn">'Run' started</td>
                  <td class="blankMessageColumn">-</td>
                  <td colspan="2" class="centre">-</td>
                </tr>
              </xsl:when>
              <xsl:when test="type = 'END_RUN'">
                <tr>
                  <td class="dateAndTimeColumn"><xsl:value-of select="time"/></td>
                  <td class="eventColumn">TS</td>
                  <td class="reasonColumn">'Run' finished</td>
                  <td class="blankMessageColumn">-</td>
                  <td colspan="2" class="centre">-</td>
                </tr>
              </xsl:when>
              <xsl:when test="type = 'START_TEAR_DOWN'">
                <tr>
                  <td class="dateAndTimeColumn"><xsl:value-of select="time"/></td>
                  <td class="eventColumn">TS</td>
                  <td class="reasonColumn">'TearDown' started</td>
                  <td class="blankMessageColumn">-</td>
                  <td colspan="2" class="centre">-</td>
                </tr>
              </xsl:when>
              <xsl:when test="type = 'END_TEAR_DOWN'">
                <tr>
                  <td class="dateAndTimeColumn"><xsl:value-of select="time"/></td>
                  <td class="eventColumn">TS</td>
                  <td class="reasonColumn">'TearDown' finished</td>
                  <td class="blankMessageColumn">-</td>
                  <td colspan="2" class="centre">-</td>
                </tr>
              </xsl:when>
            </xsl:choose>
          </xsl:for-each>
        </tbody>
      </table>
    </xsl:for-each>
    
    <h2><a id="eventsKey">Events key</a></h2>
    <p>A list of events that can occur in a test along with their meaning:</p>
    <ul>
      <li><strong>PASS</strong> - An assertion has passed successfully</li>
      <li><strong>PASS_MSG</strong> - A users pass message</li>
      <li><strong>FAIL</strong> - An assertion has failed</li>
      <li><strong>FAIL_MSG</strong> - A users fail message</li>
      <li><strong>ERROR</strong> - An assertion could not be carried out - need to fix the code!</li>
      <li><strong>USER MSG</strong> - A user message, for comentary</li>
      <li><strong>SYS MSG</strong> - A test system message</li>
      <li><strong>EXCEPTION</strong> - An exception has thrown and uncaught - need to fix the code!</li>
      <li><strong>TS</strong> - A time stamp marker</li>
    </ul>
  </body>
</html>
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
  /// \param[in] string $extension The extension to use for the file. Defaults to 'html'
  /// \return Returns one of the following values:
  ///         - 0  - All tests passed;
  ///         - -1 - One or more tests failed;
  ///         - -2 - Unable to open file;
  ///         - -3 - Unable to write to file.
  //////////////////////////////////////////////////////
  public function Run(TestSuite &$suite,
                      $filename = null,
                      $extension = 'html')
  {
    return parent::Run($suite, $filename, $extension);
  }
}
?>
