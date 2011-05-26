<?php
//////////////////////////////////////////////////////////
///
/// $Author: edheal $
/// $Date: 2011-04-28 02:27:54 +0100 (Thu, 28 Apr 2011) $
/// $Id: php_unit_test.php 32 2011-04-28 01:27:54Z edheal $
///
/// \file
/// \brief Code for the implementation of PHP Unit tests.
/// \details
/// 
/// Please see the main page of the documentation.
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

//////////////////////////////////////////////////////////
///
/// \mainpage
/// \section license License
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
/// \section intro Introduction
///
/// This is a PHP Unit testing framework that is based upon
/// CPPUnit/JUnit. To implement unit tests using this framework
/// you need to follow the following steps.
///
/// \section implement Implement the test cases
///
/// To implement a test case you need to extend the class TestCase
/// and implemented the functions SetUp, Run and TearDown i.e.
///
/// \code
/// class MyTestCase extends TestCase
/// {
///   public function SetUp()
///   {
///     // Code to perform set up for test case.
///   }
///   public function Run()
///   {
///     // Code to perform the test case.
///     $this->AssertEquals('hello', 'there', 'Should fail!');
///     $this->AssertEquals('howdy', 'howdy', 'Should pass!');
///   }
///
///   public function TearDown()
///   {
///     // Code to tidy up afterwards.
///   }
/// }
/// \endcode
///
/// The SetUp function should contain all the tasks that are required
/// before the test should begin. For example:
/// - Making connections to databases;
/// - Constructing objects used for the test;
/// - Creating temporary files;
/// - Ensuring that the environment is in a suitable state for the test.
///
/// The Run method is where the testing occurs. Within this function you
/// use the methods defined in the Assert class to perform assertions.
/// These methods will perform logging of the test steps and is used in
/// the creation of the test report.
///
/// Finally the TearDown method is executed at the end of the test to
/// tidy up afterwards. This is always executed and contains tasks such
/// as:
///
/// - Disconnecting from a database;
/// - Removing temporary files.
///
/// If the SetUp function throws an exception, the Run method will
/// not be called, but the TearDown will be called.
/// If any of the three methods (SetUp, Run and TearDown) thrown an
/// exception the test is considered a failure.
///
/// \section suite Creating a test suite
///
/// Next task is to construct a test suite and add the test cases
/// to it. This is demonstrated by the code snippet below:
///
/// \code
/// $suite = new TestSuite;
/// $suite->AddTest('MyTestCase');
/// $suite->AddTest('MyOtherTestCase');
/// \endcode
///
/// This creates a test suite containg two test cases that are 
/// defined by the classes 'MyTestCase' and 'MyOtherTestCase'.
/// You can add as many test cases as you wish.
///
/// \section running Running the tests
///
/// To run the tests you need to construct a test runner. This
/// will define the format of the output of the results.
///
/// Currently three have been defined:
/// - TextTestRunner - Output as ASCII text;
/// - XMLTestRunner - Output as XML;
/// - XHTMLTestRunner - Output as XHTML.
/// 
/// The code snippet below shows how to construct a test runner
/// and the run the test suite to generate XHTML output.
///
/// \code
/// $runner = new XHTMLTestRunner;
/// $runner->Run($suite, '');
/// \endcode
///
/// Note that the second parameter of the Run method defines if
/// the output goes to <em>stdout</em>, to a named file or to a temporary
/// file (see TestRunner).
/// \section finally Finally
///
/// If you have any comments, please contact me at 
/// <a href="mailto:ed.heal@yahoo.co.uk">ed.heal@yahoo.co.uk</a>.
///
/// I hope that this small library will help in the development
/// of good quality PHP software.
//////////////////////////////////////////////////////////

//////////////////////////////////////////////////////////
/// \brief Enumerated type to indicate type of event.
///
/// An enumerate type to specify the type of
/// event that is stored in the test case report.
/// The static members return an object of type EventType
/// that will be stored in the Event class.
//////////////////////////////////////////////////////////
class EventType
{  
  private $value; // Stores the event type
  
  //////////////////////////////////////////////////////////
  /// Constructs the obect
  /// \param[in] integer $value The value for the enumerated type.
  //////////////////////////////////////////////////////////
  private function __construct($value)
  {
    $this->value = $value;
  }
  
  ////////////////////////////////////////////////////////
  /// Indicates the the assert evaluates to true.
  ////////////////////////////////////////////////////////
  public static function PASS()
  {
    return new EventType(1);
  }

  ////////////////////////////////////////////////////////
  /// Indicates a pass message.
  ////////////////////////////////////////////////////////
  public static function PASS_MSG()
  {
    return new EventType(2);
  }

  ////////////////////////////////////////////////////////
  /// Indicates that the assert has evaluated to false.
  ////////////////////////////////////////////////////////
  public static function FAIL()
  {
    return new EventType(3);
  }

  ////////////////////////////////////////////////////////
  /// Indicates a fail message.
  ////////////////////////////////////////////////////////
  public static function FAIL_MSG()
  {
    return new EventType(4);
  }
  
  ////////////////////////////////////////////////////////
  /// Indicates that the assert cannot be carried out
  ///  (i.e. fix your code).
  ////////////////////////////////////////////////////////
  public static function ERROR()
  {
    return new EventType(5);
  }
  ////////////////////////////////////////////////////////
  /// Indicates the the event is a user defined message.
  /// Used for commentary.
  ////////////////////////////////////////////////////////
  public static function USER_MSG()
  { 
    return new EventType(6); 
  }

  ////////////////////////////////////////////////////////
  /// Indicates that the event is a system message - i.e.
  /// indicating steps with the test case.
  ////////////////////////////////////////////////////////
  public static function SYS_MSG() 
  { 
    return new EventType(7); 
  }

  ////////////////////////////////////////////////////////
  /// Indicates the an exception is thrown during one of 
  /// the steps of the test case and the exception was 
  /// not caught.
  ////////////////////////////////////////////////////////
  public static function EXCEPTION_THROWN() 
  {
    return new EventType(8);
  }

  ////////////////////////////////////////////////////////
  /// A time marker indicating the start of the set up phase.
  ////////////////////////////////////////////////////////
  public static function START_SETUP() 
  { 
    return new EventType(9);
  }

  ////////////////////////////////////////////////////////
  /// A time marker indicating the end of the set up phase.
  ///
  ////////////////////////////////////////////////////////
  public static function END_SETUP()    
  { 
    return new EventType(10);
  }

  ////////////////////////////////////////////////////////
  /// A time marker indicating the start of the run phase.
  ////////////////////////////////////////////////////////
  public static function START_RUN() 
  { 
    return new EventType(11); 
  }

  ////////////////////////////////////////////////////////
  /// A time marker indicating the end of the run phase.
  ////////////////////////////////////////////////////////
  public static function END_RUN()      
  {
    return new EventType(12); 
  }

  ////////////////////////////////////////////////////////
  /// A time marker indicating the start of the tear down phase.
  ////////////////////////////////////////////////////////
  public static function START_TEAR_DOWN()  
  {
    return new EventType(13); 
  }

  ////////////////////////////////////////////////////////
  /// A time marker indicating the end of the tear down phase.
  ////////////////////////////////////////////////////////
  public static function END_TEAR_DOWN() 
  {
    return new EventType(14); 
  }
}

//////////////////////////////////////////////////////////
/// \brief Stores test case result event details.
///
/// This class stores details about one event that has
/// occurred during the execution of the test case.
/// There are a variety of events and the $type variable
/// (of type EventType) determines which other of the member
/// variables are significant.
///
/// The Get method are used in the generation of the test report.
//////////////////////////////////////////////////////////
class Event
{
  private $type;       // The event type.
  private $time;       // The time of the event.
  private $reason;     // The reason for the event.
  private $message;    // A user defined message for the event (commentary).
  private $actual;     // The actual value
  private $comparison; // What was used in the comparison
  private $file;       // The filename where the event occurred.
  private $line;       // And the line number

  ////////////////////////////////////////////////////////
  /// Creates a value for the actual/comparison but 
  /// converting the $value into a string and storing
  /// the type.
  /// \param[in] mixed $value The value to convert.
  /// \return An associated array of type and value.
  ////////////////////////////////////////////////////////
  private static function CreateValue($value)
  {
    $type = gettype($value);
    switch ($type)
    {
      case 'NULL':
        $str = 'NULL';
        break;
      case 'string':
      case 'integer':
      case 'double':
        $str = (string)$value;
        break;
      case 'boolean':
        $str = ($value ? 'True' : 'False');
        break;
      default:
        $str = var_export($value, true);
        break;
    }
    return array('type' => $type, 'value' => $str);
  }

  ////////////////////////////////////////////////////////
  /// Constructs the event object.
  ///
  /// \param[in] EventType $type The type for the event.
  /// \param[in] string $reason The reason for this event.
  /// \param[in] string $message A user defined message for this event.
  /// \param[in] mixed $actual The actual value calculated during testing.
  /// \param[in] mixed $comparison The comparison value.
  /// \param[in] string $file The filename where the event occurred.
  /// \param[in] string $line The line number where the event occurred.
  ////////////////////////////////////////////////////////
  public function __construct(EventType $type,
                              $reason,
                              $message,
                              $actual,
                              $comparison,
                              $file,
                              $line)
  {
    list($usec, $sec) = explode(' ', microtime());
    $usec = substr($usec . '00000', 2, 3);
    $now = new DateTime('@' . $sec);
    $this->time = $now->format('d M y, H:i:s.' . $usec .  ' O');
    $this->type = $type;
    $this->reason = $reason;
    $this->message = $message;
    $this->actual = Event::CreateValue($actual);
    $this->comparison = Event::CreateValue($comparison);
    $this->file = $file;
    $this->line = $line;
  }
  
  ////////////////////////////////////////////////////////
  /// Returns the event type.
  /// \return Returns the event type as an object of type EventType
  ////////////////////////////////////////////////////////
  public function GetType()
  {
    return $this->type;
  }
  
  ////////////////////////////////////////////////////////
  /// Returns the event type as a string.
  /// \return Returns the event type as a string.
  ////////////////////////////////////////////////////////
  public function GetTypeAsString()
  {
    switch ($this->type)
    {
      case EventType::PASS():
        return 'PASS';
      case EventType::PASS_MSG():
        return 'PASS_MSG';
      case EventType::FAIL():
        return 'FAIL';
      case EventType::FAIL_MSG():
        return 'FAIL_MSG';
      case EventType::ERROR():
        return 'ERROR';
      case EventType::USER_MSG():
        return 'USER_MSG';
      case EventType::SYS_MSG():
        return 'SYS_MSG';
      case EventType::EXCEPTION_THROWN():
        return 'EXCEPTION_THROWN';
      case EventType::START_SETUP():
        return 'START_SETUP';
      case EventType::END_SETUP():
        return 'END_SETUP';
      case EventType::START_RUN():
        return 'START_RUN';
      case EventType::END_RUN():
        return 'END_RUN';
      case EventType::START_TEAR_DOWN():
        return 'START_TEAR_DOWN';
      case EventType::END_TEAR_DOWN():
        return 'END_TEAR_DOWN';
    }
    return 'This chould never occur!';
  }
  
  ////////////////////////////////////////////////////////
  /// Returns the time when the event occurred.
  /// \return The time of the event as string in the format
  ///         as shown in this example:
  ///         - 03 Apr 07, 12:29:11.804 +0000.
  ////////////////////////////////////////////////////////
  public function GetTime()
  {
    return $this->time;
  }
  
  //////////////////////////////////////////////////////////
  /// Returns the events reason.
  /// \return string The reason for the event.
  //////////////////////////////////////////////////////////
  public function GetReason()
  {
    return $this->reason;
  }
  
  ////////////////////////////////////////////////////////
  /// Returns the message
  /// \return Returns the user defined message string for 
  /// the event.
  ////////////////////////////////////////////////////////
  public function GetMessage()
  {
    return $this->message;
  }
  
  ////////////////////////////////////////////////////////
  /// Returns the type of the actual value.
  /// \return The actual values type in the assert comparison is
  /// returned as a string representation.
  ////////////////////////////////////////////////////////
  public function GetActualType()
  {
    return $this->actual['type'];
  }
  
  ////////////////////////////////////////////////////////
  /// Returns the actual value.
  /// \return The actual value in the assert comparison is
  /// returned as a string representation.
  ////////////////////////////////////////////////////////
  public function GetActualValue()
  {
    return $this->actual['value'];
  }
  
  ////////////////////////////////////////////////////////
  /// Returns the type of the comparison value.
  /// \return The comparison values type in the assert comparison is
  /// returned as a string representation.
  ////////////////////////////////////////////////////////
  public function GetComparisonType()
  {
    return $this->comparison['type'];
  }
  
  ////////////////////////////////////////////////////////
  /// Returns the comparison value.
  /// \return The comparison value in the assert comparison is
  /// returned as a string representation.
  ////////////////////////////////////////////////////////
  public function GetComparisonValue()
  {
    return $this->comparison['value'];
  }

  ////////////////////////////////////////////////////////
  /// Returns the filename.
  /// \return The filename of the code where the event occurred.
  ////////////////////////////////////////////////////////
  public function GetFile()
  {
    return $this->file;
  }
  
  ////////////////////////////////////////////////////////
  /// Returns line number.
  /// \return The line number of the code where the event occurred.
  ////////////////////////////////////////////////////////
  public function GetLine()
  {
    return $this->line;
  }
}

//////////////////////////////////////////////////////////
/// \brief Stores events for a test case.
///
/// Provides access to the events in a test case along with:
/// - If the test case was a success;
/// - The name, description and test ID for the test case.
//////////////////////////////////////////////////////////
class TestCaseResult
{
  ////////////////////////////////////////////////////////
  /// Constructs an object to store events for a test case
  /// execution.
  /// \param[in] string $name A name for this test case.
  /// \param[in] string $id A test ID for this test case.
  ////////////////////////////////////////////////////////
  protected function __construct($name, $id)
  {
    $this->success = true;
    $this->testEvents = array();
    $this->name = $name;
    $this->testCaseId = $id;
    $this->description = 'No description.';
  }
  
  ////////////////////////////////////////////////////////
  /// Returns the test case name.
  /// \return The test case name.
  ////////////////////////////////////////////////////////
  public function GetName()
  {
    return $this->name;
  }

  ////////////////////////////////////////////////////////
  /// Returns the test case ID.
  /// \return The test case ID.
  ////////////////////////////////////////////////////////
  public function GetID()
  {
    return $this->testCaseId;
  }
  
  ////////////////////////////////////////////////////////
  /// Returns a description.
  /// \return Returns a description of the test case.
  ////////////////////////////////////////////////////////
  public function GetDescription()
  {
    return $this->description;
  }
  
  //////////////////////////////////////////////////////////
  /// Returns the number of events that make up the result
  /// for this test case.
  //////////////////////////////////////////////////////////
  public function GetNumberOfEvents()
  {
    return count($this->testEvents);
  }
  
  //////////////////////////////////////////////////////////
  /// Fetches an event as indexed by $index.
  /// \param[in] integer $index An index into the list of events.
  /// \return A event that makes up the test case result.
  ///         The return type is Event.
  //////////////////////////////////////////////////////////
  public function GetEvent($index)
  {
    return $this->testEvents[$index];
  }
  
  //////////////////////////////////////////////////////////
  /// Returns true if the test passed.
  /// \return A boolean value; true if test passed, or false
  /// if it failed.
  //////////////////////////////////////////////////////////
  public function TestPassed()
  {
    return $this->success;
  }
}

//////////////////////////////////////////////////////////
/// \brief Constructs test case results.
///
/// A class to hide the construction of the test case results
/// from the TestRunner::Report function. This is to enable
/// external developer of a TestRunner to be decoupled from
/// the internals of this framework.
//////////////////////////////////////////////////////////
class TestCaseResultConstructor extends TestCaseResult
{
  protected $success;       ///< True if the test passes, false otherwise.
  protected $testCaseId;    ///< Test case ID
  protected $name;          ///< A name for the test case.
  protected $description;   ///< And a description.
  protected $testEvents;    ///< List of events for the result of this rest case. 
  
  ////////////////////////////////////////////////////////
  /// Constructs the TestCaseResultConstructor.
  /// \param[in] string $name The name for the test case.
  /// \param[in] string $id Test case ID.
  ////////////////////////////////////////////////////////
  public function __construct($name, $id)
  {
    parent::__construct($name, $id);
  }
  
  //////////////////////////////////////////////////////////
  /// Adds a event to the test case result
  /// \param[in] EventType $type The event type.
  /// \param[in] string $reason The reason for this event (string).
  /// \param[in] string $message A user defined message (string). Enables commentary.
  /// \param[in] mixed $actual The actual value computed when running the test case.
  /// \param[in] mixed $comparison The comparison value used in the test case.
  //////////////////////////////////////////////////////////
  public function AddDetail(EventType $type,
                            $reason,
                            $message = '',
                            $actual = '',
                            $comparison = '')
  {
    if (EventType::FAIL() == $type ||
        EventType::FAIL_MSG() == $type ||
        EventType::ERROR() == $type)
    {
      $this->success = false;
    }
    $trace = debug_backtrace();
    $line = '';
    $file = '';
    foreach ($trace as $item)
    {
      if ('Assert' == $item['class'])
      {
        $line = $item['line'];
        $file = $item['file'];
        break;        
      }
    }
    $this->testEvents[] = new Event($type,
                                    $reason,
                                    $message,
                                    $actual,
                                    $comparison,
                                    $file,
                                    $line);
  }
  
  //////////////////////////////////////////////////////////
  /// Adds a exception to the test case result.
  /// \param[in] string $place The phase in the test case where exception
  ///            occurred.
  /// \param[in] Exception $exception The exception.
  //////////////////////////////////////////////////////////  
  public function AddException($place, Exception &$exception)
  {
    $this->success = false;
    $this->testEvents[] = new Event(EventType::EXCEPTION_THROWN(),
                                    $place,
                                    $exception->getMessage(),
                                    null,
                                    null,
                                    $exception->getFile(),
                                    $exception->getLine());
  
  }
  
  //////////////////////////////////////////////////////////
  /// Adds a time stamp to the test case result to indicate
  /// a stage in the test case (e.g. start/end of set up).
  /// \param[in] EventType $type The type of time stamp to be added.
  //////////////////////////////////////////////////////////
  public function AddTimeStamp(EventType $type)
  {
    $this->AddDetail($type, 'Marker');
  }
  
  //////////////////////////////////////////////////////////
  /// Adds a message to the test case result.
  /// \param[in] string $message The text for the message.
  /// \param[in] EventType $type The type of the Message.
  //////////////////////////////////////////////////////////
  public function AddMessage($message, $type)
  {
    if (EventType::SYS_MSG() == $type)
    {
      $this->AddDetail($type, $message);
    }
    else
    {
      $this->AddDetail($type, '', $message);
    }
  }
  
  //////////////////////////////////////////////////////////
  /// Sets the test case name, ID and description.
  /// \param[in] string $name The name for the test case.
  /// \param[in] string $id The ID for the test case.
  /// \param[in] string $description The description for the test case.
  //////////////////////////////////////////////////////////
  public function SetDetails($name, $id, $description)
  {
    $this->name = $name;
    $this->description = $description;
    $this->testCaseId = $id;
  }
}

//////////////////////////////////////////////////////////
/// \brief Base class for Test cases.
///
/// The base for TestCase to hide the internals from the
/// functions TestCase::SetUp, TestCase::Run and TestCase::TearDown.
//////////////////////////////////////////////////////////
class TestCaseBase
{
  protected $testCaseResults; ///< Where to place the results when running the test case.
  private $name;              // The name for the test case.
  private $id;                // The test case ID.
  private $description;       // A description for the test case.
  private static $testCaseCounter = 1; // For when test case ID is not given.

  ////////////////////////////////////////////////////////
  /// Initialize the test case as used by TestSuite::Run
  /// \param[in] TestCaseResultConstructor $testCaseResults Where
  /// to store the results as a collection of events.
  ////////////////////////////////////////////////////////
  public function Init(TestCaseResultConstructor &$testCaseResults)
  {
    $this->testCaseResults = $testCaseResults;
  }
  
  ////////////////////////////////////////////////////////
  /// Constructs the object.
  /// \param[in] string $name A name for test case.
  /// \param[in] string $description A description for the test case.
  /// \param[in] string $id Test case ID.
  ////////////////////////////////////////////////////////
  protected function __construct($name, $description, $id)
  {
    $this->name = (null == $name ? get_class($this) : $name);
    $this->description = (null == $description ? 'No description.' : $description);
    if (null == $id)
    {
      $id = 'TEST' . TestCaseBase::$testCaseCounter++;
    }
    $this->id = $id;
  }

  ////////////////////////////////////////////////////////
  /// Returns the name for the test case.
  /// \return Returns a string storing the name for the test case.
  ////////////////////////////////////////////////////////
  public function GetName()
  {
    return $this->name;
  }

  ////////////////////////////////////////////////////////
  /// Returns the ID for the test case.
  /// \return Returns a string storing the ID for the test case.
  ////////////////////////////////////////////////////////
  public function GetID()
  {
    return $this->id;
  }
  
  ////////////////////////////////////////////////////////
  /// Returns the description for the test case.
  /// \return Returns a string storing the description for the test case.
  ////////////////////////////////////////////////////////
  public function GetDescription()
  {
    return $this->description;
  }
}

//////////////////////////////////////////////////////////
/// \brief Interface for performing equality tests on objects.
///
/// An interface used by Assert to identify objects that
/// can be compared for equality.
//////////////////////////////////////////////////////////
interface Equality
{
  ////////////////////////////////////////////////////////
  /// Should be implemented to compare the $obj with itself
  /// returning true if they are 'equal'.
  /// \param[in] object $obj Object for the comparison.
  ////////////////////////////////////////////////////////
  public function Equals($obj);
}

//////////////////////////////////////////////////////////
/// \brief Enumerate type for comparing values. See 
///        Assert::CompareValues (private function).
//////////////////////////////////////////////////////////
class CompareValuesResult
{
  private $value;
  
  ////////////////////////////////////////////////////////
  /// Constructs the object for the enumerated type.
  /// param[in] integer $value The value for the enumerated type
  ////////////////////////////////////////////////////////
  private function __construct($value)
  {
    $this->value = $value;
  }

  ////////////////////////////////////////////////////////
  /// Value for when the two values are equal.
  ////////////////////////////////////////////////////////
  public static function EQUAL()
  {
    return new CompareValuesResult(1);
  }

  ////////////////////////////////////////////////////////
  /// Value for when the two values are not equal.
  ////////////////////////////////////////////////////////
  public static function NOT_EQUAL()
  {
    return new CompareValuesResult(2);
  }

  ////////////////////////////////////////////////////////
  /// Value for when the two values are:
  /// - Of different type;
  /// - A resource;
  /// - An object without Equality::Equals implemented with the expected value.
  ////////////////////////////////////////////////////////
  public static function INCOMPARABLE()
  {
    return new CompareValuesResult(3);
  }
}

//////////////////////////////////////////////////////////
/// \brief Assertions that can be made in test cases.
///
/// This class provides the assertions that can be performed in
/// a test case and enables the user to add a commentary to the 
/// test report.
//////////////////////////////////////////////////////////
class Assert extends TestCaseBase
{
  private static $buffers = 0;
  
  //////////////////////////////////////////////////////////
  /// Call back so that errors/warnings do not go missing
  /// (due to output being captured). The program with abort.
  /// \param[in] integer $errNo The error number.
  /// \param[in] string $errStr A string representing the error.
  /// \param[in] string $file The file that the error occurred in.
  /// \param[in] integer $line And the line number.
  //////////////////////////////////////////////////////////
  public static function DieOnError($errNo, $errStr, $file, $line)
  {
    die("Error: [$errNo] $errStr in $file at $line");
  }
  
  //////////////////////////////////////////////////////////
  /// Constructs the object by setting an error handler
  /// and starting output capture.
  /// \param[in] string $name Name for the test case.
  /// \param[in] string $description And a description for the test case.
  /// \param[in] string $id Test case ID.
  //////////////////////////////////////////////////////////
  protected function __construct($name, $description, $id)
  {
    parent::__construct($name, $description, $id);
    set_error_handler('Assert::DieOnError'); // So warnings etc are not lost!
    ob_start();
  }
  
  //////////////////////////////////////////////////////////
  /// Destroys the object by stopping output capture.
  //////////////////////////////////////////////////////////
  public function __destruct()
  {
    ob_end_flush();
  }

  ////////////////////////////////////////////////////////
  /// Adds a user message to the test case results for commentary.
  /// \param[in] string $message The message to add.
  ////////////////////////////////////////////////////////
  public function AddMessage($message)
  {
    $this->testCaseResults->AddMessage($message, EventType::USER_MSG());
  }

  ////////////////////////////////////////////////////////
  /// Adds a failure message and fails the test.
  /// \param[in] string $message A user defined message (for commentary). 
  ////////////////////////////////////////////////////////
  public function Fail($message)
  {
    $this->testCaseResults->AddMessage($message, EventType::FAIL_MSG());
  }

  ////////////////////////////////////////////////////////
  /// Adds a pass message to the test results.
  /// \param[in] string $message A user defined message (for commentary). 
  ////////////////////////////////////////////////////////
  public function Pass($message)
  {
    $this->testCaseResults->AddMessage($message, EventType::PASS_MSG());
  }
  
  ////////////////////////////////////////////////////////
  /// Returns true if the value can be compared using '==' operator.
  /// \param[in] mixed $value Value to be checked to see if the '==' operator can
  //                          compare it.
  ////////////////////////////////////////////////////////
  private static function CanCompare($value)
  {
    $type = gettype($value);
    return ('string' == $type || 'integer' == $type || 'double' == $type ||
            'NULL' == $type   || 'boolean' == $type); 
  }
 
  
  //////////////////////////////////////////////////////////
  /// Function to implement the comparison. See Assert::AssertEquals.
  /// \param[in] mixed $actual First value.
  /// \param[in] mixed $toBeComparedWith Second value.
  //////////////////////////////////////////////////////////
  private static function CompareValues($actual, $toBeComparedWith)
  {
    $actualType = gettype($actual);
    $toBeComparedWithType = gettype($toBeComparedWith);
    if ($actualType != $toBeComparedWithType)
    {
      return CompareValuesResult::INCOMPARABLE();
    }
    
    // Type are the same type
    
    switch ($toBeComparedWithType)
    {
      case 'NULL':
      case 'boolean':
      case 'integer':
      case 'double':
      case 'string':
        return ($actual == $toBeComparedWith ? CompareValuesResult::EQUAL()
                                             : CompareValuesResult::NOT_EQUAL());
      case 'resource':
        return CompareValuesResult::INCOMPARABLE();
      case 'array':
        foreach ($toBeComparedWith as $k => $v)
        {
          if (array_key_exists($k, $actual))
          {
            $result = Assert::CompareValues($actual[$k], $v);
            if ($result != CompareValuesResult::EQUAL()) return $result;
          }
          else
          {
            return CompareValuesResult::INCOMPARABLE();
          }
        
        }
        return CompareValuesResult::EQUAL();
      case 'object':
        if ($toBeComparedWith instanceof Equality)
        {
          return ($toBeComparedWith->Equals($actual) ? CompareValuesResult::EQUAL()
                                                     : CompareValuesResult::NOT_EQUAL());
        }
        else
        {
          return CompareValuesResult::INCOMPARABLE();
        }
      default:
        return CompareValuesResult::INCOMPARABLE();
    }
  }
  
  //////////////////////////////////////////////////////////
  /// This function compares the two values together and the comparison is performed as follows:
  ///   
  /// - If the two values are of different types an error is reported as they are incomparable.
  ///   However the function tries to compare them if they are strings,
  ///   integers, double or null to aid in debugging. You might just have the $expected as
  ///   the wrong type!
  /// - If the two values that are either strings, integers, doubles or nulls
  ///   the '==' operator is used to determine equality.
  /// - If the two values are resources then an error is reported as resources cannot
  ///   be compared.
  /// - If the two values are objects and the $expected value implements the Equality interface,
  ///   the Equality::Equals function is used to determine equality. Otherwise an error is
  ///   reported as the two objects are incomparable.
  ///     
  ///   The following code can be used to perform the implementation of the Equality interface
  ///   without the need to change your implementation of the class under test:
  ///   \code
  ///   class MyTestClass extends ClassToBeTested implements Equality
  ///   {
  ///     public function Equals($obj)
  ///     {
  ///       // Return true of false by using ClassToBeTested public/protected functions.
  ///     }     
  ///   }
  ///   \endcode
  /// - If the two values are arrays then the each of the keys from the $expected are 
  ///   checked in turn. If the corresponding key in $actual does not exist then the arrays
  ///   are deemed to be incomparable. Otherwise the two values are retrieved from both arrays
  ///   and are compared using the above description. If the two values are incomparable
  ///   or not equal the scan is terminated as the result (incomparable or not equal) has been
  ///   found.
  ///
  ///   After all the keys are checked and found to be equal then the two arrays are deemed to
  ///   be equal.
  ///    
  ///   If any of the items in the array is an array then all the values in that array
  ///   are checked as described above. 
  ///   
  /// After performing this comparison without error and the two values are equal the assert
  /// reports are pass or fail otherwise.
  ///
  /// \param[in] mixed $actual The actual value in the test.
  /// \param[in] mixed $expected The expected value in the test.
  /// \param[in] string $message An optional user defined message (for commentary).
  //////////////////////////////////////////////////////////
  public function AssertEquals($actual, $expected, $message = '')
  {
    switch (Assert::CompareValues($actual, $expected))
    {
      case CompareValuesResult::EQUAL():
        $this->testCaseResults->AddDetail(EventType::PASS(),
                                          'AssertEquals: The values are equal.',
                                          $message,
                                          $actual,
                                          $expected);
        break;
      case CompareValuesResult::NOT_EQUAL():
        $this->testCaseResults->AddDetail(EventType::FAIL(),
                                          'AssertEquals: The values are not equal.',
                                          $message,
                                          $actual,
                                          $expected);
        break;
      default:
        if (Assert::CanCompare($actual) && Assert::CanCompare($expected))
        {
          if ($actual == $expected)
          {
            $this->testCaseResults->AddDetail(EventType::ERROR(),
                                              'AssertEquals: Values cannot be compared. After type conversion they are equal.',
                                              $message,
                                              $actual,
                                              $expected);
          }
          else
          {
            $this->testCaseResults->AddDetail(EventType::ERROR(),
                                              'AssertEquals: Values cannot be compared. After type conversion they are not equal.',
                                              $message,
                                              $actual,
                                              $expected);
          }
      }
      else
      {
        $this->testCaseResults->AddDetail(EventType::ERROR(),
                                          'AssertEquals: Values cannot be compared.',
                                          $message,
                                          $actual,
                                          $expected);
      }
      break;
    }
  }
  
  ////////////////////////////////////////////////////////
  /// This function compares the two values to determine if they are
  /// not equal as described above (see Assert::AssertEquals). If 
  /// not equal the assertion has passed, otherwise it has failed
  /// unless the two values are incomparable then an error is reported.
  ///
  /// \param[in] mixed $actual The actual value in the test.
  /// \param[in] mixed $expected The expected value in the test.
  /// \param[in] string $message An optional user defined message (for commentary).
  ////////////////////////////////////////////////////////
  public function AssertNotEquals($actual, $expected, $message = '')
  {
    switch (Assert::CompareValues($actual, $expected))
    {
      case CompareValuesResult::EQUAL():
        $this->testCaseResults->AddDetail(EventType::FAIL(),
                                          'AssertNotEquals: The values are equal.',
                                          $message,
                                          $actual,
                                          $expected);
        break;
      case CompareValuesResult::NOT_EQUAL():
        $this->testCaseResults->AddDetail(EventType::PASS(),
                                          'AssertNotEquals: The values are not equal.',
                                          $message,
                                          $actual,
                                          $expected);
        break;
      default:
        if (Assert::CanCompare($actual) && Assert::CanCompare($expected))
        {
          if ($actual == $expected)
          {
            $this->testCaseResults->AddDetail(EventType::ERROR(),
                                              'AssertNotEquals: Values cannot be compared. After type conversion they are equal.',
                                              $message,
                                              $actual,
                                              $expected);
          }
          else
          {
            $this->testCaseResults->AddDetail(EventType::ERROR(),
                                              'AssertNotEquals: Values cannot be compared. After type conversion they are not equal.',
                                              $message,
                                              $actual,
                                              $expected);
          }
      }
      else
      {
        $this->testCaseResults->AddDetail(EventType::ERROR(),
                                          'AssertNotEquals: Values cannot be compared.',
                                          $message,
                                          $actual,
                                          $expected);
      }
      break;
    }  
  }
  
  ////////////////////////////////////////////////////////
  /// Checks the $actual parameter matches the regular 
  /// expression $pattern.
  /// \param[in] string $actual The string to compare with.
  /// \param[in] string $pattern A regular expression to use (<code>preg</code>).
  /// \param[in] string $message A user defined message (for commentary).
  ////////////////////////////////////////////////////////
  public function AssertMatches($actual, $pattern, $message = '')
  {
    if ('string' != gettype($actual) ||
        'string' != gettype($pattern))
    {
      $this->testCaseResults->AddDetail(EventType::ERROR(),
                                        'AssertMatches: Both parameters need to be strings.',
                                        $message,
                                        $actual,
                                        $pattern);
      return;
    }
    $match = preg_match($pattern, $actual);
    $this->testCaseResults->AddDetail($match > 0 ? EventType::PASS() : EventType::FAIL(),
                                      'AssertMatches: ' . $match . ' matches found.',
                                      $message,
                                      $actual,
                                      $pattern);
  }
  
  //////////////////////////////////////////////////////////
  /// Checks the $actual parameter does not match the regular 
  /// expression $pattern.
  /// \param[in] string $actual The string to compare with.
  /// \param[in] string $pattern A regular expression to use (<code>preg</code>).
  /// \param[in] string $message A user defined message (for commentary).
  ////////////////////////////////////////////////////////
  public function AssertNotMatches($actual, $pattern, $message = '')
  {
    if ('string' != gettype($actual) ||
        'string' != gettype($pattern))
    {
      $this->testCaseResults->AddDetail(EventType::ERROR(),
                                        'AssertNotMatches: Both parameters need to be strings.',
                                        $message,
                                        $actual,
                                        $pattern);
      return;
    }
    $match = preg_match($pattern, $actual);
    $this->testCaseResults->AddDetail($match > 0 ? EventType::FAIL() : EventType::PASS(),
                                      'AssertNotMatches: ' . $match . ' matches found.',
                                      $message,
                                      $actual,
                                      $pattern);
  }
  
  //////////////////////////////////////////////////////////
  /// Restarts output capture so that you can compare the output
  /// from this point onwards.
  //////////////////////////////////////////////////////////
  public function ReStartOutputChecking()
  {
    ob_flush();
  }
  
  //////////////////////////////////////////////////////////
  /// Checks that the output from the start of the test or
  /// the last call to Assert::ReStartOutputChecking is of the 
  /// given value.
  /// \param[in] string $expected The output is expected to be this value.
  /// \param[in] string $message A user defined message (for commentary).
  //////////////////////////////////////////////////////////
  public function AssertOutputEquals($expected, $message = '')
  {
    $actual = ob_get_contents();
    if ('string' != gettype($expected))
    {
      $this->testCaseResults->AddDetail(EventType::ERROR(),
                                        'AssertOutputEquals: Expected need to be a string.',
                                        $message,
                                        $actual,
                                        $expected);
      return;
    }
    if ($actual == $expected)
    {
    
      $this->testCaseResults->AddDetail(EventType::PASS(),
                                        'AssertOutputEquals: Output matches.',
                                        $message,
                                        $actual,
                                        $expected);
    }
    else
    {
      $this->testCaseResults->AddDetail(EventType::FAIL(),
                                        'AssertOutputEquals: Output does not match.',
                                        $message,
                                        $actual,
                                        $expected);
    }
  
  }
  
  //////////////////////////////////////////////////////////
  /// Checks that the output from the start of the test or
  /// the last call to Assert::ReStartOutputChecking is not of the 
  /// given value.
  /// \param[in] string $expected The output is not expected to be this value.
  /// \param[in] string $message A user defined message (for commentary).
  //////////////////////////////////////////////////////////
  public function AssertOutputDiffers($expected, $message = '')
  {
    $actual = ob_get_contents();
    if ('string' != gettype($expected))
    {
      $this->testCaseResults->AddDetail(EventType::ERROR(),
                                        'AssertOutputDiffers: Expected need to be a string.',
                                        $message,
                                        $actual,
                                        $expected);
      return;
    }
    if ($actual == $expected)
    {
    
      $this->testCaseResults->AddDetail(EventType::FAIL(),
                                        'AssertOutputDiffers: Output matches.',
                                        $message,
                                        $actual,
                                        $expected);
    }
    else
    {
      $this->testCaseResults->AddDetail(EventType::PASS(),
                                        'AssertOutputDiffers: Output does not match.',
                                        $message,
                                        $actual,
                                        $expected);
    }
  }

  //////////////////////////////////////////////////////////
  /// Checks that the output from the start of the test or
  /// the last call to Assert::ReStartOutputChecking does match
  /// the pattern.
  /// \param[in] string $pattern The output is expected match this pattern.
  /// \param[in] string $message A user defined message (for commentary).
  //////////////////////////////////////////////////////////
  public function AssertOutputMatches($pattern, $message = '')
  {
    $actual = ob_get_contents();
    $actual = ob_get_contents();
    if ('string' != gettype($pattern))
    {
      $this->testCaseResults->AddDetail(EventType::ERROR(),
                                        'AssertOutputMatches: Expected need to be a string.',
                                        $message,
                                        $actual,
                                        $pattern);
      return;
    }
    
    $match = preg_match($pattern, $actual);
    $this->testCaseResults->AddDetail($match > 0 ? EventType::PASS() : EventType::FAIL(),
                                      'AssertOutputMatches: ' . $match . ' matches found.',
                                      $message,
                                      $actual,
                                      $pattern);
  }
  
  //////////////////////////////////////////////////////////
  /// Checks that the output from the start of the test or
  /// the last call to Assert::ReStartOutputChecking does not match the 
  /// pattern.
  /// \param[in] string $pattern The output is not expected to match this pattern.
  /// \param[in] string $message A user defined message (for commentary).
  //////////////////////////////////////////////////////////
  public function AssertOutputNotMatches($pattern, $message = '')
  {
    $actual = ob_get_contents();
    $actual = ob_get_contents();
    if ('string' != gettype($pattern))
    {
      $this->testCaseResults->AddDetail(EventType::ERROR(),
                                        'AssertOutputNotMatches: Expected need to be a string.',
                                        $message,
                                        $actual,
                                        $pattern);
      return;
    }

    $match = preg_match($pattern, $actual);
    $this->testCaseResults->AddDetail($match > 0 ? EventType::FAIL() : EventType::PASS(),
                                      'AssertOutputNotMatches: ' . $match . ' matches found.',
                                      $message,
                                      $actual,
                                      $pattern);
  }
}

//////////////////////////////////////////////////////////
/// \brief Abstract class the unit test cases authors need to implement.
///
/// This abstract class needs to be extended for your test
/// cases. The following methods need to be implemented:
/// - TestCase::SetUp;
/// - TestCase::Run;
/// - TestCase::TearDown.
//////////////////////////////////////////////////////////
abstract class TestCase extends Assert
{
  ////////////////////////////////////////////////////////
  /// Uses to construct the test case.
  /// \param[in] string $name Name for the test case (defaults to the class name).
  /// \param[in] string $description Description for the test case (defaults to 'No description.').
  /// \param[in] string $id Test case ID (uses a counter if no ID is given).
  ////////////////////////////////////////////////////////
  public function __construct($name = null, $description = null, $id = null)
  {
    parent::__construct($name, $description, $id);
  }

  ////////////////////////////////////////////////////////
  /// This method needs to be implemented with code to
  /// perform the set up for the test.
  ////////////////////////////////////////////////////////
  public abstract function SetUp();
  
  ////////////////////////////////////////////////////////
  /// This method needs to be implemented with code to
  /// perform the test case. This runs only if the SetUp does 
  /// not throw an exception or an assertion fails.
  ////////////////////////////////////////////////////////
  public abstract function Run();

  ////////////////////////////////////////////////////////
  /// This method needs to be implemented with code to
  /// perform the tidying up after the test has completed.
  /// It is always executed.
  ////////////////////////////////////////////////////////
  public abstract function TearDown();
}

//////////////////////////////////////////////////////////
/// \brief Collects together test cases and enables them to be run together.
///
/// This class collects together a series of test cases.
/// - To add a test case call the TestSuite::AddTest with a string containing
///   the name of the class (which should be defined by extending
///   TestCase).
/// - To run all the test cases use the TestSuite::Run method.
/// - The method TestSuite::AllPassed returns true if all the tests pass.
//////////////////////////////////////////////////////////
class TestSuite
{
  private $testCases;    // A list of test cases.
  private $allPassed;    // True if all passed, false otherwise.
  
  //////////////////////////////////////////////////////////
  /// Constructs the test suite object.
  //////////////////////////////////////////////////////////
  public function __construct()
  {
    $this->anyFailed = false;
    $this->testCases = array();
  }
 
  //////////////////////////////////////////////////////////
  /// Returns true if all the tests pass, false otherwise.
  //////////////////////////////////////////////////////////
  public function AllPassed()
  {
    return $this->allPassed;
  }
  
  //////////////////////////////////////////////////////////
  /// Adds a test case to the test suite.
  /// \param[in] string $className Adds the test case to the suite.
  //////////////////////////////////////////////////////////
  public function AddTest($className)
  {
    $this->testCases[] = $className;
  }
  
  //////////////////////////////////////////////////////////
  /// Runs all the test case placing the results in $results.
  /// This should only be used by the TestRunner class.
  /// \param[in,out] TestSuiteResult &$results Object to store
  /// the results.
  //////////////////////////////////////////////////////////
  public function Run(TestSuiteResult &$results)
  {
    $this->allPassed = true;
    
    foreach ($this->testCases as $className)
    {
      // Loop through test cases
      
      $testCase = new $className();
      $testCaseResult = new TestCaseResultConstructor($className, $testCase->GetID());
      if (!$testCase instanceof TestCase)
      {
        throw new Exception('The class "' . $className . '" is not an instance of TestCase!');
      }
      
      // Initialise the test case
      $testCase->Init($testCaseResult);
      
      // Perform set up
      $testCaseResult->AddTimeStamp(EventType::START_SETUP());
      try
      {
        $testCase->SetUp();
        $okToRun = $testCaseResult->TestPassed();
      }
      catch (Exception $e)
      {
        $testCaseResult->SetDetails($testCase->GetName(),
                                    $testCase->GetID(),
                                    $testCase->GetDescription());
        $testCaseResult->AddException('Uncaught exception thrown in \'SetUp\'.',
                                      $e);
        $testCaseResult->AddTimeStamp(EventType::END_SETUP());
        $okToRun = false;
      }

      $testCaseResult->SetDetails($testCase->GetName(),
                                  $testCase->GetID(),
                                  $testCase->GetDescription());

      if ($okToRun)
      {
        // As SetUp ran successfully, run the test
        $testCaseResult->AddMessage('Setup completed.', EventType::SYS_MSG());
        $testCaseResult->AddTimeStamp(EventType::END_SETUP());
        try
        {
          $testCaseResult->AddTimeStamp(EventType::START_RUN());
          $testCase->Run();
          $testCaseResult->AddMessage('Test case completed.', EventType::SYS_MSG());
          $testCaseResult->AddTimeStamp(EventType::END_RUN());
        }
        catch (Exception $e)
        {
          $testCaseResult->AddException('Uncaught exception thrown in \'Run\'.',
                                        $e);
          $testCaseResult->AddTimeStamp(EventType::END_RUN());
          // Try to tear down so potentially other tests can run!
        }
      }
      else
      {
        $testCaseResult->AddMessage('Setup failed - Not running test case.', EventType::SYS_MSG());
      }

      // Tidy up
      try
      {
        $testCaseResult->AddTimeStamp(EventType::START_TEAR_DOWN());
        $testCase->TearDown();
        $testCaseResult->AddMessage('TearDown completed.', EventType::SYS_MSG());
        $testCaseResult->AddTimeStamp(EventType::END_TEAR_DOWN());
      }
      catch (Exception $e)
      {
        $testCaseResult->AddException('Uncaught exception thrown in \'TearDown\'.',
                                      $e);
        $testCaseResult->AddTimeStamp(EventType::END_TEAR_DOWN());
      }
      $results->AddTestCaseResult($testCaseResult);
      $this->allPassed = $this->allPassed && $testCaseResult->TestPassed();
      $testCase = null; // Should call the garbage collector
    }
  }
}

//////////////////////////////////////////////////////////
/// \brief Stores the results for all the test cases.
///
/// This class is to enable addition of test case results
/// to the list. It hides this ability from TestRunner so that
/// external developers of test runners are decoupled.
//////////////////////////////////////////////////////////
class TestSuiteResult
{
  protected $testCaseResults;  ///< List of all the test case results.
  
  ////////////////////////////////////////////////////////
  /// Constructs the set of test case results.
  ////////////////////////////////////////////////////////
  public function __construct()
  {
    $testCaseResults = array();
  }
  
  ////////////////////////////////////////////////////////
  /// Adds a test case result to the collection.
  /// \param[in] TestCaseResultConstructor &$result Test case
  /// results to add to the list.
  ////////////////////////////////////////////////////////
  public function AddTestCaseResult(TestCaseResultConstructor &$result)
  {
    $this->testCaseResults[] = $result;
  }
}

////////////////////////////////////////////////////////
/// \brief Enables the tests to be executed and a report produced.
///
/// Constructs a runner, to run and report all the 
/// test cases. Needs to be sub-classed to generate a report
/// of the appropriate format.
///
/// See XMLTestRunner for example.
///
/// \todo
/// Produce a test runner for rich text format.
////////////////////////////////////////////////////////
abstract class TestRunner extends TestSuiteResult
{
  //////////////////////////////////////////////////////
  /// Returns the number of test case results.
  /// \return The number of test cases in the results.
  //////////////////////////////////////////////////////
  public function GetNumberOfTestCaseResults()
  {
    return count($this->testCaseResults);
  }
  
  //////////////////////////////////////////////////////
  /// Returns a test case result for a particular test case.
  /// \param[in] integer $index An index for the test case.
  /// \return Returns a TestCaseResult.
  //////////////////////////////////////////////////////
  public function GetTestCaseResult($index)
  {
    return $this->testCaseResults[$index];
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
  /// \param[in] string $extension The extension to use for the file.
  /// \return Returns one of the following values:
  ///         - 0  - All tests passed;
  ///         - -1 - One or more tests failed;
  ///         - -2 - Unable to open file;
  ///         - -3 - Unable to write to file.
  //////////////////////////////////////////////////////
  protected function Run(TestSuite &$suite,
                         $filename,
                         $extension)
  {
    $suite->Run($this);
    $report = $this->Report();
    if ('string' != gettype($filename))
    {
      echo $report;
    }
    else
    {
      if ('' == $filename)
      {
        $filename = tempnam(sys_get_temp_dir(), 'rep');
      }
      $filename .= '.' . $extension;
      $fh = fopen($filename, 'w');
      if (!$fh)
      {
        echo 'Unable to open file to write to: ' . $filename . "\n";
        return -2;
      }
      echo 'Writing results to: ' . $filename . "\n";
      if (!fwrite($fh, $report))
      {
        echo 'Unable to write to file: ' . $filename . "\n";
        return -3;
      }
      fclose($fh);      
    }
    return ($suite->AllPassed() ? 0 : -1);
  }
  
  ////////////////////////////////////////////////////////
  /// This abstract method to be implemented to product the report
  /// in the desired format.
  ////////////////////////////////////////////////////////
  public abstract function Report();
}
?>
