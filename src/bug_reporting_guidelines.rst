===============================
How To Create a Good Bug Report
===============================
Please insure to take into consideration at least six of the points mentioned
below in your bug report. Incomplete bug reports are very hard to understand,
reproduce and fix.

- Use `<http://jira.incubaid.com>`_ to report the bug.

- Submit a test case, the smaller, the better. You can do this by clicking on
  the "Create a new attachment" link. When attaching a file, please set the
  correct MIME type from the list. For source code, "Plain text" is okay.

- Attach your Arakoon configuration files used on every Arakoon instance.

- Fill in the revision field of your Arakoon setup. This can be found by
  executing the command 'arakoon --version'.

- If the test involves libraries or assemblies that are not part of Arakoon,
  add infomation on where to download all dependencies, and how to
  compile/install them.

- Provide information about the version of the software you're using. This
  applies to both Arakoon and the operating system or relevant libraries.

- Provide the output you expect the test case to produce.

- Provide the actual output you get from the test case.

- If you are new to bug reporting, understand how you should set "priority"
  for your report. People tend to set CRITICAL or BLOCKER where they should
  not. Please keep in mind that no matter how important the bug is for you,
  it has nothing to do with the importance of the bug itself.

- Do not expect us to debug your software. We can not debug every application
  that is submitted to us, to improve the response time, you should create a
  self-contained test case that isolates the problem.
