#!/bin/bash -xue

/opt/qbase3/qshell -c "
q.testrunner.list()

test_spec = 'arakoon_system_tests.server.right'

output_format = q.enumerators.TestRunnerOutputFormat.XML
output_folder = '${WORKSPACE}/testresults'

arguments = q.testrunner._parseArgs('%s_results' % test_spec, output_format, output_folder, False)

arguments.extend([
    '--logging-format=%(asctime)-15s %(message)s',
    '--with-coverage',
    '--cover-package=arakoon',
    '--cover-erase',
    '--with-xcoverage',
])
arguments.append(q.testrunner._convertTestSpec(test_spec))

q.testrunner._runTests(arguments)
"

RESULT=$?

test -f /opt/qbase3/var/tests/coverage.xml && mv /opt/qbase3/var/tests/coverage.xml ${WORKSPACE}/coverage.xml

cat ${WORKSPACE}/coverage.xml | python ${WORKSPACE}/_tools/fix_coverage.py "${WORKSPACE}" ".opt.qbase3.lib.python.site-packages." ".var.hudson.workspace.pyrakoon-tip-system-tests.python_env.pyrakoon." > ${WORKSPACE}/coverage_fixed.xml

if test $RESULT -eq 0; then true; else false; fi
