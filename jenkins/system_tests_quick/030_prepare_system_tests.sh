#!/bin/bash -xue

/opt/qbase3/qshell ./jenkins/prepareSystemTests.py
mkdir -p ${WORKSPACE}/testresults
