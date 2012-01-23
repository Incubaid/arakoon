#!/bin/bash -xue

LATEST_ARAKOON_URI=http://cimaster.incubaid.com/view/arakoon-1.0/job/arakoon-1.0-base/lastSuccessfulBuild/artifact/arakoon
TEST_BINARY=src/client/php/test/arakoon

test -f $TEST_BINARY && rm $TEST_BINARY
wget -O $TEST_BINARY $LATEST_ARAKOON_URI
chmod a+x $TEST_BINARY
