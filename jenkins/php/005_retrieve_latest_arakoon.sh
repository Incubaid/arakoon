#!/bin/bash -xue

BRANCH=$(hg branch)
LATEST_ARAKOON_URI=$JENKINS_URL/view/arakoon-${BRANCH}/job/arakoon-${BRANCH}-base/lastSuccessfulBuild/artifact/arakoon
TEST_BINARY=./src/client/php/test/arakoon

echo JENKINS_USER=${JENKINS_USER}
# Fails due to Jenkins auth...
#test -f $TEST_BINARY && rm $TEST_BINARY
#wget -O $TEST_BINARY $LATEST_ARAKOON_URI --user=${JENKINS_USER} --password=${JENKINS_PASSWORD} 

#chmod a+x $TEST_BINARY
