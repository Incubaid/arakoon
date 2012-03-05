#!/bin/bash -xue

#What to do here? sandbox arakoon or not ???

#LATEST_ARAKOON_URI=$JENKINS_URL/view/arakoon-1.0/job/arakoon-1.0-base/lastSuccessfulBuild/artifact/arakoon
#TEST_BINARY=/opt/qbase3/apps/arakoon/bin/arakoon

# Fails due to Jenkins auth...
#test -f $TEST_BINARY && rm $TEST_BINARY
#wget -O $TEST_BINARY $LATEST_ARAKOON_URI

#chmod a+x $TEST_BINARY
