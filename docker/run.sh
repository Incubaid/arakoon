#!/bin/bash -xue

IMAGE=$1
shift

if [ -t 1 ] ; then TTY="-t"; else TTY=""; fi

docker build --rm=true --tag=arakoon_$IMAGE ./docker/$IMAGE
docker run -i $TTY --privileged=true -e UID=${UID} \
       -v ${PWD}:/home/jenkins/arakoon \
       -w /home/jenkins/arakoon arakoon_$IMAGE \
       bash -l -c "cd arakoon && ./docker/suites.sh $@"
