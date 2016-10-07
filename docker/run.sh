#!/bin/bash -xue

IMAGE=$1
shift

docker build --rm=true --tag=arakoon_$IMAGE ./docker/$IMAGE

if [ -t 1 ];
then TTY="-t";
else
    # this path is taken on jenkins, clean previous builds first
    TTY="";
    docker run -i $TTY --privileged=true -e UID=${UID} \
           -v ${PWD}:/home/jenkins/arakoon \
           -w /home/jenkins/arakoon arakoon_$IMAGE \
           bash clean
fi


docker run -i $TTY --privileged=true -e UID=${UID} \
       --env ARAKOON_PYTHON_CLIENT \
       -v ${PWD}:/home/jenkins/arakoon \
       arakoon_$IMAGE \
       bash $@
