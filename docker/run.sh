#!/bin/bash -xue

IMAGE=$1
shift

docker build --rm=true \
       --tag=arakoon_$IMAGE \
       -f ./docker/$IMAGE/Dockerfile ./docker/ \


if [ -t 1 ];
then TTY="-t";
else TTY="";
fi

docker run -i $TTY --privileged=true -e UID=${UID} \
       --env ARAKOON_PYTHON_CLIENT \
       --env TRAVIS \
       -v ${PWD}:/home/jenkins/arakoon \
       arakoon_$IMAGE \
       bash $@
