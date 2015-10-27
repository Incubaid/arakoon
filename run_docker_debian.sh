PROJECT_HOME=/home/romain/workspace/ARAKOON/arakoon
INSIDE=/home/jenkins/workspace/the_job/
docker run --rm=true --interactive=true --privileged -t -v \
       ${PROJECT_HOME}:${INSIDE} \
       --env "SUITE=$1" \
       -w ${INSIDE} arakoon_debian $@
