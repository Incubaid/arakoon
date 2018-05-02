#!/bin/bash -xue

# this script is executed at each startup of the container

# hack to make sure we have access to files in the jenkins home directory
# the UID of jenkins in the container should match our UID on the host
if [ ${UID} -ne 1500 ]
then
    cat /etc/passwd
    sed -i "s/x:1500:/x:${UID}:/" /etc/passwd

    chown ${UID} /home/jenkins
    chown ${UID} /home/jenkins/OPAM
    chown ${UID} /home/jenkins/.bash* || true
fi

# finally execute the command the user requested
command="cd arakoon && ./docker/suites.sh $@"
sudo -E -i -u jenkins bash -l -c "$command"
