#!/bin/bash -xue

#TODO Fill in job name, e.g. 'base' or 'php'
JOB=...

for script in `ls jenkins/$JOB/* | grep -e "\/[0-9]\{3\}_"`; do
    cd "$WORKSPACE"
    $script
done
