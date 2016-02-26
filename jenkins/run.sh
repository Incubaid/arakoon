#!/bin/bash

set -e

./jenkins/common.sh

for f in $(ls ./jenkins/$1);
do
    extension="${f##*.}"
    if [ $extension = "sh" ]; then
        echo $f
        ./jenkins/$1/$f
    fi
done;
