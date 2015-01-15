#!/bin/bash

set -e

for f in $(ls ./jenkins/$1);
do
   echo $f
   ./jenkins/$1/$f
done;
