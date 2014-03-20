#!/bin/bash -xue

echo $PWD
#source ./_virtualenv/bin/activate

python test_it.py --with-xunit --xunit-file=$PWD/kwik.xml ./server/quick
