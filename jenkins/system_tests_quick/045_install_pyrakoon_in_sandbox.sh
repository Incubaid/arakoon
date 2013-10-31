#!/bin/bash -xue

rm -rf pyrakoon

git clone git://github.com/Incubaid/pyrakoon.git pyrakoon
pushd pyrakoon
git describe --all --long --always --dirty
popd

ln -s `pwd`/pyrakoon/pyrakoon /opt/qbase3/lib/python/site-packages/pyrakoon

/opt/qbase3/bin/python << EOF
import inspect

import arakoon
import pyrakoon

print '\'arakoon\' at', inspect.getfile(arakoon)
print '\'pyrakoon\' at', inspect.getfile(pyrakoon)
EOF
