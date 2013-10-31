#!/bin/bash -xue

rm -rf pyrakoon

git clone git://github.com/Incubaid/pyrakoon.git pyrakoon
pushd pyrakoon
git describe --all --long --always --dirty
popd

rm -rf /opt/qbase3/lib/python/site-packages/arakoon/
mkdir -p /opt/qbase3/lib/python/site-packages/arakoon/

ln -s `pwd`/pyrakoon/pyrakoon /opt/qbase3/lib/python/site-packages/pyrakoon

pushd /opt/qbase3/lib/python/site-packages/arakoon
touch __init__.py
cat > Arakoon.py << EOF
from pyrakoon.compat import ArakoonClient, ArakoonClientConfig
EOF
popd
