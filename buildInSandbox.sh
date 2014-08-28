#!/bin/bash -xue

ocamlbuild -use-ocamlfind arakoon.native

mkdir -p /opt/qbase3/apps/arakoon/bin/
cp arakoon.native /opt/qbase3/apps/arakoon/bin/arakoon
cp -r pylabs/extensions/* /opt/qbase3/lib/pymonkey/extensions/
cp pylabs/Compat.py /opt/qbase3/lib/pymonkey/extensions/Compat.py

mkdir -p /opt/qbase3/var/tests/arakoon_system_tests/
touch /opt/qbase3/var/tests/arakoon_system_tests/__init__.py
cp -r pylabs/test/* /opt/qbase3/var/tests/arakoon_system_tests/
mkdir -p /opt/qbase3/lib/python/site-packages/
ln -s `pwd`/pylabs/pyrakoon/pyrakoon /opt/qbase3/lib/python/site-packages/pyrakoon

ARAKOON_PACKAGE=/opt/qbase3/lib/python/site-packages/arakoon
mkdir -p $ARAKOON_PACKAGE
touch $ARAKOON_PACKAGE/__init__.py
cat > $ARAKOON_PACKAGE/Arakoon.py << EOF
from pyrakoon.compat import *
EOF
cat > $ARAKOON_PACKAGE/ArakoonExceptions.py << EOF
from pyrakoon.compat import *
EOF
cat > $ARAKOON_PACKAGE/ArakoonProtocol.py << EOF
from pyrakoon.compat import *
EOF
