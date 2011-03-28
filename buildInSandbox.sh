#!/bin/bash
set -e


ocamlbuild $@ arakoon.native

mkdir -p /opt/qbase3/apps/arakoon/bin/
cp arakoon.native /opt/qbase3/apps/arakoon/bin/arakoon
mkdir -p /opt/qbase3/lib/pymonkey/extensions/arakoon/server/
mkdir -p /opt/qbase3/lib/pymonkey/extensions/arakoon/client/
cp -r extension/server/* /opt/qbase3/lib/pymonkey/extensions/arakoon/server/
cp -r extension/client/* /opt/qbase3/lib/pymonkey/extensions/arakoon/client/

mkdir -p /opt/qbase3/var/tests/arakoon_system_tests/
touch /opt/qbase3/var/tests/arakoon_system_tests/__init__.py
cp -r extension/test/* /opt/qbase3/var/tests/arakoon_system_tests/
mkdir -p /opt/qbase3/lib/python/site-packages/arakoon/
cp src/client/python/*.py /opt/qbase3/lib/python/site-packages/arakoon/

