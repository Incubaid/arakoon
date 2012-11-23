#!/bin/bash
set -e


ocamlbuild $@ -use-ocamlfind arakoon.native

sudo mkdir -p /opt/qbase3/apps/arakoon/bin/
sudo cp arakoon.native /opt/qbase3/apps/arakoon/bin/arakoon
sudo mkdir -p /opt/qbase3/lib/pymonkey/extensions/arakoon/server/
sudo mkdir -p /opt/qbase3/lib/pymonkey/extensions/arakoon/client/
sudo cp -r extension/server/* /opt/qbase3/lib/pymonkey/extensions/arakoon/server/
sudo cp -r extension/client/* /opt/qbase3/lib/pymonkey/extensions/arakoon/client/

sudo mkdir -p /opt/qbase3/var/tests/arakoon_system_tests/
sudo touch /opt/qbase3/var/tests/arakoon_system_tests/__init__.py
sudo cp -r extension/test/* /opt/qbase3/var/tests/arakoon_system_tests/
sudo mkdir -p /opt/qbase3/lib/python/site-packages/arakoon/
sudo cp src/client/python/*.py /opt/qbase3/lib/python/site-packages/arakoon/

