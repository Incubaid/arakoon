#!/bin/bash -xue

ocamlbuild -use-ocamlfind arakoon.native

sudo mkdir -p /opt/qbase3/apps/arakoon/bin/
sudo cp arakoon.native /opt/qbase3/apps/arakoon/bin/arakoon
sudo cp -r pylabs/extensions/* /opt/qbase3/lib/pymonkey/extensions/
sudo cp pylabs/Compat.py /opt/qbase3/lib/pymonkey/extensions/Compat.py

sudo mkdir -p /opt/qbase3/var/tests/arakoon_system_tests/
sudo touch /opt/qbase3/var/tests/arakoon_system_tests/__init__.py
sudo cp -r pylabs/test/* /opt/qbase3/var/tests/arakoon_system_tests/
sudo mkdir -p /opt/qbase3/lib/python/site-packages/arakoon/
sudo cp src/client/python/*.py /opt/qbase3/lib/python/site-packages/arakoon/

