#!/bin/bash -e

source /opt/arakoon_doc_env/bin/activate

cd doc
make html
make latexpdf

deactivate
