#!/bin/bash -ex

$VENV=_virtualenv

if [ ! -d $VENV ]; then
        virtualenv --no-site-packages $VENV
        source $VENV/bin/activate

        pip install sphinx

        deactivate
fi

source $VENV/bin/activate

cd doc
make html
make latexpdf

deactivate
