#!/bin/bash -l
# this script is executed at each startup of the container

trap 'rc=$?; echo "ERR at line ${LINENO} (rc: $rc)"; exit $rc' ERR
set -e

if [ $HOST_UID -ne $UID ]; then
    echo "UID mismatch; please build and run container under same UID" 2>&1
    exit 1
fi

cd $HOME/arakoon

export TEST_HOME='/home/jenkins/arakoon/TESTS'
export OPAMROOT=/home/jenkins/OPAM

eval $(opam config env --root=${OPAMROOT})

echo $PATH

# finally execute the command the user requested
cmd=${1-bash}
case "$cmd" in
  bash|sh)
	shift || true
	exec $cmd "$@"
	;;
  clean)
	make clean
	;;
  build)
	make build
	;;
  *)    # everything else is considered as a jenkins suite
	docker/suites.sh "$@"
	;;
esac
