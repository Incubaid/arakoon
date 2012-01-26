#!/bin/bash

set -e

BRANCH="default"
ARAKOON_BUILD_ENV_ROOT="/opt/arakoon_build_env/${BRANCH}"

get_latest_build_env () {
	sudo mkdir -p "${ARAKOON_BUILD_ENV_ROOT}"
	sudo chown -R "`whoami`" "${ARAKOON_BUILD_ENV_ROOT}"
	cd "${ARAKOON_BUILD_ENV_ROOT}"
	rm -Rf OCAML
	rm -f arakoon-${BRANCH}-build-env-LATEST.tar.gz
	wget -nv http://fileserver.incubaid.com/arakoon/build_envs/default/arakoon-${BRANCH}-build-env-LATEST.tar.gz
	tar -xvzf arakoon-${BRANCH}-build-env-LATEST.tar.gz
	cd -
}

get_latest_build_env
