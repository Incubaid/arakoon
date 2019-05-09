#!/bin/bash
set -ue
[[ ${JENKINS_PARAM-} =~ shell_trace ]] && set -x
[[ ${JENKINS_PARAM-} =~ env_trace ]] && env | sort

git submodule init
git submodule update

if [[ ! ${JENKINS_PARAM-} =~ no_make ]]; then
    if [[ ! ${JENKINS_PARAM-} =~ no_clean ]]; then
	make clean
    fi
    make arakoon.native
fi
