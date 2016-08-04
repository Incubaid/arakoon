#!/bin/bash -xue

install () {
    echo "Running 'install' phase"

    ./docker/run.sh ubuntu-16.04 build
}

script () {
    echo "Running 'script' phase"

    ./docker/run.sh ubuntu-16.04 unit-travis
}

case "$1" in
    install)
        install
        ;;
    script)
        script
        ;;
    *)
        echo "Usage: $0 {install|script}"
        exit 1
esac
