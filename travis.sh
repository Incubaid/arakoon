#!/bin/bash -xue

install () {
    echo "Running 'install' phase"

    date

    if [[ -e ~/cache/image.tar.gz ]];
    then docker load -i ~/cache/image.tar.gz; fi

    date

    ./docker/run.sh ubuntu-16.04 build

    date
}

script () {
    echo "Running 'script' phase"

    date

    ./docker/run.sh ubuntu-16.04 unit-travis

    date
}

before_cache () {
    echo "Running 'before_cache' phase"

    date

    mkdir ~/cache || true
    docker save -o ~/cache/image.tar.gz arakoon_ubuntu-16.04
    ls -ahl ~/cache

    date
}

case "$1" in
    install)
        install
        ;;
    script)
        script
        ;;
    before_cache)
        before_cache
        ;;
    *)
        echo "Usage: $0 {install|script}"
        exit 1
esac
