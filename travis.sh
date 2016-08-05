#!/bin/bash -xue

install () {
    echo "Running 'install' phase"

    date

    if [[ -e ~/cache/image.tar.gz ]];
    then docker load -i ~/cache/image.tar.gz; fi

    date

    ./docker/run.sh ubuntu-16.04 clean | tail -n256

    date
}

script () {
    echo "Running 'script' phase"

    date

    ./docker/run.sh ubuntu-16.04 unit 2&>1 | tail -n256
    RESULT=$PIPESTATUS

    date

    exit $RESULT
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
