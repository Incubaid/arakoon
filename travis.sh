#!/bin/bash -xue

install () {
    echo "Running 'install' phase"

    date

    if [[ -e ~/cache/image.tar.gz ]];
    then docker load -i ~/cache/image.tar.gz; fi

    START_BUILD=date
    export START_BUILD
    echo $START_BUILD

    ./run_with_timeout_and_progress.sh 9000 ./docker/run.sh ubuntu-16.04 clean

    END_BUILD=date
    export END_BUILD
    echo $END_BUILD
}

script () {
    echo "Running 'script' phase"

    date

    ./run_with_timeout_and_progress.sh 9000 ./docker/run.sh ubuntu-16.04 unit

    date
}

before_cache () {
    echo "Running 'before_cache' phase"

    date

    DIFF=$(echo "$END_BUILD - $START_BUILD" | bc)
    if [ $DIFF \> 5 ]
    then
        mkdir ~/cache || true
        docker save -o ~/cache/image.tar.gz arakoon_ubuntu-16.04
        ls -ahl ~/cache;
    else
        echo Building of container took only $DIFF seconds, not updating cache this time;
    fi

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
