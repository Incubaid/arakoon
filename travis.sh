#!/bin/bash -xue

timeout_with_progress () (
    set +x
    OUTPUT=`mktemp`
    timeout "$@" > $OUTPUT 2>&1 &
    PID=$!

    echo $PID

    while kill -0 $PID 2>/dev/null
    do
	echo -ne .
	sleep 1
    done

    wait $PID
    RESULT=$?

    tail -n256 $OUTPUT

    return $RESULT
)

install () {
    echo "Running 'install' phase"

    date

    if [ "$USE_CACHE" == "1" ];
    then
        if [[ -e ~/cache/image.tar.gz ]];
        then docker load -i ~/cache/image.tar.gz; fi;
    fi

    START_BUILD=$(date +%s.%N)
    echo $START_BUILD

    timeout_with_progress 9000 ./docker/run.sh ubuntu-16.04 clean

    END_BUILD=$(date +%s.%N)
    echo $END_BUILD

    if [ "$USE_CACHE" == "1" ];
    then
        DIFF=$(echo "$END_BUILD - $START_BUILD" | bc)
        if [ $DIFF \> 5 ]
        then
            df -h
            mkdir ~/cache || true
            rm -f ~/cache/image.tar.gz
            docker save -o ~/cache/image.tar.gz arakoon_ubuntu-16.04
            ls -ahl ~/cache;
        else
            echo Building of container took only $DIFF seconds, not updating cache this time;
        fi;
    fi
}

script () {
    echo "Running 'script' phase"

    date

    timeout_with_progress 9000 ./docker/run.sh ubuntu-16.04 unit

    date
}

case "${1-undefined}" in
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
