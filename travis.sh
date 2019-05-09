#!/bin/bash -xue
OUTPUT=temp_file.txt

timeout_with_progress () (
    set +x
    
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

    tail -n512 $OUTPUT

    return $RESULT
)

install () {
    echo "Running 'install' phase"

    date

    START_BUILD=$(date +%s.%N)
    echo $START_BUILD

    timeout_with_progress 9000 ./docker/run.sh ubuntu-16.04 clean

    END_BUILD=$(date +%s.%N)
    echo "build stopped after $END_BUILD"

}

script () {
    echo "Running 'script' phase"

    date

    timeout_with_progress 9000 ./docker/run.sh ubuntu-16.04 unit

    date
}


after_failure () {
    echo "Something went wrong"
    url= `cat temp_file.txt | nc termbin.com 9999`
    echo $url
}

case "${1-undefined}" in
    install)
        install
        ;;
    script)
        script
        ;;
    after_failure)
        after_failure
        ;;
    *)
        echo "Usage: $0 {install|script}"
        exit 1
esac
