#!/bin/bash -u

TIMEOUT=$1
COMMAND=$2
shift 2

timeout $TIMEOUT $COMMAND $@ > output 2>&1 &
PID=$!

echo $PID

while kill -0 $PID 2>/dev/null
do
    echo -ne .
    sleep 1
done

wait $PID
RESULT=$?

tail -n256 output

exit $RESULT
