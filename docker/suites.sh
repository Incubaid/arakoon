#!/bin/bash -xue

export TEST_HOME='/home/tests'

case "${1-bash}" in
    bash)
        bash
        ;;
    clean)
        make clean
        ;;
    build)
        make build
        ;;
    unit)
        make build
        ./arakoon.native --run-all-tests-xml testresults.xml || true
        cat testresults.xml
        ;;
    unit-travis)
        ./arakoon.native --run-all-tests 2>&1 | tail -n256
        exit $PIPESTATUS
        ;;
    kwik)
        ./jenkins/kwik.sh
        ;;
    b)
        ./jenkins/B.sh
        ;;
    c)
        ./jenkins/C.sh
        ;;
    d)
        ./jenkins/D.sh
        ;;
    package_deb)
        ./jenkins/package_deb.sh
        ;;
    package_rpm)
        ./jenkins/package_rpm.sh
        ;;
    *)
        echo "invalid test suite specified"
esac
