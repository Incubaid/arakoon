#!/bin/bash -ue

echo "Warning"
echo "======="
echo "This utility script should not be used to generate certificates"
echo "for a real production setup. It should be considered insecure"
echo "and has not been audited."
echo

run () {
    local oifs=$IFS
    local msg=$1
    shift
    echo "$msg:"
    IFS=" "
    echo "> $*"
    echo
    IFS=""
    $*
    echo
    IFS=$oifs
}

run "Creating CA key & CSR" \
    openssl req -new -nodes -out cacert-req.pem -keyout cacert.key -subj '/C=BE/ST=Oost-Vlaanderen/L=Lochristi/O=Incubaid BVBA/OU=Arakoon Testing/CN=Arakoon Testing CA'

run "Self-signing CA CSR" \
    openssl x509 -signkey cacert.key -req -in cacert-req.pem -out cacert.pem
run "Removing CA CSR" \
    rm cacert-req.pem

for i in `seq 0 2`; do
    run "Creating key and CSR for arakoon$i" \
        openssl req -out arakoon$i-req.pem -new -nodes -keyout arakoon$i.key -subj "/C=BE/ST=Oost-Vlaanderen/L=Lochristi/O=Incubaid BVBA/OU=Arakoon Testing/CN=arakoon$i"
    run "Signing CSR for arakoon$i" \
        openssl x509 -req -in arakoon$i-req.pem -CA cacert.pem -CAkey cacert.key -out arakoon$i.pem -set_serial 0$i
    run "Removing CSR for arakoon$i" \
        rm arakoon$i-req.pem
    run "Verifying certificate" \
        openssl verify -CAfile cacert.pem arakoon$i.pem
done
