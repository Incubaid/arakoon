#!/bin/bash -xue

WORKDIR=`pwd`
CA_PATH=`mktemp -d --tmpdir=. --suffix=ca`
CA_PATH2=`mktemp -d --tmpdir=. --suffix=ca`
NODES_PATH=`mktemp -d --tmpdir=. --suffix=nodes`

function cleanup {
    set +e
    echo "Cleaning up"
    kill `jobs -p`
    sleep 1
    kill -9 `jobs -p`
    rm -rf $CA_PATH $CA_PATH2 $NODES_PATH arakoon_tls.ini
}

trap cleanup EXIT

mkdir $NODES_PATH/arakoon_0
mkdir $NODES_PATH/arakoon_1
mkdir $NODES_PATH/arakoon_2

pushd $CA_PATH
$WORKDIR/tools/mkcerts.sh
popd

pushd $CA_PATH2
$WORKDIR/tools/mkcerts.sh
popd

cat > arakoon_tls.ini << EOF
[global]
cluster = arakoon_0, arakoon_1, arakoon_2
cluster_id = arakoon_tls_test
tls_ca_cert = $CA_PATH/cacert.pem
tls_service = true
tls_service_validate_peer = true

[arakoon_0]
ip = 127.0.0.1
client_port = 4000
messaging_port = 4010
home = $NODES_PATH/arakoon_0
log_level = debug
tls_cert = $CA_PATH/arakoon0.pem
tls_key = $CA_PATH/arakoon0.key

[arakoon_1]
ip = 127.0.0.1
client_port = 4001
messaging_port = 4011
home = $NODES_PATH/arakoon_1
log_level = debug
tls_cert = $CA_PATH/arakoon1.pem
tls_key = $CA_PATH/arakoon1.key

[arakoon_2]
ip = 127.0.0.1
client_port = 4002
messaging_port = 4012
home = $NODES_PATH/arakoon_2
log_level = debug
tls_cert = $CA_PATH2/arakoon2.pem
tls_key = $CA_PATH2/arakoon2.key
EOF

CLIENT_TLS_OPTS="-tls-ca-cert $CA_PATH/cacert.pem -tls-cert $CA_PATH/arakoon2.pem -tls-key $CA_PATH/arakoon2.key"

echo "Test #1: Make sure node listens using TLS & presents cert"
./arakoon.native -config arakoon_tls.ini --node arakoon_0 &
ARAKOON0_PID=$!
sleep 1
echo -n "" | openssl s_client -connect localhost:4010 -CAfile $CA_PATH/cacert.pem -cert $CA_PATH/arakoon1.pem -key $CA_PATH/arakoon1.key 2>&1 | grep "Verify return code: 0"
kill $ARAKOON0_PID
sleep 1

echo "Test #2: Make sure nodes can communicate"
./arakoon.native -config arakoon_tls.ini --node arakoon_0 &
ARAKOON0_PID=$!
./arakoon.native -config arakoon_tls.ini --node arakoon_1 &
ARAKOON1_PID=$!
sleep 2
./arakoon.native -config arakoon_tls.ini $CLIENT_TLS_OPTS --who-master
kill $ARAKOON0_PID
kill $ARAKOON1_PID
sleep 1

echo "Test #3: Make sure reconnection works"
./arakoon.native -config arakoon_tls.ini --node arakoon_0 &
ARAKOON0_PID=$!
./arakoon.native -config arakoon_tls.ini --node arakoon_1 &
ARAKOON1_PID=$!
sleep 2
./arakoon.native -config arakoon_tls.ini $CLIENT_TLS_OPTS --who-master
kill $ARAKOON1_PID
sleep 11
./arakoon.native -config arakoon_tls.ini $CLIENT_TLS_OPTS --who-master 2>&1 | grep "No Master"
./arakoon.native -config arakoon_tls.ini --node arakoon_1 &
ARAKOON1_PID=$!
sleep 2
./arakoon.native -config arakoon_tls.ini $CLIENT_TLS_OPTS --who-master
kill $ARAKOON0_PID
kill $ARAKOON1_PID
sleep 1

echo "Test #4: Make sure certs are validated"
# Can't use tls_service_validate_peer in this test: client can connect to both
# nodes, one of them will reject the connection due to certificate mismatch.
cat > arakoon_tls.ini << EOF
[global]
cluster = arakoon_0, arakoon_1, arakoon_2
cluster_id = arakoon_tls_test
tls_ca_cert = $CA_PATH/cacert.pem

[arakoon_0]
ip = 127.0.0.1
client_port = 4000
messaging_port = 4010
home = $NODES_PATH/arakoon_0
log_level = debug
tls_cert = $CA_PATH/arakoon0.pem
tls_key = $CA_PATH/arakoon0.key

[arakoon_1]
ip = 127.0.0.1
client_port = 4001
messaging_port = 4011
home = $NODES_PATH/arakoon_1
log_level = debug
tls_cert = $CA_PATH/arakoon1.pem
tls_key = $CA_PATH/arakoon1.key

[arakoon_2]
ip = 127.0.0.1
client_port = 4002
messaging_port = 4012
home = $NODES_PATH/arakoon_2
log_level = debug
tls_cert = $CA_PATH2/arakoon2.pem
tls_key = $CA_PATH2/arakoon2.key
EOF

./arakoon.native -config arakoon_tls.ini --node arakoon_0 &
ARAKOON0_PID=$!
./arakoon.native -config arakoon_tls.ini --node arakoon_2 &
ARAKOON2_PID=$!
sleep 11
./arakoon.native -config arakoon_tls.ini --who-master 2>&1 | grep "No Master"
kill $ARAKOON0_PID
kill $ARAKOON2_PID
sleep 1


echo "Test #5: Python client"
cat > arakoon_tls.ini << EOF
[global]
cluster = arakoon_0, arakoon_1, arakoon_2
cluster_id = arakoon_tls_test
tls_ca_cert = $CA_PATH/cacert.pem
tls_service = true
tls_service_validate_peer = true

[arakoon_0]
ip = 127.0.0.1
client_port = 4000
messaging_port = 4010
home = $NODES_PATH/arakoon_0
log_level = debug
tls_cert = $CA_PATH/arakoon0.pem
tls_key = $CA_PATH/arakoon0.key

[arakoon_1]
ip = 127.0.0.1
client_port = 4001
messaging_port = 4011
home = $NODES_PATH/arakoon_1
log_level = debug
tls_cert = $CA_PATH/arakoon1.pem
tls_key = $CA_PATH/arakoon1.key

[arakoon_2]
ip = 127.0.0.1
client_port = 4002
messaging_port = 4012
home = $NODES_PATH/arakoon_2
log_level = debug
tls_cert = $CA_PATH/arakoon2.pem
tls_key = $CA_PATH/arakoon2.key
EOF

./arakoon.native -config arakoon_tls.ini --node arakoon_0 &
ARAKOON0_PID=$!
./arakoon.native -config arakoon_tls.ini --node arakoon_1 &
ARAKOON1_PID=$!
sleep 2

PYTHONPATH=src/client/python python << EOF
import Arakoon
config = Arakoon.ArakoonClientConfig(
    'arakoon_tls_test',
    { 'arakoon_0': (['127.0.0.1'], 4000),
      'arakoon_1': (['127.0.0.1'], 4001) },
    tls=True, tls_ca_cert='$CA_PATH/cacert.pem',
    tls_cert=('$CA_PATH/arakoon2.pem', '$CA_PATH/arakoon2.key'))
client = Arakoon.ArakoonClient(config=config)
print 'Master:', client.whoMaster()
client.set('tls_test_key', 'tls_test_value')
assert client.get('tls_test_key') == 'tls_test_value'
EOF

kill $ARAKOON0_PID
kill $ARAKOON1_PID

sleep 1
