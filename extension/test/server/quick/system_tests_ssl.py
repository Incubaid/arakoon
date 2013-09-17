import subprocess
import time
#from .. import system_tests_common as C

def make_config(config_fn, arakoon_root, ca_path, ca_path2):
    with open(config_fn,'w') as f:
        contents = """
[global]
cluster = arakoon_0, arakoon_1, arakoon_2
cluster_id = arakoon_tls_test
tls_ca_cert = %s/cacert.pem
tls_service = true
tls_service_validate_peer = true

[arakoon_0]
ip = 127.0.0.1
client_port = 4000
messaging_port = 4010
home = %s/arakoon_0
log_level = debug
tls_cert = %s/arakoon_0.pem
tls_key  = %s/arakoon_0.key

[arakoon_1]
ip = 127.0.0.1
client_port = 4001
messaging_port = 4011
home = %s/arakoon_1
log_level = debug
tls_cert = %s/arakoon_1.pem
tls_key  = %s/arakoon_1.key

[arakoon_2]
ip = 127.0.0.1
client_port = 4002
messaging_port = 4012
home = %s/arakoon_2
log_level = debug
tls_cert = %s/arakoon_2.pem
tls_key  = %s/arakoon_2.key
"""
        filled_in = contents % (ca_path,
                                arakoon_root, ca_path, ca_path,
                                arakoon_root, ca_path, ca_path,
                                arakoon_root, ca_path2, ca_path2)
        f.write(filled_in)



def shell(cmd):
    rc = subprocess.check_call(cmd)
    return rc

sub = '/C=BE/ST=Oost-Vlaanderen/L=Lochristi/O=Incubaid BVBA/OU=Arakoon Testing/CN=Arakoon Testing CA'

def make_ca_key(root):
    cmd = ["openssl", "req", "-new" ,
           "-nodes", "-out","%s/cacert-req.pem" % root,
           "-keyout","%s/cacert.key" % root,
           "-subj", sub]
    shell(cmd)

def make_ca_csr(root):
    cmd=["openssl", "x509",
         "-signkey", "%s/cacert.key" % root,
         "-req",
         "-in", "%s/cacert-req.pem" % root,
         "-out", "%s/cacert.pem" % root]
    shell(cmd)

def remove_ca_csr(root):
    cmd=["rm","%s/cacert-req.pem" % root]
    shell(cmd)

def make_key_csr(root, i):
    cmd=["openssl","req",
         "-out", "%s/arakoon_%i-req.pem" % (root,i),
         "-new","-nodes",
         "-keyout","%s/arakoon_%i.key" % (root,i),
         "-subj", sub]
    shell(cmd)

def sign_csr(root,i):
    cmd = ["openssl","x509", "-req",
           "-in", "%s/arakoon_%i-req.pem" % (root,i),
           "-CA", "%s/cacert.pem" % root,
           "-CAkey", "%s/cacert.key" % root,
           "-out" , "%s/arakoon_%i.pem" % (root,i) ,
           "-set_serial", "0%i" % i]
    shell(cmd)
    shell(["rm","%s/arakoon_%i-req.pem" % (root,i)])

def verify(root,i):
    shell(["openssl", "verify", "-CAfile", "%s/cacert.pem" % root,
           "%s/arakoon_%i.pem" % (root,i)])


arakoon_root = "/tmp/arakoon"
config_fn = "%s/arakoon_tls.ini" % (arakoon_root, )

def setup():
    shell(["rm","-rf", arakoon_root])
    shell(["mkdir","-p", arakoon_root])
    home =["%s/arakoon_%i" % (arakoon_root,i) for i in range(3)]

    for i in range(3):
        shell(["mkdir","-p", home[i]])

    make_config(config_fn,
                arakoon_root,
                arakoon_root,
                arakoon_root + "2")

    make_ca_key(arakoon_root)
    make_ca_csr(arakoon_root)
    remove_ca_csr(arakoon_root)
    for i in range(3):
        make_key_csr(arakoon_root, i)
        sign_csr(arakoon_root,i)
        verify(arakoon_root,i)


def teardown():
    shell(["pkill", "-9", "arakoon"])

arakoon_bin = "/home/romain/workspace/ARAKOON/arakoon/arakoon.native"

def start_node(i):
    cmd = [arakoon_bin,"-config", config_fn,
           "--node", "arakoon_%i" % (i,)]
    p = subprocess.Popen(cmd)
    return p

def test_ssl_1():
    arakoon_0 = start_node(0)
    time.sleep(1)
    p = subprocess.Popen(["openssl", "s_client",
                          "-connect", "localhost:4010",
                          "-CAfile", "%s/cacert.pem" % arakoon_root,
                          "-cert", "%s/arakoon_1.pem" % arakoon_root,
                          "-key", "%s/arakoon_1.key" % arakoon_root],
                         stdout = subprocess.PIPE)
    data =p.stdout.read()
    p.wait()
    lines = data.split("\n")
    if lines[-3].find("0 (ok)") < 0:
        raise Exception("failed")

    arakoon_0.kill()

def cmd_line_client(ara_cmd):
    cmd = [arakoon_bin, "-config", config_fn,
           "-tls-ca-cert","%s/cacert.pem" % arakoon_root,
           "-tls-cert",   "%s/arakoon_2.pem" % arakoon_root,
           "-tls-key",    "%s/arakoon_2.key" % arakoon_root
          ]
    cmd.extend(ara_cmd)
    shell(cmd)

def _assert_master():
    cmd_line_client(["--who-master"])

def test_ssl_2():
    arakoon_0 = start_node(0)
    arakoon_1 = start_node(1)
    _assert_master()
    arakoon_0.kill()
    arakoon_1.kill()

def test_ssl_3():
    arakoon_0 = start_node(0)
    arakoon_1 = start_node(1)
    _assert_master()

    arakoon_0.kill()
    arakoon_0 = start_node(0)
    time.sleep(11)
    _assert_master()

def test_ssl_4():
    todo = '''
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

'''
    raise Exception("todo")

def test_ssl_5():
    todo = '''
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
'''
    raise Exception("todo")

if __name__ == "__main__":
    setup()
    test_ssl_2()
