"""
Copyright (2010-2014) INCUBAID BVBA

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""


import os.path
import time
import errno
import shutil
import socket
import logging
import tempfile
import unittest
import contextlib


from .. import system_tests_common as C
from Compat import X

LOGGER = logging.getLogger(__name__)
CONFIG = C.CONFIG
class Timeout(Exception): pass

def wait_for_server(host, port, timeout):
    start = time.time()
    end = start + timeout

    ok = False

    while time.time() <= end:
        try:
            s = socket.create_connection(
                (host, port), timeout=(end - time.time()))
            s.close()
            ok = True
            break
        except:
            LOGGER.exception('Failure to create socket')

        time.sleep(0.1)

    if not ok:
        raise Timeout('Unable to connect to %s:%d in time' % (host, port))

def wait_for_master(cfg, nodes, settings, ca_path, key, timeout, interval):
    tls_ca_cert = os.path.join(ca_path, 'cacert.pem')
    tls_cert = os.path.join(ca_path, '%s.pem' % key)
    tls_key = os.path.join(ca_path, '%s.key' % key)

    args = [
        CONFIG.binary_full_path,
        '-config', cfg,
        '-tls-ca-cert', tls_ca_cert,
        '-tls-cert', tls_cert,
        '-tls-key', tls_key,
        '--who-master'
    ]

    start = time.time()
    end = start + timeout

    for node in nodes:
        wait_for_server(
            settings['%s_ip' % node], settings['%s_client_port' % node],
            end - time.time())

    while time.time() < end:
        LOGGER.info('Retrieving master')

        proc = X.subprocess.Popen(args, close_fds=True,
                                  stdout = X.subprocess.PIPE, stderr = X.subprocess.PIPE)

        rc = proc.wait()

        LOGGER.info('rc: %s', rc)

        if rc == 0:
            return proc.stdout.read().strip()
        else:
            e = proc.stderr.read()
            LOGGER.info('`--who-master` returned %r: %r', rc, e)

        time.sleep(interval)

    raise Timeout('Unable to find master')


class ProcessManager(object):
    def __init__(self):
        self._processes = set()

    def __enter__(self):
        return self

    def __exit__(self, t, v, b):
        procs = list(self._processes)

        try:
            map(self.kill, procs)
        except:
            LOGGER.exception('Error during ProcessManager cleanup')

        if t is not None:
            raise t, v, b

    def start_node(self, config_path, node_name):
        return self.run_async(
            [CONFIG.binary_full_path, '-config', config_path, '--node', node_name],
            close_fds=True)

    def start_nodes(self, config_path, nodes):
        return map(lambda n: self.start_node(config_path, n), nodes)

    def run_async(self, *a, **k):
        proc = X.subprocess.Popen(*a, **k)
        self._processes.add(proc)
        return proc

    def kill(self, proc):
        self._processes.remove(proc)

        if proc.poll() is not None:
            return

        try:
            for i in xrange(100):
                LOGGER.info('Terminating process %s (%d/100)',
                str(proc.pid) if proc.pid else '<unknown>', i)
                try:
                    proc.terminate()
                except OSError, ex:
                    if ex.errno == errno.ESRCH:
                        pass
                    else:
                        raise

                if proc.poll() is not None:
                    return

                time.sleep(0.1)

            LOGGER.info('Killing process %s',
                str(proc.pid) if proc.pid else '<unknown>')
            proc.kill()
        except:
            LOGGER.exception('Failure while terminating process')



def make_ca(path):
    # CA key & CSR
    subject = \
        '/C=BE/ST=Oost-Vlaanderen/L=Lochristi/O=Incubaid BVBA/OU=Arakoon Testing/CN=Arakoon Testing CA'
    args = \
        'openssl req -new -nodes -out cacert-req.pem -keyout cacert.key -subj'.split()
    args.append(subject)

    X.subprocess.check_call(args, close_fds=True, cwd=path)

    # Self-sign CA CSR
    args = \
        'openssl x509 -signkey cacert.key -req -in cacert-req.pem -out cacert.pem'.split()
    X.subprocess.check_call(args, close_fds=True, cwd=path)

    os.unlink(os.path.join(path, 'cacert-req.pem'))

def make_node_cert(path, name, serial):
    # CSR
    subject = \
        '/C=BE/ST=Oost-Vlaanderen/L=Lochristi/O=Incubaid BVBA/OU=Arakoon Testing/CN=%s' % name
    args = \
        'openssl req -out %(name)s-req.pem -new -nodes -keyout %(name)s.key -subj' % {'name': name}
    args = args.split()
    args.append(subject)

    X.subprocess.check_call(args, close_fds=True, cwd=path)

    # Sign
    args = \
        'openssl x509 -req -in %(name)s-req.pem -CA cacert.pem -CAkey cacert.key -out %(name)s.pem -set_serial 0%(serial)d' % \
        {'name': name, 'serial': serial}
    args = args.split()

    X.subprocess.check_call(args, close_fds=True, cwd=path)

    os.unlink(os.path.join(path, '%s-req.pem' % name))

    # Verify
    args = \
        'openssl verify -CAfile cacert.pem %(name)s.pem' % {'name': name}
    args = args.split()

    X.subprocess.check_call(args, close_fds=True, cwd=path)

@contextlib.contextmanager
def config(template, nodes):
    ca_path = tempfile.mkdtemp(suffix='ca')
    ca_path2 = tempfile.mkdtemp(suffix='ca')
    nodes_path = tempfile.mkdtemp(suffix='nodes')

    make_ca(ca_path)
    make_ca(ca_path2)

    settings = {
        'CLUSTER_ID': 'arakoon_tls_test',
        'NODES_PATH': nodes_path,
        'CA_PATH': ca_path,
        'CA_PATH2': ca_path2,
        'cluster': ', '.join(nodes)
    }

    for (idx, node) in enumerate(nodes):
        os.mkdir(os.path.join(nodes_path, node))
        make_node_cert(ca_path, node, idx)
        make_node_cert(ca_path2, node, idx)

        settings['%s_ip' % node] = '127.0.0.1'
        settings['%s_client_port' % node] = 4000 + idx
        settings['%s_messaging_port' % node] = 5000 + idx

    config_path = os.path.join(nodes_path, 'arakoon.ini')
    config = template % settings

    with open(config_path, 'w') as fd:
        fd.write(config)

    try:
        yield (config_path, settings)
    except:
        raise
    else:
        shutil.rmtree(nodes_path)
        shutil.rmtree(ca_path)
        shutil.rmtree(ca_path2)


TEMPLATE1 = '''
[global]
cluster = %(cluster)s
cluster_id = %(CLUSTER_ID)s
tls_ca_cert = %(CA_PATH)s/cacert.pem
tls_service = true
tls_service_validate_peer = true

[arakoon_0]
ip = %(arakoon_0_ip)s
client_port = %(arakoon_0_client_port)s
messaging_port = %(arakoon_0_messaging_port)s
home = %(NODES_PATH)s/arakoon_0
log_level = debug
tls_cert = %(CA_PATH)s/arakoon_0.pem
tls_key = %(CA_PATH)s/arakoon_0.key

[arakoon_1]
ip = %(arakoon_1_ip)s
client_port = %(arakoon_1_client_port)s
messaging_port = %(arakoon_1_messaging_port)s
home = %(NODES_PATH)s/arakoon_1
log_level = debug
tls_cert = %(CA_PATH)s/arakoon_1.pem
tls_key = %(CA_PATH)s/arakoon_1.key

[arakoon_2]
ip = %(arakoon_2_ip)s
client_port = %(arakoon_2_client_port)s
messaging_port = %(arakoon_2_messaging_port)s
home = %(NODES_PATH)s/arakoon_2
log_level = debug
tls_cert = %(CA_PATH)s/arakoon_2.pem
tls_key = %(CA_PATH)s/arakoon_2.key
'''
TEMPLATE1_NODES = ['arakoon_0', 'arakoon_1', 'arakoon_2']

TEMPLATE2 = '''
[global]
cluster = %(cluster)s
cluster_id = %(CLUSTER_ID)s
tls_ca_cert = %(CA_PATH)s/cacert.pem

[arakoon_0]
ip = %(arakoon_0_ip)s
client_port = %(arakoon_0_client_port)s
messaging_port = %(arakoon_0_messaging_port)s
home = %(NODES_PATH)s/arakoon_0
log_level = debug
tls_cert = %(CA_PATH)s/arakoon_0.pem
tls_key = %(CA_PATH)s/arakoon_0.key

[arakoon_1]
ip = %(arakoon_1_ip)s
client_port = %(arakoon_1_client_port)s
messaging_port = %(arakoon_1_messaging_port)s
home = %(NODES_PATH)s/arakoon_1
log_level = debug
tls_cert = %(CA_PATH)s/arakoon_1.pem
tls_key = %(CA_PATH)s/arakoon_1.key

[arakoon_2]
ip = %(arakoon_2_ip)s
client_port = %(arakoon_2_client_port)s
messaging_port = %(arakoon_2_messaging_port)s
home = %(NODES_PATH)s/arakoon_2
log_level = debug
tls_cert = %(CA_PATH2)s/arakoon_2.pem
tls_key = %(CA_PATH2)s/arakoon_2.key

'''
TEMPLATE2_NODES = ['arakoon_0', 'arakoon_1', 'arakoon_2']

class TestTLS(unittest.TestCase):
    def test_cert(self):
        # For now, CI doesn't like this
        try:
            raise unittest.SkipTest('Jenkins doesn\'t like this')
        except AttributeError: # Very old Python
            try:
                import nose
            except ImportError:
                return # Bleh
            else:
                raise nose.SkipTest('Jenkins doesn\'t like this')

        with config(TEMPLATE1, TEMPLATE1_NODES) as (cfg, settings):
            with ProcessManager() as pm:
                pm.start_node(cfg, 'arakoon_0')
                wait_for_server(
                    settings['arakoon_0_ip'], settings['arakoon_0_client_port'],
                    10)
                time.sleep(5)

                args = 'openssl s_client -connect %(arakoon_0_ip)s:%(arakoon_0_client_port)d -CAfile %(CA_PATH)s/cacert.pem -cert %(CA_PATH)s/arakoon_1.pem -key %(CA_PATH)s/arakoon_1.key'
                args = args % settings

                args = args.split()

                proc = X.subprocess.Popen(args, stdin = X.subprocess.PIPE,
                    stdout=subprocess.PIPE, stderr= X.subprocess.PIPE,
                    close_fds=True)
                proc.stdin.close()

                rc = proc.wait()
                self.assertEqual(rc, 0)

                data = '%s\n%s' % \
                    (proc.stdout.read().strip(), proc.stderr.read().strip())
                LOGGER.info('openssl returned: %s', data)
                self.assert_('Verify return code: 0' in data)

    def test_inter_node_communication(self):
        with config(TEMPLATE1, TEMPLATE1_NODES) as (cfg, settings):
            with ProcessManager() as pm:
                pm.start_nodes(cfg, ['arakoon_0', 'arakoon_1'])

                wait_for_master(
                    cfg, ['arakoon_0', 'arakoon_1'],
                    settings,
                    settings['CA_PATH'], 'arakoon_2',
                    10, 1)

    def test_reconnection(self):
        with config(TEMPLATE1, TEMPLATE1_NODES) as (cfg, settings):
            with ProcessManager() as pm:
                procs = pm.start_nodes(cfg, ['arakoon_0', 'arakoon_1'])

                wait_for_master(
                    cfg, ['arakoon_0', 'arakoon_1'],
                    settings,
                    settings['CA_PATH'], 'arakoon_2',
                    10, 1)

                pm.kill(procs[1])
                time.sleep(11)

                self.assertRaises(Timeout, wait_for_master,
                    cfg, ['arakoon_0'],
                    settings,
                    settings['CA_PATH'], 'arakoon_2',
                    10, 1)

                pm.start_node(cfg, 'arakoon_1')

                wait_for_master(
                    cfg, ['arakoon_0', 'arakoon_1'],
                    settings,
                    settings['CA_PATH'], 'arakoon_2',
                    10, 1)

    def test_reject_invalid_server_cert(self):
        with config(TEMPLATE2, TEMPLATE2_NODES) as (cfg, settings):
            with ProcessManager() as pm:
                pm.start_nodes(cfg, ['arakoon_0', 'arakoon_2'])

                self.assertRaises(Timeout, wait_for_master,
                    cfg, ['arakoon_0', 'arakoon_2'],
                    settings,
                    settings['CA_PATH'], 'arakoon_1',
                    15, 1)

    def test_python_client(self):
        def f(settings):
            try:
                import Arakoon
            except ImportError:
                try:
                    from arakoon import Arakoon
                except ImportError:
                    LOGGER.warning('Unable to import `Arakoon`, fixing path')

                    import sys

                    client_path = os.path.abspath(os.path.join(
                        os.path.dirname(os.path.abspath(__file__)),
                        os.path.pardir, os.path.pardir, os.path.pardir,
                        os.path.pardir,
                        'src', 'client', 'python'))
                    LOGGER.info('Appending %r to `sys.path`', client_path)
                    sys.path.append(client_path)

                    import Arakoon

            nodes = dict(
                (node, ([settings['%s_ip' % node]], settings['%s_client_port' % node]))
                for node in ['arakoon_0', 'arakoon_1', 'arakoon_2'])

            ca_path = settings['CA_PATH']
            ca_cert = os.path.join(ca_path, 'cacert.pem')
            pem = os.path.join(ca_path, 'arakoon_2.pem')
            key = os.path.join(ca_path, 'arakoon_2.key')

            config = Arakoon.ArakoonClientConfig(
                settings['CLUSTER_ID'], nodes,
                tls=True, tls_ca_cert=ca_cert,
                tls_cert=(pem, key))

            client = Arakoon.ArakoonClient(config=config)

            print 'Master:', client.whoMaster()
            client.set('tls_test_key', 'tls_test_value')
            self.assertEqual(client.get('tls_test_key'), 'tls_test_value')

        with config(TEMPLATE1, TEMPLATE1_NODES) as (cfg, settings):
            with ProcessManager() as pm:
                pm.start_nodes(cfg, ['arakoon_0', 'arakoon_1'])

                wait_for_master(
                    cfg, ['arakoon_0', 'arakoon_1'],
                    settings,
                    settings['CA_PATH'], 'arakoon_2',
                    10, 1)

                f(settings)
