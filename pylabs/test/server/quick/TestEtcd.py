"""
Copyright 2016 iNuron NV

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

from Compat import X
from .. import system_tests_common as C
from nose.tools import *
import etcd
import logging
import subprocess
import ConfigParser
from unittest import TestCase
import time
import os

class EtcdClient:
    def __init__(self,addresses, prefix):
        logging.debug("etcd: __init__(%s,%s)", addresses, prefix)
        self._addresses = addresses
        self._prefix = prefix

    def _get_client(self):
        host,port = self._addresses[0]
        client = etcd.client.Client( host = host,
                                     port = port
        )
        return client

    def _real_key(self,key):
        return self._prefix + key

    def get(self, key):
        client = self._get_client()
        k = self._real_key(key)
        logging.debug("etcd: GET %s",k)
        v = client.get(k)
        return v

    def set(self, key,value):
        client = self._get_client()
        k = self._real_key(key)
        logging.debug("etcd: SET %s %s", k, value)
        client.set(k,value)

    def delete(self, key):
        k = self._real_key(key)
        logging.debug("etcd: DELETE %s",k)
        client.delete(k)

class EtcdServer:
    def __init__(self, home, host,port, prefix):
        self._home = home
        self._port = port
        self._host = host
        self._prefix = prefix

    def start(self):
        logging.debug("ETCD:start on %s", self._home)
        X.removeDirTree(self._home)
        X.createDir(self._home)
        address = "http://%s:%i" % (self._host, self._port)
        real_cmd = [
               'nohup', 'etcd',
               '-advertise-client-urls=%s' % address,
               '-listen-client-urls=%s' % address,
               '-data-dir', self._home,
               '>> %s/stdout' % self._home ,
               '2>&1 &'
               ]
        real_cmd_s = ' '.join(real_cmd)
        fn = '%s/start.sh' % self._home
        with open(fn, 'w') as f:
            print >>f, real_cmd_s
        os.chmod(fn, 0755)
        os.system(fn)

        time.sleep(5)

    def stop(self):
        self.kill()

    def url(self, key):
        return "etcd://%s:%i/%s%s" % (self._host,
                                      self._port,
                                      self._prefix,key)
    def kill(self):
        cmd = ["fuser -n tcp %i -k" % self._port ]
        subprocess.call(cmd, shell = True)


class TestEtcd(TestCase):

    def __init__(self, x):
        TestCase.__init__(self,x)
        host = "127.0.0.1"
        port = 5000
        prefix = "arakoon/test_etcd/"
        self._root ='/'.join([X.tmpDir,'arakoon_system_tests', 'test_etcd' ])
        self._etcd_home = '%s/%s' % (self._root, 'etcd_server')
        self._server = EtcdServer(self._etcd_home, host, port, prefix)
        self._etcdClient = EtcdClient([(host, port)], prefix)
        self._cluster_id = "etcd_based_cluster"
        self._arakoon_port = 4000
        self._nodes = ['etcd_ara0']


        ConfigParser.ConfigParser()

    def _config(self):
        cfg = ConfigParser.ConfigParser()

        cfg.add_section('global')

        cfg.set('global', 'cluster_id', self._cluster_id)
        cfg.set('global', 'cluster', ','.join(self._nodes))
        index = 0
        for node in self._nodes:
            cfg.add_section(node)
            cfg.set(node, 'ip','127.0.0.1')
            cfg.set(node, 'client_port', self._arakoon_port + index)
            cfg.set(node, 'messaging_port', self._arakoon_port + 10 + index)
            cfg.set(node, 'log_level', 'debug')
            home = '%s/%s' % (self._root, node)
            logging.debug("home=%s", home)
            cfg.set(node, 'home', home)
            index = index + 1
        return cfg

    def setUp(self):
        logging.debug("setUp")
        for node in self._nodes:
            home = '%s/%s' % (self._root, node)
            X.removeDirTree(home)
            X.createDir(home)

        self._server.start()
        cfg = self._config()
        cfg_s = X.cfg2str(cfg)
        logging.debug("cfg_s='%s'",cfg_s)
        self._etcdClient.set(self._cluster_id,cfg_s)


    def tearDown(self):
        logging.debug("tearDown")
        try:
            subprocess.call("pkill arakoon", shell = True)
            self._server.stop()
        except Exception, e:
            print e
            try :
                subprocess.call("pkill -9 etcd", shell = True)
            except:
                pass
        time.sleep(5)

    def testEtcdUsage(self):
        logging.debug("testUsage")
        url = self._server.url(self._cluster_id)

        subprocess.call("for x in $(pgrep arakoon); do cat /proc/$x/cmdline; echo; done",
                        shell = True)

        # startup
        def _arakoon_cli(x, tail = None):
            cmd = [C.get_arakoon_binary()]
            cmd.extend(x)
            cmd.extend(["-config", url])
            if tail <> None:
                cmd.extend(tail)
            return cmd

        cmd = _arakoon_cli(["--node", 'etcd_ara0'], tail = ["-daemonize"])
        logging.debug('calling: %s', ' '.join(cmd))
        r = subprocess.call(cmd, close_fds = True)

        # cli node-state, & verify server's running
        def wait_for_master():
            time.sleep(1.0)
            have_master = False
            count = 0
            master_states = [
                'Stable_master',
                'Master_dictate',
                'Accepteds_check_done'
            ]

            while (not have_master) and (count < 10) :
                cmd = _arakoon_cli(['--node-state','etcd_ara0'])
                logging.debug('calling: %s', ' '.join(cmd))
                r = subprocess.check_output(cmd).strip()
                logging.debug("node state:%s", r)

                if r in master_states:
                    have_master = True
                else:
                    count = count + 1

            if have_master:
                return
            else:
                raise Exception("no master");

        wait_for_master()

        key = "some_key"
        value = "some_value"
        cmd = _arakoon_cli(['--set', key, value])
        subprocess.call(cmd)

        cmd = _arakoon_cli(['--get', key])
        v2 = subprocess.check_output(cmd).strip()
        self.assertEquals(v2, '"%s"' % value)

        cmd = _arakoon_cli(['--delete', key])
        subprocess.call(cmd)
        def get() :
            cmd = _arakoon_cli(['--get', key])
            return subprocess.check_output(cmd)
        self.assertRaises(Exception, get)

        # log_level:
        cfg = self._config()
        node = 'etcd_ara0'
        cfg.set(node, 'log_level','debug')
        cfg_s = X.cfg2str(cfg)
        logging.debug("cfg_s= '%s'", cfg_s)
        self._etcdClient.set(self._cluster_id, cfg_s)
        cmd = ["fuser","-n","tcp", "4000","-k", "-%s" % 'USR1' ]
        subprocess.call(cmd)
        time.sleep(5)
        # check if we see stuff in logging
        log_file = '%s/%s/%s.log' % (self._root, node, node)
        tail = subprocess.check_output("tail -40 %s" % log_file, shell = True)
        logging.debug("tail of %s = %s", log_file, tail)
        cmd = "grep ETCD %s | wc" % log_file
        r = subprocess.check_output(cmd, shell =True).strip().split()[0]
        logging.debug("#ETCD counts:%s", r)
        self.assertTrue(int(r)>= 1) # note:first retrieval dumps to stderr
