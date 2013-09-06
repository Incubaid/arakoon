'''Test `ArakoonManagement`'''

import sys
import unittest
import ConfigParser

orig_sys_path = sys.path  #pylint: disable-msg=C0103
sys.path = ['/opt/qbase3/lib/pymonkey/extensions/arakoon/server'] + sys.path
import ArakoonManagement
sys.path = orig_sys_path
del orig_sys_path

def make_file(name):
    '''Make a file with some blah content

    :param name: Filename
    :type name: `str`
    '''

    with open(name, 'w') as fd:  #pylint: disable-msg=C0103
        fd.write('file content')


class TestTLSConfiguration(unittest.TestCase):  #pylint: disable-msg=R0904
    '''Test TLS-related settings'''

    def setUp(self):  #pylint: disable-msg=C0103
        self.cluster = ArakoonManagement.ArakoonCluster('tls_test')
        config = self.cluster._getConfigFile()  #pylint: disable-msg=W0212
        config.addSection('global')
        config.addParam('global', 'cluster_id', 'tls_test')
        config.write()

    def tearDown(self):  #pylint: disable-msg=C0103
        self.cluster.remove()
        self.cluster = None

    def assertSetting(self, section, option, value):  #pylint: disable-msg=C0103
        '''Assert some config option has the expected value

        If `value` is `None`, the option should not be set.

        :param section: Option section
        :type section: `str`
        :param option: Option name
        :type option: `str`
        :param value: Expected value, or `None`
        :type value: `str`
        '''

        cfg = ConfigParser.ConfigParser()
        path = '%s.cfg' % self.cluster._getConfigFilePath()  #pylint: disable-msg=W0212,C0301

        cfg.read(path)

        if value is None:
            self.assertFalse(cfg.has_option(section, option))
        else:
            self.assertEqual(cfg.get(section, option, raw=True), value)

    def test_tls_ca_cert(self):
        '''Test `tls_ca_cert` configuration changes'''

        self.assertRaises(
            ValueError, self.cluster.setTLSCACertificate, '_invalid_')

        self.cluster.setTLSCACertificate(None)
        self.assertSetting('global', 'tls_ca_cert', None)

        make_file('cacert.pem')

        self.cluster.setTLSCACertificate('cacert.pem')
        self.assertSetting('global', 'tls_ca_cert', 'cacert.pem')

        self.cluster.setTLSCACertificate(None)
        self.assertSetting('global', 'tls_ca_cert', None)

    def test_tls_service(self):
        '''Test `tls_service` configuration changes'''

        self.assertRaises(
            Exception, self.cluster.enableTLSService)

        self.cluster.disableTLSService()
        self.assertSetting('global', 'tls_service', None)

        make_file('cacert.pem')
        self.cluster.setTLSCACertificate('cacert.pem')

        self.cluster.enableTLSService()
        self.assertSetting('global', 'tls_service', 'true')

        self.cluster.disableTLSService()
        self.assertSetting('global', 'tls_service', None)

    def test_tls_service_validate_peer(self):
        '''Test `tls_service_validate_peer` configuration changes'''

        self.assertRaises(
            Exception, self.cluster.enableTLSServiceValidatePeer)

        self.cluster.disableTLSServiceValidatePeer()
        self.assertSetting('global', 'tls_service_validate_peer', None)

        make_file('cacert.pem')
        self.cluster.setTLSCACertificate('cacert.pem')
        self.cluster.enableTLSService()

        self.cluster.enableTLSServiceValidatePeer()
        self.assertSetting('global', 'tls_service_validate_peer', 'true')

        self.cluster.disableTLSServiceValidatePeer()
        self.assertSetting('global', 'tls_service_validate_peer', None)

    def test_tls_certificate(self):
        '''Test `tls_cert` & `tls_key` configuration changes'''

        self.cluster.addNode('node')

        self.assertRaises(ValueError, self.cluster.setTLSCertificate, 'node',
            None, '_invalid_')
        self.assertRaises(ValueError, self.cluster.setTLSCertificate, 'node',
            '_invalid_', None)

        self.cluster.setTLSCertificate('node', None, None)
        self.assertSetting('node', 'tls_cert', None)
        self.assertSetting('node', 'tls_key', None)

        self.assertRaises(Exception, self.cluster.setTLSCertificate, 'node',
            '_invalid_', '_invalid_')

        make_file('cacert.pem')
        self.cluster.setTLSCACertificate('cacert.pem')

        make_file('node.pem')
        make_file('node.key')

        self.assertRaises(ValueError, self.cluster.setTLSCertificate, 'node',
            '_invalid_', 'node.key')
        self.assertRaises(ValueError, self.cluster.setTLSCertificate, 'node',
            'node.pem', '_invalid_')

        self.cluster.setTLSCertificate('node', 'node.pem', 'node.key')
        self.assertSetting('node', 'tls_cert', 'node.pem')
        self.assertSetting('node', 'tls_key', 'node.key')

        self.cluster.setTLSCertificate('node', None, None)
        self.assertSetting('node', 'tls_cert', None)
        self.assertSetting('node', 'tls_key', None)
