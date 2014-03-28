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



import ssl
import socket
from ArakoonProtocol import *
from ArakoonExceptions import *

class ArakoonClientConnection :

    def __init__ (self, nodeLocations, clusterId, config):
        self._clusterId = clusterId
        self._nodeIPs = nodeLocations[0]
        self._nodePort = nodeLocations[1]
        self._nIPs = len(self._nodeIPs)
        self._index = 0
        self._connected = False
        self._socket = None
        self._socketInfo = None
        self._config = config
        self._reconnect()

    def _reconnect(self):
        self.close()
        try :
            ip = self._nodeIPs[self._index]
            sock = socket.create_connection((ip , self._nodePort),
                                                    ArakoonClientConfig.getConnectionTimeout())

            if self._config.tls:
                kwargs = {
                    'ssl_version': ssl.PROTOCOL_TLSv1,
                    'cert_reqs': ssl.CERT_OPTIONAL,
                    'do_handshake_on_connect': True
                }

                if self._config.tls_ca_cert:
                    kwargs['cert_reqs'] = ssl.CERT_REQUIRED
                    kwargs['ca_certs'] = self._config.tls_ca_cert

                if self._config.tls_cert:
                    cert, key = self._config.tls_cert
                    kwargs['keyfile'] = key
                    kwargs['certfile'] = cert

                self._socket = ssl.wrap_socket(sock, **kwargs)
            else:
                self._socket = sock

            self._socketInfo = (ip, self._nodePort)
            sendPrologue(self._socket, self._clusterId)
            self._connected = True
        except Exception, ex :
            ArakoonClientLogger.logWarning( "Unable to connect to %s:%s (%s: '%s')" ,
                                            self._nodeIPs[self._index],
                                            self._nodePort,
                                            ex.__class__.__name__,
                                            ex  )
            self._index = (self._index + 1) % self._nIPs


    def send(self, msg):

        if not self._connected :
            self._reconnect()
            if not self._connected :
                raise ArakoonNotConnected( (self._nodeIPs, self._nodePort) )
        try:
            self._socket.sendall( msg )
        except Exception, ex:
            self.close()
            ArakoonClientLogger.logWarning( "Error while sending data to (%s,%s) => %s: '%s'" ,
                self._nodeIPs[self._index], self._nodePort, ex.__class__.__name__, ex  )
            raise ArakoonSockSendError ()

    def close(self):
        if self._connected and self._socket is not None :
            try:
                self._socket.close()
            except Exception, ex:
                ArakoonClientLogger.logError( "Error while closing socket to %s:%s (%s: '%s')" ,
                    self._nodeIPs[self._index], self._nodePort, ex.__class__.__name__, ex  )
            self._socketInfo = None
            self._connected = False

    def decodeStringResult(self) :
        return ArakoonProtocol.decodeStringResult ( self )

    def decodeBoolResult(self) :
        return ArakoonProtocol.decodeBoolResult ( self )

    def decodeVoidResult(self) :
        ArakoonProtocol.decodeVoidResult ( self )

    def decodeStringOptionResult (self):
        return ArakoonProtocol.decodeStringOptionResult ( self )

    def decodeStringArrayResult(self) :
        return ArakoonProtocol.decodeStringArrayResult ( self )

    def decodeStringListResult(self):
        return ArakoonProtocol.decodeStringListResult(self)

    def decodeStringOptionArrayResult(self):
        return ArakoonProtocol.decodeStringOptionArrayResult(self)

    def decodeStringPairListResult(self) :
        return ArakoonProtocol.decodeStringPairListResult ( self )

    def decodeStatistics(self):
        return ArakoonProtocol.decodeStatistics(self)

    def decodeInt64Result(self):
        return ArakoonProtocol.decodeInt64Result(self)

    def decodeIntResult(self):
        return ArakoonProtocol.decodeIntResult(self)

    def decodeNurseryCfgResult(self):
        return ArakoonProtocol.decodeNurseryCfgResult(self)

    def decodeVersionResult(self):
        return ArakoonProtocol.decodeVersionResult(self)

    def decodeGetTxidResult(self):
        return ArakoonProtocol.decodeGetTxidResult(self)

