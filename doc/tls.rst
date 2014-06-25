Using TLS
=========
Arakoon can be configured to use TLS connections to communicate with other
nodes, and (optionally) require clients to use TLS as well.

Setting up PKI infrastructure is outside the scope of this document or the
Arakoon project, as such we assume a CA is available, and infrastructure is in
place to generate and distribute keys & signatures, as well as managing CRLs.

In this document, *cacert.pem* will be used as the path of the CA
certificate and *arakoonN.pem* and *arakoonN.key* (for some N) will be the
path of node certificates (signed by the CA) & keys.

Configuring inter-node TLS communication
----------------------------------------
A cluster of Arakoon nodes can be configured to use TLS for inter-node
communication. If a cluster uses TLS, this should be configured for *all* nodes
in the cluster.

To enable node-to-node TLS communication, the following values need to be
provided in the configuration file:

*global.tls_ca_cert*
    Path to *cacert.pem*

*arakoonN.tls_cert*
    Path to *arakoonN.pem*

*arakoonN.tls_key*
    Path to *arakoonN.key*

In the above, *arakoonN* denotes a per-node configuration section.

When connecting to another node, the node certificate will be requested on both
sides and validated against the CA certificate. Self-signed certificated
without a shared CA can't be used.

Next to the configuration settings listed above, 2 more settings are available
which influence TLS communication:

*global.tls_version*
    Configure a specific TLS version to use. Valid values are ``1.0``, ``1.1``
    and ``1.2``.
    For backwards compatibility, ``1.0`` is the default.

*global.tls_cipher_list*
    Configure a specific TLS cipher list. See ``man 1 ciphers`` and ``man 3
    SSL_set_cipher_list`` for more documentation about the meaning of this
    string, how unknown settings are handled, etc.
    The default is to set no specific cipher list, i.e. use the default setting
    as used by your system SSL library.

Configuring client TLS communication
------------------------------------
Once inter-node TLS communication is configured and working, nodes can be
configured to use TLS for client connections as well.

This can be enabled by setting the *global.tls_service* setting to *true*. In
this configuration, clients will need to connect using TLS, and can (optionally)
validate the certificate provided by the node it connected to against the CA
certificate. Nodes can optionally provide a client certificate to the server
node, but this will not be validated.

If validation of client certificates is desired, the
*global.tls_service_validate_peer* option can be set to *true*. In this
configuration, clients connecting to nodes are required to provide a certificate
upon connection, which will be validated by the node against the CA certificate.
If a client fails to provide a certificate, or this is not signed by the CA, the
connection will be rejected.

The *global.tls_version* and *global.tls_cipher_list* settings are also
applicable to client connections.

Using the CLI interface
-----------------------
The Arakoon binary provides a CLI interface to connect to clusters and query
them (e.g. using *--who-master*), perform database operations (*--get*,
*--set* etc.), or perform administrative operations (e.g. *--drop-master*). When
nodes are configured to use TLS for client connections as described in
`Configuring client TLS communication`_, some extra options should be passed on
the command line.

To use a TLS connection and validate the node certificate against a CA
certificate (i.e. when *global.tls_service* is set, but
*global.tls_service_validate_peer* is not), the *-tls-ca-cert* flag should be
provided, with the path of the CA certificate.

In case a client certificate is required (when
*global.tls_service_validate_peer* is used), the *-tls-cert* and *-tls-key*
options should be used, both pointing to the certificate & key files to be used
when connecting.

To select a specific TLS version, the *-tls-version* option should be used.
Valid values are ``1.0``, ``1.1`` and ``1.2``. For backwards compatibility, this
defaults to ``1.0``.
