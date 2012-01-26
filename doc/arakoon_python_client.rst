=====================
Arakoon Python Client
=====================
Installing the Python Client library (Debian/Ubuntu)
====================================================
See :doc:`Installing Arakoon <installing_arakoon>`.

Simple Example
==============
The python API is best explained through a simple example. The code below
creates a client, and sets a few values.

.. sourcecode:: python

    from arakoon import Arakoon

    def make_client ():
        clusterId = 'ricky'
        config = Arakoon.ArakoonClientConfig(
            clusterId,
            {
            "arakoon_0":("127.0.0.1",4000),
            "arakoon_1":("127.0.0.1",4001),
            "arakoon_2":("127.0.0.1",4002)})
        client = Arakoon.ArakoonClient(config)
        return client

    if __name__ == '__main__':
        c = make_client()
        c.set('foo','bar')
        print "foo='%s'" % c.get('foo')
        c['bla_bla'] = 'whatever'
        print "bla_bla='%s'" % (c['bla_bla'])
        c.delete('foo')
        try:
            v = c['foo']
        except KeyError:
            print "foo is no longer there, since we deleted it"

        del c['bla_bla']
        print "have bla_bla?", 'bla_bla' in c
        
        print "\nHave fun,\n"
