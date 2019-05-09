import Arakoon
import ArakoonProtocol

import logging
class ArakoonAdmin(Arakoon.ArakoonClient):

    def collapse(self, node_id, n):
        """
        @param node_id: which node to collapse
        @param n : the number of tlogs that should remain after collapse
        """
        msg = ArakoonProtocol.ArakoonProtocol.encodeCollapse(n)
        conn = None
        conn = self._sendMessage(node_id, msg )
        ArakoonProtocol.ArakoonProtocol._evaluateErrorCode(conn)
        collapse_count = ArakoonProtocol._recvInt(conn)
        logging.debug("collapse_count %i", collapse_count)
        for i in xrange(collapse_count):
            logging.debug("i=%i", i)
            ArakoonProtocol.ArakoonProtocol._evaluateErrorCode(conn)
            v = ArakoonProtocol._recvInt64(conn)
            logging.info("v=%i",v)
        
