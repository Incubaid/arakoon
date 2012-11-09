"""
This file is part of Arakoon, a distributed key-value store. Copyright
(C) 2010 Incubaid BVBA

Licensees holding a valid Incubaid license may use this file in
accordance with Incubaid's Arakoon commercial license agreement. For
more information on how to enter into this agreement, please contact
Incubaid (contact details can be found on www.arakoon.org/licensing).

Alternatively, this file may be redistributed and/or modified under
the terms of the GNU Affero General Public License version 3, as
published by the Free Software Foundation. Under this license, this
file is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.

See the GNU Affero General Public License for more details.
You should have received a copy of the
GNU Affero General Public License along with this program (file "COPYING").
If not, see <http://www.gnu.org/licenses/>.
"""

from Arakoon import ArakoonClient 
from NurseryRouting import RoutingInfo 
from ArakoonExceptions import NurseryRangeError, NurseryInvalidConfig,ArakoonException
from functools import wraps
import time
import logging

maxDuration = 3

def retryDuringMigration (f):
    @wraps(f)    
    def retrying_f (self,*args,**kwargs):
        naptime = 0.1
        duration = 0.0
        start = time.time()
        
        callSucceeded = False
        while (not callSucceeded and duration < maxDuration ):
            try:
                retVal = f(self,*args,**kwargs)
                callSucceeded = True
            except (NurseryRangeError, NurseryInvalidConfig) as ex:
                logging.warning("Nursery range or config error (%s). Sleep %f before next attempt" % (ex, naptime) )
                time.sleep(naptime)       
                duration = time.time() - start
                naptime *= 1.5    
                self._fetchNurseryConfig()
        
        if( duration >= maxDuration) :
            raise ArakoonException("Failed to process nursery request in a timely fashion")
        
        return retVal
    
    return retrying_f

class NurseryClient:
    
    def __init__(self,clientConfig):
        self.nurseryClusterId = clientConfig.getClusterId()
        self._keeperClient = ArakoonClient(clientConfig)
        self._clusterClients = dict ()
        self._fetchNurseryConfig()
    
    def _fetchNurseryConfig(self):
        (routing,cfgs) = self._keeperClient.getNurseryConfig()
        self._routing = routing
        logging.debug( "Nursery client has routing: %s" % str(routing))
        for (clusterId,client) in self._clusterClients.iteritems() :
            client.dropConnections()
        
        self._clusterClients = dict()
        logging.debug("Nursery contains %d clusters", len(cfgs))
        for (clusterId,cfg) in cfgs.iteritems():
            client = ArakoonClient(cfg)
            logging.debug("Adding client for cluster %s" % clusterId )
            self._clusterClients[clusterId] = client
    
    def _getArakoonClient(self, key):
        clusterId = self._routing.getClusterId(key)
        logging.debug("Key %s goes to cluster %s" % (key, clusterId))
        if not self._clusterClients.has_key( clusterId ):
            raise NurseryInvalidConfig()
        return self._clusterClients[clusterId]
    
    @retryDuringMigration
    def set(self, key, value):
        """
        Update the value associated with the given key.

        If the key does not yet have a value associated with it, a new key value pair will be created.
        If the key does have a value associated with it, it is overwritten.
        For conditional value updates see L{testAndSet}

        @type key: string
        @type value: string
        @param key: The key whose associated value you want to update
        @param value: The value you want to store with the associated key

        @rtype: void
        """
        client = self._getArakoonClient(key)
        client.set(key,value)
        
    @retryDuringMigration
    def get(self, key):
        """
        Retrieve a single value from the nursery.

        @type key: string
        @param key: The key you are interested in
        @rtype: string
        @return: The value associated with the given key
        """

        client = self._getArakoonClient(key)
        return client.get(key)
    
    @retryDuringMigration
    def delete(self, key):
        """
        Remove a key-value pair from the nursery.

        @type key: string
        @param key: Remove this key and its associated value from the nursery

        @rtype: void
        """
        client = self._getArakoonClient(key)
        client.delete(key)

