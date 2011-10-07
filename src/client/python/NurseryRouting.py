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
import logging

class RoutingInfo:
    
    @staticmethod
    def unpack(buffer, offset, decodeBool, decodeString ):
        isLeaf, offset = decodeBool (buffer, offset)
        if isLeaf:
            clusterId, offset = decodeString(buffer, offset)
            return LeafRoutingNode(clusterId), offset
        else:
            boundary, offset = decodeString(buffer, offset)
            left, offset = RoutingInfo.unpack(buffer, offset, decodeBool, decodeString)
            right, offset = RoutingInfo.unpack(buffer, offset, decodeBool, decodeString)
            return InternalRoutingNode(left,boundary,right), offset
        
    def __init__(self, rootNode):
        self.__root = rootNode
    
    def __str__(self):
        return self.toString(0)
        
    def toString(self, indent):
        return self.__root.toString(indent)
    
    def split(self, newBoundary, clusterId):
        self.__root = self.__root.split(newBoundary, clusterId)
        
    def serialize(self, serBool, serString):
        return self.__root.serialize(serBool, serString)
    
    def getClusterId(self, key):
        return self.__root.getClusterId(key)
    
    def contains(self, clusterId):
        return self.__root.contains(clusterId)
    
class InternalRoutingNode(RoutingInfo):
    
    def __init__(self, routingLeft, boundary, routingRight):
        self._left= routingLeft
        self._boundary = boundary
        self._right = routingRight
        
    def toString (self, indent):
        indentStr = "  " * indent
        result = "%s%s:\n%sLEFT:\n%s\n%sRIGHT:\n%s" % (indentStr, self._boundary, 
                                    indentStr, self._left.toString(indent+1),
                                    indentStr, self._right.toString(indent+1))
        return result
    
    def split(self, newBoundary, clusterId):
        if newBoundary < self._boundary :
            self._left =  self._left.split(newBoundary, clusterId)
        elif newBoundary > self._boundary:
            self._right =  self._right.split(newBoundary, clusterId)
        else:
            raise ValueError("Boundary %s already exists in the routing table")
        
        return self 
    
    def contains(self,clusterId):
        if self._left.contains(clusterId):
            return True
        return self._right.contains(clusterId)
        
            
    def serialize(self, serBool, serString):
        return serBool(False) \
            + serString( self._boundary) \
            + self._left.serialize(serBool, serString) \
            + self._right.serialize(serBool, serString) 
    
    def getClusterId(self, key):
        if key >= self._boundary :
            return self._right.getClusterId(key)
        else:
            return self._left.getClusterId(key)
        
class LeafRoutingNode(RoutingInfo):
    
    def __init__(self, clusterId):
        self._clusterId = clusterId
    
    def toString(self, indent):
        return "%s%s" % ("  " * indent, self._clusterId)
    
    def contains(self, clusterId):
        return self._clusterId == clusterId
    
    def split(self, newBoundary, clusterId):
        left = self
        right = LeafRoutingNode( clusterId )
        return InternalRoutingNode(left, newBoundary, right)
    
    def serialize(self, serBool, serString):
        return serBool(True) + serString( self._clusterId) 
    
    def getClusterId(self, key):
        return self._clusterId
    
