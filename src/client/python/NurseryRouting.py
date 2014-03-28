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
    
