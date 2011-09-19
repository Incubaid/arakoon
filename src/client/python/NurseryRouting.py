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
            right, offset = decodeString(buffer, offset)
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
    
    def split(self, newBoundary, clusterId):
        left = self
        right = LeafRoutingNode( clusterId )
        return InternalRoutingNode(left, newBoundary, right)
    
    def serialize(self, serBool, serString):
        return serBool(True) + serString( self._clusterId) 
    
    def getClusterId(self, key):
        return self._clusterId
