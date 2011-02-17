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

from pymonkey import q

class ArakoonConfig:
    """
    Configuration of Arakoon nodes
    """
    def addNode(self, name, ip, client_port):
        """
        Add a node to the client configuration

        @param name: the name of the node
        @param ip: the ip  the node
        @param client_port: the port of the node
        """
        self.__validateName(name)

        config = q.config.getInifile("arakoonnodes")

        if not config.checkSection("global"):
            config.addSection("global")
            config.addParam("global","nodes", "")

        nodes = self.__getNodes(config)

        if name in nodes:
            raise Exception("There is already a node with name %s configured" % name)

        nodes.append(name)
        config.addSection(name)
        config.addParam(name, "name", name)
        config.addParam(name, "ip", ip) 
        config.addParam(name, "client_port", client_port)

        config.setParam("global","nodes", ",".join(nodes))

        config.write()

    def removeNode(self, name):
        """
        Remove a node

        @param name the name of the node
        """
        self.__validateName(name)

        config = q.config.getInifile("arakoonnodes")

        if not config.checkSection("global"):
            raise Exception("No node with name %s configured" % name)

        nodes = self.__getNodes(config)

        if name in nodes:
            config.removeSection(name)

            nodes.remove(name)
            config.setParam("global","nodes", ",".join(nodes))

            config.write()
            return

        raise Exception("No node with name %s configured" % name)

    def getNodes(self):
        """
        Get an object that contains all node information

        @return dict the dict can be used as param for the ArakoonConfig object
        """
        config = q.config.getInifile("arakoonnodes")
    
        clientconfig = dict()

        if config.checkSection("global"):
            nodes = self.__getNodes(config)

            for name in nodes:
                clientconfig[name] = (config.getValue(name, "ip"),
                                      config.getValue(name, "client_port"))

        return clientconfig

    def generateClientConfigFromServerConfig(self):
        """
        Generate the client config file from the servers
        """
        serverConfig = q.config.getInifile("arakoon")

        if serverConfig.checkSection("global"):
            nodes = self.__getNodes(serverConfig)

            for name in nodes:
                self.addNode(name,
                             serverConfig.getValue(name, "ip"),
                             serverConfig.getValue(name, "client_port"))

    def __getNodes(self, config):
        if not config.checkSection("global"):
            return []

        nodes = config.getValue("global", "nodes").strip()
        # "".split(",") -> ['']
        if nodes == "":
            return []
        nodes = nodes.split(",")
        nodes = map(lambda x: x.strip(), nodes)

        return nodes


    def __validateName(self, name):
        if name is None:
            raise Exception("A name should be passed. None is not an option")

        if not type(name) == type(str()):
            raise Exception("Name should be of type string")

        for char in [' ', ',', '#']:
            if char in name:
                raise Exception("name should not contain %s" % char)
            
