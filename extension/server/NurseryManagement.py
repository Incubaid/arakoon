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

from Compat import X
import ArakoonManagement

class NurseryManagement:
    def getNursery(self, clusterId ):
        """
        Retrieve the nursery manager for the nursery with the provided keeper

        @param clusterId:  The keeper of the nursery
        @type clusterId:   string

        @rtype:            NurseryManager
        """
        return NurseryManager( clusterId )

    @staticmethod
    def getConfigLocation(clusterId):
        cfg = X.getConfig( "arakoonclusters" )
        if cfg.has_key( clusterId ):
            return "%s/%s.cfg" % (cfg[ clusterId ]["path"], clusterId)
        else:
            raise RuntimeError("Unknown nursery cluster %s" % clusterId)

class NurseryManager:
    def __init__(self,clusterId):
        self._keeperCfg = NurseryManagement.getConfigLocation(clusterId)

    def migrate(self, leftClusterId, separator, rightClusterId):
        """
        Trigger a migration of key range in the nursery.

        Either leftClusterId or rightClusterId must already be part of the nursery.
        So it is not possible to add two clusters at the same time.

        The separator will serve as the boundary of the key range that is migrated.

        For more documentation see the docs on our portal (www.arakoon.org)

        @param leftClusterId:   The cluster that will be responsible for the key range up to the separator
        @type leftClusterId:    string
        @param separator:       The separator separating the key ranges between the two clusters
        @type separator:        string
        @param rightClusterId:  The cluster that will be responsible for the key range starting with the separator
        @type rightClusterId:   string
        @rtype:                 void
        """
        cmd = "%s -config %s --nursery-migrate %s %s %s" % (self.__which(), self._keeperCfg, leftClusterId, separator, rightClusterId)
        self.__runCmd ( cmd )

    def initialize(self, firstClusterId):
        """
        Initialize the routing of the nursery so it only contains the provided cluster

        A nursery can only be initialized once

        @param firstClusterId:  The first cluster of the nursery
        @type firstClusterId:   string

        @rtype void
        """
        cmd = "%s -config %s --nursery-init %s" % (self.__which(), self._keeperCfg, firstClusterId)
        self.__runCmd ( cmd )

    def delete(self, clusterId, separator = None):
        """
        Remove a cluster from the nursery. If the cluster is a boundary cluster, no separator can be provided.

        @param clusterId:  The cluster to be removed
        @type clusterId:   string

        @param separator:  Separator separating the clusters neighbouring the cluster that will be removed (not valid when deleting boundary clusters
        @type separator:   string

        @rtype void
        """
        sep_string = None
        if separator is None:
            sep_string = '""'
        else:
            sep_string = separator

        cmd = "%s -config %s --nursery-delete %s %s" % (self.__which(), self._keeperCfg, clusterId, sep_string)
        self.__runCmd( cmd )

    def __runCmd(self, cmd):
        rc  = X.subprocess.check_call(cmd)
        if rc :
            raise RuntimeError("rc=%i", rc)

    def __which(self):
        return ArakoonManagement.which_arakoon()
