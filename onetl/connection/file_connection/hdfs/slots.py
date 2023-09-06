#  Copyright 2023 MTS (Mobile Telesystems)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from __future__ import annotations

from onetl.hooks import slot, support_hooks


@support_hooks
class HDFSSlots:
    """Slots that could be implemented by third-party plugins"""

    @slot
    @staticmethod
    def normalize_cluster_name(cluster: str) -> str | None:
        """
        Normalize cluster name passed into HDFS constructor.

        If hooks didn't return anything, cluster name is left intact.

        Parameters
        ----------
        cluster : :obj:`str`
            Cluster name

        Returns
        -------
        str | None
            Normalized cluster name.

            If hook cannot be applied to a specific cluster, it should return ``None``.

        Examples
        --------

        .. code:: python

            from onetl.connection import HDFS
            from onetl.hooks import hook


            @HDFS.Slots.normalize_cluster_name.bind
            @hook
            def normalize_cluster_name(cluster: str) -> str:
                return cluster.lower()
        """

    @slot
    @staticmethod
    def normalize_namenode_host(host: str, cluster: str | None) -> str | None:
        """
        Normalize namenode host passed into HDFS constructor.

        If hooks didn't return anything, host is left intact.

        Parameters
        ----------
        host : :obj:`str`
            Namenode host (raw)

        cluster : :obj:`str` or :obj:`None`
            Cluster name (normalized), if set

        Returns
        -------
        str | None
            Normalized namenode host name.

            If hook cannot be applied to a specific host name, it should return ``None``.

        Examples
        --------

        .. code:: python

            from onetl.connection import HDFS
            from onetl.hooks import hook


            @HDFS.Slots.normalize_namenode_host.bind
            @hook
            def normalize_namenode_host(host: str, cluster: str) -> str | None:
                if cluster == "rnd-dwh":
                    if not host.endswith(".domain.com"):
                        # fix missing domain name
                        host += ".domain.com"
                    return host

                return None
        """

    @slot
    @staticmethod
    def get_known_clusters() -> set[str] | None:
        """
        Return collection of known clusters.

        Cluster passed into HDFS constructor should be present in this list.
        If hooks didn't return anything, no validation will be performed.

        Returns
        -------
        set[str] | None
            Collection of cluster names (in normalized form).

            If hook cannot be applied, it should return ``None``.

        Examples
        --------

        .. code:: python

            from onetl.connection import HDFS
            from onetl.hooks import hook


            @HDFS.Slots.get_known_clusters.bind
            @hook
            def get_known_clusters() -> str[str]:
                return {"rnd-dwh", "rnd-prod"}
        """

    @slot
    @staticmethod
    def get_cluster_namenodes(cluster: str) -> set[str] | None:
        """
        Return collection of known namenodes for the cluster.

        Namenode host passed into HDFS constructor should be present in this list.
        If hooks didn't return anything, no validation will be performed.

        Parameters
        ----------
        cluster : :obj:`str`
            Cluster name (normalized)

        Returns
        -------
        set[str] | None
            Collection of host names (in normalized form).

            If hook cannot be applied, it should return ``None``.

        Examples
        --------

        .. code:: python

            from onetl.connection import HDFS
            from onetl.hooks import hook


            @HDFS.Slots.get_cluster_namenodes.bind
            @hook
            def get_cluster_namenodes(cluster: str) -> str[str] | None:
                if cluster == "rnd-dwh":
                    return {"namenode1.domain.com", "namenode2.domain.com"}
                return None
        """

    @slot
    @staticmethod
    def get_current_cluster() -> str | None:
        """
        Get current cluster name.

        Used in :obj:`~get_current_cluster` to  automatically fill up ``cluster`` attribute of a connection.
        If hooks didn't return anything, calling the method above will raise an exception.

        Returns
        -------
        str | None
            Current cluster name (in normalized form).

            If hook cannot be applied, it should return ``None``.

        Examples
        --------

        .. code:: python

            from onetl.connection import HDFS
            from onetl.hooks import hook


            @HDFS.Slots.get_current_cluster.bind
            @hook
            def get_current_cluster() -> str:
                # some magic here
                return "rnd-dwh"
        """

    @slot
    @staticmethod
    def get_webhdfs_port(cluster: str) -> int | None:
        """
        Get WebHDFS port number for a specific cluster.

        Used by constructor to automatically set port number if omitted.

        Parameters
        ----------
        cluster : :obj:`str`
            Cluster name (normalized)

        Returns
        -------
        int | None
            WebHDFS port number.

            If hook cannot be applied, it should return ``None``.

        Examples
        --------

        .. code:: python

            from onetl.connection import HDFS
            from onetl.hooks import hook


            @HDFS.Slots.get_webhdfs_port.bind
            @hook
            def get_webhdfs_port(cluster: str) -> int | None:
                if cluster == "rnd-dwh":
                    return 50007  # Cloudera
                return None
        """

    @slot
    @staticmethod
    def is_namenode_active(host: str, cluster: str | None) -> bool | None:
        """
        Check whether a namenode of a specified cluster is active (=not standby) or not.

        Used for:
            * If HDFS connection is created without ``host``

                Connector will iterate over :obj:`~get_cluster_namenodes` of a cluster to get active namenode,
                and then use it instead of ``host`` attribute.

            * If HDFS connection is created with ``host``

                :obj:`~check` will determine whether this host is active.

        Parameters
        ----------
        host : :obj:`str`
            Namenode host (normalized)

        cluster : :obj:`str` or :obj:`None`
            Cluster name (normalized), if set

        Returns
        -------
        bool | None
            ``True`` if namenode is active, ``False`` if not.

            If hook cannot be applied, it should return ``None``.

        Examples
        --------

        .. code:: python

            from onetl.connection import HDFS
            from onetl.hooks import hook


            @HDFS.Slots.is_namenode_active.bind
            @hook
            def is_namenode_active(host: str, cluster: str | None) -> bool:
                # some magic here
                return True
        """
