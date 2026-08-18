# SPDX-FileCopyrightText: 2023-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from onetl.hooks import slot, support_hooks


@support_hooks
class HiveSlots:
    """[Slots][DBR-onetl-hooks-design-high-level-design] that could be implemented by third-party plugins.

    !!! success "Added in 0.7.0"
    """

    @slot
    @staticmethod
    def normalize_cluster_name(cluster: str) -> str | None:
        """
        Normalize cluster name passed into Hive constructor. [![support hooks](https://img.shields.io/badge/%20-support%20hooks-blue)][DBR-onetl-hooks]

        If hooks didn't return anything, cluster name is left intact.

        !!! success "Added in 0.7.0"

        Parameters
        ----------
        cluster
            Cluster name (raw)

        Returns
        -------
        :
            Normalized cluster name.

            If hook cannot be applied to a specific cluster, it should return `None`.

        Examples
        --------

        ```python
        from onetl.connection import Hive
        from onetl.hooks import hook


        @Hive.Slots.normalize_cluster_name.bind
        @hook
        def normalize_cluster_name(cluster: str) -> str:
            return cluster.lower()
        ```
        """

    @slot
    @staticmethod
    def get_known_clusters() -> set[str] | None:
        """
        Return collection of known clusters. [![support hooks](https://img.shields.io/badge/%20-support%20hooks-blue)][DBR-onetl-hooks]

        Cluster passed into Hive constructor should be present in this list.
        If hooks didn't return anything, no validation will be performed.

        !!! success "Added in 0.7.0"

        Returns
        -------
        :
            Collection of cluster names (normalized).

            If hook cannot be applied, it should return `None`.

        Examples
        --------

        ```python
        from onetl.connection import Hive
        from onetl.hooks import hook


        @Hive.Slots.get_known_clusters.bind
        @hook
        def get_known_clusters() -> str[str]:
            return {"rnd-dwh", "rnd-prod"}
        ```
        """

    @slot
    @staticmethod
    def get_current_cluster() -> str | None:
        """
        Get current cluster name. [![support hooks](https://img.shields.io/badge/%20-support%20hooks-blue)][DBR-onetl-hooks]

        Used in [onetl.connection.db_connection.hive.connection.Hive.check][] method to verify that connection is created only from the same cluster.
        If hooks didn't return anything, no validation will be performed.

        !!! success "Added in 0.7.0"

        Returns
        -------
        :
            Current cluster name (normalized).

            If hook cannot be applied, it should return `None`.

        Examples
        --------

        ```python
        from onetl.connection import Hive
        from onetl.hooks import hook


        @Hive.Slots.get_current_cluster.bind
        @hook
        def get_current_cluster() -> str:
            # some magic here
            return "rnd-dwh"
        ```
        """
