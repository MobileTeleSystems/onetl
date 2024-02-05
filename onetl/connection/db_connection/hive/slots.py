# SPDX-FileCopyrightText: 2021-2024 MTS (Mobile Telesystems)
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from onetl.hooks import slot, support_hooks


@support_hooks
class HiveSlots:
    """:ref:`Slots <slot-decorator>` that could be implemented by third-party plugins."""

    @slot
    @staticmethod
    def normalize_cluster_name(cluster: str) -> str | None:
        """
        Normalize cluster name passed into Hive constructor. |support_hooks|

        If hooks didn't return anything, cluster name is left intact.

        Parameters
        ----------
        cluster : :obj:`str`
            Cluster name (raw)

        Returns
        -------
        str | None
            Normalized cluster name.

            If hook cannot be applied to a specific cluster, it should return ``None``.

        Examples
        --------

        .. code:: python

            from onetl.connection import Hive
            from onetl.hooks import hook


            @Hive.Slots.normalize_cluster_name.bind
            @hook
            def normalize_cluster_name(cluster: str) -> str:
                return cluster.lower()
        """

    @slot
    @staticmethod
    def get_known_clusters() -> set[str] | None:
        """
        Return collection of known clusters. |support_hooks|

        Cluster passed into Hive constructor should be present in this list.
        If hooks didn't return anything, no validation will be performed.

        Returns
        -------
        set[str] | None
            Collection of cluster names (normalized).

            If hook cannot be applied, it should return ``None``.

        Examples
        --------

        .. code:: python

            from onetl.connection import Hive
            from onetl.hooks import hook


            @Hive.Slots.get_known_clusters.bind
            @hook
            def get_known_clusters() -> str[str]:
                return {"rnd-dwh", "rnd-prod"}
        """

    @slot
    @staticmethod
    def get_current_cluster() -> str | None:
        """
        Get current cluster name. |support_hooks|

        Used in :obj:`~check` method to verify that connection is created only from the same cluster.
        If hooks didn't return anything, no validation will be performed.

        Returns
        -------
        str | None
            Current cluster name (normalized).

            If hook cannot be applied, it should return ``None``.

        Examples
        --------

        .. code:: python

            from onetl.connection import Hive
            from onetl.hooks import hook


            @Hive.Slots.get_current_cluster.bind
            @hook
            def get_current_cluster() -> str:
                # some magic here
                return "rnd-dwh"
        """
