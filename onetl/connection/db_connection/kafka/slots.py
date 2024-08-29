# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from onetl.hooks import slot, support_hooks


@support_hooks
class KafkaSlots:
    """
    Kafka slots that could be implemented by third-party plugins

    .. versionadded:: 0.9.0
    """

    @slot
    @staticmethod
    def normalize_cluster_name(cluster: str) -> str | None:
        """
        Normalize the given Kafka cluster name.

        This can be used to ensure that the Kafka cluster name conforms to specific naming conventions.

        .. versionadded:: 0.9.0

        Parameters
        ----------
        cluster : str
            The original Kafka cluster name.

        Returns
        -------
        str | None
            The normalized Kafka cluster name. If the hook cannot be applied, return ``None``.

        Examples
        --------

        .. code:: python

            from onetl.connection import Kafka
            from onetl.hooks import hook


            @Kafka.Slots.normalize_cluster_name.bind
            @hook
            def normalize_cluster_name(cluster: str) -> str | None:
                return cluster.lower()
        """

    @slot
    @staticmethod
    def get_known_clusters() -> set[str] | None:
        """
        Retrieve the collection of known Kafka clusters.

        This can be used to validate if the provided Kafka cluster name is recognized in the system.

        .. versionadded:: 0.9.0

        Returns
        -------
        set[str] | None
            A collection of known Kafka cluster names. If the hook cannot be applied, return ``None``.

        Examples
        --------

        .. code:: python

            from onetl.connection import Kafka
            from onetl.hooks import hook


            @Kafka.Slots.get_known_clusters.bind
            @hook
            def get_known_clusters() -> set[str] | None:
                return {"kafka-cluster", "local"}
        """

    @slot
    @staticmethod
    def normalize_address(address: str, cluster: str) -> str | None:
        """
        Normalize the given broker address for a specific Kafka cluster.

        This can be used to format the broker address according to specific rules, such as adding default ports.

        .. versionadded:: 0.9.0

        Parameters
        ----------
        address : str
            The original broker address.
        cluster : str
            The Kafka cluster name for which the address should be normalized.

        Returns
        -------
        str | None
            The normalized broker address. If the hook cannot be applied to the specific address, return ``None``.

        Examples
        --------

        .. code:: python

            from onetl.connection import Kafka
            from onetl.hooks import hook


            @Kafka.Slots.normalize_address.bind
            @hook
            def normalize_address(address: str, cluster: str) -> str | None:
                if cluster == "kafka-cluster" and ":" not in address:
                    return f"{address}:9092"
                return None
        """

    @slot
    @staticmethod
    def get_cluster_addresses(cluster: str) -> list[str] | None:
        """
        Retrieve a collection of known broker addresses for the specified Kafka cluster.

        This can be used to obtain the broker addresses dynamically.

        .. versionadded:: 0.9.0

        Parameters
        ----------
        cluster : str
            The Kafka cluster name.

        Returns
        -------
        list[str] | None
            A collection of broker addresses for the specified Kafka cluster. If the hook cannot be applied, return ``None``.

        Examples
        --------

        .. code:: python

            from onetl.connection import Kafka
            from onetl.hooks import hook


            @Kafka.Slots.get_cluster_addresses.bind
            @hook
            def get_cluster_addresses(cluster: str) -> list[str] | None:
                if cluster == "kafka_cluster":
                    return ["192.168.1.1:9092", "192.168.1.2:9092", "192.168.1.3:9092"]
                return None
        """
