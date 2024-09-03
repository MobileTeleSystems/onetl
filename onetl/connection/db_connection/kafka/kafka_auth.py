# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from onetl.connection import Kafka


class KafkaAuth(ABC):
    """
    Interface for Kafka connection Auth classes.

    .. versionadded:: 0.9.0
    """

    @abstractmethod
    def get_options(self, kafka: Kafka) -> dict:
        """
        Get options for Kafka connection

        Parameters
        ----------
        kafka : :obj:`Kafka <onetl.connection.db_connection.kafka.connection.Kafka>`
            Connection instance

        Returns
        -------
        dict:
            Kafka client options
        """
        ...

    @abstractmethod
    def cleanup(self, kafka: Kafka) -> None:
        """
        This method is called while closing Kafka connection.

        Implement it to cleanup resources like temporary files.

        Parameters
        ----------
        kafka : :obj:`Kafka <onetl.connection.db_connection.kafka.connection.Kafka>`
            Connection instance
        """
        ...
