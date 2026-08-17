# SPDX-FileCopyrightText: 2023-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from onetl.connection.db_connection.kafka.connection import Kafka


class KafkaProtocol(ABC):
    """
    Interface for Kafka connection Protocol classes.

    !!! success "Added in 0.9.0"
    """

    @abstractmethod
    def get_options(self, kafka: "Kafka") -> dict:
        """
        Get options for Kafka connection

        Parameters
        ----------
        kafka
            Connection instance

        Returns
        -------
        :
            Kafka client options
        """
        ...

    @abstractmethod
    def cleanup(self, kafka: "Kafka") -> None:
        """
        This method is called while closing Kafka connection.

        Implement it to cleanup resources like temporary files.

        Parameters
        ----------
        kafka
            Connection instance
        """
        ...
