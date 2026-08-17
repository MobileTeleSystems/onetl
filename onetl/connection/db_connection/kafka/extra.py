# SPDX-FileCopyrightText: 2023-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from pydantic import ConfigDict

from onetl.impl import GenericOptions

PROHIBITED_OPTIONS = frozenset(
    (
        # filled by onETL classes
        "bootstrap.servers",
        "security.protocol",
        "sasl.*",
        "ssl.*",
        # Not supported by Spark
        "auto.offset.reset",
        "enable.auto.commit",
        "interceptor.classes",
        "key.deserializer",
        "key.serializer",
        "value.deserializer",
        "value.serializer",
    ),
)


class KafkaExtra(GenericOptions):
    """
    Extra options for Kafka connection.

    You can pass here any parameters supported by Kafka consumer or producer,
    even if it is not mentioned in this documentation.

    See:

    * [Producer options documentation](https://kafka.apache.org/documentation/#producerconfigs)
    * [consumer options documentation](https://kafka.apache.org/documentation/#consumerconfigs)
    """

    model_config = ConfigDict(strip_prefixes=("kafka.",), prohibited_options=PROHIBITED_OPTIONS, extra="allow")  # type: ignore[typeddict-unknown-key]
