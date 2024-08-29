# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
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
    This class is responsible for validating additional options that are passed from the user
    to the Kafka connection. These extra options are configurations that can be provided to the
    Kafka, which aren't part of the core connection options.

    See Connection `producer options documentation <https://kafka.apache.org/documentation/#producerconfigs>`_,
    `consumer options documentation <https://kafka.apache.org/documentation/#consumerconfigs>`_
    for more details
    """

    class Config:
        strip_prefixes = ["kafka."]
        prohibited_options = PROHIBITED_OPTIONS
        extra = "allow"
