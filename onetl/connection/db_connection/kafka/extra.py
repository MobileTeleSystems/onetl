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
