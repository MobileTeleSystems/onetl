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


import re

from onetl.impl import GenericOptions

PROHIBITED_OPTIONS = frozenset(
    (
        # This options should be set not in Hadoop config, but in Java system properties
        "com.amazonaws.*",
        # These options are set by SparkS3 connection class
        "access.key",
        "endpoint.region",
        "endpoint",
        "impl",
        "secret.key",
        "session.token",
        # These options are used by legacy connectors
        "fs.s3.*",
        "fs.s3n.*",
    ),
)


class SparkS3Extra(GenericOptions):
    """
    Extra options for SparkS3 connection.

    See SparkS3 documentation.
    """

    class Config:
        strip_prefixes = ["spark.hadoop.", "fs.s3a.", re.compile(r"bucket\.[^.]+\.")]
        prohibited_options = PROHIBITED_OPTIONS
        extra = "allow"
