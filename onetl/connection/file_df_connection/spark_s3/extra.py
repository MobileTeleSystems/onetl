# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
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
