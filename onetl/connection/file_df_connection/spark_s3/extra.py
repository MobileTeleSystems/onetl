# SPDX-FileCopyrightText: 2023-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import re

from pydantic import ConfigDict

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

    model_config = ConfigDict(
        strip_prefixes=("spark.hadoop.", "fs.s3a.", re.compile(r"bucket\.[^.]+\.")),  # type: ignore[typeddict-unknown-key]
        prohibited_options=PROHIBITED_OPTIONS,  # type: ignore[typeddict-unknown-key]
        extra="allow",
    )
