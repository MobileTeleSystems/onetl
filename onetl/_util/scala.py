# SPDX-FileCopyrightText: 2023-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from onetl._util.version import Version


def get_default_scala_version(spark_version: Version) -> Version:
    """
    Get default Scala version for specific Spark version
    """
    if spark_version.major == 2:  # noqa: PLR2004
        return Version("2.11")
    if spark_version.major == 3:  # noqa: PLR2004
        return Version("2.12")
    return Version("2.13")


def scala_seq_to_python_list(seq) -> list:
    return [seq.apply(i) for i in range(seq.size())]
