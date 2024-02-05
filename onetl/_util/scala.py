# SPDX-FileCopyrightText: 2021-2024 MTS (Mobile Telesystems)
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from onetl._util.version import Version


def get_default_scala_version(spark_version: Version) -> Version:
    """
    Get default Scala version for specific Spark version
    """
    if spark_version.major < 3:
        return Version(2, 11)
    return Version(2, 12)
