# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from onetl.impl import GenericOptions

PROHIBITED_OPTIONS = frozenset(
    (
        # These options should be passed in Spark session config
        "spark.*",
    ),
)


class IcebergReadOptions(GenericOptions):
    """Iceberg source reading options."""

    class Config:
        extra = "allow"
        known_options: frozenset = frozenset()
        prohibited_options = PROHIBITED_OPTIONS


class IcebergWriteOptions(GenericOptions):
    """Iceberg source writing options."""

    class Config:
        extra = "allow"
        known_options: frozenset = frozenset()
        prohibited_options = PROHIBITED_OPTIONS
