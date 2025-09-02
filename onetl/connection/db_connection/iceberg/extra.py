# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from onetl.impl import GenericOptions


class IcebergExtra(GenericOptions):
    """
    This class is responsible for validating additional options that are passed from the user
    to the Iceberg connection. These extra options are configurations that can be provided to the
    Iceberg, which aren't part of the core connection options.

    See `documentation <https://iceberg.apache.org/docs/latest/spark-configuration/>`_
    for more details
    """

    class Config:
        extra = "allow"
