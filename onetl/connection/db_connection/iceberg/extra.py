# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from pydantic import ConfigDict

from onetl.impl import GenericOptions


class IcebergExtra(GenericOptions):
    """
    This class is responsible for validating additional options that are passed from the user
    to the Iceberg connection. These extra options are configurations that can be provided to the
    Iceberg, which aren't part of the core connection options.

    See [documentation](https://iceberg.apache.org/docs/latest/spark-configuration/)
    for more details
    """

    model_config = ConfigDict(extra="allow")
