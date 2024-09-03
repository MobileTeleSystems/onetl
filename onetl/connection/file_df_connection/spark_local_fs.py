# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import os
import socket
from pathlib import Path

try:
    from pydantic.v1 import validator
except (ImportError, AttributeError):
    from pydantic import validator  # type: ignore[no-redef, assignment]

from onetl.base import PurePathProtocol
from onetl.connection.file_df_connection.spark_file_df_connection import (
    SparkFileDFConnection,
)
from onetl.hooks import slot, support_hooks
from onetl.impl import LocalPath


@support_hooks
class SparkLocalFS(SparkFileDFConnection):
    """
    Spark connection to local filesystem. |support_hooks|

    Based on `Spark Generic File Data Source <https://spark.apache.org/docs/latest/sql-data-sources-generic-options.html>`_.

    .. warning::

        To use SparkHDFS connector you should have PySpark installed (or injected to ``sys.path``)
        BEFORE creating the connector instance.

        See :ref:`install-spark` installation instruction for more details.

    .. warning::

        Currently supports only Spark sessions created with option ``spark.master: local``.

    .. note::

        Supports only reading files as Spark DataFrame and writing DataFrame to files.

        Does NOT support file operations, like create, delete, rename, etc.

    .. versionadded:: 0.9.0

    Parameters
    ----------
    spark : :class:`pyspark.sql.SparkSession`
        Spark session

    Examples
    --------

    .. code:: python

        from onetl.connection import SparkLocalFS
        from pyspark.sql import SparkSession

        # create Spark session
        spark = SparkSession.builder.master("local").appName("spark-app-name").getOrCreate()

        # create connection
        local_fs = SparkLocalFS(spark=spark).check()
    """

    @slot
    def path_from_string(self, path: os.PathLike | str) -> Path:
        return LocalPath(os.fspath(path))

    @property
    def instance_url(self):
        fqdn = socket.getfqdn()
        return f"file://{fqdn}"

    def __str__(self):
        # str should not make network requests
        return "LocalFS"

    @validator("spark")
    def _validate_spark(cls, spark):
        master = spark.conf.get("spark.master")
        if not master.startswith("local"):
            raise ValueError(f"Currently supports only spark.master='local', got {master!r}")
        return spark

    def _convert_to_url(self, path: PurePathProtocol) -> str:
        # "file:///absolute/path" on Unix
        # "file:///c:/absolute/path" on Windows
        # relative paths cannot be passed using file:// syntax
        return "file:///" + path.as_posix().lstrip("/")

    def _get_default_path(self):
        return LocalPath(os.getcwd())
