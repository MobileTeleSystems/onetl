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

from __future__ import annotations

import os
import socket
from pathlib import Path

from pydantic import validator

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

        To use SparkLocalFS connector you should have PySpark installed (or injected to ``sys.path``)
        BEFORE creating the connector instance.

        You can install PySpark as follows:

        .. code:: bash

            pip install onetl[spark]  # latest PySpark version

            # or
            pip install onetl pyspark=3.5.0  # pass specific PySpark version

        See :ref:`install-spark` installation instruction for more details.

    .. warning::

        Currently supports only Spark sessions created with option ``spark.master: local``.

    .. note::

        Supports only reading files as Spark DataFrame and writing DataFrame to files.

        Does NOT support file operations, like create, delete, rename, etc.

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
