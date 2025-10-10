# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import Dict

from onetl.connection.file_df_connection.spark_s3.connection import SparkS3

try:
    from pydantic.v1 import validator
except (ImportError, AttributeError):
    from pydantic import validator  # type: ignore[no-redef, assignment]

from onetl.base import PurePathProtocol
from onetl.connection.db_connection.iceberg.warehouse import IcebergWarehouse
from onetl.connection.file_df_connection.spark_file_df_connection import (
    SparkFileDFConnection,
)
from onetl.impl.frozen_model import FrozenModel


class IcebergFilesystemWarehouse(IcebergWarehouse, FrozenModel):
    """Iceberg Filesystem Warehouse.

    .. versionadded:: 0.14.1

    .. note::

        This warehouse uses **FileDFConnection** classes to access data at the warehouse location.
        It relies on **Spark's filesystem configuration and behavior**.

    Parameters
    ----------
    connection : SparkFileDFConnection
        File connection for data storage

    path : str
        Warehouse path

    Examples
    --------

    .. tabs::

        .. code-tab:: python Local filesystem

            from onetl.connection import Iceberg, SparkLocalFS

            local_fs_connection = SparkLocalFS(spark=spark)

            warehouse = Iceberg.FilesystemWarehouse(
                connection=local_fs_connection,
                path="/warehouse/path",
            )

        .. code-tab:: python HDFS

            from onetl.connection import Iceberg, SparkHDFS

            hdfs_connection = SparkHDFS(
                host="namenode",
                cluster="my-cluster",
                spark=spark,
            )

            warehouse = Iceberg.FilesystemWarehouse(
                connection=hdfs_connection,
                path="/warehouse/path",
            )

        .. code-tab:: python S3

            from onetl.connection import Iceberg, SparkS3

            s3_connection = SparkS3(
                host="s3.domain.com",
                protocol="http",
                bucket="my-bucket",
                access_key="access_key",
                secret_key="secret_key",
                path_style_access=True,
                region="us-east-1",
                spark=spark,
            )

            warehouse = Iceberg.FilesystemWarehouse(
                connection=s3_connection,
                path="/warehouse/path"
            )
    """

    connection: SparkFileDFConnection
    path: PurePathProtocol

    def get_config(self) -> Dict[str, str]:
        config = {
            "warehouse": self.connection._convert_to_url(self.path),
        }
        if isinstance(self.connection, SparkS3):
            prefix = self.connection._get_hadoop_config_prefix()
            hadoop_config = {"hadoop." + k: v for k, v in self.connection._get_expected_hadoop_config(prefix).items()}
            config.update(hadoop_config)

        return config

    @validator("path", pre=True)
    def _validate_path(cls, path, values):
        connection = values.get("connection")
        if isinstance(connection, SparkFileDFConnection):
            return connection.path_from_string(path)
        return path
