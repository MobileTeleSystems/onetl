# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from pydantic import ValidationInfo, field_validator

from onetl._util.spark import try_import_pyspark
from onetl.base import PurePathProtocol
from onetl.connection.db_connection.iceberg.warehouse import IcebergWarehouse
from onetl.connection.file_df_connection.spark_file_df_connection import (
    SparkFileDFConnection,
)
from onetl.connection.file_df_connection.spark_s3.connection import SparkS3
from onetl.hooks import slot, support_hooks
from onetl.impl import FrozenModel


@support_hooks
class IcebergFilesystemWarehouse(IcebergWarehouse, FrozenModel):
    """Iceberg Filesystem Warehouse.

    !!! success "Added in 0.15.0"

    !!! note

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

    === "Local filesystem"

        ```python
        from onetl.connection import Iceberg, SparkLocalFS

        local_fs_connection = SparkLocalFS(spark=spark)

        warehouse = Iceberg.FilesystemWarehouse(
            connection=local_fs_connection,
            path="/warehouse/path",
        )
        ```

    === "HDFS"

        ```python
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
        ```

    === "S3"

        ```python
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
        ```
    """

    connection: SparkFileDFConnection
    path: PurePathProtocol

    def __new__(cls, *args, **kwargs):
        try_import_pyspark()

        from pyspark.sql import SparkSession

        _ = SparkSession

        cls.model_rebuild()
        return super().__new__(cls)

    @slot
    def get_config(self) -> dict[str, str]:
        config = {
            "warehouse": self.connection._convert_to_url(self.path),  # noqa: SLF001
            "io-impl": "org.apache.iceberg.hadoop.HadoopFileIO",
        }
        if isinstance(self.connection, SparkS3):
            prefix = self.connection._get_hadoop_config_prefix()  # noqa: SLF001
            hadoop_config = {
                "hadoop." + k: v
                for k, v in self.connection._get_expected_hadoop_config(prefix).items()  # noqa: SLF001
            }
            config.update(hadoop_config)

        return config

    @field_validator("path", mode="before")
    def _validate_path(cls, value, info: ValidationInfo):
        connection: SparkFileDFConnection | None = info.data.get("connection")
        if not connection or value is None:
            return value
        return connection.path_from_string(value)
