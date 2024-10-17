# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING

from onetl.connection.db_connection.db_connection import DBConnection
from onetl.connection.file_connection.file_connection import FileConnection
from onetl.connection.file_df_connection.spark_file_df_connection import (
    SparkFileDFConnection,
)

if TYPE_CHECKING:
    from onetl.connection.db_connection.clickhouse import Clickhouse
    from onetl.connection.db_connection.greenplum import Greenplum
    from onetl.connection.db_connection.hive import Hive
    from onetl.connection.db_connection.kafka import Kafka
    from onetl.connection.db_connection.mongodb import MongoDB
    from onetl.connection.db_connection.mssql import MSSQL
    from onetl.connection.db_connection.mysql import MySQL
    from onetl.connection.db_connection.oracle import Oracle
    from onetl.connection.db_connection.postgres import Postgres
    from onetl.connection.db_connection.teradata import Teradata
    from onetl.connection.file_connection.ftp import FTP
    from onetl.connection.file_connection.ftps import FTPS
    from onetl.connection.file_connection.hdfs import HDFS
    from onetl.connection.file_connection.s3 import S3
    from onetl.connection.file_connection.samba import Samba
    from onetl.connection.file_connection.sftp import SFTP
    from onetl.connection.file_connection.webdav import WebDAV
    from onetl.connection.file_df_connection.spark_hdfs import SparkHDFS
    from onetl.connection.file_df_connection.spark_local_fs import SparkLocalFS
    from onetl.connection.file_df_connection.spark_s3 import SparkS3

db_connection_modules = {
    "Clickhouse": "clickhouse",
    "Greenplum": "greenplum",
    "MongoDB": "mongodb",
    "Hive": "hive",
    "MSSQL": "mssql",
    "MySQL": "mysql",
    "Oracle": "oracle",
    "Postgres": "postgres",
    "Teradata": "teradata",
    "Kafka": "kafka",
}

file_connections_modules = {
    "FTP": "ftp",
    "FTPS": "ftps",
    "HDFS": "hdfs",
    "S3": "s3",
    "SFTP": "sftp",
    "Samba": "samba",
    "WebDAV": "webdav",
}

file_df_connections_modules = {
    "SparkLocalFS": "spark_local_fs",
    "SparkHDFS": "spark_hdfs",
    "SparkS3": "spark_s3",
}


def __getattr__(name: str):
    if name in db_connection_modules:
        submodule = db_connection_modules[name]
        return getattr(import_module(f"onetl.connection.db_connection.{submodule}"), name)

    if name in file_connections_modules:
        submodule = file_connections_modules[name]
        return getattr(import_module(f"onetl.connection.file_connection.{submodule}"), name)

    if name in file_df_connections_modules:
        submodule = file_df_connections_modules[name]
        return getattr(import_module(f"onetl.connection.file_df_connection.{submodule}"), name)

    raise AttributeError(name)
