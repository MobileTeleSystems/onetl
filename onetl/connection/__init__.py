#  Copyright 2022 MTS (Mobile Telesystems)
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

from importlib import import_module

from onetl.connection.db_connection.db_connection import DBConnection
from onetl.connection.file_connection.file_connection import FileConnection

db_connection_modules = {
    "Clickhouse": "clickhouse",
    "Greenplum": "greenplum",
    "MongoDB": "mongo",
    "Hive": "hive",
    "MSSQL": "mssql",
    "MySQL": "mysql",
    "Oracle": "oracle",
    "Postgres": "postgres",
    "Teradata": "teradata",
}

file_connections_modules = {
    "FTP": "ftp",
    "FTPS": "ftps",
    "HDFS": "hdfs",
    "S3": "s3",
    "SFTP": "sftp",
    "WebDAV": "webdav",
}

__all__ = [  # noqa: WPS410
    "FileConnection",
    *db_connection_modules.keys(),
    "DBConnection",
    *file_connections_modules.keys(),
]


def __getattr__(name: str):
    if name in db_connection_modules:
        submodule = db_connection_modules[name]
        return getattr(import_module(f"onetl.connection.db_connection.{submodule}"), name)

    if name in file_connections_modules:
        submodule = file_connections_modules[name]
        return getattr(import_module(f"onetl.connection.file_connection.{submodule}"), name)

    raise ImportError(f"cannot import name {name!r} from {__name__!r}")
