import logging
import shutil
import os
import secrets
from collections import namedtuple
from pathlib import Path, PurePosixPath
from time import sleep
from typing import Dict

import pytest
from mtspark import get_spark

from onetl.connection import (
    FTP,
    FTPS,
    HDFS,
    MSSQL,
    SFTP,
    Clickhouse,
    MySQL,
    Oracle,
    Postgres,
    Teradata,
)
from onetl.strategy import MemoryHWMStore
from tests.lib.clickhouse_processing import ClickhouseProcessing
from tests.lib.common import upload_files
from tests.lib.hive_processing import HiveProcessing
from tests.lib.mock_file_servers import TestFTPServer, TestSFTPServer
from tests.lib.mssql_processing import MSSQLProcessing
from tests.lib.mysql_processing import MySQLProcessing
from tests.lib.oracle_processing import OracleProcessing
from tests.lib.postgres_processing import PostgressProcessing

log = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def ftp_server(tmp_path_factory):
    server = TestFTPServer(tmp_path_factory.mktemp("FTP"))
    server.start()
    sleep(5)
    yield server
    server.stop()


@pytest.fixture(scope="session")
def ftps_server(tmp_path_factory):
    server = TestFTPServer(tmp_path_factory.mktemp("FTPS"), is_ftps=True)
    server.start()
    sleep(5)
    yield server
    server.stop()


@pytest.fixture(scope="session")
def sftp_server(tmp_path_factory):
    server = TestSFTPServer(tmp_path_factory.mktemp("SFTP"))
    server.start()
    sleep(5)
    yield server
    server.stop()


@pytest.fixture(scope="session")
def hdfs_server():
    HDFSServer = namedtuple("HDFSServer", ["host", "port"])

    return HDFSServer(
        os.getenv("ONETL_HDFS_CONN_HOST", "hive2"),
        os.getenv("ONETL_HDFS_CONN_PORT", 50070),
    )


@pytest.fixture(scope="session")
def resource_path():
    return Path(__file__).parent / "tests" / "resources" / "src"


@pytest.fixture(scope="session")
def test_files(resource_path):
    resources = resource_path / "news_parse_zp" / "2018_03_05_10_00_00"

    return [
        resources / "newsage-zp-2018_03_05_10_00_00.csv",
        resources / "newsage-zp-2018_03_05_10_10_00.csv",
    ]


@pytest.fixture
def make_test_files_copy(test_files, tmp_path_factory):
    temp_test_files = []

    tmp_dir = tmp_path_factory.mktemp("tmp_test_files")

    for file in test_files:
        new_temp_file = tmp_dir / file.name
        shutil.copy(file, str(new_temp_file))
        temp_test_files.append(new_temp_file)

    return temp_test_files


@pytest.fixture(scope="session")
def source_path():
    return PurePosixPath("/export/news_parse")


@pytest.fixture(scope="function")  # noqa: WPS231
def sftp_files(sftp_server, resource_path, source_path):
    sftp = SFTP(user=sftp_server.user, password=sftp_server.user, host=sftp_server.host, port=sftp_server.port)

    yield upload_files(resource_path, source_path, sftp)

    sftp.rmdir(source_path, recursive=True)


@pytest.fixture(scope="function")
def ftp_files(ftp_server, resource_path, source_path):
    ftp = FTP(user=ftp_server.user, password=ftp_server.password, host=ftp_server.host, port=ftp_server.port)

    yield upload_files(resource_path, source_path, ftp)

    ftp.rmdir(source_path, recursive=True)


@pytest.fixture(scope="function")
def ftps_files(ftps_server, resource_path, source_path):
    ftps = FTPS(user=ftps_server.user, password=ftps_server.password, host=ftps_server.host, port=ftps_server.port)

    yield upload_files(resource_path, source_path, ftps)

    ftps.rmdir(source_path, recursive=True)


@pytest.fixture(scope="function")
def hdfs_files(hdfs_server, resource_path, source_path):
    hdfs = HDFS(host=hdfs_server.host, port=hdfs_server.port)

    yield upload_files(resource_path, source_path, hdfs)

    hdfs.rmdir(source_path, recursive=True)


@pytest.fixture(scope="session", name="spark")
def get_mtspark_session(request):
    config = {
        "appName": "onetl",
        "spark.jars.packages": [
            Oracle.package,
            Clickhouse.package,
            Postgres.package,
            MySQL.package,
            MSSQL.package,
            Teradata.package,
        ],
    }

    if getattr(request, "param", None):
        config.update(request.param)

    spark = get_spark(
        config=config,
        fix_pyspark=False,
    )
    yield spark
    spark.sparkContext.stop()
    spark.stop()


@pytest.fixture()
def processing(request, spark):
    storage_matching: Dict = {
        "postgres": PostgressProcessing,
        "hive": HiveProcessing,
        "oracle": OracleProcessing,
        "clickhouse": ClickhouseProcessing,
        "mysql": MySQLProcessing,
        "mssql": MSSQLProcessing,
    }

    test_function = request.function

    db_storage_name = test_function.__name__.split("_")[1]  # postgres, hive, oracle, clickhouse, mysql, mssql

    if db_storage_name not in storage_matching:
        raise ValueError(f"Wrong name. Please use {list(storage_matching.keys())}")

    db_processing = storage_matching[db_storage_name]

    if db_storage_name == "hive":
        yield db_processing(spark)
    else:
        with db_processing() as result:
            yield result


@pytest.fixture
def get_schema_table(processing):
    table = f"test_{secrets.token_hex(5)}"
    schema = "onetl"

    full_name = f"{schema}.{table}"

    PreparedDbInfo = namedtuple("PreparedDbInfo", ["full_name", "schema", "table"])

    yield PreparedDbInfo(full_name=full_name, schema=schema, table=table)

    try:
        processing.drop_table(
            table=table,
            schema=schema,
        )
    except Exception:  # noqa: S110
        pass


@pytest.fixture
def prepare_schema_table(processing, request, get_schema_table):
    test_function = request.function

    test_entity = test_function.__name__.split("_")[2]

    fields = {column_name: processing.get_column_type(column_name) for column_name in processing.column_names}

    preloading_data = test_entity == "reader"  # True if _reader_, if _writer_ then False

    _, schema, table = get_schema_table

    try:
        processing.create_schema(schema=schema)
        processing.create_table(schema=schema, table=table, fields=fields)

        if preloading_data:
            processing.insert_data(
                schema=schema,
                table=table,
                values=processing.create_pandas_df(),
            )

    except Exception as error:
        log.exception(error)
        raise error

    return get_schema_table


@pytest.fixture(scope="function", autouse=True)  # noqa: WPS325
def use_memory_hwm_store(request):
    test_function = request.function
    entities = test_function.__name__.split("_")

    if "strategy" in entities:
        with MemoryHWMStore():
            yield None

    else:
        yield None
