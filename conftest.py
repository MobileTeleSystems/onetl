import logging
import os
import secrets
import shutil
from collections import namedtuple
from datetime import date, datetime, timedelta
from pathlib import Path, PurePosixPath
from time import sleep
from typing import Dict

import pytest
from etl_entities import (
    Column,
    DateHWM,
    DateTimeHWM,
    FileListHWM,
    IntHWM,
    RemoteFolder,
    Table,
)
from mtspark import get_spark
from pytest_lazyfixture import lazy_fixture

from onetl.connection import (
    FTP,
    FTPS,
    HDFS,
    MSSQL,
    SFTP,
    Clickhouse,
    Greenplum,
    MySQL,
    Oracle,
    Postgres,
    Teradata,
)
from onetl.strategy import MemoryHWMStore
from tests.lib.clickhouse_processing import ClickhouseProcessing
from tests.lib.common import upload_files
from tests.lib.greenplum_processing import GreenplumProcessing
from tests.lib.hive_processing import HiveProcessing
from tests.lib.mock_file_servers import TestFTPServer, TestSFTPServer
from tests.lib.mssql_processing import MSSQLProcessing
from tests.lib.mysql_processing import MySQLProcessing
from tests.lib.oracle_processing import OracleProcessing
from tests.lib.postgres_processing import PostgresProcessing

log = logging.getLogger(__name__)

PreparedDbInfo = namedtuple("PreparedDbInfo", ["full_name", "schema", "table"])


@pytest.fixture(scope="session")
def ftp_server(tmp_path_factory):
    server = TestFTPServer(tmp_path_factory.mktemp("FTP"))
    server.start()
    sleep(5)
    yield server
    server.stop()


@pytest.fixture(scope="function")
def ftp_connection(ftp_server):
    return FTP(host=ftp_server.host, port=ftp_server.port, user=ftp_server.user, password=ftp_server.password)


@pytest.fixture(scope="session")
def ftps_server(tmp_path_factory):
    server = TestFTPServer(tmp_path_factory.mktemp("FTPS"), is_ftps=True)
    server.start()
    sleep(5)
    yield server
    server.stop()


@pytest.fixture(scope="function")
def ftps_connection(ftps_server):
    return FTPS(host=ftps_server.host, port=ftps_server.port, user=ftps_server.user, password=ftps_server.password)


@pytest.fixture(scope="session")
def sftp_server(tmp_path_factory):
    server = TestSFTPServer(tmp_path_factory.mktemp("SFTP"))
    server.start()
    sleep(5)
    yield server
    server.stop()


@pytest.fixture(scope="function")
def sftp_connection(sftp_server):
    return SFTP(host=sftp_server.host, port=sftp_server.port, user=sftp_server.user, password=sftp_server.password)


@pytest.fixture(scope="session")
def hdfs_server():
    HDFSServer = namedtuple("HDFSServer", ["host", "port"])

    return HDFSServer(
        os.getenv("ONETL_HDFS_CONN_HOST", "hive2"),
        int(os.getenv("ONETL_HDFS_CONN_PORT", "50070")),
    )


@pytest.fixture(scope="function")
def hdfs_connection(hdfs_server):
    return HDFS(host=hdfs_server.host, port=hdfs_server.port)


@pytest.fixture(scope="function")
def resource_path(tmp_path_factory):
    original_files = Path(__file__).parent / "tests" / "resources" / "src"

    temp_dir = tmp_path_factory.mktemp("test_files") / secrets.token_hex(5)
    shutil.copytree(original_files, temp_dir)
    return temp_dir


@pytest.fixture(scope="function")
def test_files(resource_path):
    resources = resource_path / "news_parse_zp" / "2018_03_05_10_00_00"

    return [
        resources / "newsage-zp-2018_03_05_10_00_00.csv",
        resources / "newsage-zp-2018_03_05_10_10_00.csv",
    ]


@pytest.fixture(scope="function")
def upload_files_with_encoding(file_connection, source_path):
    local_root_filename = Path(__file__).parent / "tests" / "resources"
    remote_root_filename = source_path
    files = ["file_connection_utf.txt", "file_connection_ascii.txt"]

    for file in files:
        file_connection.upload_file(local_root_filename / file, remote_root_filename / file)

    return {
        "utf": remote_root_filename / "file_connection_utf.txt",
        "ascii": remote_root_filename / "file_connection_ascii.txt",
    }


@pytest.fixture(scope="session", name="spark")
def get_spark_session(request):
    config = {
        "appName": "onetl",
        "spark.jars.packages": [
            "default:skip",
            Oracle.package,
            Clickhouse.package,
            Postgres.package,
            Greenplum.package_spark_2_4,
            MySQL.package,
            MSSQL.package,
            Teradata.package,
        ],
    }

    if getattr(request, "param", None):
        config.update(request.param)

    spark = get_spark(
        config=config,
        spark_version="local",
    )
    yield spark
    spark.sparkContext.stop()
    spark.stop()


@pytest.fixture()
def processing(request, spark):
    storage_matching: Dict = {
        "greenplum": GreenplumProcessing,
        "postgres": PostgresProcessing,
        "hive": HiveProcessing,
        "oracle": OracleProcessing,
        "clickhouse": ClickhouseProcessing,
        "mysql": MySQLProcessing,
        "mssql": MSSQLProcessing,
    }

    test_function = request.function

    db_storage_name = test_function.__name__.split("_")[1]

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
    schema = processing.schema
    processing.create_schema(schema=schema)

    table = f"test_{secrets.token_hex(5)}"
    full_name = f"{schema}.{table}"

    yield PreparedDbInfo(full_name=full_name, schema=schema, table=table)

    try:
        processing.drop_table(
            table=table,
            schema=schema,
        )
    except Exception:  # noqa: S110
        pass


@pytest.fixture
def prepare_schema_table(processing, get_schema_table):
    fields = {column_name: processing.get_column_type(column_name) for column_name in processing.column_names}
    _, schema, table = get_schema_table

    processing.create_table(schema=schema, table=table, fields=fields)

    return get_schema_table


@pytest.fixture
def load_table_data(prepare_schema_table, processing):
    _, schema, table = prepare_schema_table

    processing.insert_data(
        schema=schema,
        table=table,
        values=processing.create_pandas_df(),
    )

    return prepare_schema_table


@pytest.fixture(scope="function", autouse=True)  # noqa: WPS325
def use_memory_hwm_store(request):
    test_function = request.function
    entities = test_function.__name__.split("_")

    if "strategy" in entities:
        with MemoryHWMStore():
            yield None

    else:
        yield None


@pytest.fixture(
    scope="function",
    params=[
        pytest.param(lazy_fixture("ftp_connection"), marks=pytest.mark.FTP),
        pytest.param(lazy_fixture("ftps_connection"), marks=pytest.mark.FTPS),
        pytest.param(lazy_fixture("sftp_connection"), marks=pytest.mark.SFTP),
        pytest.param(lazy_fixture("hdfs_connection"), marks=pytest.mark.HDFS),
    ],
)
def file_connection(request):
    return request.param


@pytest.fixture(scope="function")
def source_path(file_connection):
    source_path = PurePosixPath("/export/news_parse")

    file_connection.rmdir(source_path, recursive=True)
    file_connection.mkdir(source_path)
    yield source_path
    file_connection.rmdir(source_path, recursive=True)


@pytest.fixture(scope="function")
def upload_test_files(file_connection, resource_path, source_path):
    return upload_files(resource_path, source_path, file_connection)


@pytest.fixture(
    params=[
        (
            IntHWM(
                source=Table(name=secrets.token_hex(5), db=secrets.token_hex(5), instance="proto://domain.com"),
                column=Column(name=secrets.token_hex(5)),
                value=10,
            ),
            5,
        ),
        (
            DateHWM(
                source=Table(name=secrets.token_hex(5), db=secrets.token_hex(5), instance="proto://domain.com"),
                column=Column(name=secrets.token_hex(5)),
                value=date(year=2022, month=8, day=15),
            ),
            timedelta(days=31),
        ),
        (
            DateTimeHWM(
                source=Table(name=secrets.token_hex(5), db=secrets.token_hex(5), instance="proto://domain.com"),
                column=Column(name=secrets.token_hex(5)),
                value=datetime(year=2022, month=8, day=15, hour=11, minute=22, second=33),
            ),
            timedelta(seconds=50),
        ),
        (
            FileListHWM(
                source=RemoteFolder(name=f"/absolute/{secrets.token_hex(5)}", instance="ftp://ftp.server:21"),
                value=["some/path", "another.file"],
            ),
            "third.file",
        ),
    ],
)
def hwm_delta(request):
    return request.param
