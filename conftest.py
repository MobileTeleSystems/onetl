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
from pyspark.sql import SparkSession
from pytest_lazyfixture import lazy_fixture

from onetl.connection import (
    FTP,
    FTPS,
    HDFS,
    MSSQL,
    S3,
    SFTP,
    Clickhouse,
    Greenplum,
    MySQL,
    Oracle,
    Postgres,
    Teradata,
    WebDAV,
)
from onetl.hwm.store import MemoryHWMStore
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
def s3():
    s3 = S3(
        host=os.getenv("ONETL_MINIO_HOST"),
        port=os.getenv("ONETL_MINIO_PORT"),
        access_key=os.getenv("MINIO_ROOT_USER"),
        secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
        bucket=os.getenv("ONETL_MINIO_BUCKET"),
        secure=False,
    )

    if not s3.client.bucket_exists("testbucket"):
        s3.client.make_bucket("testbucket")

    yield s3


@pytest.fixture(scope="session")
def sftp_server(tmp_path_factory):
    server = TestSFTPServer(tmp_path_factory.mktemp("SFTP"))
    server.start()
    sleep(5)
    yield server
    server.stop()


@pytest.fixture(scope="session")
def webdav_connection():
    wd = WebDAV(
        host=os.getenv("ONETL_WEBDAV_HOST"),
        user=os.getenv("ONETL_WEBDAV_USER"),
        password=os.getenv("ONETL_WEBDAV_PASSWORD"),
        ssl_verify=False,
        protocol="http",
    )

    yield wd


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
def upload_files_with_encoding(file_all_connections, source_path):
    local_root_filename = Path(__file__).parent / "tests" / "resources"
    remote_root_filename = source_path
    files = ["file_connection_utf.txt", "file_connection_ascii.txt"]

    for file in files:
        file_all_connections.upload_file(local_root_filename / file, remote_root_filename / file)

    return {
        "utf": remote_root_filename / "file_connection_utf.txt",
        "ascii": remote_root_filename / "file_connection_ascii.txt",
    }


@pytest.fixture(scope="session", name="spark")
def get_spark_session(request):

    spark = (
        SparkSession.builder.config("spark.app.name", "onetl")  # noqa: WPS221
        .config("spark.master", "local[*]")
        .config(
            "spark.jars.packages",
            ",".join(
                [
                    Oracle.package,
                    Clickhouse.package,
                    Postgres.package,
                    Greenplum.package_spark_2_4,
                    MySQL.package,
                    MSSQL.package,
                    Teradata.package,
                ],
            ),
        )
        .config("spark.driver.memory", "1g")
        .config("spark.driver.maxResultSize", "1g")
        .config("spark.executor.cores", "1")
        .config("spark.executor.memory", "1g")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryoserializer.buffer.max", "256m")
        .config("spark.default.parallelism", "1")
        .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
        .config("spark.jars.ivySettings", os.fspath(Path(__file__).parent / "tests" / "ivysettings.xml"))
        .enableHiveSupport()
        .getOrCreate()
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
        pytest.param(lazy_fixture("webdav_connection"), marks=pytest.mark.WebDAV),
    ],
)
def file_connection_without_s3(request):
    return request.param


@pytest.fixture(
    scope="function",
    params=[
        pytest.param(lazy_fixture("s3"), marks=pytest.mark.S3),
        lazy_fixture("file_connection_without_s3"),
    ],
)
def file_all_connections(request):
    return request.param


@pytest.fixture(scope="function")
def source_path(file_all_connections):
    source_path = PurePosixPath("/export/news_parse")

    file_all_connections.rmdir(source_path, recursive=True)
    file_all_connections.mkdir(source_path)
    yield source_path
    file_all_connections.rmdir(source_path, recursive=True)


@pytest.fixture(scope="function")
def source_path_s3(s3):
    source_path = PurePosixPath("/export/news_parse")

    s3.rmdir(source_path, recursive=True)
    s3.mkdir(source_path)
    yield source_path
    s3.rmdir(source_path, recursive=True)


@pytest.fixture(scope="function")
def source_path_without_s3(file_connection_without_s3):
    source_path = PurePosixPath("/export/news_parse")

    file_connection_without_s3.rmdir(source_path, recursive=True)
    file_connection_without_s3.mkdir(source_path)
    yield source_path
    file_connection_without_s3.rmdir(source_path, recursive=True)


@pytest.fixture(scope="function")
def upload_test_files_without_s3(file_connection_without_s3, resource_path, source_path_without_s3):
    return upload_files(resource_path, source_path_without_s3, file_connection_without_s3)


@pytest.fixture(scope="function")
def upload_test_files(file_all_connections, resource_path, source_path):
    return upload_files(resource_path, source_path, file_all_connections)


@pytest.fixture(scope="function")
def upload_test_files_s3(s3, resource_path, source_path_s3):
    return upload_files(resource_path, source_path_s3, s3)


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
