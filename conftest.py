from __future__ import annotations

import logging
import os
import secrets
import shutil
from collections import namedtuple
from datetime import date, datetime, timedelta
from importlib import import_module
from pathlib import Path, PurePosixPath
from time import sleep
from typing import TYPE_CHECKING
from unittest.mock import Mock

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
from pytest_lazyfixture import lazy_fixture

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

# disable failing plugin import
os.environ["ONETL_PLUGINS_BLACKLIST"] = "failing-plugin"

from onetl.hwm.store import MemoryHWMStore
from tests.lib.common import upload_files

log = logging.getLogger(__name__)

PreparedDbInfo = namedtuple("PreparedDbInfo", ["full_name", "schema", "table"])


@pytest.fixture(scope="session")
def ftp_server(tmp_path_factory):
    from tests.lib.mock_file_servers import TestFTPServer

    server = TestFTPServer(tmp_path_factory.mktemp("FTP"))
    server.start()
    sleep(5)
    yield server
    server.stop()


@pytest.fixture(
    scope="function",
    params=[
        pytest.param(
            lazy_fixture("ftp_server"),
            marks=[pytest.mark.ftp, pytest.mark.file_connection, pytest.mark.connection],
        ),
    ],
)
def ftp_connection(request):
    from onetl.connection import FTP

    return FTP(
        host=request.param.host,
        port=request.param.port,
        user=request.param.user,
        password=request.param.password,
    )


@pytest.fixture(scope="session")
def ftps_server(tmp_path_factory):
    from tests.lib.mock_file_servers import TestFTPServer

    server = TestFTPServer(tmp_path_factory.mktemp("FTPS"), is_ftps=True)
    server.start()
    sleep(5)
    yield server
    server.stop()


@pytest.fixture(
    scope="function",
    params=[
        pytest.param(
            lazy_fixture("ftps_server"),
            marks=[pytest.mark.ftps, pytest.mark.file_connection, pytest.mark.connection],
        ),
    ],
)
def ftps_connection(request):
    from onetl.connection import FTPS

    return FTPS(
        host=request.param.host,
        port=request.param.port,
        user=request.param.user,
        password=request.param.password,
    )


@pytest.fixture(
    scope="function",
    params=[
        pytest.param("fake", marks=[pytest.mark.s3, pytest.mark.file_connection, pytest.mark.connection]),
    ],
)
def s3():
    from onetl.connection import S3

    s3 = S3(
        host=os.getenv("ONETL_MINIO_HOST"),
        port=os.getenv("ONETL_MINIO_PORT"),
        access_key=os.getenv("ONETL_MINIO_USER"),
        secret_key=os.getenv("ONETL_MINIO_PASSWORD"),
        bucket=os.getenv("ONETL_MINIO_BUCKET"),
        protocol="http",
    )

    if not s3.client.bucket_exists(s3.bucket):
        s3.client.make_bucket(s3.bucket)

    return s3


@pytest.fixture(scope="session")
def sftp_server(tmp_path_factory):
    from tests.lib.mock_file_servers import TestSFTPServer

    server = TestSFTPServer(tmp_path_factory.mktemp("SFTP"))
    server.start()
    sleep(5)
    yield server
    server.stop()


@pytest.fixture(
    scope="function",
    params=[
        pytest.param(
            lazy_fixture("sftp_server"),
            marks=[pytest.mark.sftp, pytest.mark.file_connection, pytest.mark.connection],
        ),
    ],
)
def sftp_connection(request):
    from onetl.connection import SFTP

    return SFTP(
        host=request.param.host,
        port=request.param.port,
        user=request.param.user,
        password=request.param.password,
    )


@pytest.fixture(
    scope="session",
    params=[
        pytest.param(
            "fake",
            marks=[pytest.mark.webdav, pytest.mark.file_connection, pytest.mark.connection],
        ),
    ],
)
def webdav_connection():
    from onetl.connection import WebDAV

    return WebDAV(
        host=os.getenv("ONETL_WEBDAV_HOST"),
        port=os.getenv("ONETL_WEBDAV_PORT"),
        user=os.getenv("ONETL_WEBDAV_USER"),
        password=os.getenv("ONETL_WEBDAV_PASSWORD"),
        ssl_verify=False,
        protocol="http",
    )


@pytest.fixture(
    scope="session",
    params=[
        pytest.param("fake", marks=[pytest.mark.hdfs, pytest.mark.file_connection, pytest.mark.connection]),
    ],
)
def hdfs_server():
    HDFSServer = namedtuple("HDFSServer", ["host", "port"])

    return HDFSServer(
        os.getenv("ONETL_HDFS_HOST"),
        int(os.getenv("ONETL_HDFS_PORT")),
    )


@pytest.fixture(scope="function")
def hdfs_connection(hdfs_server):
    from onetl.connection import HDFS

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


@pytest.fixture(scope="session")
def warehouse_dir(tmp_path_factory):
    # https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html
    path = tmp_path_factory.mktemp("spark-warehouse")
    yield path
    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture(scope="session")
def spark_metastore_dir(tmp_path_factory):
    # https://stackoverflow.com/a/44048667
    path = tmp_path_factory.mktemp("metastore_db")
    yield path
    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture(
    scope="session",
    name="spark",
    params=[
        pytest.param("fake", marks=[pytest.mark.db_connection, pytest.mark.connection]),
    ],
)
def get_spark_session(warehouse_dir, spark_metastore_dir):
    import pyspark
    from pyspark.sql import SparkSession

    from onetl.connection import (
        MSSQL,
        Clickhouse,
        MongoDB,
        MySQL,
        Oracle,
        Postgres,
        Teradata,
    )

    packages = [
        Clickhouse.package,
        MSSQL.package,
        MySQL.package,
        Oracle.package,
        Postgres.package,
        Teradata.package,
    ]

    pyspark_version = ".".join(pyspark.__version__.split(".")[:2])
    if pyspark_version == "2.3":
        packages.extend([MongoDB.package_spark_2_3])  # Greenplum.package_spark_2_3
    elif pyspark_version == "2.4":
        packages.extend([MongoDB.package_spark_2_4])  # Greenplum.package_spark_2_4
    elif pyspark_version == "3.2":
        packages.extend([MongoDB.package_spark_3_2])  # Greenplum.package_spark_3_2
    elif pyspark_version == "3.3":
        packages.extend([MongoDB.package_spark_3_3])
    else:
        raise ValueError(f"Unsupported pyspark version: {pyspark.__version__}")

    spark = (
        SparkSession.builder.config("spark.app.name", "onetl")  # noqa: WPS221
        .config("spark.master", "local[*]")
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.driver.memory", "1g")
        .config("spark.driver.maxResultSize", "1g")
        .config("spark.executor.cores", "1")
        .config("spark.executor.memory", "1g")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryoserializer.buffer.max", "256m")
        .config("spark.default.parallelism", "1")
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={os.fspath(spark_metastore_dir)}")
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .enableHiveSupport()
        .getOrCreate()
    )

    yield spark
    spark.sparkContext.stop()
    spark.stop()


@pytest.fixture(
    scope="function",
    params=[
        pytest.param("fake", marks=[pytest.mark.db_connection, pytest.mark.connection]),
    ],
)
def spark_mock() -> SparkSession:
    from pyspark.sql import SparkSession

    spark = Mock(spec=SparkSession)
    spark.sparkContext = Mock()
    spark.sparkContext.appName = "abc"
    return spark


@pytest.fixture()
def processing(request, spark):
    processing_classes = {
        "clickhouse": ("tests.lib.clickhouse_processing", "ClickhouseProcessing"),
        "greenplum": ("tests.lib.greenplum_processing", "GreenplumProcessing"),
        "hive": ("tests.lib.hive_processing", "HiveProcessing"),
        "mongodb": ("tests.lib.mongodb_processing", "MongoDBProcessing"),
        "mssql": ("tests.lib.mssql_processing", "MSSQLProcessing"),
        "mysql": ("tests.lib.mysql_processing", "MySQLProcessing"),
        "oracle": ("tests.lib.oracle_processing", "OracleProcessing"),
        "postgres": ("tests.lib.postgres_processing", "PostgresProcessing"),
    }

    db_storage_name = request.function.__name__.split("_")[1]
    if db_storage_name not in processing_classes:
        raise ValueError(f"Wrong name. Please use one of: {list(processing_classes.keys())}")

    module_name, class_name = processing_classes[db_storage_name]
    module = import_module(module_name)
    db_processing = getattr(module, class_name)

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


@pytest.fixture(scope="function", autouse=True)
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
        lazy_fixture("ftp_connection"),
        lazy_fixture("ftps_connection"),
        lazy_fixture("sftp_connection"),
        lazy_fixture("hdfs_connection"),
        lazy_fixture("webdav_connection"),
    ],
)
def file_connection_without_s3(request):
    return request.param


@pytest.fixture(
    scope="function",
    params=[
        lazy_fixture("s3"),
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
                value=date(year=2023, month=8, day=15),
            ),
            timedelta(days=31),
        ),
        (
            DateTimeHWM(
                source=Table(name=secrets.token_hex(5), db=secrets.token_hex(5), instance="proto://domain.com"),
                column=Column(name=secrets.token_hex(5)),
                value=datetime(year=2023, month=8, day=15, hour=11, minute=22, second=33),
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
