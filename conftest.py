import os
import logging
from time import sleep
import secrets
from typing import Dict
from pathlib import Path
from collections import namedtuple

import pytest
from onetl.strategy.hwm_store.memory_hwm_store import MemoryHWMStore

from mtspark import get_spark
from onetl.connection.file_connection import SFTP, FTP, FTPS
from onetl.connection.db_connection import Oracle, Clickhouse, Postgres, MySQL, MSSQL, Teradata
from tests.lib.postgres_processing import PostgressProcessing
from tests.lib.hive_processing import HiveProcessing
from tests.lib.oracle_processing import OracleProcessing
from tests.lib.clickhouse_processing import ClickhouseProcessing
from tests.lib.mysql_processing import MySQLProcessing
from tests.lib.mssql_processing import MSSQLProcessing
from tests.lib.common import upload_files

from tests.lib.mock_file_servers import TestFTPServer, TestSFTPServer

log = logging.getLogger(__name__)

# ****************************Environment Variables*****************************
SELF_PATH = os.path.abspath(os.path.dirname(__file__))


@pytest.fixture(scope="session")
def source_path():
    return "/export/news_parse"


@pytest.fixture(scope="session")
def ftp_server():
    server = TestFTPServer(os.path.join("/tmp", "FTP_HOME"))
    server.start()
    sleep(5)
    yield server
    server.stop()


@pytest.fixture(scope="session")
def ftps_server():
    server = TestFTPServer(os.path.join("/tmp", "FTPS_HOME"), is_ftps=True)
    server.start()
    sleep(5)
    yield server
    server.stop()


@pytest.fixture(scope="session")
def sftp_server():
    server = TestSFTPServer(os.path.join("/tmp", "SFTP_HOME"))
    server.start()
    sleep(5)
    yield server
    server.stop()


@pytest.fixture(scope="session")
def resource_path():
    return Path(__file__).parent / "tests" / "resources" / "src"


@pytest.fixture(scope="session")
def test_file_path(resource_path, test_file_name):
    return resource_path / "news_parse_zp" / "2018_03_05_10_00_00" / test_file_name


@pytest.fixture(scope="session")
def test_file_name():
    return "newsage-zp-2018_03_05_10_00_00.csv"


@pytest.fixture(scope="session")
def sftp_client(sftp_server):
    """
    :param sftp_server:
    :type sftp_server: MockFtpServer
    :return:
    :rtype: SFTPClient
    """
    ssh_client_started, sftp_client_started = sftp_server.create_client()
    yield sftp_client_started
    sftp_client_started.close()
    ssh_client_started.close()


@pytest.fixture(scope="function")  # noqa: WPS231
def sftp_files(sftp_client, sftp_server, resource_path, source_path):
    sftp = SFTP(user=sftp_server.user, password=sftp_server.user, host=sftp_server.host, port=sftp_server.port)

    upload_files(resource_path, source_path, sftp)


@pytest.fixture(scope="function")
def ftp_files(ftp_server, resource_path, source_path):
    ftp = FTP(user=ftp_server.user, password=ftp_server.password, host=ftp_server.host, port=ftp_server.port)

    upload_files(resource_path, source_path, ftp)


@pytest.fixture(scope="function")
def ftps_files(ftps_server, resource_path, source_path):
    ftps = FTPS(user=ftps_server.user, password=ftps_server.password, host=ftps_server.host, port=ftps_server.port)

    upload_files(resource_path, source_path, ftps)


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
def get_schema_table(request, processing):
    test_function = request.function
    table = f"{test_function.__name__}_{secrets.token_hex(5)}"
    schema = "onetl"

    full_name = f"{schema}.{table}"

    PreparedDbInfo = namedtuple("PreparedDbInfo", ["full_name", "schema", "table"])

    yield PreparedDbInfo(full_name=full_name, schema=schema, table=table)

    processing.drop_table(
        table=table,
        schema=schema,
    )


@pytest.fixture
def prepare_schema_table(processing, request, get_schema_table, spark):
    entities = ["reader", "writer", "strategy", "hwm"]

    test_function = request.function

    test_entity = test_function.__name__.split("_")[2]

    columns_and_types = [
        {
            "column_name": column_name,
            "type": processing.get_column_type(column_name),
        }
        for column_name in processing.column_names
    ]

    preloading_data = test_entity == "reader"  # True if _reader_, if _writer_ then False

    if test_entity in entities:
        _, schema, table = get_schema_table

        try:
            processing.create_schema(schema=schema)
            processing.create_table(schema=schema, table=table, fields=columns_and_types)

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
