import os
import logging
from time import sleep
import secrets
from typing import Union, Dict, List
from collections import namedtuple

# noinspection PyPackageRequirements
import pytest

# noinspection PyPackageRequirements
from pyhive import hive
from hdfs import InsecureClient

from mtspark import get_spark
from onetl.connection.db_connection import Oracle, Clickhouse, Postgres, MySQL, MSSQL, Teradata
from tests.lib.postgres_processing import PostgressProcessing
from tests.lib.hive_processing import HiveProcessing
from tests.lib.oracle_processing import OracleProcessing

from tests.lib.mock_sftp_server import MockSFtpServer

LOG = logging.getLogger(__name__)
ConnectionType = Union["pyspark.sql.SparkSession", "psycopg2.extensions.connection"]

# ****************************Environment Variables*****************************
SELF_PATH = os.path.abspath(os.path.dirname(__file__))

HDFS_PORT = "50070"
DOCKER_HOST = "host.docker.internal"


@pytest.fixture(scope="session")
def sftp_source_path():
    return "/export/news_parse"


@pytest.fixture(scope="session")
def sftp_server():
    server = MockSFtpServer(os.path.join("/tmp", "SFTP_HOME"))
    server.start()
    sleep(0.5)
    yield server
    server.stop()


@pytest.fixture(scope="session")
def resource_path():
    return os.path.join(os.path.dirname(__file__), "tests", "resources", "src")


@pytest.fixture(scope="session")
def test_file_path(resource_path, test_file_name):
    return os.path.join(resource_path, "news_parse_zp", "2018_03_05_10_00_00", test_file_name)


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


@pytest.fixture(scope="session", name="spark")
def get_mtspark_session():
    spark = get_spark(
        config={
            "appName": "onetl",
            "spark.jars.packages": [
                Oracle.package,
                Clickhouse.package,
                Postgres.package,
                MySQL.package,
                MSSQL.package,
                Teradata.package,
            ],
        },
        fix_pyspark=False,
    )
    yield spark
    spark.stop()


@pytest.fixture(scope="session")
def hive_client():
    client = hive.connect(host="hive2")
    yield client
    client.close()


@pytest.fixture(scope="session")
def hdfs_client():
    client = InsecureClient(f"http://{DOCKER_HOST}:{HDFS_PORT}")  # NOSONAR
    yield client


@pytest.fixture()
def processing(request, spark):
    storage_matching: Dict = {
        "postgres": PostgressProcessing,
        "hive": HiveProcessing,
        "oracle": OracleProcessing,
    }

    test_function = request.function

    db_storage_name = test_function.__name__.split("_")[1]  # postgres, hive, oracle

    db_processing = storage_matching[db_storage_name]

    if db_storage_name == "hive":
        return db_processing(spark)

    return db_processing()


@pytest.fixture()
def prepare_schema_table(processing, request, spark):
    test_function = request.function
    table = f"{test_function.__name__}_{secrets.token_hex(5)}"
    schema = "onetl"

    full_name = f"{schema}.{table}"

    storages = ["postgres", "hive", "oracle"]
    entities = ["reader", "writer"]
    column_names: List = ["id_int", "text_string", "hwm_int", "hwm_date", "hwm_datetime"]

    test_function = request.function

    db_storage_name = test_function.__name__.split("_")[1]  # postgres, hive, oracle
    test_entity = test_function.__name__.split("_")[2]

    columns_and_types = [
        {
            "column_name": column_name,
            "type": processing.get_column_types_and_names_matching()[column_name],
        }
        for column_name in column_names
    ]

    if db_storage_name == "hive" and not spark:
        raise ValueError("When working with Hive, you need to pass spark session.")

    preloading_data = test_entity == "reader"  # True if _reader_, if _writer_ then False

    if db_storage_name in storages and test_entity in entities:

        try:
            processing.create_schema(schema=schema)
            processing.create_table(schema=schema, table=table, fields=columns_and_types)

            if preloading_data:
                processing.insert_data(
                    schema=schema,
                    table=table,
                    field_names=columns_and_types,
                    values=processing.create_pandas_df(),
                )

        except Exception as error:
            LOG.exception(error)
            raise error

        PreparedDbInfo = namedtuple("PreparedDbInfo", ["full_name", "table", "schema"])

        yield PreparedDbInfo(full_name=full_name, table=table, schema=schema)

        processing.drop_table(
            table=table,
            schema=schema,
        )

        processing.stop_conn()


if __name__ == "__main__":
    pytest.main(
        [
            "--verbose",
            "-s",
            "-c",
            "pytest.ini",
            os.environ.get("X-TEST-TARGET", "tests"),
        ],
    )
