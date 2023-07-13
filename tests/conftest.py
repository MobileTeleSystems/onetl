import os

# disable failing plugin import
os.environ["ONETL_PLUGINS_BLACKLIST"] = "failing-plugin"

pytest_plugins = [
    "tests.fixtures.file_connections.fixtures",
    "tests.fixtures.file_connections.ftp",
    "tests.fixtures.file_connections.ftps",
    "tests.fixtures.file_connections.hdfs",
    "tests.fixtures.file_connections.s3",
    "tests.fixtures.file_connections.sftp",
    "tests.fixtures.file_connections.webdav",
    "tests.fixtures.processing.fixtures",
    "tests.fixtures.create_keytab",
    "tests.fixtures.global_hwm_store",
    "tests.fixtures.hwm_delta",
    "tests.fixtures.spark_mock",
    "tests.fixtures.spark",
]
