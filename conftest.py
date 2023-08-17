import os

# disable failing plugin import
os.environ["ONETL_PLUGINS_BLACKLIST"] = "failing-plugin"

pytest_plugins = [
    "tests.fixtures.processing.fixtures",
    "tests.fixtures.create_keytab",
    "tests.fixtures.global_hwm_store",
    "tests.fixtures.hwm_delta",
    "tests.fixtures.spark_mock",
    "tests.fixtures.spark",
    "tests.fixtures.connections.base",
    "tests.fixtures.connections.file_connections",
    "tests.fixtures.connections.file_df_connections",
    "tests.fixtures.connections.ftp",
    "tests.fixtures.connections.ftps",
    "tests.fixtures.connections.hdfs",
    "tests.fixtures.connections.local_fs",
    "tests.fixtures.connections.s3",
    "tests.fixtures.connections.sftp",
    "tests.fixtures.connections.webdav",
]
