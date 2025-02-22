import logging
from contextlib import suppress

import pytest

from onetl import __version__ as onetl_version
from onetl._util.hadoop import get_hadoop_config
from onetl.connection import SparkS3

pytestmark = [pytest.mark.s3, pytest.mark.file_df_connection, pytest.mark.connection]


def test_spark_s3_check(s3_file_df_connection, caplog):
    s3 = s3_file_df_connection

    with caplog.at_level(logging.INFO):
        assert s3.check() == s3

    assert "|SparkS3|" in caplog.text
    assert f"host = '{s3.host}'" in caplog.text
    assert f"port = {s3.port}" in caplog.text
    assert f"protocol = '{s3.protocol}'" in caplog.text
    assert f"bucket = '{s3.bucket}'" in caplog.text
    assert f"access_key = '{s3.access_key}'" in caplog.text
    assert "secret_key = SecretStr('**********')" in caplog.text
    assert s3.secret_key.get_secret_value() not in caplog.text
    assert "session_token =" not in caplog.text
    assert "extra = {" in caplog.text
    assert "'path.style.access': True" in caplog.text

    assert "Connection is available." in caplog.text


def test_spark_s3_check_failed(spark, s3_server):
    wrong_s3 = SparkS3(
        host=s3_server.host,
        port=s3_server.port,
        bucket=s3_server.bucket,
        protocol=s3_server.protocol,
        access_key="something",
        secret_key="wrong",
        spark=spark,
    )

    with wrong_s3:
        with pytest.raises(RuntimeError, match="Connection is unavailable"):
            wrong_s3.check()


def test_spark_s3_check_hadoop_config_reset(spark, s3_server, caplog):
    wrong_s3 = SparkS3(
        host=s3_server.host,
        port=s3_server.port,
        bucket=s3_server.bucket,
        protocol=s3_server.protocol,
        access_key="wrong",
        secret_key="wrong",
        spark=spark,
    )
    with suppress(RuntimeError):
        # patch configuration with old values, and do not reset them
        wrong_s3.check()

    real_s3 = SparkS3(
        host=s3_server.host,
        port=s3_server.port,
        bucket=s3_server.bucket,
        access_key=s3_server.access_key,
        secret_key=s3_server.secret_key,
        protocol=s3_server.protocol,
        extra={
            "path.access.style": True,
            "connection.timeout": 300000,
            "committer.name": "magic",
        },
        spark=spark,
    )

    with caplog.at_level(logging.WARNING):
        msg = "Spark hadoop configuration is different from expected, it will be reset"
        hadoop_conf = get_hadoop_config(spark)

        # default values from Spark config or previous tests
        # per-bucket config
        assert hadoop_conf.get(f"fs.s3a.bucket.{s3_server.bucket}.access.key", None) == "wrong"
        assert hadoop_conf.get(f"fs.s3a.bucket.{s3_server.bucket}.path.access.style", None) is None
        assert hadoop_conf.get(f"fs.s3a.bucket.{s3_server.bucket}.connection.timeout", None) is None
        assert hadoop_conf.get(f"fs.s3a.bucket.{s3_server.bucket}.committer.name", None) is None
        assert hadoop_conf.get(f"fs.s3a.bucket.{s3_server.bucket}.user.agent.prefix", None) is None
        # root config
        assert hadoop_conf.get("fs.s3a.committer.name") == "file"
        # fs.s3a.user.agent.prefix is set by wrong_s3.check() call

        # Hadoop configuration is reset, and new S3 connection uses valid options
        real_s3.check()
        assert msg in caplog.text
        assert hadoop_conf.get(f"fs.s3a.bucket.{s3_server.bucket}.access.key", None) == s3_server.access_key
        assert hadoop_conf.get(f"fs.s3a.bucket.{s3_server.bucket}.path.access.style", None) == "true"
        assert hadoop_conf.get(f"fs.s3a.bucket.{s3_server.bucket}.connection.timeout", None) == "300000"
        assert hadoop_conf.get(f"fs.s3a.bucket.{s3_server.bucket}.committer.name", None) is None
        assert hadoop_conf.get(f"fs.s3a.bucket.{s3_server.bucket}.user.agent.prefix", None) is None
        assert hadoop_conf.get("fs.s3a.committer.name") == "magic"
        assert hadoop_conf.get("fs.s3a.user.agent.prefix").startswith("local-")
        assert hadoop_conf.get("fs.s3a.user.agent.prefix").endswith(f"onETL/{onetl_version} Spark/{spark.version}")
        caplog.clear()

        # Options are the same, nothing is changed
        real_s3.check()
        assert msg not in caplog.text
        assert hadoop_conf.get(f"fs.s3a.bucket.{s3_server.bucket}.access.key", None) == s3_server.access_key
        assert hadoop_conf.get(f"fs.s3a.bucket.{s3_server.bucket}.path.access.style", None) == "true"
        assert hadoop_conf.get(f"fs.s3a.bucket.{s3_server.bucket}.connection.timeout", None) == "300000"
        assert hadoop_conf.get(f"fs.s3a.bucket.{s3_server.bucket}.committer.name", None) is None
        assert hadoop_conf.get(f"fs.s3a.bucket.{s3_server.bucket}.user.agent.prefix", None) is None
        assert hadoop_conf.get("fs.s3a.committer.name") == "magic"
        assert hadoop_conf.get("fs.s3a.user.agent.prefix").startswith("local-")
        assert hadoop_conf.get("fs.s3a.user.agent.prefix").endswith(f"onETL/{onetl_version} Spark/{spark.version}")

        real_s3.close()
        # Options are reset
        assert hadoop_conf.get(f"fs.s3a.bucket.{s3_server.bucket}.access.key", None) is None
        assert hadoop_conf.get(f"fs.s3a.bucket.{s3_server.bucket}.path.access.style", None) is None
        assert hadoop_conf.get(f"fs.s3a.bucket.{s3_server.bucket}.connection.timeout", None) is None
        assert hadoop_conf.get(f"fs.s3a.bucket.{s3_server.bucket}.committer.name", None) is None
        assert hadoop_conf.get(f"fs.s3a.bucket.{s3_server.bucket}.user.agent.prefix", None) is None
        assert hadoop_conf.get("fs.s3a.committer.name") is None
        assert hadoop_conf.get("fs.s3a.user.agent.prefix") is None
