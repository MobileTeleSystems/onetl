import logging
from unittest.mock import Mock

import pytest

from onetl.connection import Greenplum
from onetl.connection.db_connection.greenplum.connection_limit import (
    GreenplumConnectionLimit,
)
from onetl.db import DBWriter
from onetl.exception import TooManyParallelJobsError

pytestmark = pytest.mark.greenplum


def test_greenplum_writer_wrong_table_name(spark_mock):
    greenplum = Greenplum(host="some_host", user="user", database="database", password="passwd", spark=spark_mock)

    with pytest.raises(ValueError, match="Name should be passed in `schema.name` format"):
        DBWriter(
            connection=greenplum,
            table="table",  # Required format: table="schema.table"
        )


@pytest.mark.parametrize(
    ["df_partitions", "spark_config"],
    [
        (30, {"spark.master": "local[200]"}),
        (200, {"spark.master": "local[30]"}),
        (30, {"spark.master": "yarn", "spark.executor.instances": 200, "spark.executor.cores": 1}),
        (200, {"spark.master": "yarn", "spark.executor.instances": 30, "spark.executor.cores": 1}),
        (30, {"spark.master": "yarn", "spark.executor.instances": 20, "spark.executor.cores": 10}),
        (200, {"spark.master": "yarn", "spark.executor.instances": 3, "spark.executor.cores": 10}),
        (
            30,
            {
                "spark.master": "yarn",
                "spark.dynamicAllocation.enabled": "true",
                "spark.dynamicAllocation.maxExecutors": 20,
                "spark.executor.cores": 10,
            },
        ),
        (
            200,
            {
                "spark.master": "yarn",
                "spark.dynamicAllocation.enabled": "true",
                "spark.dynamicAllocation.maxExecutors": 3,
                "spark.executor.cores": 10,
            },
        ),
        (
            30,
            {
                "spark.master": "yarn",
                "spark.dynamicAllocation.enabled": "true",
                "spark.dynamicAllocation.maxExecutors": 20,
                "spark.executor.cores": 5,
                "spark.dynamicAllocation.executorAllocationRatio": 2,
            },
        ),
        (
            200,
            {
                "spark.master": "yarn",
                "spark.dynamicAllocation.enabled": "true",
                "spark.dynamicAllocation.maxExecutors": 3,
                "spark.executor.cores": 5,
                "spark.dynamicAllocation.executorAllocationRatio": 2,
            },
        ),
    ],
)
def test_greenplum_writer_number_of_connections_less_than_warning_threshold(
    mocker,
    spark_mock,
    caplog,
    df_partitions,
    spark_config,
):
    greenplum = Greenplum(host="some_host", user="user", database="database", password="passwd", spark=spark_mock)
    mocker.patch.object(Greenplum, "check")
    mocker.patch.object(Greenplum, "_get_connections_limits")
    greenplum._get_connections_limits.return_value = GreenplumConnectionLimit(maximum=100, reserved=10, occupied=5)

    mocker.patch("onetl.db.db_writer.db_writer.SparkMetricsRecorder")

    writer = DBWriter(
        connection=greenplum,
        table="schema.table",
    )

    conf = spark_mock.sparkContext.getConf()
    conf.update(spark_config)

    if "local" in conf["spark.master"]:
        parallelism = conf["spark.master"].replace("local[", "").replace("]", "")
        spark_mock.sparkContext.defaultParallelism = int(parallelism)

    df = spark_mock.read.format().options().load()
    df.isStreaming = False
    df.rdd.getNumPartitions = Mock(return_value=df_partitions)

    with caplog.at_level(logging.WARNING):
        writer.run(df)

    assert not caplog.records


@pytest.mark.parametrize(
    ["df_partitions", "spark_config", "parallel_connections"],
    [
        (31, {"spark.master": "local[200]"}, 31),
        (200, {"spark.master": "local[31]"}, 31),
        (99, {"spark.master": "local[200]"}, 99),
        (200, {"spark.master": "local[99]"}, 99),
        (31, {"spark.master": "yarn", "spark.executor.instances": 200, "spark.executor.cores": 1}, 31),
        (200, {"spark.master": "yarn", "spark.executor.instances": 31, "spark.executor.cores": 1}, 31),
        (99, {"spark.master": "yarn", "spark.executor.instances": 200, "spark.executor.cores": 1}, 99),
        (200, {"spark.master": "yarn", "spark.executor.instances": 99, "spark.executor.cores": 1}, 99),
        (31, {"spark.master": "yarn", "spark.executor.instances": 20, "spark.executor.cores": 10}, 31),
        (200, {"spark.master": "yarn", "spark.executor.instances": 4, "spark.executor.cores": 10}, 40),
        (
            31,
            {
                "spark.master": "yarn",
                "spark.dynamicAllocation.enabled": "true",
                "spark.dynamicAllocation.maxExecutors": 20,
                "spark.executor.cores": 10,
            },
            31,
        ),
        (
            99,
            {
                "spark.master": "yarn",
                "spark.dynamicAllocation.enabled": "true",
                "spark.dynamicAllocation.maxExecutors": 20,
                "spark.executor.cores": 10,
            },
            99,
        ),
        (
            200,
            {
                "spark.master": "yarn",
                "spark.dynamicAllocation.enabled": "true",
                "spark.dynamicAllocation.maxExecutors": 4,
                "spark.executor.cores": 10,
            },
            40,
        ),
        (
            31,
            {
                "spark.master": "yarn",
                "spark.dynamicAllocation.enabled": "true",
                "spark.dynamicAllocation.maxExecutors": 20,
                "spark.executor.cores": 5,
                "spark.dynamicAllocation.executorAllocationRatio": 2,
            },
            31,
        ),
        (
            200,
            {
                "spark.master": "yarn",
                "spark.dynamicAllocation.enabled": "true",
                "spark.dynamicAllocation.maxExecutors": 4,
                "spark.executor.cores": 5,
                "spark.dynamicAllocation.executorAllocationRatio": 2,
            },
            40,
        ),
    ],
)
def test_greenplum_writer_number_of_connections_higher_than_warning_threshold(
    mocker,
    spark_mock,
    caplog,
    df_partitions,
    spark_config,
    parallel_connections,
):
    greenplum = Greenplum(host="some_host", user="user", database="database", password="passwd", spark=spark_mock)
    mocker.patch.object(Greenplum, "check")
    mocker.patch.object(Greenplum, "_get_connections_limits")
    greenplum._get_connections_limits.return_value = GreenplumConnectionLimit(maximum=100, reserved=10, occupied=5)

    mocker.patch("onetl.db.db_writer.db_writer.SparkMetricsRecorder")

    writer = DBWriter(
        connection=greenplum,
        table="schema.table",
    )

    conf = spark_mock.sparkContext.getConf()
    conf.update(spark_config)

    if "local" in conf["spark.master"]:
        parallelism = conf["spark.master"].replace("local[", "").replace("]", "")
        spark_mock.sparkContext.defaultParallelism = int(parallelism)

    df = spark_mock.read.format().options().load()
    df.isStreaming = False
    df.rdd.getNumPartitions = Mock(return_value=df_partitions)

    with caplog.at_level(logging.WARNING):
        writer.run(df)

    assert f"Each parallel job of {parallel_connections} opens a separate connection" in caplog.text


@pytest.mark.parametrize(
    ["df_partitions", "spark_config", "parallel_connections"],
    [
        (100, {"spark.master": "local[200]"}, 100),
        (200, {"spark.master": "local[100]"}, 100),
        (100, {"spark.master": "yarn", "spark.executor.instances": 200, "spark.executor.cores": 1}, 100),
        (200, {"spark.master": "yarn", "spark.executor.instances": 100, "spark.executor.cores": 1}, 100),
        (100, {"spark.master": "yarn", "spark.executor.instances": 20, "spark.executor.cores": 10}, 100),
        (200, {"spark.master": "yarn", "spark.executor.instances": 10, "spark.executor.cores": 10}, 100),
        (
            100,
            {
                "spark.master": "yarn",
                "spark.dynamicAllocation.enabled": "true",
                "spark.dynamicAllocation.maxExecutors": 20,
                "spark.executor.cores": 10,
            },
            100,
        ),
        (
            200,
            {
                "spark.master": "yarn",
                "spark.dynamicAllocation.enabled": "true",
                "spark.dynamicAllocation.maxExecutors": 10,
                "spark.executor.cores": 10,
            },
            100,
        ),
        (
            100,
            {
                "spark.master": "yarn",
                "spark.dynamicAllocation.enabled": "true",
                "spark.dynamicAllocation.maxExecutors": 20,
                "spark.executor.cores": 5,
                "spark.dynamicAllocation.executorAllocationRatio": 2,
            },
            100,
        ),
        (
            200,
            {
                "spark.master": "yarn",
                "spark.dynamicAllocation.enabled": "true",
                "spark.dynamicAllocation.maxExecutors": 10,
                "spark.executor.cores": 5,
                "spark.dynamicAllocation.executorAllocationRatio": 2,
            },
            100,
        ),
        (
            100,
            {
                "spark.master": "yarn",
                "spark.dynamicAllocation.enabled": "true",
                "spark.dynamicAllocation.maxExecutors": "infinity",
            },
            100,
        ),
        (
            200,
            {
                "spark.master": "yarn",
                "spark.dynamicAllocation.enabled": "true",
                "spark.dynamicAllocation.maxExecutors": "infinity",
            },
            200,
        ),
    ],
)
def test_greenplum_writer_number_of_connections_higher_than_exception_threshold(
    mocker,
    spark_mock,
    df_partitions,
    spark_config,
    parallel_connections,
):
    greenplum = Greenplum(host="some_host", user="user", database="database", password="passwd", spark=spark_mock)
    mocker.patch.object(Greenplum, "check")
    mocker.patch.object(Greenplum, "_get_connections_limits")
    greenplum._get_connections_limits.return_value = GreenplumConnectionLimit(maximum=100, reserved=10, occupied=5)

    mocker.patch("onetl.db.db_writer.db_writer.SparkMetricsRecorder")

    writer = DBWriter(
        connection=greenplum,
        table="schema.table",
    )

    conf = spark_mock.sparkContext.getConf()
    conf.update(spark_config)

    if "local" in conf["spark.master"]:
        parallelism = conf["spark.master"].replace("local[", "").replace("]", "")
        spark_mock.sparkContext.defaultParallelism = int(parallelism)

    df = spark_mock.read.format().options().load()
    df.isStreaming = False
    df.rdd.getNumPartitions = Mock(return_value=df_partitions)

    with pytest.raises(
        TooManyParallelJobsError,
        match=f"Each parallel job of {parallel_connections} opens a separate connection",
    ):
        writer.run(df)
