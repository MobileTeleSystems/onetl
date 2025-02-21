import logging
from unittest.mock import Mock

import pytest

from onetl.connection import Greenplum
from onetl.connection.db_connection.greenplum.connection_limit import (
    GreenplumConnectionLimit,
)
from onetl.db import DBReader
from onetl.exception import TooManyParallelJobsError

pytestmark = pytest.mark.greenplum


def test_greenplum_reader_snapshot_error_pass_df_schema(spark_mock):
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    df_schema = StructType(
        [
            StructField("_id", IntegerType()),
            StructField("text_string", StringType()),
            StructField("hwm_int", IntegerType()),
            StructField("hwm_datetime", TimestampType()),
            StructField("float_value", DoubleType()),
        ],
    )

    greenplum = Greenplum(host="some_host", user="user", database="database", password="passwd", spark=spark_mock)

    with pytest.raises(ValueError, match="'df_schema' parameter is not supported by Greenplum"):
        DBReader(
            connection=greenplum,
            table="schema.table",
            df_schema=df_schema,
        )


def test_greenplum_reader_wrong_table_name(spark_mock):
    greenplum = Greenplum(host="some_host", user="user", database="database", password="passwd", spark=spark_mock)

    with pytest.raises(ValueError, match="Name should be passed in `schema.name` format"):
        DBReader(
            connection=greenplum,
            table="table",  # Required format: table="schema.table"
        )


def test_greenplum_reader_hint_unsupported(spark_mock):
    greenplum = Greenplum(host="some_host", user="user", database="database", password="passwd", spark=spark_mock)

    with pytest.raises(
        ValueError,
        match="'hint' parameter is not supported by Greenplum",
    ):
        DBReader(
            connection=greenplum,
            hint="col1",
            table="schema.table",
        )


def test_greenplum_reader_wrong_where_type(spark_mock):
    greenplum = Greenplum(host="some_host", user="user", database="database", password="passwd", spark=spark_mock)

    with pytest.raises(
        ValueError,
        match="Greenplum requires 'where' parameter type to be 'str', got 'dict'",
    ):
        DBReader(
            connection=greenplum,
            where={"col1": 1},
            table="schema.table",
        )


@pytest.mark.parametrize(
    ["df_partitions", "spark_config"],
    [
        pytest.param(30, {"spark.master": "local[200]"}, id="small_df, local[200]"),
        pytest.param(200, {"spark.master": "local[30]"}, id="large_df, local[30]"),
        pytest.param(
            30,
            {"spark.master": "yarn", "spark.executor.instances": 200, "spark.executor.cores": 1},
            id="yarn[small_df, 200, static]",
        ),
        pytest.param(
            200,
            {"spark.master": "yarn", "spark.executor.instances": 30, "spark.executor.cores": 1},
            id="yarn[large_df, 30, static]",
        ),
        pytest.param(
            30,
            {"spark.master": "yarn", "spark.executor.instances": 20, "spark.executor.cores": 10},
            id="yarn[small_df, 20*10, static]",
        ),
        pytest.param(
            200,
            {"spark.master": "yarn", "spark.executor.instances": 3, "spark.executor.cores": 10},
            id="yarn[large_df, 3*10, static]",
        ),
        pytest.param(
            30,
            {
                "spark.master": "yarn",
                "spark.dynamicAllocation.enabled": "true",
                "spark.dynamicAllocation.maxExecutors": 20,
                "spark.executor.cores": 10,
            },
            id="yarn[small_df, 20*10, dynamic]",
        ),
        pytest.param(
            200,
            {
                "spark.master": "yarn",
                "spark.dynamicAllocation.enabled": "true",
                "spark.dynamicAllocation.maxExecutors": 3,
                "spark.executor.cores": 10,
            },
            id="yarn[large_df, 3*10, dynamic]",
        ),
        pytest.param(
            30,
            {
                "spark.master": "yarn",
                "spark.dynamicAllocation.enabled": "true",
                "spark.dynamicAllocation.maxExecutors": 20,
                "spark.executor.cores": 5,
                "spark.dynamicAllocation.executorAllocationRatio": 2,
            },
            id="yarn[small_df, 20*5*2, dynamic]",
        ),
        pytest.param(
            200,
            {
                "spark.master": "yarn",
                "spark.dynamicAllocation.enabled": "true",
                "spark.dynamicAllocation.maxExecutors": 3,
                "spark.executor.cores": 5,
                "spark.dynamicAllocation.executorAllocationRatio": 2,
            },
            id="yarn[large_df, 3*5*2, dynamic]",
        ),
    ],
)
def test_greenplum_reader_number_of_connections_less_than_warning_threshold(
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

    reader = DBReader(
        connection=greenplum,
        table="schema.table",
    )

    conf = spark_mock.sparkContext.getConf()
    conf.update(spark_config)

    if "local" in conf["spark.master"]:
        parallelism = conf["spark.master"].replace("local[", "").replace("]", "")
        spark_mock._jsc.sc().schedulerBackend().totalCores.return_value = int(parallelism)

    df = spark_mock.read.format().options().load()
    df.rdd.getNumPartitions = Mock(return_value=df_partitions)

    with caplog.at_level(logging.WARNING):
        df = reader.run()

    assert not caplog.records


@pytest.mark.parametrize(
    ["df_partitions", "spark_config", "parallel_connections"],
    [
        pytest.param(31, {"spark.master": "local[200]"}, 31, id="small_df, local[200]"),
        pytest.param(200, {"spark.master": "local[31]"}, 31, id="large_df, local[31]"),
        pytest.param(99, {"spark.master": "local[200]"}, 99, id="medium_df, local[200]"),
        pytest.param(200, {"spark.master": "local[99]"}, 99, id="large_df, local[99]"),
        pytest.param(
            31,
            {"spark.master": "yarn", "spark.executor.instances": 200, "spark.executor.cores": 1},
            31,
            id="small_df, yarn[200, static]",
        ),
        pytest.param(
            200,
            {"spark.master": "yarn", "spark.executor.instances": 31, "spark.executor.cores": 1},
            31,
            id="large_df, yarn[31, static]",
        ),
        pytest.param(
            99,
            {"spark.master": "yarn", "spark.executor.instances": 200, "spark.executor.cores": 1},
            99,
            id="medium_df, yarn[200, static]",
        ),
        pytest.param(
            200,
            {"spark.master": "yarn", "spark.executor.instances": 99, "spark.executor.cores": 1},
            99,
            id="large_df, yarn[99, static]",
        ),
        pytest.param(
            31,
            {"spark.master": "yarn", "spark.executor.instances": 20, "spark.executor.cores": 10},
            31,
            id="small_df, yarn[20*10, static]",
        ),
        pytest.param(
            200,
            {"spark.master": "yarn", "spark.executor.instances": 4, "spark.executor.cores": 10},
            40,
            id="large_df, yarn[4*10, static]",
        ),
        pytest.param(
            31,
            {
                "spark.master": "yarn",
                "spark.dynamicAllocation.enabled": "true",
                "spark.dynamicAllocation.maxExecutors": 20,
                "spark.executor.cores": 10,
            },
            31,
            id="small_df, yarn[20*10, dynamic]",
        ),
        pytest.param(
            99,
            {
                "spark.master": "yarn",
                "spark.dynamicAllocation.enabled": "true",
                "spark.dynamicAllocation.maxExecutors": 20,
                "spark.executor.cores": 10,
            },
            99,
            id="medium_df, yarn[20*10, dynamic]",
        ),
        pytest.param(
            200,
            {
                "spark.master": "yarn",
                "spark.dynamicAllocation.enabled": "true",
                "spark.dynamicAllocation.maxExecutors": 4,
                "spark.executor.cores": 10,
            },
            40,
            id="large_df, yarn[4*10, dynamic]",
        ),
        pytest.param(
            31,
            {
                "spark.master": "yarn",
                "spark.dynamicAllocation.enabled": "true",
                "spark.dynamicAllocation.maxExecutors": 20,
                "spark.executor.cores": 5,
                "spark.dynamicAllocation.executorAllocationRatio": 2,
            },
            31,
            id="small_df, yarn[20*5*2, dynamic]",
        ),
        pytest.param(
            200,
            {
                "spark.master": "yarn",
                "spark.dynamicAllocation.enabled": "true",
                "spark.dynamicAllocation.maxExecutors": 4,
                "spark.executor.cores": 5,
                "spark.dynamicAllocation.executorAllocationRatio": 2,
            },
            40,
            id="large_df, yarn[4*5*2, dynamic]",
        ),
    ],
)
def test_greenplum_reader_number_of_connections_higher_than_warning_threshold(
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

    reader = DBReader(
        connection=greenplum,
        table="schema.table",
    )

    conf = spark_mock.sparkContext.getConf()
    conf.update(spark_config)

    if "local" in conf["spark.master"]:
        parallelism = conf["spark.master"].replace("local[", "").replace("]", "")
        spark_mock._jsc.sc().schedulerBackend().totalCores.return_value = int(parallelism)

    df = spark_mock.read.format().options().load()
    df.rdd.getNumPartitions = Mock(return_value=df_partitions)

    with caplog.at_level(logging.WARNING):
        df = reader.run()

    assert f"Each parallel job of {parallel_connections} opens a separate connection" in caplog.text


@pytest.mark.parametrize(
    ["df_partitions", "spark_config", "parallel_connections"],
    [
        pytest.param(100, {"spark.master": "local[200]"}, 100, id="large_df, local[200]"),
        pytest.param(200, {"spark.master": "local[100]"}, 100, id="extra_large_df, local[100]"),
        pytest.param(
            100,
            {"spark.master": "yarn", "spark.executor.instances": 200, "spark.executor.cores": 1},
            100,
            id="large_df, yarn[200, static]",
        ),
        pytest.param(
            200,
            {"spark.master": "yarn", "spark.executor.instances": 100, "spark.executor.cores": 1},
            100,
            id="extra_large_df, yarn[100, static]",
        ),
        pytest.param(
            100,
            {"spark.master": "yarn", "spark.executor.instances": 20, "spark.executor.cores": 10},
            100,
            id="large_df, yarn[20*10, static]",
        ),
        pytest.param(
            200,
            {"spark.master": "yarn", "spark.executor.instances": 10, "spark.executor.cores": 10},
            100,
            id="extra_large_df, yarn[10*10, static]",
        ),
        pytest.param(
            100,
            {
                "spark.master": "yarn",
                "spark.dynamicAllocation.enabled": "true",
                "spark.dynamicAllocation.maxExecutors": 20,
                "spark.executor.cores": 10,
            },
            100,
            id="large_df, yarn[20*10, dynamic]",
        ),
        pytest.param(
            200,
            {
                "spark.master": "yarn",
                "spark.dynamicAllocation.enabled": "true",
                "spark.dynamicAllocation.maxExecutors": 10,
                "spark.executor.cores": 10,
            },
            100,
            id="extra_large_df, yarn[10*10, dynamic]",
        ),
        pytest.param(
            100,
            {
                "spark.master": "yarn",
                "spark.dynamicAllocation.enabled": "true",
                "spark.dynamicAllocation.maxExecutors": 20,
                "spark.executor.cores": 5,
                "spark.dynamicAllocation.executorAllocationRatio": 2,
            },
            100,
            id="large_df, yarn[20*5*2, dynamic]",
        ),
        pytest.param(
            200,
            {
                "spark.master": "yarn",
                "spark.dynamicAllocation.enabled": "true",
                "spark.dynamicAllocation.maxExecutors": 10,
                "spark.executor.cores": 5,
                "spark.dynamicAllocation.executorAllocationRatio": 2,
            },
            100,
            id="extra_large_df, yarn[10*5*2, dynamic]",
        ),
        pytest.param(
            100,
            {
                "spark.master": "yarn",
                "spark.dynamicAllocation.enabled": "true",
                "spark.dynamicAllocation.maxExecutors": "infinity",
            },
            100,
            id="large_df, yarn[infinity, dynamic]",
        ),
        pytest.param(
            200,
            {
                "spark.master": "yarn",
                "spark.dynamicAllocation.enabled": "true",
                "spark.dynamicAllocation.maxExecutors": "infinity",
            },
            200,
            id="extra_large_df, yarn[infinity, dynamic]",
        ),
    ],
)
def test_greenplum_reader_number_of_connections_higher_than_exception_threshold(
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

    reader = DBReader(
        connection=greenplum,
        table="schema.table",
    )

    conf = spark_mock.sparkContext.getConf()
    conf.update(spark_config)

    if "local" in conf["spark.master"]:
        parallelism = conf["spark.master"].replace("local[", "").replace("]", "")
        spark_mock._jsc.sc().schedulerBackend().totalCores.return_value = int(parallelism)

    df = spark_mock.read.format().options().load()
    df.rdd.getNumPartitions = Mock(return_value=df_partitions)

    with pytest.raises(
        TooManyParallelJobsError,
        match=f"Each parallel job of {parallel_connections} opens a separate connection",
    ):
        df = reader.run()
