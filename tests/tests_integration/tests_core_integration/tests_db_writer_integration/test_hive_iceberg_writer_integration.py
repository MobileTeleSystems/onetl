import pytest

from onetl.connection import Hive
from onetl.db import DBWriter
from onetl.file.format import Iceberg

pytestmark = [pytest.mark.hive, pytest.mark.iceberg]


def test_hive_iceberg_writer(spark_with_hdfs_iceberg, processing, get_schema_table, request):
    df = processing.create_spark_df(spark_with_hdfs_iceberg)
    hive = Hive(cluster="rnd-dwh", spark=spark_with_hdfs_iceberg)
    catalog = "hadoop"
    _, schema, table = get_schema_table

    writer = DBWriter(
        connection=hive,
        target=f"{catalog}.{schema}.{table}",
        options=Hive.WriteOptions(
            if_exists="replace_entire_table",
            format=Iceberg(),
        ),
    )
    writer.run(df)

    def finalizer():
        spark_with_hdfs_iceberg.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.{table}")

    request.addfinalizer(finalizer)

    ddl = hive.sql(f"SHOW CREATE TABLE {catalog}.{schema}.{table}").collect()[0][0]
    assert "USING iceberg" in ddl

    processing.assert_equal_df(
        schema=f"{catalog}.{schema}",
        table=table,
        df=df,
        order_by="id_int",
    )
