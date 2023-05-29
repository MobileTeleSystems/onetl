import pytest

from onetl.connection import Clickhouse
from onetl.db import DBWriter

pytestmark = pytest.mark.clickhouse


def test_clickhouse_writer_snapshot(spark, processing, prepare_schema_table):
    df = processing.create_spark_df(spark=spark)

    clickhouse = Clickhouse(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    writer = DBWriter(
        connection=clickhouse,
        target=prepare_schema_table.full_name,
    )
    writer.run(df)

    processing.assert_equal_df(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        df=df,
    )
