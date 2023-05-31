import pytest

from onetl.connection import Oracle
from onetl.db import DBWriter

pytestmark = pytest.mark.oracle


def test_oracle_writer_snapshot(spark, processing, prepare_schema_table):
    df = processing.create_spark_df(spark=spark)

    oracle = Oracle(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        spark=spark,
        sid=processing.sid,
        service_name=processing.service_name,
    )

    writer = DBWriter(
        connection=oracle,
        target=prepare_schema_table.full_name,
    )

    writer.run(df)

    processing.assert_equal_df(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        df=df,
    )
