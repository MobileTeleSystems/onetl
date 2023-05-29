import pytest

from onetl.connection import MongoDB
from onetl.db import DBWriter

pytestmark = pytest.mark.mongodb


@pytest.mark.flaky(reruns=2)
def test_mongodb_writer_snapshot(spark, processing, prepare_schema_table):
    df = processing.create_spark_df(spark=spark)

    mongo = MongoDB(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    writer = DBWriter(
        connection=mongo,
        table=prepare_schema_table.table,
    )
    writer.run(df)

    processing.assert_equal_df(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        df=df,
    )
