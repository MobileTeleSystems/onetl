import pytest

from onetl.db import DBWriter

pytestmark = pytest.mark.iceberg


def test_iceberg_writer(spark, iceberg_connection, processing, get_schema_table):
    df = processing.create_spark_df(spark)

    writer = DBWriter(
        connection=iceberg_connection,
        target=get_schema_table.full_name,
    )
    writer.run(df)

    ddl = iceberg_connection.sql(f"SHOW CREATE TABLE {get_schema_table.full_name}").collect()[0][0]
    assert "USING iceberg" in ddl

    processing.assert_equal_df(
        schema=get_schema_table.schema,
        table=get_schema_table.table,
        df=df,
        order_by="id_int",
    )
