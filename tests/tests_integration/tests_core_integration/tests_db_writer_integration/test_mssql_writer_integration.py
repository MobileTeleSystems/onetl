from onetl.core import DBWriter
from onetl.connection import MSSQL


def test_mssql_writer_snapshot(spark, processing, prepare_schema_table):
    df = processing.create_spark_df(spark=spark)
    mssql = MSSQL(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
        extra={"trustServerCertificate": "true"},
    )
    writer = DBWriter(
        connection=mssql,
        table=prepare_schema_table.full_name,
    )
    writer.run(df)
    processing.assert_equal_df(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        df=df,
    )
