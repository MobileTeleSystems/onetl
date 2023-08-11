import pytest

from onetl.connection import MongoDB
from onetl.db import DBWriter

pytestmark = pytest.mark.mongodb


def test_mongodb_writer_with_streaming_df(spark, processing, prepare_schema_table):
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

    streaming_df = spark.readStream.format("rate").load()
    assert streaming_df.isStreaming
    with pytest.raises(ValueError, match="DataFrame is streaming. DBWriter supports only batch DataFrames."):
        writer.run(streaming_df)
