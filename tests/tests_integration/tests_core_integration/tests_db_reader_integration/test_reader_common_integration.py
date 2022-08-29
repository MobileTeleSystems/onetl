import pytest

from onetl.connection import Postgres
from onetl.core import DBReader


@pytest.mark.parametrize(
    "columns,expected_columns",
    [
        ("*", ["id_int", "text_string", "hwm_int", "hwm_date", "hwm_datetime", "float_value"]),
        (["*"], ["id_int", "text_string", "hwm_int", "hwm_date", "hwm_datetime", "float_value"]),
        (["id_int", "text_string", "hwm_int", "hwm_date"], ["id_int", "text_string", "hwm_int", "hwm_date"]),
    ],
    ids=["*", '"[*]"', "columns"],
)
def test_postgres_reader_columns(spark, processing, load_table_data, columns, expected_columns):
    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    reader = DBReader(connection=postgres, table=load_table_data.full_name, columns=columns)

    table_df = reader.run()

    expected_df = processing.get_expected_dataframe(schema=load_table_data.schema, table=load_table_data.table)
    expected_df = expected_df[expected_columns]

    processing.assert_equal_df(other_frame=expected_df, df=table_df)
