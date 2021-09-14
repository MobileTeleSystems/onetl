from onetl.reader import DBReader
from onetl.connection.db_connection import Oracle


class TestDBReader:

    def test_db_reader_columns_list(self):
        reader = DBReader(connection=Oracle(), table='default.test', columns=['d_id', 'd_name', 'd_age'])

        assert isinstance(reader.columns, str)
        assert reader.columns == 'd_id, d_name, d_age'

    def test_db_reader_table_sql_without_extra_params(self):
        reader = DBReader(connection=Oracle(), table='default.test')
        table_sql = reader.get_table_sql_text()

        assert table_sql == 'default.test T'

    def test_db_reader_table_sql_with_extra_params(self):
        reader = DBReader(
            connection=Oracle(),
            table='default.test',
            sql_where='d_id > 100',
            sql_hint='NOWAIT',
            columns=['d_id', 'd_name', 'd_age'],
        )
        table_sql = reader.get_table_sql_text()
        expected_sql = '(SELECT  NOWAIT d_id, d_name, d_age  FROM default.test  WHERE (d_id > 100)) T'

        assert table_sql == expected_sql
