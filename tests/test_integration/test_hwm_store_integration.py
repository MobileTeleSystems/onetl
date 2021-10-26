# noinspection PyPackageRequirements

import pytest

from onetl.connection import Postgres
from onetl.reader.db_reader import DBReader
from onetl.strategy import IncrementalStrategy
from onetl.strategy.hwm_store import YAMLHWMStore
from onetl.strategy.hwm_store.hwm_store_manager import HWMStoreManager
from onetl.strategy.hwm_store.memory_hwm_store import MemoryHWMStore


@pytest.mark.parametrize(
    "hwm_store",
    [
        MemoryHWMStore(),
        YAMLHWMStore(),
    ],
)
def test_postgres_hwm_store_integration(spark, processing, prepare_schema_table, hwm_store):
    postgres = Postgres(
        host=processing.host,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    hwm_column = "hwm_int"
    reader = DBReader(connection=postgres, table=prepare_schema_table.full_name, hwm_column=hwm_column)

    # there is a span
    span_length = 100

    # 0..100
    span_begin = 0
    span_end = span_begin + span_length

    span = processing.create_pandas_df(min_id=span_begin, max_id=span_end)

    # insert span
    processing.insert_data(
        schema=prepare_schema_table.schema,
        table=prepare_schema_table.table,
        values=span,
    )

    with hwm_store:
        # incremental run
        with IncrementalStrategy():
            reader.run()

        store = HWMStoreManager.get_current()
        hwm = store.get(f"{prepare_schema_table.full_name}.{hwm_column}")

        # HWM value was updated in the storage
        assert hwm.value == span[hwm_column].max()
