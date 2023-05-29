import tempfile

import pytest

from onetl.connection import Postgres
from onetl.db import DBReader
from onetl.hwm.store import MemoryHWMStore, YAMLHWMStore
from onetl.strategy import IncrementalStrategy

hwm_store = [
    MemoryHWMStore(),
    YAMLHWMStore(path=tempfile.mktemp("hwmstore")),  # noqa: S306 NOSONAR
]


@pytest.mark.postgres
@pytest.mark.parametrize("hwm_store", hwm_store)
def test_postgres_hwm_store_integration_with_reader(spark, processing, prepare_schema_table, hwm_store):
    postgres = Postgres(
        host=processing.host,
        port=processing.port,
        user=processing.user,
        password=processing.password,
        database=processing.database,
        spark=spark,
    )

    hwm_column = "hwm_int"
    reader = DBReader(connection=postgres, source=prepare_schema_table.full_name, hwm_column=hwm_column)

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
        with IncrementalStrategy() as strategy:
            reader.run()
            strategy_hwm = strategy.hwm

        # HWM value was saved into the storage
        saved_hwm = hwm_store.get(strategy_hwm.qualified_name)

        assert saved_hwm.value == span[hwm_column].max()
