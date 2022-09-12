from __future__ import annotations

from typing import Any, Optional

from etl_entities import HWM

from onetl.impl import BaseModel
from onetl.strategy.batch_hwm_strategy import BatchHWMStrategy
from onetl.strategy.hwm_strategy import HWMStrategy


class OffsetMixin(BaseModel):
    hwm: Optional[HWM] = None
    offset: Any = None

    def fetch_hwm(self) -> None:
        super().fetch_hwm()

        if self.hwm and self.offset is not None:
            self.hwm -= self.offset  # noqa: WPS601


class IncrementalStrategy(OffsetMixin, HWMStrategy):
    """Incremental strategy for DBReader.

    First incremental run is just the same as :obj:`onetl.strategy.snapshot_strategy.SnapshotStrategy`:

    .. code:: sql

        SELECT id, data FROM mydata;

    After first max value of ``id`` column (e.g. ``1000``) will be saved as HWM to HWM Store.

    Next incremental run will read only new data from the source:

    .. code:: sql

        SELECT id, data FROM mydata WHERE id > 1000; -- hwm value

    Pay attention to resulting dataframe **does not include** row with ``id=1000`` because it has been read before.

    Parameters
    ----------
    offset : Any, default: ``None``

        If passed, the offset value will be used to read rows which appeared in the source after the previous read.

        For example, previous incremental run returned rows with ``id`` 897, 898, 899 and 1000.
        Current HWM value is 1000.

        But since then few more rows appeared in the source with ``id`` between 900 and 999,
        and you need to read them too.

        So you can set ``offset`` to ``100``, so next incremental run will start with a query:

        .. code:: sql

            SELECT id, data FROM public.mydata WHERE id > 900;
            -- 900 = 1000 - 100 = hwm - offset

        .. warning::

            This can lead to reading duplicated values from the table.
            You probably need additional deduplication step to handle them

        .. warning::

            You cannot set offset while using :obj:`onetl.core.file_downloader.file_downloader.FileDownloader`
            with ``hwm_type`` parameter

    Examples
    --------

    Incremental run

    .. code:: python

        from onetl.connection import Postgres
        from onetl.core import DBReader
        from onetl.strategy import IncrementalStrategy

        from mtspark import get_spark

        spark = get_spark({"appName": "spark-app-name"})

        postgres = Postgres(
            host="test-db-vip.msk.mts.ru",
            user="appmetrica_test",
            password="*****",
            database="target_database",
            spark=spark,
        )

        reader = DBReader(
            postgres,
            table="public.mydata",
            columns=["id", "data"],
            hwm_column="id",
        )

        writer = DBWriter(hive, "newtable")

        with IncrementalStrategy():
            df = reader.run()

    .. code:: sql

        -- previous HWM value was 1000
        -- DBReader will generate query like:

        SELECT id, data
        FROM public.mydata
        WHERE id > 1000; --- from HWM (EXCLUDING first row)

    Incremental run with offset

    .. code:: python

        ...

        with IncrementalStrategy(offset=100):
            df = reader.run()
            writer.run(df)

    .. code:: sql

        -- previous HWM value was 1000
        -- DBReader will generate query like:

        SELECT id, data
        FROM public.mydata
        WHERE id > 900; --- from HWM-offset (EXCLUDING first row)

    ``offset`` could be any HWM type, not only integer

    .. code:: python

        from datetime import timedelta

        reader = DBReader(
            postgres,
            table="public.mydata",
            columns=["business_dt", "data"],
            hwm_column="business_dt",
        )

        with IncrementalStrategy(offset=timedelta(days=1)):
            df = reader.run()
            writer.run(df)

    .. code:: sql

        -- previous HWM value was '2021-01-10'
        -- DBReader will generate query like:

        SELECT business_dt, data
        FROM public.mydata
        WHERE business_dt > cast('2021-01-09' as timestamp);
    """


class IncrementalBatchStrategy(OffsetMixin, BatchHWMStrategy):
    """Incremental batch strategy for DBReader.

    Same as :obj:`onetl.strategy.incremental_strategy.IncrementalStrategy`,
    but reads data from the source in batches (1..N) like:

    .. code:: sql

        1: SELECT id, data FROM public.mydata WHERE id > 1000 AND id <= 1100; -- previous HWM value is 1000, step is 100
        2: SELECT id, data FROM public.mydata WHERE id > 1100 AND id <= 1200;
        3: SELECT id, data FROM public.mydata WHERE id > 1200 AND id <= 1200;
        N: SELECT id, data FROM public.mydata WHERE id > 1300 AND id <= 1400; -- until stop

    This allows to use less resources than reading all the data in the one batch.

    Parameters
    ----------
    step : Any

        The value of step which will be used to generate batch SQL queries like:

        .. code:: sql

            SELECT id, data
            FROM public.mydata
            WHERE id > 1000 AND id <= 1100; -- 1000 is previous HWM value, step is 100

    stop : Any, default: ``None``

        If passed, the value will be used as a maximum value of ``hwm_column`` which will be read from the source.

        If not set, the value is determined by a separated query:

        .. code:: sql

            SELECT max(id) as stop
            FROM public.mydata
            WHERE id > 1000; -- 1000 is previous HWM value (if any)

    offset : Any, default: ``None``

        If passed, the offset value will be used to read rows which appeared in the source after the previous read.

        For example, previous incremental run returned rows with ``id`` 897, 898, 899 and 1000.
        So HWM value is 1000.

        But since then few more rows appeared in the source with ``id`` between 900 and 999,
        and you need to read them too.

        So you can set ``offset`` to ``100``, so next incremental start with a query like:

        .. code:: sql

            SELECT id, data
            FROM public.mydata
            WHERE id > 900 AND id <= 1000; -- 900 = 1000 - 100 = HWM - offset

        .. warning::

            This can lead to reading duplicated values from the table.
            You probably need additional deduplication step to handle them

    Examples
    --------

    IncrementalBatch run

    .. code:: python

        from onetl.connection import Postgres, Hive
        from onetl.core import DBReader
        from onetl.strategy import IncrementalStrategy

        from mtspark import get_spark

        spark = get_spark({"appName": "spark-app-name"})

        postgres = Postgres(
            host="test-db-vip.msk.mts.ru",
            user="appmetrica_test",
            password="*****",
            database="target_database",
            spark=spark,
        )

        hive = Hive(spark=spark)

        reader = DBReader(
            postgres,
            table="public.mydata",
            columns=["id", "data"],
            hwm_column="id",
        )

        writer = DBWriter(hive, "newtable")

        with IncrementalBatchStrategy(step=100) as batches:
            for _ in batches:
                df = reader.run()
                writer.run(df)

    .. code:: sql

        -- previous HWM value was 1000
        -- each batch (1..N) will perform a query which return some part of input data

        1:  SELECT id, data
            FROM public.mydata
            WHERE id > 1100 AND id <= 1200; --- from HWM to HWM+step (EXCLUDING first row)

        2:  WHERE id > 1200 AND id <= 1300; -- + step
        N:  WHERE id > 1300 AND id <= 1400; -- until max value of HWM column

    IncrementalBatch run with stop value

    .. code:: python

        with IncrementalBatchStrategy(step=100, stop=2000) as batches:
            for _ in batches:
                df = reader.run()
                writer.run(df)

    .. code:: sql

        -- previous HWM value was 1000
        -- each batch (1..N) will perform a query which return some part of input data

        1:  SELECT id, data
            FROM public.mydata
            WHERE id > 1000 AND id <= 1100; --- from HWM to HWM+step (EXCLUDING first row)

        2:  WHERE id > 1100 AND id <= 1200; -- + step
        ...
        N:  WHERE id > 1900 AND id <= 2000; -- until stop

    IncrementalBatch run with offset value

    .. code:: python

        with IncrementalBatchStrategy(step=100, offset=100) as batches:
            for _ in batches:
                df = reader.run()
                writer.run(df)

    .. code:: sql

        -- previous HWM value was 1000
        -- each batch (1..N) will perform a query which return some part of input data

        1:  SELECT id, data
            FROM public.mydata
            WHERE id >  900 AND id <= 1000; --- from HWM-offset to HWM-offset+step (EXCLUDING first row)

        2:  WHERE id > 1000 AND id <= 1100; -- + step
        3:  WHERE id > 1100 AND id <= 1200; -- + step
        ...
        N:  WHERE id > 1300 AND id <= 1400; -- until max value of HWM column

    IncrementalBatch run with all possible options

    .. code:: python

        with IncrementalBatchStrategy(
            step=100,
            stop=2000,
            offset=100,
        ) as batches:
            for _ in batches:
                df = reader.run()
                writer.run(df)

    .. code:: sql

        -- previous HWM value was 1000
        -- each batch (1..N) will perform a query which return some part of input data

        1:  SELECT id, data
            FROM public.mydata
            WHERE id > 900 AND id <= 1000; --- from HWM-offset to HWM-offset+step (EXCLUDING first row)

        2:  WHERE id > 1000 AND id <= 1100; -- + step
        3:  WHERE id > 1100 AND id <= 1200; -- + step
        ...
        N:  WHERE id > 1900 AND id <= 2000; -- until stop

    ``step``, ``stop`` and ``offset`` could be any HWM type, not only integer

    .. code:: python

        from datetime import date, timedelta

        reader = DBReader(
            postgres,
            table="public.mydata",
            columns=["business_dt", "data"],
            hwm_column="business_dt",
        )

        with IncrementalBatchStrategy(
            step=timedelta(days=5),
            stop=date("2021-01-31"),
            offset=timedelta(days=1),
        ) as batches:
            for _ in batches:
                df = reader.run()
                writer.run(df)

    .. code:: sql

        -- previous HWM value was '2021-01-10'
        -- each batch (1..N) will perform a query which return some part of input data

        1:  SELECT business_dt, data
            FROM public.mydata
            WHERE business_dt  > CAST('2021-01-09' AS DATE)  -- from HWM-offset (EXCLUDING first row)
            AND   business_dt <= CAST('2021-01-14' AS DATE); -- to HWM-offset+step

        2:  WHERE business_dt  > CAST('2021-01-14' AS DATE) -- + step
            AND   business_dt <= CAST('2021-01-19' AS DATE);

        3:  WHERE business_dt  > CAST('2021-01-19' AS DATE) -- + step
            AND   business_dt <= CAST('2021-01-24' AS DATE);

        ...

        N:  WHERE business_dt  > CAST('2021-01-29' AS DATE)
            AND   business_dt <= CAST('2021-01-31' AS DATE); -- until stop

    """

    def __next__(self):
        result = super().__next__()

        self.save_hwm()

        return result

    @classmethod
    def _log_exclude_fields(cls) -> set[str]:
        return super()._log_exclude_fields() | {"start"}
