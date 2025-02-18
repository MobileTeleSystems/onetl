# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging

from onetl.strategy.base_strategy import BaseStrategy
from onetl.strategy.batch_hwm_strategy import BatchHWMStrategy

log = logging.getLogger(__name__)


class SnapshotStrategy(BaseStrategy):
    """Snapshot strategy for :ref:`db-reader`/:ref:`file-downloader`.

    Used for fetching all the rows/files from a source. Does not support HWM.

    .. note::

        This is a default strategy.

    For :ref:`db-reader`:
        Every snapshot run is executing the simple query which fetches all the table data:

        .. code:: sql

            SELECT id, data FROM public.mydata;

    For :ref:`file-downloader`:
        Every snapshot run is downloading all the files (from the source, or user-defined list):

        .. code:: bash

            $ hdfs dfs -ls /path

            /path/my/file1
            /path/my/file2

        .. code:: python

            DownloadResult(
                ...,
                successful={
                    LocalFile("/downloaded/file1"),
                    LocalFile("/downloaded/file2"),
                },
            )

    .. versionadded:: 0.1.0

    Examples
    --------

    .. tabs::

        .. code-tab:: py Snapshot run with :ref:`db-reader`

            from onetl.db import DBReader, DBWriter
            from onetl.strategy import SnapshotStrategy

            reader = DBReader(
                connection=postgres,
                source="public.mydata",
                columns=["id", "data"],
                hwm=DBReader.AutoDetectHWM(name="some_hwm_name", expression="id"),
            )

            writer = DBWriter(connection=hive, target="db.newtable")

            with SnapshotStrategy():
                df = reader.run()
                writer.run(df)

            # current run will execute following query:

            # SELECT id, data FROM public.mydata;

        .. code-tab:: py Snapshot run with :ref:`file-downloader`

            from onetl.file import FileDownloader
            from onetl.strategy import SnapshotStrategy

            downloader = FileDownloader(
                connection=sftp,
                source_path="/remote",
                local_path="/local",
            )

            with SnapshotStrategy():
                df = downloader.run()

            # current run will download all files from 'source_path'
    """


class SnapshotBatchStrategy(BatchHWMStrategy):
    """Snapshot batch strategy for :ref:`db-reader`.

    .. note::

        Cannot be used with :ref:`file-downloader`

    Same as :obj:`SnapshotStrategy <onetl.strategy.snapshot_strategy.SnapshotStrategy>`,
    but reads data from the source in sequential batches (1..N) like:

    .. code:: sql

        1:  SELECT id, data
            FROM public.mydata
            WHERE id >= 1000 AND id <= 1100; -- from start to start+step (INCLUDING first row)

        2:  WHERE id > 1100 AND id <= 1200; -- + step
        3:  WHERE id > 1200 AND id <= 1200; -- + step
        N:  WHERE id > 1300 AND id <= 1400; -- until stop

    This allows to use less CPU and RAM on Spark cluster than reading all the data in parallel,
    but takes proportionally more time.

    .. note::

        This strategy uses HWM column value to filter data for each batch,
        but does **NOT** save it into :ref:`HWM Store <hwm>`.
        So every run starts from the beginning, not from the previous HWM value.

    .. note::

        If you only need to reduce number of rows read by Spark from opened cursor,
        use :obj:`onetl.connection.db_connection.postgres.Postgres.ReadOptions.fetchsize` instead

    .. warning::

        Not every :ref:`DB connection <db-connections>`
        supports batch strategy. For example, Kafka connection doesn't support it.
        Make sure the connection you use is compatible with the SnapshotBatchStrategy.

    .. versionadded:: 0.1.0

    Parameters
    ----------
    step : Any

        Step size used for generating batch SQL queries like:

        .. code:: sql

            SELECT id, data
            FROM public.mydata
            WHERE id >= 1000 AND id <= 1100; -- 1000 is start value, step is 100

        .. note::

            Step defines a range of values will be fetched by each batch. This is **not**
            a number of rows, it depends on a table content and value distribution across the rows.

        .. note::

            ``step`` value will be added to the HWM, so it should have a proper type.

            For example, for ``TIMESTAMP`` column ``step`` type should be :obj:`datetime.timedelta`, not :obj:`int`

    start : Any, default: ``None``

        If passed, the value will be used for generating WHERE clauses with ``hwm.expression`` filter,
        as a start value for the first batch.

        If not set, the value is determined by a separated query:

        .. code:: sql

            SELECT MIN(id) as start
            FROM public.mydata
            WHERE id <= 1400; -- 1400 here is stop value (if set)

        .. note::

            ``start`` should be the same type as ``hwm.expression`` value,
            e.g. :obj:`datetime.datetime` for ``TIMESTAMP`` column, :obj:`datetime.date` for ``DATE``, and so on

    stop : Any, default: ``None``

        If passed, the value will be used for generating WHERE clauses with ``hwm.expression`` filter,
        as a stop value for the last batch.

        If not set, the value is determined by a separated query:

        .. code:: sql

            SELECT MAX(id) as stop
            FROM public.mydata
            WHERE id >= 1000; -- 1000 here is start value (if set)

        .. note::

            ``stop`` should be the same type as ``hwm.expression`` value,
            e.g. :obj:`datetime.datetime` for ``TIMESTAMP`` column, :obj:`datetime.date` for ``DATE``, and so on

    Examples
    --------

    .. tabs::

        .. tab:: SnapshotBatch run

            .. code:: python

                from onetl.db import DBReader, DBWriter
                from onetl.strategy import SnapshotBatchStrategy

                reader = DBReader(
                    connection=postgres,
                    source="public.mydata",
                    columns=["id", "data"],
                    hwm=DBReader.AutoDetectHWM(name="some_hwm_name", expression="id"),
                )

                writer = DBWriter(connection=hive, target="db.newtable")

                with SnapshotBatchStrategy(step=100) as batches:
                    for _ in batches:
                        df = reader.run()
                        writer.run(df)

            .. code:: sql

                -- get start and stop values

                    SELECT MIN(id) as start, MAX(id) as stop
                    FROM public.mydata;

                -- for example, start=1000 and stop=2345

                -- when each batch (1..N) will perform a query which return some part of input data

                1:  SELECT id, data
                    FROM public.mydata
                    WHERE id >= 1000 AND id <= 1100; -- from start to start+step (INCLUDING first row)

                2:  WHERE id > 1100 AND id <= 1200; -- + step
                3:  WHERE id > 1200 AND id <= 1300; -- + step
                N:  WHERE id > 2300 AND id <= 2345; -- until stop

        .. tab:: SnapshotBatch run with ``stop`` value

            .. code:: python

                ...

                with SnapshotBatchStrategy(step=100, stop=1234) as batches:
                    for _ in batches:
                        df = reader.run()
                        writer.run(df)

            .. code:: sql

                -- stop value is set, so there is no need to fetch it from DB
                -- get start value

                    SELECT MIN(id) as start
                    FROM public.mydata
                    WHERE id <= 1234; -- until stop

                -- for example, start=1000.
                -- when each batch (1..N) will perform a query which return some part of input data

                1:  SELECT id, data
                    FROM public.mydata
                    WHERE id >= 1000 AND id <= 1100; -- from start to start+step (INCLUDING first row)

                2:  WHERE id >  1100 AND id <= 1200; -- + step
                3:  WHERE id >  1200 AND id <= 1300; -- + step
                N:  WHERE id >  1300 AND id <= 1234; -- until stop

        .. tab:: SnapshotBatch run with ``start`` value

            .. code:: python

                ...

                with SnapshotBatchStrategy(step=100, start=500) as batches:
                    for _ in batches:
                        df = reader.run()
                        writer.run(df)

            .. code:: sql

                -- start value is set, so there is no need to fetch it from DB
                -- get only stop value

                    SELECT MAX(id) as stop
                    FROM public.mydata
                    WHERE id >= 500; -- from start

                -- for example, stop=2345.
                -- when each batch (1..N) will perform a query which return some part of input data

                1:  SELECT id, data
                    FROM public.mydata
                    WHERE id >= 500 AND id <=  600; -- from start to start+step (INCLUDING first row)

                2:  WHERE id >  600 AND id <=  700; -- + step
                3:  WHERE id >  700 AND id <=  800; -- + step
                ...
                N:  WHERE id > 2300 AND id <= 2345; -- until stop

        .. tab:: SnapshotBatch run with all options

            .. code:: python

                ...

                with SnapshotBatchStrategy(
                    start=1000,
                    step=100,
                    stop=2000,
                ) as batches:
                    for _ in batches:
                        df = reader.run()
                        writer.run(df)

            .. code:: sql

                -- start and stop values are set, so no need to fetch boundaries from DB
                -- each batch (1..N) will perform a query which return some part of input data

                1:    SELECT id, data
                    FROM public.mydata
                    WHERE id >= 1000 AND id <= 1100; -- from start to start+step (INCLUDING first row)

                2:  WHERE id >  1100 AND id <= 1200; -- + step
                3:  WHERE id >  1200 AND id <= 1300; -- + step
                ...
                N:  WHERE id >  1900 AND id <= 2000; -- until stop

        .. tab:: SnapshotBatch run over non-integer column

            ``hwm.expression``, ``start`` and ``stop`` can be a date or datetime, not only integer:

            .. code:: python

                from datetime import date, timedelta

                reader = DBReader(
                    connection=postgres,
                    source="public.mydata",
                    columns=["business_dt", "data"],
                    hwm=DBReader.AutoDetectHWM(name="some_hwm_name", expression="business_dt"),
                )

                with SnapshotBatchStrategy(
                    start=date("2021-01-01"),
                    step=timedelta(days=5),
                    stop=date("2021-01-31"),
                ) as batches:
                    for _ in batches:
                        df = reader.run()
                        writer.run(df)

            .. code:: sql

                -- start and stop values are set, so no need to fetch boundaries from DB
                -- each batch will perform a query which return some part of input data
                -- HWM value will casted to match column type


                1:  SELECT business_dt, data
                    FROM public.mydata
                    WHERE business_dt >= CAST('2020-01-01' AS DATE) -- from start to start+step (INCLUDING first row)
                    AND   business_dt <= CAST('2021-01-05' AS DATE);

                2:  WHERE business_dt >  CAST('2021-01-05' AS DATE) -- + step
                    AND   business_dt <= CAST('2021-01-10' AS DATE);

                3:  WHERE business_dt >  CAST('2021-01-10' AS DATE) -- + step
                    AND   business_dt <= CAST('2021-01-15' AS DATE);

                ...

                N:  WHERE business_dt >  CAST('2021-01-30' AS DATE)
                    AND   business_dt <= CAST('2021-01-31' AS DATE); -- until stop

    """

    def fetch_hwm(self) -> None:
        pass

    def save_hwm(self) -> None:
        pass
