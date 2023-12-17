#  Copyright 2023 MTS (Mobile Telesystems)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from __future__ import annotations

from typing import Any, Optional

from etl_entities.hwm import HWM

from onetl.impl import BaseModel
from onetl.strategy.batch_hwm_strategy import BatchHWMStrategy
from onetl.strategy.hwm_strategy import HWMStrategy


class OffsetMixin(BaseModel):
    hwm: Optional[HWM] = None
    offset: Any = None

    def fetch_hwm(self) -> None:
        super().fetch_hwm()

        if self.hwm and self.hwm.value is not None and self.offset is not None:
            self.hwm -= self.offset


class IncrementalStrategy(OffsetMixin, HWMStrategy):
    """Incremental strategy for :ref:`db-reader`/:ref:`file-downloader`.

    Used for fetching only new rows/files from a source
    by filtering items not covered by the previous :ref:`HWM` value.

    For :ref:`db-reader`:
        First incremental run is just the same as :obj:`SnapshotStrategy <onetl.strategy.snapshot_strategy.SnapshotStrategy>`:

        .. code:: sql

            SELECT id, data FROM mydata;

        Then the max value of ``id`` column (e.g. ``1000``) will be saved as ``HWM`` to :ref:`HWM Store <hwm>`.

        Next incremental run will read only new data from the source:

        .. code:: sql

            SELECT id, data FROM mydata WHERE id > 1000; -- hwm value

        Pay attention to resulting dataframe **does not include** row with ``id=1000`` because it has been read before.

        .. warning::

            If code inside the context manager raised an exception, like:

            .. code:: python

                with IncrementalStrategy():
                    df = reader.run()  # something went wrong here
                    writer.run(df)  # or here
                    # or here...

            When DBReader will **NOT** update HWM in HWM Store.
            This allows to resume reading process from the *last successful run*.

    For :ref:`file-downloader`:
        Behavior depends on ``hwm`` type.

        ``hwm=FileListHWM(...)``:
            First incremental run is just the same as :obj:`SnapshotStrategy <onetl.strategy.snapshot_strategy.SnapshotStrategy>` -
            all files are downloaded:

            .. code:: bash

                $ hdfs dfs -ls /path

                /path/my/file1
                /path/my/file2

            .. code:: python

                assert download_result == DownloadResult(
                    successful=[
                        "/path/my/file1",
                        "/path/my/file2",
                    ]
                )

            Then the downloaded files list is saved as ``FileListHWM`` object into :ref:`HWM Store <hwm>`:

            .. code:: python

                [
                    "/path/my/file1",
                    "/path/my/file2",
                ]

            Next incremental run will download only new files from the source:

            .. code:: bash

                $ hdfs dfs -ls /path

                /path/my/file1
                /path/my/file2
                /path/my/file3

            .. code:: python

                # only files which are not in FileListHWM

                assert download_result == DownloadResult(
                    successful=[
                        "/path/my/file3",
                    ]
                )

            New files will be added to the ``FileListHWM`` and saved to :ref:`HWM Store <hwm>`:

            .. code:: python

                [
                    "/path/my/file1",
                    "/path/my/file2",
                    "/path/my/file3",
                ]

        .. warning::

            FileDownload updates HWM in HWM Store at the end of ``.run()`` call,
            **NOT** while exiting strategy context. This is because:

            * FileDownloader does not raise exceptions if some file cannot be downloaded.
            * FileDownloader creates files on local filesystem, and file content may differ for different :obj:`modes <onetl.file.file_downloader.file_downloader.FileDownloader.Options.mode>`.
            * It can remove files from the source if :obj:`delete_source <onetl.file.file_downloader.file_downloader.FileDownloader.Options.delete_source>` is set to ``True``.

    Parameters
    ----------
    offset : Any, default: ``None``

        If passed, the offset value will be used to read rows which appeared in the source after the previous read.

        For example, previous incremental run returned rows:

        .. code::

            898
            899
            900
            1000

        Current HWM value is 1000.

        But since then few more rows appeared in the source:

        .. code::

            898
            899
            900
            901 # new
            902 # new
            ...
            999 # new
            1000

        and you need to read them too.

        So you can set ``offset=100``, so a next incremental run will generate SQL query like:

        .. code:: sql

            SELECT id, data FROM public.mydata WHERE id > 900;
            -- 900 = 1000 - 100 = hwm - offset

        and return rows since 901 (**not** 900), **including** 1000 which was already captured by HWM.

        .. warning::

            This can lead to reading duplicated values from the table.
            You probably need additional deduplication step to handle them

        .. warning::

            Cannot be used with :ref:`file-downloader` and ``hwm=FileListHWM(...)``

        .. note::

            ``offset`` value will be subtracted from the HWM, so it should have a proper type.

            For example, for ``TIMESTAMP`` column ``offset`` type should be :obj:`datetime.timedelta`, not :obj:`int`

    Examples
    --------

    Incremental run with :ref:`db-reader`:

    .. code:: python

        from onetl.connection import Postgres
        from onetl.db import DBReader
        from onetl.strategy import IncrementalStrategy
        from onetl.hwm import AutoDetectHWM

        from pyspark.sql import SparkSession

        maven_packages = Postgres.get_packages()
        spark = (
            SparkSession.builder.appName("spark-app-name")
            .config("spark.jars.packages", ",".join(maven_packages))
            .getOrCreate()
        )

        postgres = Postgres(
            host="postgres.domain.com",
            user="myuser",
            password="*****",
            database="target_database",
            spark=spark,
        )

        reader = DBReader(
            connection=postgres,
            source="public.mydata",
            columns=["id", "data"],
            hwm=DBReader.AutoDetectHWM(name="some_hwm_name", expression="id"),
        )

        writer = DBWriter(connection=hive, target="newtable")

        with IncrementalStrategy():
            df = reader.run()
            writer.run(df)

    .. code:: sql

        -- previous HWM value was 1000
        -- DBReader will generate query like:

        SELECT id, data
        FROM public.mydata
        WHERE id > 1000; --- from HWM (EXCLUDING first row)

    Incremental run with :ref:`db-reader` and ``offset``:

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
        WHERE id > 900; -- from HWM-offset (EXCLUDING first row)

    ``hwm.expression`` can be a date or datetime, not only integer:

    .. code:: python

        from datetime import timedelta

        reader = DBReader(
            connection=postgres,
            source="public.mydata",
            columns=["business_dt", "data"],
            hwm=DBReader.AutoDetectHWM(name="some_hwm_name", expression="business_dt"),
        )

        with IncrementalStrategy(offset=timedelta(days=1)):
            df = reader.run()
            writer.run(df)

    .. code:: sql

        -- previous HWM value was '2021-01-10'
        -- DBReader will generate query like:

        SELECT business_dt, data
        FROM public.mydata
        WHERE business_dt > CAST('2021-01-09' AS DATE); -- from HWM-offset (EXCLUDING first row)

    Incremental run with :ref:`file-downloader` and ``hwm=FileListHWM(...)``:

    .. code:: python

        from onetl.connection import SFTP
        from onetl.file import FileDownloader
        from onetl.strategy import SnapshotStrategy
        from etl_entities import FileListHWM

        sftp = SFTP(
            host="sftp.domain.com",
            user="user",
            password="*****",
        )

        downloader = FileDownloader(
            connection=sftp,
            source_path="/remote",
            local_path="/local",
            hwm=FileListHWM(name="some_hwm_name"),
        )

        with IncrementalStrategy():
            df = downloader.run()

        # current run will download only files which were not downloaded in previous runs
    """


class IncrementalBatchStrategy(OffsetMixin, BatchHWMStrategy):
    """Incremental batch strategy for :ref:`db-reader`.

    .. note::

        Cannot be used with :ref:`file-downloader`

    Same as :obj:`IncrementalStrategy <onetl.strategy.incremental_strategy.IncrementalStrategy>`,
    but reads data from the source in sequential batches (1..N) like:

    .. code:: sql

        1: SELECT id, data
           FROM public.mydata
           WHERE id > 1000 AND id <= 1100; -- previous HWM value is 1000, step is 100

        2: WHERE id > 1100 AND id <= 1200; -- + step
        3: WHERE id > 1200 AND id <= 1200; -- + step
        N: WHERE id > 1300 AND id <= 1400; -- until stop

    This allows to use less CPU and RAM than reading all the data in the one batch,
    but takes proportionally more time.

    .. warning::

        Unlike :obj:`SnapshotBatchStrategy <onetl.strategy.snapshot_strategy.SnapshotBatchStrategy>`,
        it **saves** current HWM value after **each batch** into :ref:`HWM Store <hwm>`.

        So if code inside the context manager raised an exception, like:

        .. code:: python

            with IncrementalBatchStrategy() as batches:
                for _ in batches:
                    df = reader.run()  # something went wrong here
                    writer.run(df)  # or here
                    # or here...

        DBReader will **NOT** update HWM in HWM Store for the failed batch.

        All of that allows to resume reading process from the *last successful batch*.

    .. warning::

        Not every :ref:`DB connection <db-connections>`
        supports batch strategy. For example, Kafka connection doesn't support it.
        Make sure the connection you use is compatible with the IncrementalBatchStrategy.

    Parameters
    ----------
    step : Any

        Step size used for generating batch SQL queries like:

        .. code:: sql

            SELECT id, data
            FROM public.mydata
            WHERE id > 1000 AND id <= 1100; -- 1000 is previous HWM value, step is 100

        .. note::

            Step defines a range of values will be fetched by each batch. This is **not**
            a number of rows, it depends on a table content and value distribution across the rows.

        .. note::

            ``step`` value will be added to the HWM, so it should have a proper type.

            For example, for ``TIMESTAMP`` column ``step`` type should be :obj:`datetime.timedelta`, not :obj:`int`

    stop : Any, default: ``None``

        If passed, the value will be used for generating WHERE clauses with ``hwm.expression`` filter,
        as a stop value for the last batch.

        If not set, the value is determined by a separated query:

        .. code:: sql

            SELECT MAX(id) as stop
            FROM public.mydata
            WHERE id > 1000; -- 1000 is previous HWM value (if any)

        .. note::

            ``stop`` should be the same type as ``hwm.expression`` value,
            e.g. :obj:`datetime.datetime` for ``TIMESTAMP`` column, :obj:`datetime.date` for ``DATE``, and so on

    offset : Any, default: ``None``

        If passed, the offset value will be used to read rows which appeared in the source after the previous read.

        For example, previous incremental run returned rows:

        .. code::

            898
            899
            900
            1000

        Current HWM value is 1000.

        But since then few more rows appeared in the source:

        .. code::

            898
            899
            900
            901 # new
            902 # new
            ...
            999 # new
            1000

        and you need to read them too.

        So you can set ``offset=100``, so the first batch of a next incremental run will look like:

        .. code:: sql

            SELECT id, data
            FROM public.mydata
            WHERE id > 900 AND id <= 1000; -- 900 = 1000 - 100 = HWM - offset

        and return rows from 901 (**not** 900) to **1000** (duplicate).

        .. warning::

            This can lead to reading duplicated values from the table.
            You probably need additional deduplication step to handle them

        .. note::

            ``offset`` value will be subtracted from the HWM, so it should have a proper type.

            For example, for ``TIMESTAMP`` column ``offset`` type should be :obj:`datetime.timedelta`, not :obj:`int`

    Examples
    --------

    IncrementalBatch run:

    .. code:: python

        from onetl.connection import Postgres, Hive
        from onetl.db import DBReader
        from onetl.strategy import IncrementalBatchStrategy
        from onetl.hwm import AutoDetectHWM

        from pyspark.sql import SparkSession

        maven_packages = Postgres.get_packages()
        spark = (
            SparkSession.builder.appName("spark-app-name")
            .config("spark.jars.packages", ",".join(maven_packages))
            .getOrCreate()
        )

        postgres = Postgres(
            host="postgres.domain.com",
            user="myuser",
            password="*****",
            database="target_database",
            spark=spark,
        )

        hive = Hive(cluster="rnd-dwh", spark=spark)

        reader = DBReader(
            connection=postgres,
            source="public.mydata",
            columns=["id", "data"],
            hwm=DBReader.AutoDetectHWM(name="some_hwm_name", expression="id"),
        )

        writer = DBWriter(connection=hive, target="newtable")

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

    IncrementalBatch run with ``stop`` value:

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

    IncrementalBatch run with ``offset`` value:

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

    IncrementalBatch run with all possible options:

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

    ``hwm.expression`` can be a date or datetime, not only integer:

    .. code:: python

        from datetime import date, timedelta

        reader = DBReader(
            connection=postgres,
            source="public.mydata",
            columns=["business_dt", "data"],
            hwm=DBReader.AutoDetectHWM(name="some_hwm_name", expression="business_dt"),
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
        self.save_hwm()
        return super().__next__()

    @classmethod
    def _log_exclude_fields(cls) -> set[str]:
        return super()._log_exclude_fields() | {"start"}
