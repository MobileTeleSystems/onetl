<a id="incremental-strategy"></a>

# Incremental Strategy

### *class* onetl.strategy.incremental_strategy.IncrementalStrategy(\*, hwm: HWM | None = None, offset: Any = None)

Incremental strategy for [DB Reader](../db/db_reader.md#db-reader)/[File Downloader](../file/file_downloader/file_downloader.md#file-downloader).

Used for fetching only new rows/files from a source
by filtering items not covered by the previous [HWM](../hwm_store/index.md#hwm) value.

For [DB Reader](../db/db_reader.md#db-reader):
: First incremental run is just the same as [`SnapshotStrategy`](snapshot_strategy.md#onetl.strategy.snapshot_strategy.SnapshotStrategy):
  <br/>
  ```sql
  SELECT id, data FROM mydata;
  ```
  <br/>
  Then the max value of `id` column (e.g. `1000`) will be saved as `HWM` to [HWM Store](../hwm_store/index.md#hwm).
  <br/>
  Next incremental run will read only new data from the source:
  <br/>
  ```sql
  SELECT id, data FROM mydata WHERE id > 1000; -- hwm value
  ```
  <br/>
  Pay attention to resulting dataframe **does not include** row with `id=1000` because it has been read before.
  <br/>
  #### WARNING
  If code inside the context manager raised an exception, like:
  <br/>
  ```python
  with IncrementalStrategy():
      df = reader.run()  # something went wrong here
      writer.run(df)  # or here
      # or here...
  ```
  <br/>
  When DBReader will **NOT** update HWM in HWM Store.
  This allows to resume reading process from the *last successful run*.

For [File Downloader](../file/file_downloader/file_downloader.md#file-downloader):
: Behavior depends on `hwm` type.
  <br/>
  FileListHWM
  <br/>
  First incremental run is just the same as [`SnapshotStrategy`](snapshot_strategy.md#onetl.strategy.snapshot_strategy.SnapshotStrategy) -
  all files are downloaded:
  <br/>
  ```bash
  $ hdfs dfs -ls /path
  <br/>
  /path/my/file1
  /path/my/file2
  ```
  <br/>
  ```python
  DownloadResult(
      ...,
      successful={
          LocalFile("/downloaded/file1"),
          LocalFile("/downloaded/file2"),
      },
  )
  ```
  <br/>
  Then the list of original file paths is saved as `FileListHWM` object into [HWM Store](../hwm_store/index.md#hwm):
  <br/>
  ```python
  FileListHWM(
      ...,
      entity="/path",
      value=[
          "/path/my/file1",
          "/path/my/file2",
      ],
  )
  ```
  <br/>
  Next incremental run will download only new files which were added to the source since previous run:
  <br/>
  ```bash
  $ hdfs dfs -ls /path
  <br/>
  /path/my/file1
  /path/my/file2
  /path/my/file3
  ```
  <br/>
  ```python
  # only files which are not covered by FileListHWM
  DownloadResult(
      ...,
      successful={
          LocalFile("/downloaded/file3"),
      },
  )
  ```
  <br/>
  Value of `FileListHWM` will be updated and saved to [HWM Store](../hwm_store/index.md#hwm):
  <br/>
  ```python
  FileListHWM(
      ...,
      directory="/path",
      value=[
          "/path/my/file1",
          "/path/my/file2",
          "/path/my/file3",
      ],
  )
  ```
  <br/>
  FileModifiedTimeHWM
  <br/>
  First incremental run is just the same as [`SnapshotStrategy`](snapshot_strategy.md#onetl.strategy.snapshot_strategy.SnapshotStrategy) -
  all files are downloaded:
  <br/>
  ```bash
  $ hdfs dfs -ls /path
  <br/>
  /path/my/file1
  /path/my/file2
  ```
  <br/>
  ```python
  DownloadResult(
      ...,
      successful={
          LocalFile("/downloaded/file1"),
          LocalFile("/downloaded/file2"),
      },
  )
  ```
  <br/>
  Then the maximum modified time of original files is saved as `FileModifiedTimeHWM` object into [HWM Store](../hwm_store/index.md#hwm):
  <br/>
  ```python
  FileModifiedTimeHWM(
      ...,
      directory="/path",
      value=datetime.datetime(2025, 1, 1, 11, 22, 33, 456789, tzinfo=timezone.utc),
  )
  ```
  <br/>
  Next incremental run will download only files from the source which were modified or created since previous run:
  <br/>
  ```bash
  $ hdfs dfs -ls /path
  <br/>
  /path/my/file1
  /path/my/file2
  /path/my/file3
  ```
  <br/>
  ```python
  # only files which are not covered by FileModifiedTimeHWM
  DownloadResult(
      ...,
      successful={
          LocalFile("/downloaded/file3"),
      },
  )
  ```
  <br/>
  Value of `FileModifiedTimeHWM` will be updated and and saved to [HWM Store](../hwm_store/index.md#hwm):
  <br/>
  ```python
  FileModifiedTimeHWM(
      ...,
      directory="/path",
      value=datetime.datetime(2025, 1, 1, 22, 33, 44, 567890, tzinfo=timezone.utc),
  )
  ```
  <br/>
  #### WARNING
  FileDownload updates HWM in HWM Store at the end of `.run()` call,
  **NOT** while exiting strategy context. This is because:
  <br/>
  * FileDownloader does not raise exceptions if some file cannot be downloaded.
  * FileDownloader creates files on local filesystem, and file content may differ for different `modes`.
  * It can remove files from the source if `delete_source` is set to `True`.

#### Versionadded
Added in version 0.1.0.

* **Parameters:**
  **offset**
  : If passed, the offset value will be used to read rows which appeared in the source after the previous read.
    <br/>
    For example, previous incremental run returned rows:
    ```default
    898
    899
    900
    1000
    ```
    <br/>
    Current HWM value is 1000.
    <br/>
    But since then few more rows appeared in the source:
    ```default
    898
    899
    900
    901 # new
    902 # new
    ...
    999 # new
    1000
    ```
    <br/>
    and you need to read them too.
    <br/>
    So you can set `offset=100`, so a next incremental run will generate SQL query like:
    ```sql
    SELECT id, data FROM public.mydata WHERE id > 900;
    -- 900 = 1000 - 100 = hwm - offset
    ```
    <br/>
    and return rows since 901 (**not** 900), **including** 1000 which was already captured by HWM.
    <br/>
    #### WARNING
    This can lead to reading duplicated values from the table.
    You probably need additional deduplication step to handle them
    <br/>
    #### WARNING
    Cannot be used with [File Downloader](../file/file_downloader/file_downloader.md#file-downloader)
    <br/>
    #### NOTE
    `offset` value will be subtracted from the HWM, so it should have a proper type.
    <br/>
    For example, for `TIMESTAMP` column `offset` type should be `datetime.timedelta`, not `int`

### Examples

Incremental run with [DB Reader](../db/db_reader.md#db-reader)

```python
from onetl.db import DBReader, DBWriter
from onetl.strategy import IncrementalStrategy

reader = DBReader(
    connection=postgres,
    source="public.mydata",
    columns=["id", "data"],
    hwm=DBReader.AutoDetectHWM(name="some_hwm_name", expression="id"),
)

writer = DBWriter(connection=hive, target="db.newtable")

with IncrementalStrategy():
    df = reader.run()
    writer.run(df)
```

```sql
-- previous HWM value was 1000
-- DBReader will generate query like:

SELECT id, data
FROM public.mydata
WHERE id > 1000; --- from HWM (EXCLUDING first row)
```

Incremental run with [DB Reader](../db/db_reader.md#db-reader) and `IncrementalStrategy(offset=...)`

```python
from onetl.db import DBReader, DBWriter
from onetl.strategy import IncrementalStrategy

reader = DBReader(
    connection=postgres,
    source="public.mydata",
    columns=["id", "data"],
    hwm=DBReader.AutoDetectHWM(name="some_hwm_name", expression="id"),
)

writer = DBWriter(connection=hive, target="db.newtable")

with IncrementalStrategy(offset=100):
    df = reader.run()
    writer.run(df)
```

```sql
-- previous HWM value was 1000
-- DBReader will generate query like:

SELECT id, data
FROM public.mydata
WHERE id > 900; -- from HWM-offset (EXCLUDING first row)
```

`offset` and `hwm.expression` can be a date or datetime, not only integer:

```python
from onetl.db import DBReader, DBWriter
from datetime import timedelta

reader = DBReader(
    connection=postgres,
    source="public.mydata",
    columns=["business_dt", "data"],
    hwm=DBReader.AutoDetectHWM(name="some_hwm_name", expression="business_dt"),
)

writer = DBWriter(connection=hive, target="db.newtable")

with IncrementalStrategy(offset=timedelta(days=1)):
    df = reader.run()
    writer.run(df)
```

```sql
-- previous HWM value was '2021-01-10'
-- DBReader will generate query like:

SELECT business_dt, data
FROM public.mydata
WHERE business_dt > CAST('2021-01-09' AS DATE); -- from HWM-offset (EXCLUDING first row)
```

Incremental run with [DB Reader](../db/db_reader.md#db-reader) and [Kafka](../connection/db_connection/kafka/index.md#kafka)

```py
from onetl.db import DBReader, DBWriter
from onetl.strategy import IncrementalStrategy

reader = DBReader(
    connection=kafka,
    source="topic_name",
    hwm=DBReader.AutoDetectHWM(name="some_hwm_name", expression="offset"),
)

writer = DBWriter(connection=hive, target="db.newtable")

with IncrementalStrategy():
    df = reader.run()

# current run will fetch only messages which were added since previous run
```

Incremental run with [File Downloader](../file/file_downloader/file_downloader.md#file-downloader) and `hwm=FileListHWM(...)`

```py
from onetl.file import FileDownloader
from onetl.strategy import SnapshotStrategy
from etl_entities.hwm import FileListHWM

downloader = FileDownloader(
    connection=sftp,
    source_path="/remote",
    local_path="/local",
    hwm=FileListHWM(  # mandatory for IncrementalStrategy
        name="my_unique_hwm_name",
    ),
)

with IncrementalStrategy():
    df = downloader.run()

# current run will download only files which were added since previous run
```

Incremental run with [File Downloader](../file/file_downloader/file_downloader.md#file-downloader) and `hwm=FileModifiedTimeHWM(...)`

```py
from onetl.file import FileDownloader
from onetl.strategy import SnapshotStrategy
from etl_entities.hwm import FileModifiedTimeHWM

downloader = FileDownloader(
    connection=sftp,
    source_path="/remote",
    local_path="/local",
    hwm=FileModifiedTimeHWM(  # mandatory for IncrementalStrategy
        name="my_unique_hwm_name",
    ),
)

with IncrementalStrategy():
    df = downloader.run()

# current run will download only files which were modified/created since previous run
```

<!-- !! processed by numpydoc !! -->

#### \_\_init_\_(\*\*kwargs)

Create a new model by parsing and validating input data from keyword arguments.

Raises ValidationError if the input data cannot be parsed to form a valid model.

<!-- !! processed by numpydoc !! -->
