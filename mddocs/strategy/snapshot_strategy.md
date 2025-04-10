<a id="snapshot-strategy"></a>

# Snapshot Strategy

### *class* onetl.strategy.snapshot_strategy.SnapshotStrategy

Snapshot strategy for [DB Reader](../db/db_reader.md#db-reader)/[File Downloader](../file/file_downloader/file_downloader.md#file-downloader).

Used for fetching all the rows/files from a source. Does not support HWM.

#### NOTE
This is a default strategy.

For [DB Reader](../db/db_reader.md#db-reader):
: Every snapshot run is executing the simple query which fetches all the table data:
  <br/>
  ```sql
  SELECT id, data FROM public.mydata;
  ```

For [File Downloader](../file/file_downloader/file_downloader.md#file-downloader):
: Every snapshot run is downloading all the files (from the source, or user-defined list):
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

#### Versionadded
Added in version 0.1.0.

### Examples

Snapshot run with [DB Reader](../db/db_reader.md#db-reader)

```py
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
```

Snapshot run with [File Downloader](../file/file_downloader/file_downloader.md#file-downloader)

```py
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
```

<!-- !! processed by numpydoc !! -->

#### \_\_init_\_(\*\*kwargs)

Create a new model by parsing and validating input data from keyword arguments.

Raises ValidationError if the input data cannot be parsed to form a valid model.

<!-- !! processed by numpydoc !! -->
