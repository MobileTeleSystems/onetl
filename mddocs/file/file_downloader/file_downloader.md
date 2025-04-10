<a id="file-downloader"></a>

# File Downloader

| [`FileDownloader`](#onetl.file.file_downloader.file_downloader.FileDownloader)                         | Allows you to download files from a remote source with specified file connection and parameters, and return an object with download result summary.   |
|--------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------|
| [`FileDownloader.run`](#onetl.file.file_downloader.file_downloader.FileDownloader.run)([files])        | Method for downloading files from source to local directory.                                                                                          |
| [`FileDownloader.view_files`](#onetl.file.file_downloader.file_downloader.FileDownloader.view_files)() | Get file list in the `source_path`, after `filter`, `limit` and `hwm` applied (if any).                                                               |

### *class* onetl.file.file_downloader.file_downloader.FileDownloader(\*, connection: ~onetl.base.base_file_connection.BaseFileConnection, local_path: ~onetl.impl.local_path.LocalPath, source_path: ~onetl.impl.remote_path.RemotePath | None = None, temp_path: ~onetl.impl.local_path.LocalPath | None = None, filter: ~typing.List[~onetl.base.base_file_filter.BaseFileFilter] = None, limit: ~typing.List[~onetl.base.base_file_limit.BaseFileLimit] = None, hwm: ~etl_entities.hwm.file.file_hwm.FileHWM | None = None, hwm_type: ~typing.Type[~etl_entities.old_hwm.file_list_hwm.FileListHWM] | str | None = None, options: ~onetl.file.file_downloader.options.FileDownloaderOptions = FileDownloaderOptions(if_exists=<FileExistBehavior.ERROR: 'error'>, delete_source=False, workers=1))

Allows you to download files from a remote source with specified file connection
and parameters, and return an object with download result summary. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

#### NOTE
FileDownloader can return different results depending on [Read Strategies](../../strategy/index.md#strategy)

#### NOTE
This class is used to download files **only** from remote directory to the local one.

It does NOT support direct file transfer between filesystems, like `FTP -> SFTP`.
You should use FileDownloader + [File Uploader](../file_uploader/file_uploader.md#file-uploader) to implement `FTP -> local dir -> SFTP`.

#### Versionadded
Added in version 0.1.0.

#### Versionchanged
Changed in version 0.8.0: Moved `onetl.core.FileDownloader` → `onetl.file.FileDownloader`

* **Parameters:**
  **connection**
  : Class which contains File system connection properties. See [File Connections](../../connection/file_connection/index.md#file-connections) section.

  **local_path**
  : Local path where you download files

  **source_path**
  : Remote path to download files from.
    <br/>
    Could be `None`, but only if you pass absolute file paths directly to
    [`run`](#onetl.file.file_downloader.file_downloader.FileDownloader.run) method

  **temp_path**
  : If set, this path will be used for downloading a file, and then renaming it to the target file path.
    If `None` is passed, files are downloaded directly to `target_path`.
    <br/>
    #### WARNING
    In case of production ETL pipelines, please set a value for `temp_path` (NOT `None`).
    This allows to properly handle download interruption,
    without creating half-downloaded files in the target,
    because unlike file download, `rename` call is atomic.
    <br/>
    #### WARNING
    In case of connections like SFTP or FTP, which can have multiple underlying filesystems,
    please pass to `temp_path` path on the SAME filesystem as `target_path`.
    Otherwise instead of `rename`, remote OS will move file between filesystems,
    which is NOT atomic operation.
    <br/>
    #### Versionadded
    Added in version 0.5.0.

  **filters**
  : Return only files/directories matching these filters. See [File Filters](../file_filters/index.md#file-filters)
    <br/>
    #### Versionchanged
    Changed in version 0.3.0: Replaces old `source_path_pattern: str` and `exclude_dirs: str` options.
    <br/>
    #### Versionchanged
    Changed in version 0.8.0: Renamed `filter` → `filters`

  **limits**
  : Apply limits to the list of files/directories, and stop if one of the limits is reached.
    See [File Limits](../file_limits/index.md#file-limits)
    <br/>
    #### Versionadded
    Added in version 0.4.0.
    <br/>
    #### Versionchanged
    Changed in version 0.8.0: Renamed `limit` → `limits`

  **options**
  : File downloading options. See [`FileDownloader.Options`](options.md#onetl.file.file_downloader.options.FileDownloaderOptions)
    <br/>
    #### Versionadded
    Added in version 0.3.0.

  **hwm**
  : HWM class to detect changes in incremental run. See [File HWM](https://etl-entities.readthedocs.io/en/stable/hwm/file/index.html)
    <br/>
    #### WARNING
    Used only in [`IncrementalStrategy`](../../strategy/incremental_strategy.md#onetl.strategy.incremental_strategy.IncrementalStrategy).
    <br/>
    #### Versionadded
    Added in version 0.5.0.
    <br/>
    #### Versionchanged
    Changed in version 0.10.0: Replaces deprecated `hwm_type` attribute

### Examples

Minimal example

```py
from onetl.connection import SFTP
from onetl.file import FileDownloader

sftp = SFTP(...)

# create downloader
downloader = FileDownloader(
    connection=sftp,
    source_path="/path/to/remote/source",
    local_path="/path/to/local",
)

# download files to "/path/to/local"
downloader.run()
```

Full example

```py
from onetl.connection import SFTP
from onetl.file import FileDownloader
from onetl.file.filter import Glob, ExcludeDir
from onetl.file.limit import MaxFilesCount, TotalFileSize

sftp = SFTP(...)

# create downloader with a bunch of options
downloader = FileDownloader(
    connection=sftp,
    source_path="/path/to/remote/source",
    local_path="/path/to/local",
    temp_path="/tmp",
    filters=[
        Glob("*.txt"),
        ExcludeDir("/path/to/remote/source/exclude_dir"),
    ],
    limits=[MaxFilesCount(100), TotalFileSize("10GiB")],
    options=FileDownloader.Options(delete_source=True, if_exists="replace_file"),
)

# download files to "/path/to/local",
# but only *.txt,
# excluding files from "/path/to/remote/source/exclude_dir" directory
# and stop before downloading 101 file
downloader.run()
```

Incremental download (by tracking list of file paths)

```py
from onetl.connection import SFTP
from onetl.file import FileDownloader
from onetl.strategy import IncrementalStrategy
from etl_entities.hwm import FileListHWM

sftp = SFTP(...)

# create downloader
downloader = FileDownloader(
    connection=sftp,
    source_path="/path/to/remote/source",
    local_path="/path/to/local",
    hwm=FileListHWM(  # mandatory for IncrementalStrategy
        name="my_unique_hwm_name",
    ),
)

# download files to "/path/to/local", but only added since previous run
with IncrementalStrategy():
    downloader.run()
```

Incremental download (by tracking file modification time)

```py
from onetl.connection import SFTP
from onetl.file import FileDownloader
from onetl.strategy import IncrementalStrategy
from etl_entities.hwm import FileModifiedTimeHWM

sftp = SFTP(...)

# create downloader
downloader = FileDownloader(
    connection=sftp,
    source_path="/path/to/remote/source",
    local_path="/path/to/local",
    hwm=FileModifiedTimeHWM(  # mandatory for IncrementalStrategy
        name="my_unique_hwm_name",
    ),
)

# download files to "/path/to/local", but only modified/created since previous run
with IncrementalStrategy():
    downloader.run()
```

<!-- !! processed by numpydoc !! -->

#### run(files: Iterable[str | PathLike] | None = None) → [DownloadResult](result.md#onetl.file.file_downloader.result.DownloadResult)

Method for downloading files from source to local directory. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

#### NOTE
This method can return different results depending on [Read Strategies](../../strategy/index.md#strategy)

#### Versionadded
Added in version 0.1.0.

* **Parameters:**
  **files**
  : File list to download.
    <br/>
    If empty, download files from `source_path` to `local_path`,
    applying `filter`, `limit` and `hwm` to each one (if set).
    <br/>
    If not, download to `local_path` **all** input files, **ignoring**
    filters, limits and HWM.
    <br/>
    #### Versionadded
    Added in version 0.3.0.
* **Returns:**
  `DownloadResult`
  : Download result object
* **Raises:**
  `onetl.exception.DirectoryNotFoundError`
  : `source_path` does not found

  NotADirectoryError
  : `source_path` or `local_path` is not a directory

### Examples

Download files from `source_path` to `local_path`:

```pycon
>>> from onetl.file import FileDownloader
>>> downloader = FileDownloader(source_path="/remote", local_path="/local", ...)
>>> download_result = downloader.run()
>>> download_result
DownloadResult(
    successful=FileSet([
        LocalPath("/local/file1.txt"),
        LocalPath("/local/file2.txt"),
        # directory structure is preserved
        LocalPath("/local/nested/path/file3.txt"),
    ]),
    failed=FileSet([
        FailedRemoteFile("/remote/failed.file"),
    ]),
    skipped=FileSet([
        RemoteFile("/remote/already.exists"),
    ]),
    missing=FileSet([
        RemotePath("/remote/missing.file"),
    ]),
)
```

Download only certain files from `source_path`:

```pycon
>>> from onetl.file import FileDownloader
>>> downloader = FileDownloader(source_path="/remote", local_path="/local", ...)
>>> # paths could be relative or absolute, but all should be in "/remote"
>>> download_result = downloader.run(
...     [
...         "/remote/file1.txt",
...         "/remote/nested/path/file3.txt",
...         # excluding "/remote/file2.txt"
...     ]
... )
>>> download_result
DownloadResult(
    successful=FileSet([
        LocalPath("/local/file1.txt"),
        # directory structure is preserved
        LocalPath("/local/nested/path/file3.txt"),
    ]),
    failed=FileSet([]),
    skipped=FileSet([]),
    missing=FileSet([]),
)
```

Download certain files from any folder:

```pycon
>>> from onetl.file import FileDownloader
>>> downloader = FileDownloader(local_path="/local", ...)  # no source_path set
>>> # only absolute paths
>>> download_result = downloader.run(
...     [
...         "/remote/file1.txt",
...         "/any/nested/path/file2.txt",
...     ]
... )
>>> download_result
DownloadResult(
    successful=FileSet([
        LocalPath("/local/file1.txt"),
        # directory structure is NOT preserved without source_path
        LocalPath("/local/file2.txt"),
    ]),
    failed=FileSet([]),
    skipped=FileSet([]),
    missing=FileSet([]),
)
```

<!-- !! processed by numpydoc !! -->

#### view_files() → FileSet[RemoteFile]

Get file list in the `source_path`,
after `filter`, `limit` and `hwm` applied (if any). [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

#### NOTE
This method can return different results depending on [Read Strategies](../../strategy/index.md#strategy)

#### Versionadded
Added in version 0.3.0.

* **Returns:**
  FileSet[RemoteFile]
  : Set of files in `source_path`, which will be downloaded by [`run`](#onetl.file.file_downloader.file_downloader.FileDownloader.run) method
* **Raises:**
  `onetl.exception.DirectoryNotFoundError`
  : `source_path` does not found

  NotADirectoryError
  : `source_path` is not a directory

### Examples

View files:

```pycon
>>> from onetl.file import FileDownloader
>>> downloader = FileDownloader(source_path="/remote", ...)
>>> downloader.view_files()
FileSet([
    RemoteFile("/remote/file1.txt"),
    RemoteFile("/remote/file3.txt"),
    RemoteFile("/remote/nested/file3.txt"),
])
```

<!-- !! processed by numpydoc !! -->
