<a id="file-mover"></a>

# File Mover

| [`FileMover`](#onetl.file.file_mover.file_mover.FileMover)                         | Allows you to move files between different directories in a filesystem, and return an object with move result summary.   |
|------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------|
| [`FileMover.run`](#onetl.file.file_mover.file_mover.FileMover.run)([files])        | Method for moving files from source to target directory.                                                                 |
| [`FileMover.view_files`](#onetl.file.file_mover.file_mover.FileMover.view_files)() | Get file list in the `source_path`, after `filter` and `limit` applied (if any).                                         |

### *class* onetl.file.file_mover.file_mover.FileMover(\*, connection: ~onetl.base.base_file_connection.BaseFileConnection, target_path: ~onetl.impl.remote_path.RemotePath, source_path: ~onetl.impl.remote_path.RemotePath | None = None, filters: ~typing.List[~onetl.base.base_file_filter.BaseFileFilter] = None, limits: ~typing.List[~onetl.base.base_file_limit.BaseFileLimit] = None, options: ~onetl.file.file_mover.options.FileMoverOptions = FileMoverOptions(if_exists=<FileExistBehavior.ERROR: 'error'>, workers=1))

Allows you to move files between different directories in a filesystem,
and return an object with move result summary. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

#### NOTE
This class is used to move files **only** within the same connection,

It does NOT support direct file transfer between filesystems, like `FTP -> SFTP`.
You should use [File Downloader](../file_downloader/file_downloader.md#file-downloader) + [File Uploader](../file_uploader/file_uploader.md#file-uploader) to implement `FTP -> local dir -> SFTP`.

#### WARNING
This class does **not** support read strategies.

#### Versionadded
Added in version 0.8.0.

* **Parameters:**
  **connection**
  : Class which contains File system connection properties. See [File Connections](../../connection/file_connection/index.md#file-connections) section.

  **target_path**
  : Remote path to move files to

  **source_path**
  : Remote path to move files from.
    <br/>
    Could be `None`, but only if you pass absolute file paths directly to
    [`run`](#onetl.file.file_mover.file_mover.FileMover.run) method

  **filters**
  : Return only files/directories matching these filters. See [File Filters](../file_filters/index.md#file-filters)

  **limits**
  : Apply limits to the list of files/directories, and stop if one of the limits is reached.
    See [File Limits](../file_limits/index.md#file-limits)

  **options**
  : File moving options. See [`FileMover.Options`](options.md#onetl.file.file_mover.options.FileMoverOptions)

### Examples

Minimal example

```py
from onetl.connection import SFTP
from onetl.file import FileMover

sftp = SFTP(...)

# create mover
mover = FileMover(
    connection=sftp,
    source_path="/path/to/source/dir",
    target_path="/path/to/target/dir",
)

# move files from "/path/to/source/dir" to "/path/to/target/dir"
mover.run()
```

Full example

```py
from onetl.connection import SFTP
from onetl.file import FileMover
from onetl.file.filter import Glob, ExcludeDir
from onetl.file.limit import MaxFilesCount, TotalFilesSize

sftp = SFTP(...)

# create mover with a bunch of options
mover = FileMover(
    connection=sftp,
    source_path="/path/to/source/dir",
    target_path="/path/to/target/dir",
    filters=[
        Glob("*.txt"),
        ExcludeDir("/path/to/source/dir/exclude"),
    ],
    limits=[MaxFilesCount(100), TotalFileSize("10GiB")],
    options=FileMover.Options(if_exists="replace_file"),
)

# move files from "/path/to/source/dir" to "/path/to/target/dir",
# but only *.txt files
# excluding files from "/path/to/source/dir/exclude" directory
# and stop before downloading 101 file
mover.run()
```

<!-- !! processed by numpydoc !! -->

#### run(files: Iterable[str | PathLike] | None = None) → [MoveResult](result.md#onetl.file.file_mover.result.MoveResult)

Method for moving files from source to target directory. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

#### Versionadded
Added in version 0.8.0.

* **Parameters:**
  **files**
  : File list to move.
    <br/>
    If empty, move files from `source_path` to `target_path`,
    applying `filter` and `limit` to each one (if set).
    <br/>
    If not, move to `target_path` **all** input files, **without**
    any filtering and limiting.
* **Returns:**
  `MoveResult`
  : Move result object
* **Raises:**
  `onetl.exception.DirectoryNotFoundError`
  : `source_path` does not found

  NotADirectoryError
  : `source_path` or `target_path` is not a directory

### Examples

Move files from `source_path`:

```pycon
>>> from onetl.file import FileMover
>>> mover = FileMover(source_path="/source", target_path="/target", ...)
>>> move_result = mover.run()
>>> move_result
MoveResult(
    successful=FileSet([
        RemoteFile("/target/file1.txt"),
        RemoteFile("/target/file2.txt"),
        # directory structure is preserved
        RemoteFile("/target/nested/path/file3.txt"),
    ]),
    failed=FileSet([
        FailedRemoteFile("/source/failed.file"),
    ]),
    skipped=FileSet([
        RemoteFile("/source/already.exists"),
    ]),
    missing=FileSet([
        RemotePath("/source/missing.file"),
    ]),
)
```

Move only certain files from `source_path`:

```pycon
>>> from onetl.file import FileMover
>>> mover = FileMover(source_path="/source", target_path="/target", ...)
>>> # paths could be relative or absolute, but all should be in "/source"
>>> move_result = mover.run(
...     [
...         "/source/file1.txt",
...         "/source/nested/path/file3.txt",
...         # excluding "/source/file2.txt"
...     ]
... )
>>> move_result
MoveResult(
    successful=FileSet([
        RemoteFile("/target/file1.txt"),
        # directory structure is preserved
        RemoteFile("/target/nested/path/file3.txt"),
    ]),
    failed=FileSet([]),
    skipped=FileSet([]),
    missing=FileSet([]),
)
```

Move certain files from any folder:

```pycon
>>> from onetl.file import FileMover
>>> mover = FileMover(target_path="/target", ...)  # no source_path set
>>> # only absolute paths
>>> move_result = mover.run(
...     [
...         "/remote/file1.txt",
...         "/any/nested/path/file3.txt",
...     ]
... )
>>> move_result
MoveResult(
    successful=FileSet([
        RemoteFile("/target/file1.txt"),
        # directory structure is NOT preserved without source_path
        RemoteFile("/target/file3.txt"),
    ]),
    failed=FileSet([]),
    skipped=FileSet([]),
    missing=FileSet([]),
)
```

<!-- !! processed by numpydoc !! -->

#### view_files() → FileSet[RemoteFile]

Get file list in the `source_path`,
after `filter` and `limit` applied (if any). [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

#### Versionadded
Added in version 0.8.0.

* **Returns:**
  FileSet[RemoteFile]
  : Set of files in `source_path`, which will be moved by [`run`](#onetl.file.file_mover.file_mover.FileMover.run) method
* **Raises:**
  `onetl.exception.DirectoryNotFoundError`
  : `source_path` does not found

  NotADirectoryError
  : `source_path` is not a directory

### Examples

View files:

```pycon
>>> from onetl.file import FileMover
>>> mover = FileMover(source_path="/remote", ...)
>>> mover.view_files()
FileSet([
    RemoteFile("/remote/file1.txt"),
    RemoteFile("/remote/file2.txt"),
    RemoteFile("/remote/nested/path/file3.txt"),
])
```

<!-- !! processed by numpydoc !! -->
