<a id="ftps"></a>

# FTPS connection

### *class* onetl.connection.file_connection.ftps.FTPS(\*, host: Host, port: int = 21, user: str | None = None, password: SecretStr | None = None)

FTPS file connection. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

Based on [FTPUtil library](https://pypi.org/project/ftputil/).

#### WARNING
Since onETL v0.7.0 to use FTPS connector you should install package as follows:

```bash
pip install onetl[ftps]

# or
pip install onetl[files]
```

See [File connections](../../install/files.md#install-files) installation instruction for more details.

#### Versionadded
Added in version 0.1.0.

* **Parameters:**
  **host**
  : Host of FTPS source. For example: `ftps.domain.com`

  **port**
  : Port of FTPS source

  **user**
  : User, which have access to the file source. For example: `someuser`.
    <br/>
    `None` means that the user is anonymous.

  **password**
  : Password for file source connection.
    <br/>
    `None` means that the user is anonymous.

### Examples

Create and check FTPS connection:

```python
from onetl.connection import FTPS

ftps = FTPS(
    host="ftps.domain.com",
    user="someuser",
    password="*****",
).check()
```

<!-- !! processed by numpydoc !! -->

#### \_\_init_\_(\*\*kwargs)

<!-- !! processed by numpydoc !! -->

#### check()

Check source availability. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

If not, an exception will be raised.

* **Returns:**
  Connection itself
* **Raises:**
  RuntimeError
  : If the connection is not available

### Examples

```python
connection.check()
```

<!-- !! processed by numpydoc !! -->

#### create_dir(path: PathLike | str) → RemoteDirectory

Creates directory tree on remote filesystem. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

#### Versionadded
Added in version 0.8.0.

* **Parameters:**
  **path**
  : Directory path
* **Returns:**
  Created directory with stats
* **Raises:**
  `onetl.exception.NotAFileError`
  : Path is not a file

### Examples

```pycon
>>> dir_path = connection.create_dir("/path/to/dir")
>>> os.fspath(dir_path)
'/path/to/dir'
```

<!-- !! processed by numpydoc !! -->

#### download_file(remote_file_path: PathLike | str, local_file_path: PathLike | str, replace: bool = True) → LocalPath

Downloads file from the remote filesystem to a local path. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

#### WARNING
Supports only one file download per call. Directory download is **NOT** supported, use [File Downloader](../../file/file_downloader/file_downloader.md#file-downloader) instead.

#### Versionadded
Added in version 0.8.0.

* **Parameters:**
  **remote_file_path**
  : Remote file path to read from

  **local_file_path**
  : Local file path to create

  **replace**
  : If `True`, existing file will be replaced
* **Returns:**
  Local file with stats.
* **Raises:**
  `onetl.exception.NotAFileError`
  : Remote or local path is not a file

  FileNotFoundError
  : Remote file does not exist

  FileExistsError
  : Local file already exists, and `replace=False`

  `onetl.exception.FileSizeMismatchError`
  : Target file size after download is different from source file size.

### Examples

```pycon
>>> local_file = connection.download_file(
...     remote_file_path="/path/to/source.csv",
...     local_file_path="/path/to/target.csv",
... )
>>> os.fspath(local_file)
'/path/to/target.csv'
>>> local_file.exists()
True
>>> local_file.stat().st_size  # in bytes
1024
>>> connection.get_stat("/path/to/source.csv").st_size  # same size
1024
```

<!-- !! processed by numpydoc !! -->

#### get_stat(path: PathLike | str) → PathStatProtocol

Returns stats for a specific path. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

#### Versionadded
Added in version 0.8.0.

* **Parameters:**
  **path**
  : Path to get stats for
* **Returns:**
  Stats object
* **Raises:**
  Any underlying client exception

### Examples

```pycon
>>> stat = connection.get_stat("/path/to/file.csv")
>>> stat.st_size  # in bytes
1024
>>> stat.st_uid  # owner id or name
12345
```

<!-- !! processed by numpydoc !! -->

#### is_dir(path: PathLike | str) → bool

Check if specified path is a directory. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

#### Versionadded
Added in version 0.8.0.

* **Parameters:**
  **path**
  : Path to check
* **Returns:**
  `True` if path is a directory, `False` otherwise.
* **Raises:**
  `onetl.exception.DirectoryNotFoundError`
  : Path does not exist

### Examples

```pycon
>>> connection.is_dir("/path/to/dir")
True
>>> connection.is_dir("/path/to/dir/file.csv")
False
```

<!-- !! processed by numpydoc !! -->

#### is_file(path: PathLike | str) → bool

Check if specified path is a file. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

#### Versionadded
Added in version 0.8.0.

* **Parameters:**
  **path**
  : Path to check
* **Returns:**
  `True` if path is a file, `False` otherwise.
* **Raises:**
  FileNotFoundError
  : Path does not exist

### Examples

```pycon
>>> connection.is_file("/path/to/dir/file.csv")
True
>>> connection.is_file("/path/to/dir")
False
```

<!-- !! processed by numpydoc !! -->

#### list_dir(path: PathLike | str, filters: Iterable[[BaseFileFilter](../../file/file_filters/base.md#onetl.base.base_file_filter.BaseFileFilter)] | None = None, limits: Iterable[[BaseFileLimit](../../file/file_limits/base.md#onetl.base.base_file_limit.BaseFileLimit)] | None = None) → list[RemoteDirectory | RemoteFile]

Return list of child files/directories in a specific directory. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

#### Versionadded
Added in version 0.8.0.

* **Parameters:**
  **path**
  : Directory path to list contents.

  **filters**
  : Return only files/directories matching these filters. See [File Filters](../../file/file_filters/index.md#file-filters)

  **limits**
  : Apply limits to the list of files/directories, and stop if one of the limits is reached.
    See [File Limits](../../file/file_limits/index.md#file-limits)
* **Returns:**
  List of `onetl.base.PathWithStatsProtocol`
* **Raises:**
  NotADirectoryError
  : Path is not a directory

  `onetl.exception.DirectoryNotFoundError`
  : Path does not exist

### Examples

```pycon
>>> dir_content = connection.list_dir("/path/to/dir")
>>> os.fspath(dir_content[0])
'file.csv'
>>> connection.path_exists("/path/to/dir/file.csv")
True
```

<!-- !! processed by numpydoc !! -->

#### path_exists(path: PathLike | str) → bool

Check if specified path exists on remote filesystem. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html).

#### Versionadded
Added in version 0.8.0.

* **Parameters:**
  **path**
  : Path to check
* **Returns:**
  `True` if path exists, `False` otherwise

### Examples

```pycon
>>> connection.path_exists("/path/to/file.csv")
True
>>> connection.path_exists("/path/to/dir")
True
>>> connection.path_exists("/path/to/missing")
False
```

<!-- !! processed by numpydoc !! -->

#### remove_dir(path: PathLike | str, recursive: bool = False) → bool

Remove directory or directory tree. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

If directory does not exist, no exception is raised.

#### Versionadded
Added in version 0.8.0.

* **Parameters:**
  **path**
  : Directory path to remote

  **recursive**
  : If `True`, remove directory tree recursively.
* **Returns:**
  `True` if directory was removed, `False` if directory does not exist in the first place.
* **Raises:**
  NotADirectoryError
  : Path is not a directory

### Examples

```pycon
>>> connection.remove_dir("/path/to/dir")
True
>>> connection.path_exists("/path/to/dir")
False
>>> connection.path_exists("/path/to/dir/file.csv")
False
>>> connection.remove_dir("/path/to/dir")  # already deleted, no error
False
```

<!-- !! processed by numpydoc !! -->

#### remove_file(path: PathLike | str) → bool

Removes file on remote filesystem. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

If file does not exist, no exception is raised.

#### WARNING
Supports only one file removal per call. Directory removal is **NOT** supported, use [`remove_dir`](#onetl.connection.file_connection.ftps.FTPS.remove_dir) instead.

#### Versionadded
Added in version 0.8.0.

* **Parameters:**
  **path**
  : File path
* **Returns:**
  `True` if file was removed, `False` if file does not exist in the first place.
* **Raises:**
  `onetl.exception.NotAFileError`
  : Path is not a file

### Examples

```pycon
>>> connection.remove_file("/path/to/file.csv")
True
>>> connection.path_exists("/path/to/dir/file.csv")
False
>>> connection.remove_file("/path/to/file.csv")  # already deleted, no error
False
```

<!-- !! processed by numpydoc !! -->

#### rename_dir(source_dir_path: PathLike | str, target_dir_path: PathLike | str, replace: bool = False) → RemoteDirectory

Rename or move dir on remote filesystem.

#### Versionadded
Added in version 0.8.0.

* **Parameters:**
  **source_dir_path**
  : Old directory path

  **target_dir_path**
  : New directory path

  **replace**
  : If `True`, existing directory will be replaced.
* **Returns:**
  New directory path with stats.
* **Raises:**
  NotADirectoryError
  : Path is not a directory

  `onetl.exception.DirectoryNotFoundError`
  : Path does not exist

  `onetl.exception.DirectoryExistsError`
  : Directory already exists, and `replace=False`

### Examples

```pycon
>>> new_dir = connection.rename_dir("/path/to/dir1", "/path/to/dir2")
>>> os.fspath(new_dir)
'/path/to/dir2'
>>> connection.path_exists("/path/to/dir1")
False
>>> connection.path_exists("/path/to/dir2")
True
```

<!-- !! processed by numpydoc !! -->

#### rename_file(source_file_path: PathLike | str, target_file_path: PathLike | str, replace: bool = False) → RemoteFile

Rename or move file on remote filesystem. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

#### WARNING
Supports only one file move per call. Directory move/rename is **NOT** supported.

#### Versionadded
Added in version 0.8.0.

* **Parameters:**
  **source_file_path**
  : Old file path

  **target_file_path**
  : New file path

  **replace**
  : If `True`, existing file will be replaced.
* **Returns:**
  New file path with stats.
* **Raises:**
  `onetl.exception.NotAFileError`
  : Source or target path is not a file

  FileNotFoundError
  : File does not exist

  FileExistsError
  : File already exists, and `replace=False`

### Examples

```pycon
>>> new_file = connection.rename_file("/path/to/file1.csv", "/path/to/file2.csv")
>>> os.fspath(new_file)
'/path/to/file2.csv'
>>> connection.path_exists("/path/to/file2.csv")
True
>>> connection.path_exists("/path/to/file1.csv")
False
```

<!-- !! processed by numpydoc !! -->

#### resolve_dir(path: PathLike | str) → RemoteDirectory

Returns directory at specific path, with stats. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

#### Versionadded
Added in version 0.8.0.

* **Parameters:**
  **path**
  : Path to resolve
* **Returns:**
  Directory path with stats
* **Raises:**
  `onetl.exception.DirectoryNotFoundError`
  : Path does not exist

  NotADirectoryError
  : Path is not a directory

### Examples

```pycon
>>> dir_path = connection.resolve_dir("/path/to/dir")
>>> os.fspath(dir_path)
'/path/to/dir'
>>> dir_path.stat().st_uid  # owner id
12345
```

<!-- !! processed by numpydoc !! -->

#### resolve_file(path: PathLike | str) → RemoteFile

Returns file at specific path, with stats. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

#### Versionadded
Added in version 0.8.0.

* **Parameters:**
  **path**
  : Path to resolve
* **Returns:**
  File path with stats
* **Raises:**
  FileNotFoundError
  : Path does not exist

  `onetl.exception.NotAFileError`
  : Path is not a file

### Examples

```pycon
>>> file_path = connection.resolve_file("/path/to/dir/file.csv")
>>> os.fspath(file_path)
'/path/to/dir/file.csv'
>>> file_path.stat().st_uid  # owner id
12345
```

<!-- !! processed by numpydoc !! -->

#### upload_file(local_file_path: PathLike | str, remote_file_path: PathLike | str, replace: bool = False) → RemoteFile

Uploads local file to a remote filesystem. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

#### WARNING
Supports only one file upload per call. Directory upload is **NOT** supported, use [File Uploader](../../file/file_uploader/file_uploader.md#file-uploader) instead.

#### Versionadded
Added in version 0.8.0.

* **Parameters:**
  **local_file_path**
  : Local file path to read from

  **remote_file_path**
  : Remote file path to create

  **replace**
  : If `True`, existing file will be replaced
* **Returns:**
  Remote file with stats.
* **Raises:**
  `onetl.exception.NotAFileError`
  : Remote or local path is not a file

  FileNotFoundError
  : Local file does not exist

  FileExistsError
  : Remote file already exists, and `replace=False`

  `onetl.exception.FileSizeMismatchError`
  : Target file size after upload is different from source file size.

### Examples

```pycon
>>> remote_file = connection.upload(
...     local_file_path="/path/to/source.csv",
...     remote_file_path="/path/to/target.csv",
... )
>>> os.fspath(remote_file)
'/path/to/target.csv'
>>> connection.path_exists("/path/to/target.csv")
True
>>> remote_file.stat().st_size  # in bytes
1024
>>> os.stat("/path/to/source.csv").st_size  # same as source
1024
```

<!-- !! processed by numpydoc !! -->

#### walk(root: PathLike | str, topdown: bool = True, filters: Iterable[[BaseFileFilter](../../file/file_filters/base.md#onetl.base.base_file_filter.BaseFileFilter)] | None = None, limits: Iterable[[BaseFileLimit](../../file/file_limits/base.md#onetl.base.base_file_limit.BaseFileLimit)] | None = None) → Iterator[tuple[RemoteDirectory, list[RemoteDirectory], list[RemoteFile]]]

Walk into directory tree, and iterate over its content in all nesting levels. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

Just like `os.walk`, but with additional filter/limit logic.

#### Versionadded
Added in version 0.8.0.

* **Parameters:**
  **root**
  : Directory path to walk into.

  **topdown**
  : If `True`, walk in top-down order, otherwise walk in bottom-up order.

  **filters**
  : Return only files/directories matching these filters. See [File Filters](../../file/file_filters/index.md#file-filters).

  **limits**
  : Apply limits to the list of files/directories, and immediately stop if any of these limits is reached.
    See [File Limits](../../file/file_limits/index.md#file-limits).
* **Returns:**
  `Iterator[tuple[root, dirs, files]]`, like `os.walk`.

  But all the paths are not strings, instead path classes with embedded stats are returned.
* **Raises:**
  NotADirectoryError
  : Path is not a directory

  `onetl.exception.DirectoryNotFoundError`
  : Path does not exist

### Examples

```pycon
>>> for root, dirs, files in connection.walk("/path/to/dir"):
...    break
>>> os.fspath(root)
'/path/to/dir'
>>> dirs
[]
>>> os.fspath(files[0])
'file.csv'
>>> connection.path_exists("/path/to/dir/file.csv")
True
```

<!-- !! processed by numpydoc !! -->
