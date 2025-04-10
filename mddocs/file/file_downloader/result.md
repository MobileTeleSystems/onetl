<a id="file-downloader-result"></a>

# File Downloader Result

### *class* onetl.file.file_downloader.result.DownloadResult(\*, successful: FileSet[LocalPath] = None, failed: FileSet[FailedRemoteFile] = None, skipped: FileSet[RemoteFile] = None, missing: FileSet[RemotePath] = None)

Representation of file download result.

Container for file paths, divided into certain categories:

* [`successful`](#onetl.file.file_downloader.result.DownloadResult.successful)
* [`failed`](#onetl.file.file_downloader.result.DownloadResult.failed)
* [`skipped`](#onetl.file.file_downloader.result.DownloadResult.skipped)
* [`missing`](#onetl.file.file_downloader.result.DownloadResult.missing)

#### Versionadded
Added in version 0.3.0.

### Examples

```pycon
>>> from onetl.file import FileDownloader
>>> downloader = FileDownloader(local_path="/local", ...)
>>> download_result = downloader.run(
...     [
...         "/remote/file1",
...         "/remote/file2",
...         "/failed/file",
...         "/existing/file",
...         "/missing/file",
...     ]
... )
>>> download_result
DownloadResult(
    successful=FileSet([
        LocalPath("/local/file1"),
        LocalPath("/local/file2"),
    ]),
    failed=FileSet([
        FailedLocalFile("/failed/file")
    ]),
    skipped=FileSet([
        RemoteFile("/existing/file")
    ]),
    missing=FileSet([
        RemotePath("/missing/file")
    ]),
)
```

<!-- !! processed by numpydoc !! -->

#### *property* details *: str*

Return detailed information about files in the result object

### Examples

```pycon
>>> from onetl.impl import FailedRemoteFile, LocalPath, RemoteFile, RemotePathStat
>>> from onetl.exception import NotAFileError
>>> from onetl.file.file_result import FileResult
>>> file_result1 = FileResult(
...     successful={
...         RemoteFile("/local/file", stats=RemotePathStat(st_size=1024)),
...         RemoteFile("/local/another.file", stats=RemotePathStat(st_size=1024)),
...     },
...     failed={
...         FailedRemoteFile(
...             path="/remote/file1",
...             stats=RemotePathStat(st_size=0),
...             exception=NotAFileError("'/remote/file1' is not a file"),
...         ),
...         FailedRemoteFile(
...             path="/remote/file2",
...             stats=RemotePathStat(st_size=0),
...             exception=PermissionError("'/remote/file2': [Errno 13] Permission denied"),
...         ),
...     },
...     skipped={LocalPath("/skipped/file1"), LocalPath("/skipped/file2")},
...     missing={LocalPath("/missing/file1"), LocalPath("/missing/file2")},
... )
>>> print(file_result1.details)
Total: 8 files (size='2.0 kB')

Successful 2 files (size='2.0 kB'):
    '/local/another.file' (size='1.0 kB')
    '/local/file' (size='1.0 kB')

Failed 2 files (size='0 Bytes'):
    '/remote/file2' (size='0 Bytes')
        PermissionError("'/remote/file2': [Errno 13] Permission denied")
    '/remote/file1' (size='0 Bytes')
        NotAFileError("'/remote/file1' is not a file")

Skipped 2 files (size='0 Bytes'):
    '/skipped/file1'
    '/skipped/file2'

Missing 2 files:
    '/missing/file2'
    '/missing/file1'
```

```pycon
>>> file_result2 = FileResult()
>>> print(file_result2.details)
No successful files

No failed files

No skipped files

No missing files
```

<!-- !! processed by numpydoc !! -->

#### dict(\*, include: 'AbstractSetIntStr' | 'MappingIntStrAny' | None = None, exclude: 'AbstractSetIntStr' | 'MappingIntStrAny' | None = None, by_alias: bool = False, skip_defaults: bool | None = None, exclude_unset: bool = False, exclude_defaults: bool = False, exclude_none: bool = False) → DictStrAny

Generate a dictionary representation of the model, optionally specifying which fields to include or exclude.

<!-- !! processed by numpydoc !! -->

#### *field* failed *: FileSet[FailedRemoteFile]* *[Optional]*

File paths (remote) which were not downloaded because of some failure

<!-- !! processed by numpydoc !! -->

#### *property* failed_count *: int*

Get number of failed files

### Examples

```pycon
>>> from onetl.impl import RemoteFile
>>> from onetl.file.file_result import FileResult
>>> file_result = FileResult(
...     failed={RemoteFile("/some/file"), RemoteFile("/some/another.file")},
... )
>>> file_result.failed_count
2
```

<!-- !! processed by numpydoc !! -->

#### *property* failed_size *: int*

Get size (in bytes) of failed files

### Examples

```pycon
>>> from onetl.impl import RemoteFile, RemotePathStat
>>> from onetl.file.file_result import FileResult
>>> file_result = FileResult(
...     failed={
...         RemoteFile("/some/file", stats=RemotePathStat(st_size=1024)),
...         RemoteFile("/some/another.file"), stats=RemotePathStat(st_size=1024)),
...     },
... )
>>> file_result.failed_size  # in bytes
2048
```

<!-- !! processed by numpydoc !! -->

#### *property* is_empty *: bool*

Returns `True` if there are no files in `successful`, `failed` and `skipped` attributes

### Examples

```pycon
>>> from onetl.impl import LocalPath
>>> from onetl.file.file_result import FileResult
>>> file_result1 = FileResult()
>>> file_result1.is_empty
True
>>> file_result2 = FileResult(
...     successful={LocalPath("/local/file"), LocalPath("/local/another.file")},
... )
>>> file_result2.is_empty
False
```

<!-- !! processed by numpydoc !! -->

#### json(\*, include: 'AbstractSetIntStr' | 'MappingIntStrAny' | None = None, exclude: 'AbstractSetIntStr' | 'MappingIntStrAny' | None = None, by_alias: bool = False, skip_defaults: bool | None = None, exclude_unset: bool = False, exclude_defaults: bool = False, exclude_none: bool = False, encoder: Callable[[Any], Any] | None = None, models_as_dict: bool = True, \*\*dumps_kwargs: Any) → str

Generate a JSON representation of the model, include and exclude arguments as per dict().

encoder is an optional function to supply as default to json.dumps(), other arguments as per json.dumps().

<!-- !! processed by numpydoc !! -->

#### *field* missing *: FileSet[RemotePath]* *[Optional]*

File paths (remote) which are not present in the remote file system

<!-- !! processed by numpydoc !! -->

#### *property* missing_count *: int*

Get number of missing files

### Examples

```pycon
>>> from onetl.impl import LocalPath
>>> from onetl.file.file_result import FileResult
>>> file_result = FileResult(
...     missing={LocalPath("/some/file"), LocalPath("/some/another.file")},
... )
>>> file_result.missing_count
2
```

<!-- !! processed by numpydoc !! -->

#### raise_if_contains_zero_size() → None

Raise exception if `successful` attribute contains a file with zero size

* **Raises:**
  ZeroFileSizeError
  : `successful` file set contains a file with zero size

### Examples

```pycon
>>> from onetl.exception import ZeroFileSizeError
>>> from onetl.impl import LocalPath
>>> from onetl.file.file_result import FileResult
>>> file_result = FileResult(
...     successful={
...         LocalPath("/local/empty1.file"),
...         LocalPath("/local/empty2.file"),
...         LocalPath("/local/normal.file"),
...     },
... )
>>> file_result.raise_if_contains_zero_size()
Traceback (most recent call last):
    ...
onetl.exception.ZeroFileSizeError: 2 files out of 3 have zero size:
    '/local/empty1.file'
    '/local/empty2.file'
```

<!-- !! processed by numpydoc !! -->

#### raise_if_empty() → None

Raise exception if there are no files in `successful`, `failed` and `skipped` attributes

* **Raises:**
  EmptyFilesError
  : `successful`, `failed` and `skipped` file sets are empty

### Examples

```pycon
>>> from onetl.file.file_result import FileResult
>>> file_result = FileResult()
>>> file_result.raise_if_empty()
Traceback (most recent call last):
    ...
onetl.exception.EmptyFilesError: There are no files in the result
```

<!-- !! processed by numpydoc !! -->

#### raise_if_failed() → None

Raise exception if there are some files in `failed` attribute

* **Raises:**
  FailedFilesError
  : `failed` file set is not empty

### Examples

```pycon
>>> from onetl.impl import FailedRemoteFile, RemotePathStat
>>> from onetl.exception import NotAFileError, FileMissingError
>>> from onetl.file.file_result import FileResult
>>> files_with_exception = [
...     FailedRemoteFile(
...         path="/remote/file1",
...         stats=RemotePathStat(st_size=0),
...         exception=NotAFileError("'/remote/file1' is not a file"),
...     ),
...     FailedRemoteFile(
...         path="/remote/file2",
...         stats=RemotePathStat(st_size=0),
...         exception=PermissionError("'/remote/file2': [Errno 13] Permission denied"),
...     ),
... ]
>>> file_result = FileResult(failed=files_with_exception)
>>> file_result.raise_if_failed()
Traceback (most recent call last)
...
onetl.exception.FailedFilesError: Failed 2 files (size='0 bytes'):
    '/remote/file1' (size='0 bytes')
        NotAFileError("'/remote/file1' is not a file")

    '/remote/file2' (size='0 Bytes')
        PermissionError("'/remote/file2': [Errno 13] Permission denied")
```

<!-- !! processed by numpydoc !! -->

#### raise_if_missing() → None

Raise exception if there are some files in `missing` attribute

* **Raises:**
  MissingFilesError
  : `missing` file set is not empty

### Examples

```pycon
>>> from onetl.impl import LocalPath
>>> from onetl.file.file_result import FileResult
>>> file_result = FileResult(
...     missing={
...         LocalPath("/missing/file1"),
...         LocalPath("/missing/file2"),
...     },
... )
>>> file_result.raise_if_missing()
Traceback (most recent call last):
    ...
onetl.exception.MissingFilesError: Missing 2 files:
    '/missing/file1'
    '/missing/file2'
```

<!-- !! processed by numpydoc !! -->

#### raise_if_skipped() → None

Raise exception if there are some files in `skipped` attribute

* **Raises:**
  SkippedFilesError
  : `skipped` file set is not empty

### Examples

```pycon
>>> from onetl.impl import LocalPath
>>> from onetl.file.file_result import FileResult
>>> file_result = FileResult(
...     skipped={
...         LocalPath("/skipped/file1"),
...         LocalPath("/skipped/file2"),
...     },
... )
>>> file_result.raise_if_skipped()
Traceback (most recent call last):
    ...
onetl.exception.SkippedFilesError: Skipped 2 files (15 kB):
    '/skipped/file1' (10kB)
    '/skipped/file2' (5 kB)
```

<!-- !! processed by numpydoc !! -->

#### *field* skipped *: FileSet[RemoteFile]* *[Optional]*

File paths (remote) which were skipped because of some reason

<!-- !! processed by numpydoc !! -->

#### *property* skipped_count *: int*

Get number of skipped files

### Examples

```pycon
>>> from onetl.impl import LocalPath
>>> from onetl.file.file_result import FileResult
>>> file_result = FileResult(
...     skipped={LocalPath("/some/file"), LocalPath("/some/another.file")},
... )
>>> file_result.skipped_count
2
```

<!-- !! processed by numpydoc !! -->

#### *property* skipped_size *: int*

Get size (in bytes) of skipped files

### Examples

```pycon
>>> from onetl.impl import LocalPath
>>> from onetl.file.file_result import FileResult
>>> file_result = FileResult(
...     skipped={LocalPath("/some/file"), LocalPath("/some/another.file")},
... )
>>> file_result.skipped_size  # in bytes
1024
```

<!-- !! processed by numpydoc !! -->

#### *field* successful *: FileSet[LocalPath]* *[Optional]*

File paths (local) which were downloaded successfully

<!-- !! processed by numpydoc !! -->

#### *property* successful_count *: int*

Get number of successful files

### Examples

```pycon
>>> from onetl.impl import LocalPath
>>> from onetl.file.file_result import FileResult
>>> file_result = FileResult(
...     successful={LocalPath("/some/file"), LocalPath("/some/another.file")},
... )
>>> file_result.successful_count
2
```

<!-- !! processed by numpydoc !! -->

#### *property* successful_size *: int*

Get size (in bytes) of successful files

### Examples

```pycon
>>> from onetl.impl import LocalPath
>>> from onetl.file.file_result import FileResult
>>> file_result = FileResult(
...     successful={LocalPath("/some/file"), LocalPath("/some/another.file")},
... )
>>> file_result.successful_size  # in bytes
1024
```

<!-- !! processed by numpydoc !! -->

#### *property* summary *: str*

Return short summary about files in the result object

### Examples

```pycon
>>> from onetl.impl import FailedRemoteFile, LocalPath, RemoteFile, RemotePathStat
>>> from onetl.exception import NotAFileError
>>> from onetl.file.file_result import FileResult
>>> file_result1 = FileResult(
...     successful={
...         RemoteFile("/local/file", stats=RemotePathStat(st_size=1024)),
...         RemoteFile("/local/another.file", stats=RemotePathStat(st_size=1024)),
...     },
...     failed={
...         FailedRemoteFile(
...             path="/remote/file1",
...             stats=RemotePathStat(st_size=0),
...             exception=NotAFileError("'/remote/file1' is not a file"),
...         ),
...         FailedRemoteFile(
...             path="/remote/file2",
...             stats=RemotePathStat(st_size=0),
...             exception=PermissionError("'/remote/file2': [Errno 13] Permission denied"),
...         ),
...     },
...     skipped={LocalPath("/skipped/file1"), LocalPath("/skipped/file2")},
...     missing={LocalPath("/missing/file1"), LocalPath("/missing/file2")},
... )
>>> print(file_result1.summary)
Total: 8 files (size='2.0 kB')

Successful: 2 files (size='2.0 kB')

Failed: 2 files (size='0 Bytes')

Skipped: 2 files (size='0 Bytes')

Missing: 2 files
```

```pycon
>>> file_result2 = FileResult()
>>> print(file_result2.summary)
No files
```

<!-- !! processed by numpydoc !! -->

#### *property* total_count *: int*

Get total number of all files

### Examples

```pycon
>>> from onetl.impl import RemoteFile
>>> from onetl.file.file_result import FileResult
>>> file_result = FileResult(
...     successful={LocalPath("/local/file"), LocalPath("/local/another.file")},
...     failed={RemoteFile("/remote/file"), RemoteFile("/remote/another.file")},
...     skipped={LocalPath("/skipped/file")},
...     missing={LocalPath("/missing/file")},
... )
>>> file_result.total_count
6
```

<!-- !! processed by numpydoc !! -->

#### *property* total_size *: int*

Get total size (in bytes) of all files

### Examples

```pycon
>>> from onetl.impl import RemoteFile, RemotePathStat, LocalPath
>>> from onetl.file.file_result import FileResult
>>> file_result = FileResult(
...     successful={LocalPath("/local/file"), LocalPath("/local/another.file")},
...     failed={
...         RemoteFile("/remote/file", stats=RemotePathStat(st_size=1024)),
...         RemoteFile("/remote/another.file", stats=RemotePathStat(st_size=1024))
...     },
...     skipped={LocalPath("/skipped/file")},
...     missing={LocalPath("/missing/file")},
... )
>>> file_result.total_size  # in bytes
4096
```

<!-- !! processed by numpydoc !! -->
