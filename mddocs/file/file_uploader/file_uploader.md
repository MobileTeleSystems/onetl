<a id="file-uploader"></a>

# File Uploader

| [`FileUploader`](#onetl.file.file_uploader.file_uploader.FileUploader)                         | Allows you to upload files to a remote source with specified file connection and parameters, and return an object with upload result summary.   |
|------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------|
| [`FileUploader.run`](#onetl.file.file_uploader.file_uploader.FileUploader.run)([files])        | Method for uploading files to remote host.                                                                                                      |
| [`FileUploader.view_files`](#onetl.file.file_uploader.file_uploader.FileUploader.view_files)() | Get file list in the `local_path`.                                                                                                              |

### *class* onetl.file.file_uploader.file_uploader.FileUploader(\*, connection: ~onetl.base.base_file_connection.BaseFileConnection, target_path: ~onetl.impl.remote_path.RemotePath, local_path: ~onetl.impl.local_path.LocalPath | None = None, temp_path: ~onetl.impl.remote_path.RemotePath | None = None, options: ~onetl.file.file_uploader.options.FileUploaderOptions = FileUploaderOptions(if_exists=<FileExistBehavior.ERROR: 'error'>, delete_local=False, workers=1))

Allows you to upload files to a remote source with specified file connection
and parameters, and return an object with upload result summary. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

#### NOTE
This class is used to upload files **only** from local directory to the remote one.

It does NOT support direct file transfer between filesystems, like `FTP -> SFTP`.
You should use [File Downloader](../file_downloader/file_downloader.md#file-downloader) + FileUploader to implement `FTP -> local dir -> SFTP`.

#### WARNING
This class does **not** support read strategies.

#### Versionadded
Added in version 0.1.0.

#### Versionchanged
Changed in version 0.8.0: Moved `onetl.core.FileDownloader` → `onetl.file.FileDownloader`

* **Parameters:**
  **connection**
  : Class which contains File system connection properties. See [File Connections](../../connection/file_connection/index.md#file-connections) section.

  **target_path**
  : Remote path where want you upload files to

  **local_path**
  : The local directory from which the data is loaded.
    <br/>
    Could be `None`, but only if you pass absolute file paths directly to
    [`run`](#onetl.file.file_uploader.file_uploader.FileUploader.run) method
    <br/>
    #### Versionadded
    Added in version 0.3.0.

  **temp_path**
  : If set, this path will be used for uploading a file, and then renaming it to the target file path.
    If `None` (default since v0.5.0) is passed, files are uploaded directly to `target_path`.
    <br/>
    #### WARNING
    In case of production ETL pipelines, please set a value for `temp_path` (NOT `None`).
    This allows to properly handle upload interruption,
    without creating half-uploaded files in the target,
    because unlike file upload, `rename` call is atomic.
    <br/>
    #### WARNING
    In case of connections like SFTP or FTP, which can have multiple underlying filesystems,
    please pass `temp_path` path on the SAME filesystem as `target_path`.
    Otherwise instead of `rename`, remote OS will move file between filesystems,
    which is NOT atomic operation.
    <br/>
    #### Versionchanged
    Changed in version 0.5.0: Default changed from `/tmp` to `None`

  **options**
  : File upload options. See [`FileUploader.Options`](options.md#onetl.file.file_uploader.options.FileUploaderOptions)

### Examples

Minimal example

```py
from onetl.connection import HDFS
from onetl.file import FileUploader

hdfs = HDFS(...)

uploader = FileUploader(
    connection=hdfs,
    target_path="/path/to/remote/source",
)
```

Full example

```py
from onetl.connection import HDFS
from onetl.file import FileUploader

hdfs = HDFS(...)

uploader = FileUploader(
    connection=hdfs,
    target_path="/path/to/remote/source",
    temp_path="/user/onetl",
    local_path="/some/local/directory",
    options=FileUploader.Options(delete_local=True, if_exists="overwrite"),
)
```

<!-- !! processed by numpydoc !! -->

#### run(files: Iterable[str | PathLike] | None = None) → [UploadResult](result.md#onetl.file.file_uploader.result.UploadResult)

Method for uploading files to remote host. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

#### Versionadded
Added in version 0.1.0.

* **Parameters:**
  **files**
  : File list to upload.
    <br/>
    If empty, upload files from `local_path`.
* **Returns:**
  `UploadResult`
  : Upload result object
* **Raises:**
  `onetl.exception.DirectoryNotFoundError`
  : `local_path` does not found

  NotADirectoryError
  : `local_path` is not a directory

  ValueError
  : File in `files` argument does not match `local_path`

### Examples

Upload files from `local_path` to `target_path`:

```pycon
>>> from onetl.file import FileUploader
>>> uploader = FileUploader(local_path="/local", target_path="/remote", ...)
>>> upload_result = uploader.run()
>>> upload_result
UploadResult(
    successful=FileSet([
        RemoteFile("/remote/file1"),
        RemoteFile("/remote/file2"),
        # directory structure is preserved
        RemoteFile("/remote/nested/path/file3")
    ]),
    failed=FileSet([
        FailedLocalFile("/local/failed.file"),
    ]),
    skipped=FileSet([
        LocalPath("/local/already.exists"),
    ]),
    missing=FileSet([
        LocalPath("/local/missing.file"),
    ]),
)
```

Upload only certain files from `local_path`:

```pycon
>>> from onetl.file import FileUploader
>>> uploader = FileUploader(local_path="/local", target_path="/remote", ...)
>>> # paths could be relative or absolute, but all should be in "/local"
>>> upload_result = uploader.run(
...     [
...         "/local/file1",
...         "/local/nested/path/file3",
...         # excluding "/local/file2",
...     ]
... )
>>> upload_result
UploadResult(
    successful=FileSet([
        RemoteFile("/remote/file1"),
        # directory structure is preserved
        RemoteFile("/remote/nested/path/file3"),
    ]),
    failed=FileSet([]),
    skipped=FileSet([]),
    missing=FileSet([]),
)
```

Upload only certain files from any folder:

```pycon
>>> from onetl.file import FileUploader
>>> uploader = FileUploader(target_path="/remote", ...)  # no local_path set
>>> # only absolute paths
>>> upload_result = uploader.run(
...     [
...         "/local/file1.txt",
...         "/any/nested/path/file3.txt",
...     ]
... )
>>> upload_result
UploadResult(
    successful=FileSet([
        RemoteFile("/remote/file1.txt"),
        # directory structure is NOT preserved without local_path
        RemoteFile("/remote/file3.txt"),
    ]),
    failed=FileSet([]),
    skipped=FileSet([]),
    missing=FileSet([]),
)
```

<!-- !! processed by numpydoc !! -->

#### view_files() → FileSet[LocalPath]

Get file list in the `local_path`. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

#### Versionadded
Added in version 0.3.0.

* **Returns:**
  FileSet[LocalPath]
  : Set of files in `local_path`
* **Raises:**
  `onetl.exception.DirectoryNotFoundError`
  : `local_path` does not found

  NotADirectoryError
  : `local_path` is not a directory

### Examples

View files:

```pycon
>>> from onetl.file import FileUploader
>>> uploader = FileUploader(local_path="/local", ...)
>>> uploader.view_files()
FileSet([
    LocalPath("/local/file1.txt"),
    LocalPath("/local/file3.txt"),
    LocalPath("/local/nested/path/file3.txt"),
])
```

<!-- !! processed by numpydoc !! -->
