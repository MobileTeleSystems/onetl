<a id="file-downloader-options"></a>

# File Downloader Options

### *pydantic model* onetl.file.file_downloader.options.FileDownloaderOptions

File downloading options.

#### Versionadded
Added in version 0.3.0.

### Examples

```python
from onetl.file import FileDownloader

options = FileDownloader.Options(
    if_exists="replace_entire_directory",
    delete_source=True,
    workers=4,
)
```

<!-- !! processed by numpydoc !! -->

#### *field* if_exists *: FileExistBehavior* *= FileExistBehavior.ERROR* *(alias 'mode')*

How to handle existing files in the local directory.

Possible values:
: * `error` (default) - mark file as failed
  * `ignore` - mark file as skipped
  * `replace_file` - replace existing file with a new one
  * `replace_entire_directory` - delete local directory content before downloading files

#### Versionchanged
Changed in version 0.9.0: Renamed `mode` → `if_exists`

<!-- !! processed by numpydoc !! -->

#### *field* delete_source *: bool* *= False*

If `True`, remove source file after successful download.

If download failed, file will left intact.

#### Versionadded
Added in version 0.2.0.

#### Versionchanged
Changed in version 0.3.0: Move `FileUploader.delete_local` to `FileUploaderOptions`

<!-- !! processed by numpydoc !! -->

#### *field* workers *: int* *= 1*

Number of workers to create for parallel file download.

1 (default) means files will me downloaded sequentially.
2 or more means files will be downloaded in parallel workers.

Recommended value is `min(32, os.cpu_count() + 4)`, e.g. `5`.

#### Versionadded
Added in version 0.8.1.

<!-- !! processed by numpydoc !! -->
