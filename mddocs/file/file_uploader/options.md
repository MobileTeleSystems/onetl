<a id="file-uploader-options"></a>

# File Uploader Options

### *pydantic model* onetl.file.file_uploader.options.FileUploaderOptions

File uploading options.

#### Versionadded
Added in version 0.3.0.

### Examples

```python
from onetl.file import FileUploader

options = FileUploader.Options(
    if_exists="replace_entire_directory",
    delete_local=True,
    workers=4,
)
```

<!-- !! processed by numpydoc !! -->

#### *field* if_exists *: FileExistBehavior* *= FileExistBehavior.ERROR* *(alias 'mode')*

How to handle existing files in the target directory.

Possible values:
: * `error` (default) - mark file as failed
  * `ignore` - mark file as skipped
  * `replace_file` - replace existing file with a new one
  * `replace_entire_directory` - delete local directory content before downloading files

#### Versionchanged
Changed in version 0.9.0: Renamed `mode` → `if_exists`

<!-- !! processed by numpydoc !! -->

#### *field* delete_local *: bool* *= False*

If `True`, remove local file after successful download.

If download failed, file will left intact.

#### Versionadded
Added in version 0.2.0.

#### Versionchanged
Changed in version 0.3.0: Move `FileUploader.delete_local` to `FileUploaderOptions`

<!-- !! processed by numpydoc !! -->

#### *field* workers *: int* *= 1*

Number of workers to create for parallel file upload.

1 (default) means files will me uploaded sequentially.
2 or more means files will be uploaded in parallel workers.

Recommended value is `min(32, os.cpu_count() + 4)`, e.g. `5`.

#### Versionadded
Added in version 0.8.1.

<!-- !! processed by numpydoc !! -->
