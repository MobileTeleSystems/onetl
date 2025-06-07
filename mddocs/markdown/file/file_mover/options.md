<a id="file-mover-options"></a>

# File Mover Options

### *pydantic model* onetl.file.file_mover.options.FileMoverOptions

File moving options.

#### Versionadded
Added in version 0.8.0.

### Examples

```python
from onetl.file import FileMover

options = FileMover.Options(
    if_exists="replace_entire_directory",
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
  * `replace_entire_directory` - delete directory content before moving files

#### Versionadded
Added in version 0.8.0.

#### Versionchanged
Changed in version 0.9.0: Renamed `mode` → `if_exists`

<!-- !! processed by numpydoc !! -->

#### *field* workers *: int* *= 1*

Number of workers to create for parallel file moving.

1 (default) means files will me moved sequentially.
2 or more means files will be moved in parallel workers.

Recommended value is `min(32, os.cpu_count() + 4)`, e.g. `5`.

#### Versionadded
Added in version 0.8.1.

<!-- !! processed by numpydoc !! -->
