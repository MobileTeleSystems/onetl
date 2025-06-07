<a id="max-files-count"></a>

# MaxFilesCount

### *class* onetl.file.limit.max_files_count.MaxFilesCount(limit: int)

Limits the total number of files handled by [File Downloader](../file_downloader/file_downloader.md#file-downloader) or [File Mover](../file_mover/file_mover.md#file-mover).

All files until specified limit (including) will be downloaded/moved, but `limit+1` will not.

This doesn’t apply to directories.

#### Versionadded
Added in version 0.8.0: Replaces deprecated `onetl.core.FileLimit`

* **Parameters:**
  **limit**

### Examples

Create filter which allows to download/move up to 100 files, but stops on 101:

```python
from onetl.file.limit import MaxFilesCount

limit = MaxFilesCount(100)
```

<!-- !! processed by numpydoc !! -->

#### *property* is_reached *: bool*

Check if limit is reached.

#### Versionadded
Added in version 0.8.0.

* **Returns:**
  `True` if limit is reached, `False` otherwise.

### Examples

```pycon
>>> from onetl.impl import LocalPath
>>> limit.is_reached
False
>>> limit.stops_at(LocalPath("/path/to/file.csv"))
False
>>> limit.is_reached
False
>>> # after limit is reached
>>> limit.stops_at(LocalPath("/path/to/file.csv"))
True
>>> limit.is_reached
True
```

<!-- !! processed by numpydoc !! -->

#### reset()

Resets the internal limit state.

#### Versionadded
Added in version 0.8.0.

* **Returns:**
  Returns a filter of the same type, but with non-reached state.

  It could be the same filter or a new one, this is an implementation detail.

### Examples

```pycon
>>> limit.is_reached
True
>>> new_limit = limit.reset()
>>> new_limit.is_reached
False
```

<!-- !! processed by numpydoc !! -->

#### stops_at(path: PathProtocol) → bool

Update internal state and return current state.

#### Versionadded
Added in version 0.8.0.

* **Parameters:**
  **path**
  : Path to check
* **Returns:**
  `True` if limit is reached, `False` otherwise.

### Examples

```pycon
>>> from onetl.impl import LocalPath
>>> # limit is not reached yet
>>> limit.stops_at(LocalPath("/path/to/file.csv"))
False
>>> # after limit is reached
>>> limit.stops_at(LocalPath("/path/to/another.csv"))
True
>>> # at this point, .stops_at() and .is_reached will always return True,
>>> # even on inputs that returned False before.
>>> # it will be in the same state until .reset() is called
>>> limit.stops_at(LocalPath("/path/to/file.csv"))
True
```

<!-- !! processed by numpydoc !! -->
