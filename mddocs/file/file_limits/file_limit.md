<a id="file-limit"></a>

# File Limit (legacy)

### *class* onetl.core.file_limit.file_limit.FileLimit(\*, count_limit: int = 100)

Limits the number of downloaded files.

#### Deprecated
Deprecated since version 0.8.0: Use [`MaxFilesCount`](max_files_count.md#onetl.file.limit.max_files_count.MaxFilesCount) instead.

* **Parameters:**
  **count_limit**
  : Number of downloaded files at a time.

### Examples

Create a FileLimit object and set the amount in it:

```python
from onetl.core import FileLimit

limit = FileLimit(count_limit=1500)
```

If you create a [`onetl.file.file_downloader.file_downloader.FileDownloader`](../file_downloader/file_downloader.md#onetl.file.file_downloader.file_downloader.FileDownloader) object without
specifying the limit option, it will download with a limit of 100 files.

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
