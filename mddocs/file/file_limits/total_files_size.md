<a id="total-files-size-limit"></a>

# TotalFilesSize

### *class* onetl.file.limit.total_files_size.TotalFilesSize(limit: int | str)

Limits the total size of files handled by [File Downloader](../file_downloader/file_downloader.md#file-downloader) or [File Mover](../file_mover/file_mover.md#file-mover).

Calculates the sum of downloaded/moved files size (`.stat().st_size`),
and checks that this sum is less or equal to specified limit.

After limit is reached, no more files will be downloaded/moved.

Doesn’t affect directories, paths without `.stat()` method or files with zero size.

#### Versionadded
Added in version 0.13.0.

#### NOTE
[SI unit prefixes](https://en.wikipedia.org/wiki/Byte#Multiple-byte_units)
means that `1KB` == `1 kilobyte` == `1000 bytes`.
If you need `1024 bytes`, use `1 KiB` == `1 kibibyte`.

* **Parameters:**
  **limit**

### Examples

Create filter which allows to download/move files with total size up to 1GiB, but not higher:

```python
from onetl.file.limit import MaxFilesCount

limit = TotalFilesSize("1GiB")
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
