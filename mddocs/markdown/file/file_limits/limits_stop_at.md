<a id="limits-stop-at"></a>

# limits_stop_at

### onetl.file.limit.limits_stop_at.limits_stop_at(path: PathProtocol, limits: Iterable[[BaseFileLimit](base.md#onetl.base.base_file_limit.BaseFileLimit)]) → bool

Check if some of limits stops at given path.

#### Versionadded
Added in version 0.8.0.

* **Parameters:**
  **path**
  : Path to check.

  **limits**
  : Limits to test path against.
* **Returns:**
  `True` if any of limit is reached while handling the path, `False` otherwise.

  If no limits are passed, returns `False`.

### Examples

```pycon
>>> from onetl.file.limit import MaxFilesCount, limits_stop_at
>>> from onetl.impl import LocalPath
>>> limits = [MaxFilesCount(2)]
>>> limits_stop_at(LocalPath("/path/to/file1.csv"), limits)
False
>>> limits_stop_at(LocalPath("/path/to/file2.csv"), limits)
False
>>> limits_stop_at(LocalPath("/path/to/file3.csv"), limits)
True
```

<!-- !! processed by numpydoc !! -->
