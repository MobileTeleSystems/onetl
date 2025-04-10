<a id="limits-reached"></a>

# limits_reached

### onetl.file.limit.limits_reached.limits_reached(limits: Iterable[[BaseFileLimit](base.md#onetl.base.base_file_limit.BaseFileLimit)]) → bool

Check if any of limits reached.

#### Versionadded
Added in version 0.8.0.

* **Parameters:**
  **limits**
  : Limits to test.
* **Returns:**
  `True` if any of limits is reached, `False` otherwise.

  If no limits are passed, returns `False`.

### Examples

```pycon
>>> from onetl.file.limit import MaxFilesCount, limits_reached, limits_stop_at
>>> from onetl.impl import LocalPath
>>> limits = [MaxFilesCount(2)]
>>> limits_reached(limits)
False
>>> limits_stop_at(LocalPath("/path/to/file1.csv"), limits)
False
>>> limits_stop_at(LocalPath("/path/to/file2.csv"), limits)
False
>>> limits_stop_at(LocalPath("/path/to/file3.csv"), limits)
True
>>> limits_reached(limits)
True
```

<!-- !! processed by numpydoc !! -->
