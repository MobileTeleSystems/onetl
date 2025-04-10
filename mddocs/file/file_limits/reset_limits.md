<a id="reset-limits"></a>

# reset_limits

### onetl.file.limit.reset_limits.reset_limits(limits: Iterable[[BaseFileLimit](base.md#onetl.base.base_file_limit.BaseFileLimit)]) → list[[BaseFileLimit](base.md#onetl.base.base_file_limit.BaseFileLimit)]

Reset limits state.

* **Parameters:**
  **limits**
  : Limits to reset.
* **Returns:**
  List with limits, but with reset state.

  List may contain original filters with reset state, or new copies.

  This is an implementation detail of [`reset`](base.md#onetl.base.base_file_limit.BaseFileLimit.reset) method.

### Examples

```pycon
>>> from onetl.file.limit import MaxFilesCount, limits_reached, limits_stop_at, reset_limits
>>> from onetl.impl import LocalPath
>>> limits = [MaxFilesCount(1)]
>>> limits_reached(limits)
False
>>> # do something
>>> limits_stop_at(LocalPath("/path/to/file1.csv"), limits)
False
>>> limits_stop_at(LocalPath("/path/to/file2.csv"), limits)
True
>>> limits_reached(limits)
True
>>> new_limits = reset_limits(limits)
>>> limits_reached(new_limits)
False
```

<!-- !! processed by numpydoc !! -->
