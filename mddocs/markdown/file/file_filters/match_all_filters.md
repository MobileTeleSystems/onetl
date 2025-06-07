<a id="match-all-filters"></a>

# match_all_filters

### onetl.file.filter.match_all_filters.match_all_filters(path: PathProtocol, filters: Iterable[[BaseFileFilter](base.md#onetl.base.base_file_filter.BaseFileFilter)]) → bool

Check if input path satisfies all the filters.

#### Versionadded
Added in version 0.8.0.

* **Parameters:**
  **path**
  : Path to check.

  **filters**
  : Filters to test path against.
* **Returns:**
  `True` if path matches all the filters, `False` otherwise.

  If filters are empty, returns `True`.

### Examples

```pycon
>>> from onetl.file.filter import Glob, ExcludeDir, match_all_filters
>>> from onetl.impl import RemoteFile, RemotePathStat
>>> filters = [Glob("*.csv"), ExcludeDir("/excluded")]
>>> match_all_filters(RemoteFile("/path/to/file.csv", stats=RemotePathStat()), filters)
True
>>> match_all_filters(RemoteFile("/path/to/file.txt", stats=RemotePathStat()), filters)
False
>>> match_all_filters(RemoteFile("/excluded/path/file.csv", stats=RemotePathStat()), filters)
False
```

<!-- !! processed by numpydoc !! -->
