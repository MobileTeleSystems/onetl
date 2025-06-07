<a id="exclude-dir-filter"></a>

# ExcludeDir

### *class* onetl.file.filter.exclude_dir.ExcludeDir(path: str | os.PathLike)

Filter files or directories which are included in a specific directory.

#### Versionadded
Added in version 0.8.0: Replaces deprecated `onetl.core.FileFilter`

* **Parameters:**
  **path**
  : Path to directory which should be excluded.

### Examples

Create exclude dir filter:

```python
from onetl.file.filter import ExcludeDir

exclude_dir = ExcludeDir("/export/news_parse/exclude_dir")
```

<!-- !! processed by numpydoc !! -->

#### match(path: PathProtocol) → bool

Returns `True` if path is matching the filter, `False` otherwise

#### Versionadded
Added in version 0.8.0.

### Examples

```pycon
>>> from onetl.impl import LocalPath
>>> filter.match(LocalPath("/path/to/file.csv"))
True
>>> filter.match(LocalPath("/path/to/excluded.csv"))
False
>>> filter.match(LocalPath("/path/to/file.csv"))
True
```

<!-- !! processed by numpydoc !! -->
