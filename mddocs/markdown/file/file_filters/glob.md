<a id="glob-filter"></a>

# Glob

### *class* onetl.file.filter.glob.Glob(pattern: str)

Filter files or directories with path matching a glob expression.

#### Versionadded
Added in version 0.8.0: Replaces deprecated `onetl.core.FileFilter`

* **Parameters:**
  **pattern**
  : Pattern (e.g. `*.csv`) for which any **file** (only file) path should match

### Examples

Create glob filter:

```python
from onetl.file.filter import Glob

glob = Glob("*.csv")
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
