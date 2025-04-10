<a id="regexp-filter"></a>

# Regexp

### *class* onetl.file.filter.regexp.Regexp(pattern: str)

Filter files or directories with path matching a regular expression.

#### Versionadded
Added in version 0.8.0: Replaces deprecated `onetl.core.FileFilter`

* **Parameters:**
  **pattern**
  : Regular expression (e.g. `\d+\.csv`) for which any **file** (only file) path should match.
    <br/>
    If input is a string, regular expression will be compiles using `re.IGNORECASE` and `re.DOTALL` flags.

### Examples

Create regexp filter from string:

```python
from onetl.file.filter import Regexp

regexp = Regexp(r"\d+\.csv")
```

Create regexp filter from `re.Pattern`:

```python
import re

from onetl.file.filter import Regexp

regexp = Regexp(re.compile(r"\d+\.csv", re.IGNORECASE | re.DOTALL))
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
