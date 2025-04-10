<a id="file-filter"></a>

# File Filter (legacy)

### *class* onetl.core.file_filter.file_filter.FileFilter(\*, glob: str | None = None, regexp: Pattern | None = None, exclude_dirs: List[RemotePath] = None)

Filter files or directories by their path.

#### Deprecated
Deprecated since version 0.8.0: Use [`Glob`](glob.md#onetl.file.filter.glob.Glob), [`Regexp`](regexp.md#onetl.file.filter.regexp.Regexp)
or [`ExcludeDir`](exclude_dir.md#onetl.file.filter.exclude_dir.ExcludeDir) instead.

* **Parameters:**
  **glob**
  : Pattern (e.g. `*.csv`) for which any **file** (only file) path should match
    <br/>
    #### WARNING
    Mutually exclusive with `regexp`

  **regexp**
  : Regular expression (e.g. `\d+\.csv`) for which any **file** (only file) path should match.
    <br/>
    If input is a string, regular expression will be compiles using `re.IGNORECASE` and `re.DOTALL` flags
    <br/>
    #### WARNING
    Mutually exclusive with `glob`

  **exclude_dirs**
  : List of directories which should not be a part of a file or directory path

### Examples

Create exclude_dir filter:

```python
from onetl.core import FileFilter

file_filter = FileFilter(exclude_dirs=["/export/news_parse/exclude_dir"])
```

Create glob filter:

```python
from onetl.core import FileFilter

file_filter = FileFilter(glob="*.csv")
```

Create regexp filter:

```python
from onetl.core import FileFilter

file_filter = FileFilter(regexp=r"\d+\.csv")

# or

import re

file_filter = FileFilter(regexp=re.compile("\d+\.csv"))
```

Not allowed:

```python
from onetl.core import FileFilter

FileFilter()  # will raise ValueError, at least one argument should be passed
```

<!-- !! processed by numpydoc !! -->

#### match(path: PathProtocol) → bool

False means it does not match the template by which you want to receive files

<!-- !! processed by numpydoc !! -->
