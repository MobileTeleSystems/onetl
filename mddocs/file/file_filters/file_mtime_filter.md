<a id="file-modificatiom-time"></a>

# FileModifiedTime

### *class* onetl.file.filter.file_mtime.FileModifiedTime(\*, since: datetime | None = None, until: datetime | None = None)

Filter files matching a specified modification time.

If file modification time (`.stat().st_mtime`) doesn’t match range, it will be excluded.
Doesn’t affect directories or paths without `.stat()` method.

#### NOTE
Some filesystems return timestamps truncated to whole seconds (without millisecond part).
obj:~since and :obj\`~until\`\` values should be adjusted accordingly.

#### Versionadded
Added in version 0.13.0.

* **Parameters:**
  **since**
  : Minimal allowed file modification time. `None` means no limit.

  **until**
  : Maximum allowed file modification time. `None` means no limit.

### Examples

Select files modified between start of the day (`00:00:00`) and hour ago:

```python
from datetime import datetime, timedelta
from onetl.file.filter import FileModifiedTime

hour_ago = datetime.now() - timedelta(hours=1)
day_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
file_mtime = FileModifiedTime(since=day_start, until=hour_ago)
```

Select only files modified since hour ago:

```python
from datetime import datetime, timedelta
from onetl.file.filter import FileModifiedTime

hour_ago = datetime.now() - timedelta(hours=1)
file_mtime = FileModifiedTime(since=hour_ago)
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
