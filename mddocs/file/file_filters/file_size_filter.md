<a id="file-size-range"></a>

# FileSizeRange

### *class* onetl.file.filter.file_size.FileSizeRange(\*, min: ByteSize | None = None, max: ByteSize | None = None)

Filter files matching a specified size.

If file size (`.stat().st_size`) doesn’t match the range, it will be excluded.
Doesn’t affect directories or paths without `.stat()` method.

#### Versionadded
Added in version 0.13.0.

#### NOTE
[SI unit prefixes](https://en.wikipedia.org/wiki/Byte#Multiple-byte_units)
means that `1KB` == `1 kilobyte` == `1000 bytes`.
If you need `1024 bytes`, use `1 KiB` == `1 kibibyte`.

* **Parameters:**
  **min**
  : Minimal allowed file size. `None` means no limit.

  **max**
  : Maximum allowed file size. `None` means no limit.

### Examples

Specify min and max file sizes:

```python
from onetl.file.filter import FileSizeRange

file_size = FileSizeRange(min="1KiB", max="100MiB")
```

Specify only min file size:

```python
from onetl.file.filter import FileSizeRange

file_size = FileSizeRange(min="1KiB")
```

Specify only max file size:

```python
from onetl.file.filter import FileSizeRange

file_size = FileSizeRange(max="100MiB")
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
