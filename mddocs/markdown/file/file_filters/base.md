<a id="base-file-filter"></a>

# Base interface

| [`BaseFileFilter`](#onetl.base.base_file_filter.BaseFileFilter)()                 | Base file filter interface.                                      |
|-----------------------------------------------------------------------------------|------------------------------------------------------------------|
| [`BaseFileFilter.match`](#onetl.base.base_file_filter.BaseFileFilter.match)(path) | Returns `True` if path is matching the filter, `False` otherwise |

### *class* onetl.base.base_file_filter.BaseFileFilter

Base file filter interface.

Filters used by several onETL components, including [File Downloader](../file_downloader/file_downloader.md#file-downloader) and [File Mover](../file_mover/file_mover.md#file-mover),
to determine if a file should be handled or not.

All filters are stateless.

#### Versionadded
Added in version 0.8.0.

<!-- !! processed by numpydoc !! -->

#### *abstract* match(path: PathProtocol) → bool

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
