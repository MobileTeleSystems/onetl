<a id="base-limit"></a>

# Base interface

| [`BaseFileLimit`](#onetl.base.base_file_limit.BaseFileLimit)()                       | Base file limit interface.                      |
|--------------------------------------------------------------------------------------|-------------------------------------------------|
| [`BaseFileLimit.reset`](#onetl.base.base_file_limit.BaseFileLimit.reset)()           | Resets the internal limit state.                |
| [`BaseFileLimit.stops_at`](#onetl.base.base_file_limit.BaseFileLimit.stops_at)(path) | Update internal state and return current state. |
| [`BaseFileLimit.is_reached`](#onetl.base.base_file_limit.BaseFileLimit.is_reached)   | Check if limit is reached.                      |

### *class* onetl.base.base_file_limit.BaseFileLimit

Base file limit interface.

Limits used by several onETL components, including [File Downloader](../file_downloader/file_downloader.md#file-downloader) and [File Mover](../file_mover/file_mover.md#file-mover),
to determine if internal loop should be stopped.

Unlike file filters, limits have internal state which can be updated or reset.

#### Versionadded
Added in version 0.8.0.

<!-- !! processed by numpydoc !! -->

#### *abstract property* is_reached *: bool*

Check if limit is reached.

#### Versionadded
Added in version 0.8.0.

* **Returns:**
  `True` if limit is reached, `False` otherwise.

### Examples

```pycon
>>> from onetl.impl import LocalPath
>>> limit.is_reached
False
>>> limit.stops_at(LocalPath("/path/to/file.csv"))
False
>>> limit.is_reached
False
>>> # after limit is reached
>>> limit.stops_at(LocalPath("/path/to/file.csv"))
True
>>> limit.is_reached
True
```

<!-- !! processed by numpydoc !! -->

#### *abstract* reset() → Self

Resets the internal limit state.

#### Versionadded
Added in version 0.8.0.

* **Returns:**
  Returns a filter of the same type, but with non-reached state.

  It could be the same filter or a new one, this is an implementation detail.

### Examples

```pycon
>>> limit.is_reached
True
>>> new_limit = limit.reset()
>>> new_limit.is_reached
False
```

<!-- !! processed by numpydoc !! -->

#### *abstract* stops_at(path: PathProtocol) → bool

Update internal state and return current state.

#### Versionadded
Added in version 0.8.0.

* **Parameters:**
  **path**
  : Path to check
* **Returns:**
  `True` if limit is reached, `False` otherwise.

### Examples

```pycon
>>> from onetl.impl import LocalPath
>>> # limit is not reached yet
>>> limit.stops_at(LocalPath("/path/to/file.csv"))
False
>>> # after limit is reached
>>> limit.stops_at(LocalPath("/path/to/another.csv"))
True
>>> # at this point, .stops_at() and .is_reached will always return True,
>>> # even on inputs that returned False before.
>>> # it will be in the same state until .reset() is called
>>> limit.stops_at(LocalPath("/path/to/file.csv"))
True
```

<!-- !! processed by numpydoc !! -->
