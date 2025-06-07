<a id="hooks-global-state"></a>

# Hooks global state

| [`skip_all_hooks`](#onetl.hooks.hooks_state.skip_all_hooks)()     | Temporary stop all onETL hooks.   |
|-------------------------------------------------------------------|-----------------------------------|
| [`stop_all_hooks`](#onetl.hooks.hooks_state.stop_all_hooks)()     | Stop all hooks for all classes.   |
| [`resume_all_hooks`](#onetl.hooks.hooks_state.resume_all_hooks)() | Resume all onETL hooks.           |

### onetl.hooks.hooks_state.skip_all_hooks()

Temporary stop all onETL hooks. Designed to be used as context manager or decorator.

#### NOTE
If hooks were stopped by [`stop_all_hooks`](#onetl.hooks.hooks_state.stop_all_hooks), they will not be resumed
after exiting the context/decorated function.
You should call [`resume_all_hooks`](#onetl.hooks.hooks_state.resume_all_hooks) explicitly.

#### Versionadded
Added in version 0.7.0.

### Examples

Context manager syntax

```py
from onetl.hooks import skip_all_hooks

# hooks are enabled

with skip_all_hooks():
    # hooks are stopped here
    ...

# hook state is restored
```

Decorator syntax

```py
from onetl.hooks import skip_all_hooks

# hooks are enabled

@skip_all_hooks()
def main():
    # hooks are stopped here
    ...

main()
```

<!-- !! processed by numpydoc !! -->

### onetl.hooks.hooks_state.stop_all_hooks() → None

Stop all hooks for all classes.

#### Versionadded
Added in version 0.7.0.

### Examples

```python
from onetl.hooks import stop_all_hooks

# hooks are executed

stop_all_hooks()

# all hooks are stopped now
```

<!-- !! processed by numpydoc !! -->

### onetl.hooks.hooks_state.resume_all_hooks() → None

Resume all onETL hooks.

#### NOTE
This function does not enable hooks which were disabled by [`onetl.hooks.hook.Hook.disable`](hook.md#onetl.hooks.hook.Hook.disable),
or stopped by [`onetl.hooks.support_hooks.suspend_hooks`](support_hooks.md#onetl.hooks.support_hooks.suspend_hooks).

#### Versionadded
Added in version 0.7.0.

### Examples

```python
from onetl.hooks import resume_all_hooks, stop_all_hooks

stop_all_hooks()

# hooks are stopped

resume_all_hooks()

# all hooks are executed now
```

<!-- !! processed by numpydoc !! -->
