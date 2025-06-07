<a id="hook-decorator"></a>

# `@hook` decorator

| [`hook`](#onetl.hooks.hook.hook)([inp, enabled, priority])                    | Initialize hook from callable/context manager.   |
|-------------------------------------------------------------------------------|--------------------------------------------------|
| [`HookPriority`](#onetl.hooks.hook.HookPriority)(value[, names, module, ...]) | Hook priority enum.                              |
| [`Hook`](#onetl.hooks.hook.Hook)(callback[, enabled, priority])               | Hook representation.                             |
| [`Hook.enable`](#onetl.hooks.hook.Hook.enable)()                              | Enable the hook.                                 |
| [`Hook.disable`](#onetl.hooks.hook.Hook.disable)()                            | Disable the hook.                                |
| [`Hook.skip`](#onetl.hooks.hook.Hook.skip)()                                  | Temporary disable the hook.                      |

### @onetl.hooks.hook.hook(inp: Callable[[...], T] | None = None, enabled: bool = True, priority: [HookPriority](#onetl.hooks.hook.HookPriority) = HookPriority.NORMAL)

Initialize hook from callable/context manager.

#### Versionadded
Added in version 0.7.0.

### Examples

Decorate a function or generator

```py
from onetl.hooks import hook, HookPriority

@hook
def some_func(*args, **kwargs):
    ...

@hook(enabled=True, priority=HookPriority.FIRST)
def another_func(*args, **kwargs):
    ...
```

Decorate a context manager

```py
from onetl.hooks import hook, HookPriority

@hook
class SimpleContextManager:
    def __init__(self, *args, **kwargs):
        ...

    def __enter__(self):
        ...
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        ...
        return False

@hook(enabled=True, priority=HookPriority.FIRST)
class ContextManagerWithProcessResult:
    def __init__(self, *args, **kwargs):
        ...

    def __enter__(self):
        ...
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        ...
        return False

    def process_result(self, result):
        # special method to handle method result call
        return modify(result)

    ...
```

<!-- !! processed by numpydoc !! -->

### *class* onetl.hooks.hook.HookPriority(value, names=None, \*, module=None, qualname=None, type=None, start=1, boundary=None)

Hook priority enum.

All hooks within the same priority are executed in the same order they were registered.

#### Versionadded
Added in version 0.7.0.

<!-- !! processed by numpydoc !! -->

#### FIRST *= -1*

Hooks with this priority will run first.

<!-- !! processed by numpydoc !! -->

#### NORMAL *= 0*

Hooks with this priority will run after [`FIRST`](#onetl.hooks.hook.HookPriority.FIRST) but before [`LAST`](#onetl.hooks.hook.HookPriority.LAST).

<!-- !! processed by numpydoc !! -->

#### LAST *= 1*

Hooks with this priority will run last.

<!-- !! processed by numpydoc !! -->

### *class* onetl.hooks.hook.Hook(callback: Callable[[...], T], enabled: bool = True, priority: [HookPriority](#onetl.hooks.hook.HookPriority) = HookPriority.NORMAL)

Hook representation.

#### Versionadded
Added in version 0.7.0.

* **Parameters:**
  **callback**
  : Some callable object which will be wrapped into a Hook, like function or ContextManager class.

  **enabled**
  : Will hook be executed or not. Useful for debugging.

  **priority**
  : Changes hooks priority, see `HookPriority` documentation.

### Examples

```python
from onetl.hooks.hook import Hook, HookPriority

def some_func(*args, **kwargs): ...

hook = Hook(callback=some_func, enabled=True, priority=HookPriority.FIRST)
```

<!-- !! processed by numpydoc !! -->

#### enable()

Enable the hook.

#### Versionadded
Added in version 0.7.0.

### Examples

```pycon
>>> def func1(): ...
>>> hook = Hook(callback=func1, enabled=False)
>>> hook.enabled
False
>>> hook.enable()
>>> hook.enabled
True
```

<!-- !! processed by numpydoc !! -->

#### disable()

Disable the hook.

#### Versionadded
Added in version 0.7.0.

### Examples

```pycon
>>> def func1(): ...
>>> hook = Hook(callback=func1, enabled=True)
>>> hook.enabled
True
>>> hook.disable()
>>> hook.enabled
False
```

<!-- !! processed by numpydoc !! -->

#### skip()

Temporary disable the hook.

#### NOTE
If hook was created with `enabled=False`, or was disabled by [`disable`](#onetl.hooks.hook.Hook.disable),
its state will left intact after exiting the context.

You should call [`enable`](#onetl.hooks.hook.Hook.enable) explicitly to change its state.

#### Versionadded
Added in version 0.7.0.

### Examples

Context manager syntax

```pycon
>>> def func1(): ...
>>> hook = Hook(callback=func1, enabled=True)
>>> hook.enabled
True
>>> with hook.skip():
...     print(hook.enabled)
False
>>> # hook state is restored as it was before entering the context manager
>>> hook.enabled
True
```

Decorator syntax

```pycon
>>> def func1(): ...
>>> hook = Hook(callback=func1, enabled=True)
>>> hook.enabled
True
>>> @hook.skip()
... def hook_disabled():
...     print(hook.enabled)
>>> hook_disabled()
False
>>> # hook state is restored as it was before entering the context manager
>>> hook.enabled
True
```

<!-- !! processed by numpydoc !! -->
