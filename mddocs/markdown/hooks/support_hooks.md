<a id="support-hooks-decorator"></a>

# `@support_hooks` decorator

| [`support_hooks`](#onetl.hooks.support_hooks.support_hooks)(cls)   | Decorator which adds hooks functionality to a specific class.                                           |
|--------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------|
| [`skip_hooks`](#onetl.hooks.support_hooks.skip_hooks)(cls)         | Context manager (and decorator) which temporary disables hooks for all the methods of a specific class. |
| [`suspend_hooks`](#onetl.hooks.support_hooks.suspend_hooks)(cls)   | Disables all hooks for all the methods of a specific class.                                             |
| [`resume_hooks`](#onetl.hooks.support_hooks.resume_hooks)(cls)     | Enables all hooks for all the methods of a specific class.                                              |

### @onetl.hooks.support_hooks.support_hooks

Decorator which adds hooks functionality to a specific class.

Only methods decorated with `slot` can be used for connecting hooks.

Adds [`skip_hooks`](#onetl.hooks.support_hooks.skip_hooks), [`suspend_hooks`](#onetl.hooks.support_hooks.suspend_hooks) and [`resume_hooks`](#onetl.hooks.support_hooks.resume_hooks) to the class.

#### Versionadded
Added in version 0.7.0.

### Examples

```python
from onetl.hooks.hook import support_hooks, slot

@support_hooks
class MyClass:
    @slot
    def my_method(self, arg): ...

@MyClass.my_method.hook
def callback(self, arg): ...

MyClass().my_method()  # will execute callback function
```

<!-- !! processed by numpydoc !! -->

### onetl.hooks.support_hooks.skip_hooks(cls: type)

Context manager (and decorator) which temporary disables hooks for all the methods of a specific class.

### Examples

Context manager syntax

```py
@support_hooks
class MyClass:
    @slot
    def my_method(self, arg):
        ...

@MyClass.my_method.hook
def callback(self, arg):
    ...

obj = MyClass()
obj.my_method(1)  # will execute callback(obj, 1)

with MyClass.skip_hooks():
    obj.my_method()  # will NOT execute callback

# running outside the context restores previous behavior
obj.my_method(2)  # will execute callback(obj, 2)
```

Decorator syntax

```py
@support_hooks
class MyClass:
    @slot
    def my_method(self, arg):
        ...

@MyClass.my_method.hook
def callback(self, arg):
    ...

def with_hook_enabled():
    obj = MyClass()
    obj.my_method(1)

with_hook_enabled()  # will execute callback(obj, 1)

@MyClass.skip_hooks()
def with_all_hooks_disabled():
    obj = MyClass()
    obj.my_method(1)

with_all_hooks_disabled()  # will NOT execute callback function

# running outside a decorated function restores previous behavior
obj = MyClass()
obj.my_method(2)  # will execute callback(obj, 2)
```

#### Versionadded
Added in version 0.7.0.

<!-- !! processed by numpydoc !! -->

### onetl.hooks.support_hooks.suspend_hooks(cls: type) → None

Disables all hooks for all the methods of a specific class.

### Examples

```python
@support_hooks
class MyClass:
    @slot
    def my_method(self, arg): ...

@MyClass.my_method.hook
def callback(self, arg): ...

obj = MyClass()
obj.my_method(1)  # will execute callback(obj, 1)

MyClass.suspend_hooks()

obj.my_method(2)  # will NOT execute callback
```

#### Versionadded
Added in version 0.7.0.

<!-- !! processed by numpydoc !! -->

### onetl.hooks.support_hooks.resume_hooks(cls: type) → None

Enables all hooks for all the methods of a specific class.

### Examples

```python
@support_hooks
class MyClass:
    @slot
    def my_method(self, arg): ...

@MyClass.my_method.hook
def callback(self, arg): ...

obj = MyClass()

MyClass.suspend_hooks()
obj.my_method(1)  # will NOT execute callback

MyClass.resume_hooks()

obj.my_method(2)  # will execute callback(obj, 2)
```

#### Versionadded
Added in version 0.7.0.

<!-- !! processed by numpydoc !! -->
