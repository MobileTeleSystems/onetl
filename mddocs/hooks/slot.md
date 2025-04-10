<a id="slot-decorator"></a>

# `@slot` decorator

| [`slot`](#onetl.hooks.slot.slot)(method)                       | Decorator which enables hooks functionality on a specific class method.                              |
|----------------------------------------------------------------|------------------------------------------------------------------------------------------------------|
| [`Slot`](#onetl.hooks.slot.Slot)                               | Protocol which is implemented by a method after applying [`slot`](#onetl.hooks.slot.slot) decorator. |
| [`Slot.bind`](#onetl.hooks.slot.Slot.bind)([inp])              | Bind a hook to the slot.                                                                             |
| [`Slot.skip_hooks`](#onetl.hooks.slot.Slot.skip_hooks)()       | Context manager which temporary stops all the hooks bound to the slot.                               |
| [`Slot.suspend_hooks`](#onetl.hooks.slot.Slot.suspend_hooks)() | Stop all the hooks bound to the slot.                                                                |
| [`Slot.resume_hooks`](#onetl.hooks.slot.Slot.resume_hooks)()   | Resume all hooks bound to the slot.                                                                  |

### @onetl.hooks.slot.slot

Decorator which enables hooks functionality on a specific class method.

Decorated methods get additional nested methods:

> * [`onetl.hooks.slot.Slot.bind`](#onetl.hooks.slot.Slot.bind)
> * [`onetl.hooks.slot.Slot.suspend_hooks`](#onetl.hooks.slot.Slot.suspend_hooks)
> * [`onetl.hooks.slot.Slot.resume_hooks`](#onetl.hooks.slot.Slot.resume_hooks)
> * [`onetl.hooks.slot.Slot.skip_hooks`](#onetl.hooks.slot.Slot.skip_hooks)

#### NOTE
Supported method types are:

> * Regular methods
> * `@classmethod`
> * `@staticmethod`

It is not allowed to use this decorator over `_private` and `__protected` methods and `@property`.
But is allowed to use on `__dunder__` methods, like `__init__`.

#### Versionadded
Added in version 0.7.0.

### Examples

```python
from onetl.hooks import support_hooks, slot, hook

@support_hooks
class MyClass:
    @slot
    def my_method(self, arg): ...

    @slot  # decorator should be on top of all other decorators
    @classmethod
    def class_method(cls): ...

    @slot  # decorator should be on top of all other decorators
    @staticmethod
    def static_method(arg): ...

@MyClass.my_method.bind
@hook
def callback1(self, arg): ...

@MyClass.class_method.bind
@hook
def callback2(cls): ...

@MyClass.static_method.bind
@hook
def callback3(arg): ...

obj = MyClass()
obj.my_method(1)  # will execute callback1(obj, 1)
MyClass.class_method(2)  # will execute callback2(MyClass, 2)
MyClass.static_method(3)  # will execute callback3(3)
```

<!-- !! processed by numpydoc !! -->

### *protocol* onetl.hooks.slot.Slot

Protocol which is implemented by a method after applying [`slot`](#onetl.hooks.slot.slot) decorator.

#### Versionadded
Added in version 0.7.0.

<!-- !! processed by numpydoc !! -->

Classes that implement this protocol must have the following methods / attributes:

#### \_\_call_\_(\*args, \*\*kwargs)

Call self as a function.

<!-- !! processed by numpydoc !! -->

#### *property* \_\_hooks_\_ *: HookCollection*

Collection of hooks bound to the slot

<!-- !! processed by numpydoc !! -->

#### bind(inp=None)

Bind a hook to the slot.

See [High level design](design.md#hooks-design) for more details.

#### Versionadded
Added in version 0.7.0.

### Examples

```python
from onetl.hooks import support_hooks, slot, hook, HookPriority

@support_hooks
class MyClass:
    @slot
    def method(self, arg):
        pass

@MyClass.method.bind
@hook
def callable(self, arg):
    if arg == "some":
        do_something()

@MyClass.method.bind
@hook(priority=HookPriority.FIRST, enabled=True)
def another_callable(self, arg):
    if arg == "another":
        raise NotAllowed()

obj = MyClass()
obj.method(1)  # will call both callable(obj, 1) and another_callable(obj, 1)
```

<!-- !! processed by numpydoc !! -->

#### resume_hooks()

Resume all hooks bound to the slot.

#### NOTE
If hook is disabled by [`onetl.hooks.hook.Hook.disable`](hook.md#onetl.hooks.hook.Hook.disable), it will stay disabled.
You should call [`onetl.hooks.hook.Hook.enable`](hook.md#onetl.hooks.hook.Hook.enable) explicitly.

### Examples

```python
from onetl.hooks.hook import hook, support_hooks, slot

@support_hooks
class MyClass:
    @slot
    def my_method(self, arg): ...

@MyClass.my_method.bind
@hook
def callback1(self, arg): ...

obj = MyClass()
obj.my_method(1)  # will call callback1(obj, 1)

MyClass.my_method.suspend_hooks()
obj.my_method(1)  # will NOT call callback1

MyClass.my_method.resume_hooks()
obj.my_method(2)  # will call callback1(obj, 2)
```

<!-- !! processed by numpydoc !! -->

#### skip_hooks() → ContextManager[None]

Context manager which temporary stops all the hooks bound to the slot.

#### NOTE
If hooks were stopped by [`suspend_hooks`](#onetl.hooks.slot.Slot.suspend_hooks), they will not be resumed
after exiting the context/decorated function.
You should call [`resume_hooks`](#onetl.hooks.slot.Slot.resume_hooks) explicitly.

### Examples

Context manager syntax

```py
from onetl.hooks.hook import hook, support_hooks, slot

@support_hooks
class MyClass:
    @slot
    def my_method(self, arg):
        ...

@MyClass.my_method.bind
@hook
def callback1(self, arg):
    ...

obj = MyClass()
obj.my_method(1)  # will call callback1(obj, 1)

with MyClass.my_method.skip_hooks():
    obj.my_method(1)  # will NOT call callback1

obj.my_method(2)  # will call callback1(obj, 2)
```

Decorator syntax

```py
from onetl.hooks.hook import hook, support_hooks, slot

@support_hooks
class MyClass:
    @slot
    def my_method(self, arg):
        ...

@MyClass.my_method.bind
@hook
def callback1(self, arg):
    ...

@MyClass.my_method.skip_hooks()
def method_without_hooks(obj, arg):
    obj.my_method(arg)

obj = MyClass()
obj.my_method(1)  # will call callback1(obj, 1)

method_without_hooks(obj, 1)  # will NOT call callback1

obj.my_method(2)  # will call callback1(obj, 2)
```

<!-- !! processed by numpydoc !! -->

#### suspend_hooks()

Stop all the hooks bound to the slot.

### Examples

```python
from onetl.hooks.hook import hook, support_hooks, slot

@support_hooks
class MyClass:
    @slot
    def my_method(self, arg): ...

@MyClass.my_method.bind
@hook
def callback1(self, arg): ...

obj = MyClass()
obj.my_method(1)  # will call callback1(obj, 1)

MyClass.my_method.suspend_hooks()
obj.my_method(1)  # will NOT call callback1
```

<!-- !! processed by numpydoc !! -->
