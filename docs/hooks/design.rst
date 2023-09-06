.. _hooks-design:

High level design
==================

What are hooks?
---------------

Hook mechanism is a part of onETL which allows to inject some additional behavior into
existing methods of (almost) any class.

Features
~~~~~~~~

Hooks mechanism allows to:

* Inspect and validate input arguments and output results of method call
* Access, modify or replace method call result (but NOT input arguments)
* Wrap method calls with a context manager and catch raised exceptions

Hooks can be placed into :ref:`plugins`, allowing to modify onETL behavior by installing some additional package.

Limitations
~~~~~~~~~~~
* Hooks can be bound to methods of a class only (not functions).
* Only methods decorated with :ref:`slot-decorator` implement hooks mechanism. These class and methods are marked as |support_hooks|.
* Hooks can be bound to public methods only.

Terms
-----

* :ref:`slot-decorator` - method of a class with a special decorator
* ``Callback`` - function which implements some additional logic which modifies slot behavior
* :ref:`hook-decorator` - wrapper around callback which stores hook state, priority and some useful methods
* ``Hooks mechanism`` - calling ``Slot()`` will call all enabled hooks which are bound to the slot. Implemented by :ref:`support-hooks-decorator`.


How to implement hooks?
-----------------------

TL;DR
~~~~~

.. code-block:: python

    from onetl.hooks import support_hooks, slot, hook


    @support_hooks  # enabling hook mechanism for the class
    class MyClass:
        def __init__(self, data):
            self.data = data

        # this is slot
        @slot
        def method(self, arg):
            pass


    @MyClass.method.bind  # bound hook to the slot
    @hook  # this is hook
    def callback(obj, arg):  # this is callback
        print(obj.data, arg)


    obj = MyClass(1)
    obj.method(2)  # will call callback(obj, 1)

    # prints "1 2"

Define a slot
^^^^^^^^^^^^^

* Create a class with a method:

.. code:: python

    class MyClass:
        def __init__(self, data):
            self.data = data

        def method(self, arg):
            return self.data, arg

* Add :ref:`slot-decorator` to the method:

.. code:: python

    from onetl.hooks import support_hooks, slot, hook


    class MyClass:
        @slot
        def method(self, arg):
            return self.data, arg

If method has other decorators like ``@classmethod`` or ``@staticmethod``, ``@slot`` should be placed on the top:

.. code:: python

    from onetl.hooks import support_hooks, slot, hook


    class MyClass:
        @slot
        @classmethod
        def class_method(cls, arg):
            return cls, arg

        @slot
        @staticmethod
        def static_method(arg):
            return arg

* Add :ref:`support-hooks-decorator` to the class:

.. code:: python

    from onetl.hooks import support_hooks, slot, hook


    @support_hooks
    class MyClass:
        @slot
        def method(self, arg):
            return self.data, arg

Slot is created.

Define a callback
^^^^^^^^^^^^^^^^^

Define some function (a.k.a callback):

.. code:: python

    def callback(self, arg):
        print(self.data, arg)

It should have signature *compatible* with ``MyClass.method``. *Compatible* does not mean *exactly the same* -
for example, you can rename positional arguments:

.. code:: python

    def callback(obj, arg):
        print(obj.data, arg)

Use ``*args`` and ``**kwargs`` to omit arguments you don't care about:

.. code:: python

    def callback(obj, *args, **kwargs):
        print(obj.data, args, kwargs)

There is also an argument ``method_name`` which has a special meaning -
the method name which the callback is bound to is passed into this argument:

.. code:: python

    def callback(obj, *args, method_name: str, **kwargs):
        print(obj.data, args, method_name, kwargs)

.. note::

    ``method_name`` should always be a keyword argument, **NOT** positional.

.. warning::

    If callback signature is not compatible with slot signature, an exception will be raised,
    but **ONLY** while slot is called.

Define a hook
^^^^^^^^^^^^^^

Add :ref:`hook-decorator` to create a hook from your callback:

.. code:: python

    @hook
    def callback(obj, arg):
        print(obj.data, arg)

You can pass more options to the ``@hook`` decorator, like state or priority.
See decorator documentation for more details.

Bind hook to the slot
^^^^^^^^^^^^^^^^^^^^^

Use ``Slot.bind`` method to bind hook to the slot:

.. code:: python

    @MyClass.method.bind
    @hook
    def callback(obj, arg):
        print(obj, arg)

You can bind more than one hook to the same slot, and bind same hook to multiple slots:

.. code:: python

    @MyClass.method1.bind
    @MyClass.method2.bind
    @hook
    def callback1(obj, arg):
        "Will be called by both MyClass.method1 and MyClass.method2"


    @MyClass.method1.bind
    @hook
    def callback2(obj, arg):
        "Will be called by MyClass.method1 too"


How hooks are called?
---------------------

General
~~~~~~~

Just call the method decorated by ``@slot`` to trigger the hook:

.. code:: python

    obj = MyClass(1)
    obj.method(2)  # will call callback(obj, 2)

    # prints "1 2"

There are some special callback types that has a slightly different behavior.

Context managers
~~~~~~~~~~~~~~~~

``@hook`` decorator can be placed on a context manager class:

.. code:: python

    @hook
    class ContextManager:
        def __init__(self, obj, arg):
            self.obj = obj
            self.arg = arg

        def __enter__(self):
            # do something on enter
            print(obj.data, arg)
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            # do something on exit
            return False

Context manager is entered while calling the ``Slot()``, and exited then the call is finished.

If present, method ``process_result`` has a special meaning -
it can receive ``MyClass.method`` call result, and also modify/replace it:

.. code:: python

    @hook
    class ContextManager:
        def __init__(self, obj, arg):
            self.obj = obj
            self.arg = arg

        def __enter__(self):
            # do something on enter
            print(obj.data, arg)
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            # do something on exit
            return False

        def process_result(self, result):
            # do something with method call result
            return modified(result)

See examples below for more information.

Generator function
~~~~~~~~~~~~~~~~~~

``@hook`` decorator can be placed on a generator function:

.. code:: python

    @hook
    def callback(obj, arg):
        print(obj.data, arg)
        # this is called before original method body

        yield  # method is called here

        # this is called after original method body

It is converted to a context manager, in the same manner as
`contextlib.contextmanager <https://docs.python.org/3/library/contextlib.html#contextlib.contextmanager>`_.

Generator body can be wrapped with ``try..except..finally`` to catch exceptions:

.. code:: python

    @hook
    def callback(obj, arg):
        print(obj.data, arg)

        try:
            # this is called before original method body

            yield  # method is called here
        except Exception as e:
            process_exception(a)
        finally:
            # this is called after original method body
            finalizer()

There is also a special syntax which allows generator to access and modify/replace method call result:

.. code::

    @hook
    def callback(obj, arg):
        original_result = yield  # method is called here

        new_result = do_something(original_result)

        yield new_result  # modify/replace the result

Calling hooks in details
~~~~~~~~~~~~~~~~~~~~~~~~

* The callback will be called with the same arguments as the original method.

  * If slot is a regular method:

    .. code:: python

        callback_result = callback(self, *args, **kwargs)

    Here ``self`` is a class instance (``obj``).

  * If slot is a class method:

    .. code:: python

        callback_result = callback(cls, *args, **kwargs)

    Here ``cls`` is the class itself (``MyClass``).

  * If slot is a static method:

    .. code:: python

        callback_result = callback(*args, **kwargs)

    Neither object not class are passed to the callback in this case.

* If ``callback_result`` is a context manager, enter the context. Context manager can catch all the exceptions raised.

    If there are multiple hooks bound the the slot, every context manager will be entered.

* Then call the original method wrapped by ``@slot``:

  .. code:: python

      original_result = method(*args, **kwargs)

* Process ``original_result``:

  * If ``callback_result`` object has method ``process_result``, or is a generator wrapped with ``@hook``, call it:

    .. code:: python

        new_result = callback_result.process_result(original_result)

  * Otherwise set ``new_result = callback_result``.

  * If there are multiple hooks bound the the method, pass ``new_result`` through the chain:

    .. code:: python

        new_result = callback1_result.process_result(original_result)
        new_result = callback2_result.process_result(new_result or original_result)
        new_result = callback3_result.process_result(new_result or original_result)

* Finally return:

  .. code:: python

      return new_result or original_result

  All ``None`` values are ignored on every step above.

* Exit all the context managers entered during the slot call.


Hooks priority
~~~~~~~~~~~~~~

Hooks are executed in the following order:

1. Parent class slot + :obj:`FIRST <onetl.hooks.hook.HookPriority.FIRST>`
2. Inherited class slot + :obj:`FIRST <onetl.hooks.hook.HookPriority.FIRST>`
3. Parent class slot + :obj:`NORMAL <onetl.hooks.hook.HookPriority.NORMAL>`
4. Inherited class slot + :obj:`NORMAL <onetl.hooks.hook.HookPriority.NORMAL>`
5. Parent class slot + :obj:`LAST <onetl.hooks.hook.HookPriority.LAST>`
6. Inherited class slot + :obj:`LAST <onetl.hooks.hook.HookPriority.LAST>`

Hooks with the same priority and inheritance will be executed in the same order they were registered (``Slot.bind`` call).

.. note::

    Calls of ``super()`` inside inherited class methods does not trigger hooks call.
    Hooks are triggered only if method is called explicitly.

    This allow to wrap with a hook the entire slot call without influencing its internal logic.


Hook types
~~~~~~~~~~

Here are several examples of using hooks. These types are not exceptional, they can be mixed - for example,
hook can both modify method result and catch exceptions.

Before hook
^^^^^^^^^^^

Can be used for inspecting or validating input args of the original function:

.. code:: python

    @hook
    def before1(obj, arg):
        print(obj, arg)
        # original method is called after exiting this function


    @hook
    def before2(obj, arg):
        if arg == 1:
            raise ValueError("arg=1 is not allowed")
        return None  # return None is the same as no return statement

Executed before calling the original method wrapped by ``@slot``.
If hook raises an exception, method will not be called at all.


After hook
^^^^^^^^^^^

Can be used for performing some actions after original method was successfully executed:

.. code:: python

    @hook
    def after1(obj, arg):
        yield  # original method is called here
        print(obj, arg)


    @hook
    def after2(obj, arg):
        yield None  # yielding None is the same as empty yield
        if arg == 1:
            raise ValueError("arg=1 is not allowed")

If original method raises an exception, the block of code after ``yield`` will not be called.


Context hook
^^^^^^^^^^^^

Can be used for catching and handling some exceptions, or to determine that there was no exception during slot call:

.. tabs::

  .. code-tab:: py Generator syntax

      # This is just the same as using @contextlib.contextmanager

      @hook
      def context_generator(obj, arg):
          try:
              yield  # original method is called here
              print(obj, arg)  # <-- this line will not be called if method raised an exception
          except SomeException as e:
              magic(e)
          finally:
              finalizer()

  .. code-tab:: py Context manager syntax

      @hook
      class ContextManager:
          def __init__(self, obj, args):
              self.obj = obj
              self.args = args

          def __enter__(self):
              return self

          # original method is called between __enter__ and __exit__

          def __exit__(self, exc_type, exc_value, traceback):
              result = False
              if exc_type is not None and isinstance(exc_value, SomeException):
                  magic(exc_value)
                  result = True  # suppress exception
              else:
                  print(self.obj, self.arg)
              finalizer()
              return result

.. note::

    Contexts are exited in the reverse order of the hook calls.
    So if some hook raised an exception, it will be passed into the previous hook, not the next one.

    It is recommended to specify the proper priority for the hook, e.g. :obj:`FIRST <onetl.hooks.hook.HookPriority.FIRST>`

Replacing result hook
^^^^^^^^^^^^^^^^^^^^^

Replaces the output result of the original method.

Can be used for delegating some implementation details for third-party extensions.
See :ref:`hive` and :ref:`hdfs` as an example.

.. code:: python

    @hook
    def replace1(obj, arg):
        result = arg + 10  # any non-None return result

        # original method call result is ignored, output will always be arg + 10
        return result


    @hook
    def replace2(obj, arg):
        yield arg + 10  # same as above

.. note::

    If there are multiple hooks bound to the same slot, the result of last hook will be used.
    It is recommended to specify the proper priority for the hook, e.g. :obj:`LAST <onetl.hooks.hook.HookPriority.LAST>`


Accessing result hook
^^^^^^^^^^^^^^^^^^^^^

Can access output result of the original method and inspect or validate it:

.. tabs::

    .. code-tab:: py Generator syntax

        @hook
        def access_result(obj, arg):
            result = yield  # original method is called here, and result can be used in the hook
            print(result)
            yield  # does not modify result

    .. code-tab:: py Context manager syntax

        @hook
        class ModifiesResult:
            def __init__(self, obj, args):
                self.obj = obj
                self.args = args

            def __enter__(self):
                return self

            # original method is called between __enter__ and __exit__
            # result is passed into process_result method of context manager, if present

            def process_result(self, result):
                print(result)  # result can be used in the hook
                return None  # does not modify result. same as no return statement in the method

            def __exit__(self, exc_type, exc_value, traceback):
                return False


Modifying result hook
^^^^^^^^^^^^^^^^^^^^^

Can access output result of the original method, and return the modified one:

.. tabs::

    .. code-tab:: py Generator syntax

        @hook
        def modifies_result(obj, arg):
            result = yield  # original method is called here, and result can be used in the hook
            yield result + 10  # modify output result. None values are ignored

    .. code-tab:: py Context manager syntax

        @hook
        class ModifiesResult:
            def __init__(self, obj, args):
                self.obj = obj
                self.args = args

            def __enter__(self):
                return self

            # original method is called between __enter__ and __exit__
            # result is passed into process_result method of context manager, if present

            def process_result(self, result):
                print(result)  # result can be used in the hook
                return result + 10  # modify output result. None values are ignored

            def __exit__(self, exc_type, exc_value, traceback):
                return False

.. note::

    If there are multiple hooks bound to the same slot, the result of last hook will be used.
    It is recommended to specify the proper priority for the hook, e.g. :obj:`LAST <onetl.hooks.hook.HookPriority.LAST>`


How to enable/disable hooks?
----------------------------

You can enable/disable/temporary disable hooks on 4 different levels:

* Manage global hooks state (level 1):

    * :obj:`onetl.hooks.hooks_state.stop_all_hooks`
    * :obj:`onetl.hooks.hooks_state.resume_all_hooks`
    * :obj:`onetl.hooks.hooks_state.skip_all_hooks`

* Manage all hooks bound to a specific class (level 2):

    * :obj:`onetl.hooks.support_hooks.suspend_hooks`
    * :obj:`onetl.hooks.support_hooks.resume_hooks`
    * :obj:`onetl.hooks.support_hooks.skip_hooks`

* Manage all hooks bound to a specific slot (level 3):

    * :obj:`onetl.hooks.slot.Slot.suspend_hooks`
    * :obj:`onetl.hooks.slot.Slot.resume_hooks`
    * :obj:`onetl.hooks.slot.Slot.skip_hooks`

* Manage state of a specific hook (level 4):

    * :obj:`onetl.hooks.hook.Hook.enable`
    * :obj:`onetl.hooks.hook.Hook.disable`

More details in the documentation above.

.. note::

    All of these levels are independent.

    Calling ``stop`` on the level 1 has higher priority than level 2, and so on.
    But calling ``resume`` on the level 1 does not automatically resume hooks stopped in the level 2,
    they should be resumed explicitly.


How to see logs of the hook mechanism?
--------------------------------------

Hooks registration emits logs with ``DEBUG`` level:

.. code:: python

    from onetl.logs import setup_logging

    setup_logging()

.. code-block:: text

    DEBUG  |onETL| Registered hook 'mymodule.callback1' for 'MyClass.method' (enabled=True, priority=HookPriority.NORMAL)
    DEBUG  |onETL| Registered hook 'mymodule.callback2' for 'MyClass.method' (enabled=True, priority=HookPriority.NORMAL)
    DEBUG  |onETL| Registered hook 'mymodule.callback3' for 'MyClass.method' (enabled=False, priority=HookPriority.NORMAL)

But most of logs are emitted with even lower level ``NOTICE``, to make output less verbose:

.. code:: python

    from onetl.logs import NOTICE, setup_logging

    setup_logging(level=NOTICE)

.. code::

    NOTICE  |Hooks| 2 hooks registered for 'MyClass.method'
    NOTICE  |Hooks| Calling hook 'mymodule.callback1' (1/2)
    NOTICE  |Hooks| Hook is finished with returning non-None result
    NOTICE  |Hooks| Calling hook 'mymodule.callback2' (2/2)
    NOTICE  |Hooks| This is a context manager, entering ...
    NOTICE  |Hooks|   Calling original method 'MyClass.method'
    NOTICE  |Hooks|   Method call is finished
    NOTICE  |Hooks| Method call result (*NOT* None) will be replaced with result of hook 'mymodule.callback1'
    NOTICE  |Hooks|   Passing result to 'process_result' method of context manager 'mymodule.callback2'
    NOTICE  |Hooks|   Method call result (*NOT* None) is modified by hook!
