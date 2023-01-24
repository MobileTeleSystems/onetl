.. _plugins-design:

High level design
==================

What are plugins?
-----------------

Terms
~~~~~
* ``Plugin`` - some Python package which implements some extra functionality for onETL, like :ref:`hooks`
* ``Plugin autoimport`` - onETL behavior which allows to automatically import this package if it contains proper metadata (``entry_points``)

Features
~~~~~~~~

Plugins mechanism allows to:

* Automatically register :ref:`hooks` which can alter onETL behavior
* Automatically register new classes, like HWM type, HWM stores and so on

Limitations
~~~~~~~~~~~
Unlike other projects (like *Airflow 1.x*), plugins does not inject imported classes or functions to ``onetl.*`` namespace.
Users should import classes from the plugin package **explicitly** to avoid name collisions.

How to implement plugin?
------------------------

Create a Python package ``some-plugin`` with a file ``some_plugin/setup.py``:

.. code-block:: python

    # some_plugin/setup.py
    from setuptools import setup

    setup(
        # if you want to import something from onETL, add it to requirements list
        install_requires=["onetl"],
        entry_points={
            # this key enables plugins autoimport functionality
            "onetl.plugins": [
                "some-plugin-name=some_plugin.module",  # automatically import all module content
                "some-plugin-class=some_plugin.module.internals:MyClass",  # import a specific class
                "some-plugin-function=some_plugin.module.internals:my_function",  # import a specific function
            ],
        },
    )

See `setuptools documentation for entry_points <https://setuptools.pypa.io/en/latest/userguide/entry_point.html>`_


How plugins are imported?
-------------------------

* User should install a package implementing the plugin:

.. code-block:: bash

    pip install some-package

* Then user should import something from ``onetl`` module or its submodules:

.. code-block:: python

    import onetl
    from onetl.connection import SomeConnection

    # and so on

* This import automatically executes something like:

.. code-block:: python

    import some_plugin.module
    from some_plugin.module.internals import MyClass
    from some_plugin.module.internals import my_function

If specified module/class/function uses some registration capabilities of onETL,
like :ref:`hook-decorator`, it will be executed during this import.

How to enable/disable plugins?
------------------------------

By default plugins are enabled.

To disabled them, you can set environment variable ``ONETL_ENABLE_PLUGINS`` to ``false`` BEFORE
importing onETL. This will disable all plugins autoimport.

But user is still be able to explicitly import ``some_plugin.module``, executing
all decorators and registration capabilities of onETL.
