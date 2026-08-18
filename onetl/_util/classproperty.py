# SPDX-FileCopyrightText: 2023-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0


class classproperty(property):  # noqa: N801
    """
    Like `@property`, but can be used to decorate class methods.

    Examples
    --------

    ```python
    >>> class My:
    ...    @classproperty
    ...    def attribute(cls):
    ...        return 123
    >>> # no call
    >>> My.attribute
    123

    ```
    """

    def __init__(self, f):
        self.f = f

    def __get__(self, obj, objtype=None):
        return self.f(objtype or type(obj))
