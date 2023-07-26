#  Copyright 2023 MTS (Mobile Telesystems)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from __future__ import annotations


class classproperty(property):  # noqa: N801
    """
    Like ``@property``, but can be used to decorate class methods.

    Examples
    --------

    >>> class My:
    ...    @classproperty
    ...    def attribute(cls):
    ...        return 123
    >>> # no call
    >>> assert My.attribute == 123
    """

    def __init__(self, f):
        self.f = f

    def __get__(self, obj, objtype=None):
        return self.f(objtype or type(obj))
