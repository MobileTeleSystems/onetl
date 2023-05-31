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

from abc import ABC, abstractmethod

from typing_extensions import Self

from onetl.base.path_protocol import PathProtocol


class BaseFileLimit(ABC):
    """
    Base file limit interface.

    Limits used by several onETL components, including :ref:`file-downloader` and :ref:`file-mover`,
    to determine if internal loop should be stopped.

    Unlike file filters, limits have internal state which can be updated or reset.
    """

    @abstractmethod
    def reset(self) -> Self:
        """
        Resets the internal limit state.

        Returns
        -------
        Returns a filter of the same type, but with non-reached state.

        It could be the same filter or a new one, this is an implementation detail.

        Examples
        --------

        .. code:: python

            assert limit.is_reached

            new_limit = limit.reset()
            assert not new_limit.is_reached
        """

    @abstractmethod
    def stops_at(self, path: PathProtocol) -> bool:
        """
        Update internal state and return current state.

        Parameters
        ----------
        path : :obj:`onetl.base.path_protocol.PathProtocol`
            Path to check

        Returns
        -------
        ``True`` if limit is reached, ``False`` otherwise.

        Examples
        --------

        .. code:: python

            from onetl.impl import LocalPath

            assert not limit.stops_at(LocalPath("/path/to/file.csv"))
            # do this multiple times
            ...

            # stopped on some input
            assert limit.stops_at(LocalPath("/path/to/another.csv"))

            # at this point, .stops_at() and .is_reached will always return True,
            # even on inputs that returned False before.
            # it will be in the same state until .reset() is called
            assert limit.stops_at(LocalPath("/path/to/file.csv"))
        """

    @property
    @abstractmethod
    def is_reached(self) -> bool:
        """
        Check if limit is reached.

        Returns
        -------
        ``True`` if limit is reached, ``False`` otherwise.

        Examples
        --------

        .. code:: python

            from onetl.impl import LocalPath

            assert not limit.is_reached

            assert not limit.stops_at(LocalPath("/path/to/file.csv"))
            assert not limit.is_reached

            assert limit.stops_at(LocalPath("/path/to/file.csv"))
            assert limit.is_reached
        """
