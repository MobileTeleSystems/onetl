# SPDX-FileCopyrightText: 2022-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from abc import ABC, abstractmethod

from typing_extensions import Self

from onetl.base.path_protocol import PathProtocol


class BaseFileLimit(ABC):
    """
    Base file limit interface.

    Limits used by several onETL components, including [onetl.file.file_downloader.file_downloader.FileDownloader][] and [onetl.file.file_mover.file_mover.FileMover][],
    to determine if internal loop should be stopped.

    Unlike file filters, limits have internal state which can be updated or reset.

    !!! success "Added in 0.8.0"
    """

    @abstractmethod
    def reset(self) -> Self:
        """
        Resets the internal limit state.

        !!! success "Added in 0.8.0"

        Returns
        -------
        :
            Returns a filter of the same type, but with non-reached state.

            It could be the same filter or a new one, this is an implementation detail.

        Examples
        --------

        ```python
        >>> limit.is_reached
        True
        >>> new_limit = limit.reset()
        >>> new_limit.is_reached
        False
        ```
        """

    @abstractmethod
    def stops_at(self, path: PathProtocol) -> bool:
        """
        Update internal state and return current state.

        !!! success "Added in 0.8.0"

        Parameters
        ----------
        path
            Path to check

        Returns
        -------
        :
            `True` if limit is reached, `False` otherwise.

        Examples
        --------

        ```python
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
        """

    @property
    @abstractmethod
    def is_reached(self) -> bool:
        """
        Check if limit is reached.

        !!! success "Added in 0.8.0"

        Returns
        -------
        :
            `True` if limit is reached, `False` otherwise.

        Examples
        --------

        ```python
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
        """
