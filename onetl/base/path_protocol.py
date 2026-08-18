# SPDX-FileCopyrightText: 2023-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, TypeAlias, runtime_checkable

from onetl.base.path_stat_protocol import PathStatProtocol
from onetl.base.pure_path_protocol import PurePathProtocol

if TYPE_CHECKING:
    PathProtocol: TypeAlias = Path
    PathWithStatsProtocol: TypeAlias = PathProtocol
else:

    @runtime_checkable
    class PathProtocol(PurePathProtocol, Protocol):
        """
        Generic protocol for `pathlib.Path` like objects.

        Includes only minimal set of methods which allow to determine path type (file, directory) and existence
        """

        def is_dir(self) -> bool:
            """
            Checks if this path is a directory
            """

        def is_file(self) -> bool:
            """
            Checks if this path is a file
            """

        def exists(self) -> bool:
            """
            Checks if this path exists
            """

    @runtime_checkable
    class PathWithStatsProtocol(PathProtocol, Protocol):
        """
        Protocol for `pathlib.Path`-like file objects.

        Includes only minimal set of methods which allow to determine if file exists, or get stats, e.g. size
        """

        def stat(self) -> PathStatProtocol:
            """
            Returns stats object with file information
            """
