# SPDX-FileCopyrightText: 2022-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from pathlib import PurePosixPath
from typing import TYPE_CHECKING, TypeAlias

from onetl.base.pure_path_protocol import PurePathProtocol

if TYPE_CHECKING:
    RemotePath: TypeAlias = PurePathProtocol
else:
    RemotePath: TypeAlias = PurePosixPath
