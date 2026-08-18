# SPDX-FileCopyrightText: 2023-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import os
import re

from pydantic import field_validator

from onetl.base import BaseFileFilter, PathProtocol
from onetl.impl import FrozenModel


class Regexp(BaseFileFilter, FrozenModel):
    r"""Filter files or directories with path matching a regular expression.

    !!! success "Added in 0.8.0"
        Replaces deprecated `onetl.core.FileFilter`

    Parameters
    ----------

    pattern

        Regular expression (e.g. `\d+\.csv`) for which any **file** (only file) path should match.

        If input is a string, regular expression will be compiles using `re.IGNORECASE` and `re.DOTALL` flags.

    Examples
    --------

    Create regexp filter from string:

    ```python
    from onetl.file.filter import Regexp

    regexp = Regexp(r"\d+\.csv")

    ```
    Create regexp filter from compiled regexp:

    ```python
    import re

    from onetl.file.filter import Regexp

    regexp = Regexp(re.compile(r"\d+\.csv", re.IGNORECASE | re.DOTALL))

    ```
    """

    pattern: re.Pattern

    def __init__(self, pattern: str):
        # this is only to allow passing regexp as positional argument
        super().__init__(pattern=pattern)  # type: ignore[call-arg]

    def __repr__(self):
        return f"{self.__class__.__name__}({self.pattern!r})"

    def match(self, path: PathProtocol) -> bool:
        if not path.is_file():
            return True

        return self.pattern.search(os.fspath(path)) is not None

    @field_validator("pattern", mode="before")
    @classmethod
    def _validate_pattern(cls, value: re.Pattern | str) -> re.Pattern:
        if isinstance(value, re.Pattern):
            return value
        try:
            return re.compile(value, re.IGNORECASE | re.DOTALL)
        except re.error as e:
            msg = f"Invalid regexp: {value!r}"
            raise ValueError(msg) from e
