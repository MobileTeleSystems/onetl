# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import textwrap
import warnings
from importlib import import_module

from onetl.core.file_filter import *
from onetl.core.file_limit import *

module_for_class = {
    "DBReader": "db",
    "DBWriter": "db",
    "FileDownloader": "file",
    "DownloadResult": "file",
    "FileUploader": "file",
    "UploadResult": "file",
    "MoveResult": "file",
    "FileResult": "file.file_result",
    "FileSet": "file.file_set",
}


def __getattr__(name: str):
    if name in module_for_class:
        submodule = module_for_class[name]
        msg = f"""
        This import is deprecated since v0.8.0:

            from onetl.core import {name}

        Please use instead:

            from onetl.{submodule} import {name}
        """

        warnings.warn(
            textwrap.dedent(msg),
            UserWarning,
            stacklevel=2,
        )
        return getattr(import_module(f"onetl.{submodule}"), name)

    raise AttributeError(name)
