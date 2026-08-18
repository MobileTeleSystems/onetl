# SPDX-FileCopyrightText: 2023-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import warnings

from pydantic import Field, model_validator

from onetl._util.alias import avoid_alias
from onetl.impl import FileExistBehavior, GenericOptions


class FileDownloaderOptions(GenericOptions):
    """File downloading options.

    !!! success "Added in 0.3.0"

    Examples
    --------

    ```python
    from onetl.file import FileDownloader

    options = FileDownloader.Options(
        if_exists="replace_entire_directory",
        delete_source=True,
        workers=4,
    )
    ```
    """

    if_exists: FileExistBehavior = Field(  # type: ignore[literal-required]
        default=FileExistBehavior.ERROR,
        alias=avoid_alias("mode"),
    )
    """
    How to handle existing files in the local directory.

    Possible values:

    * `error` (default) - mark file as failed
    * `ignore` - mark file as skipped
    * `replace_file` - replace existing file with a new one
    * `replace_entire_directory` - delete local directory content before downloading files

    !!! info "Changed in 0.9.0"
        Renamed `mode` → `if_exists`
    """

    delete_source: bool = False
    """
    If `True`, remove source file after successful download.

    If download failed, file will left intact.

    !!! success "Added in 0.2.0"

    !!! info "Changed in 0.3.0"
        Move `FileUploader.delete_local` to `FileUploaderOptions`
    """

    workers: int = Field(default=1, ge=1)
    """
    Number of workers to create for parallel file download.

    1 (default) means files will me downloaded sequentially.
    2 or more means files will be downloaded in parallel workers.

    Recommended value is `min(32, os.cpu_count() + 4)`, e.g. `5`.

    !!! success "Added in 0.8.1"
    """

    @model_validator(mode="before")
    @classmethod
    def _mode_is_deprecated(cls, values):
        if "mode" in values:
            warnings.warn(
                "Option `FileDownloader.Options(mode=...)` is deprecated since v0.9.0 and will be removed in v1.0.0. "
                "Use `FileDownloader.Options(if_exists=...)` instead",
                category=UserWarning,
                stacklevel=3,
            )
        return values
