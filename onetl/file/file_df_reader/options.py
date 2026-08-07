# SPDX-FileCopyrightText: 2023-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from typing import TYPE_CHECKING

from pydantic import ConfigDict, Field

from onetl.base import FileDFReadOptions
from onetl.hooks import slot, support_hooks
from onetl.impl import GenericOptions

if TYPE_CHECKING:
    from pyspark.sql import DataFrameReader


@support_hooks
class FileDFReaderOptions(FileDFReadOptions, GenericOptions):
    """Options for [FileDFReader][onetl.file.file_df_reader.file_df_reader.FileDFReader].

    !!! success "Added in 0.9.0"

    Examples
    --------

    !!! note

        You can pass any value [supported by Spark](https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html),
        even if it is not mentioned in this documentation. **Option names should be in** `camelCase`!

        The set of supported options depends on Spark version.

    ```python
    from onetl.file import FileDFReader

    options = FileDFReader.Options(recursive=True)
    ```
    """

    model_config = ConfigDict(extra="allow")

    recursive: bool | None = Field(default=None, alias="recursiveFileLookup")
    """If `True`, perform recursive file lookup.

    !!! warning

        This disables partition inferring using file paths.

    !!! warning

        Can be used only in Spark 3+. See [SPARK-27990](https://issues.apache.org/jira/browse/SPARK-27990).
    """

    @slot
    def apply_to_reader(self, reader: "DataFrameReader") -> "DataFrameReader":
        """
        Apply provided format to `pyspark.sql.DataFrameReader`. [![support hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](/hooks/)

        Returns
        -------
        pyspark.sql.DataFrameReader
            Reader with options applied.
        """
        options = self.model_dump(by_alias=True, exclude_none=True)
        return reader.options(**options)
