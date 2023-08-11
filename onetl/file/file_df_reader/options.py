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

from typing import TYPE_CHECKING, Optional

from pydantic import Field, validator

from onetl._util.spark import get_pyspark_version
from onetl.base import FileDFReadOptions
from onetl.hooks import slot, support_hooks
from onetl.impl import GenericOptions

if TYPE_CHECKING:
    from pyspark.sql import DataFrameReader


@support_hooks
class FileDFReaderOptions(FileDFReadOptions, GenericOptions):
    """Options for :obj:`FileDFReader <onetl.file.file_df_reader.file_df_reader.FileDFReader>`.

    See `Spark Generic File Data Source <https://spark.apache.org/docs/latest/sql-data-sources-generic-options.html>`_
    documentation for more details.

    .. note::

        You can pass any value supported by Spark, even if it is not mentioned in this documentation.
        **Option names should be in** ``camelCase``!

        The set of supported options depends on Spark version. See link above.

    Examples
    --------
    Created reader options

    .. code:: python

        from onetl.file import FileDFReader

        options = FileDFReader.Options(recursive=True)
    """

    class Config:
        extra = "allow"

    recursive: Optional[bool] = Field(default=None, alias="recursiveFileLookup")
    """If ``True``, perform recursive file lookup.

    .. warning::

        This disables partition inferring using file paths.

    .. warning::

        Can be used only in Spark 3+. See `SPARK-27990 <https://issues.apache.org/jira/browse/SPARK-27990>`_.
    """

    @slot
    def apply_to_reader(self, reader: DataFrameReader) -> DataFrameReader:
        """
        Apply provided format to :obj:`pyspark.sql.DataFrameReader`. |support_hooks|

        Returns
        -------
        :obj:`pyspark.sql.DataFrameReader` with options applied
        """
        options = self.dict(by_alias=True, exclude_none=True)
        return reader.options(**options)

    @validator("recursive")
    def _validate_spark_version(cls, value):
        pyspark_version = get_pyspark_version()
        if pyspark_version.major < 3:
            raise RuntimeError(f"Option `recursive` can be used only in Spark 3+, got {pyspark_version}")
        return value
