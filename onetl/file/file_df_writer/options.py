# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager, nullcontext
from enum import Enum
from typing import TYPE_CHECKING, ContextManager, Iterable, List, Optional, Union

try:
    from pydantic.v1 import Field, root_validator
except (ImportError, AttributeError):
    from pydantic import Field, root_validator  # type: ignore[no-redef, assignment]

from onetl._util.spark import inject_spark_param
from onetl.base import FileDFWriteOptions
from onetl.hooks import slot, support_hooks
from onetl.impl import GenericOptions

if TYPE_CHECKING:
    from pyspark.sql import DataFrameWriter

PARTITION_OVERWRITE_MODE_PARAM = "spark.sql.sources.partitionOverwriteMode"


class FileDFExistBehavior(str, Enum):
    ERROR = "error"
    APPEND = "append"
    REPLACE_OVERLAPPING_PARTITIONS = "replace_overlapping_partitions"
    REPLACE_ENTIRE_DIRECTORY = "replace_entire_directory"
    SKIP_ENTIRE_DIRECTORY = "skip_entire_directory"

    def __str__(self) -> str:
        return str(self.value)


@support_hooks
class FileDFWriterOptions(FileDFWriteOptions, GenericOptions):
    """Options for :obj:`FileDFWriter <onetl.file.file_df_writer.file_df_writer.FileDFWriter>`.

    See `Spark Generic Load/Save Functions <https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html>`_
    documentation for more details.

    .. note::

        You can pass any value supported by Spark, even if it is not mentioned in this documentation.
        **Option names should be in** ``camelCase``!

        The set of supported options depends on Spark version. See link above.

    Examples
    --------
    Created writer options

    .. code:: python

        from onetl.file import FileDFWriter

        options = FileDFWriter.Options(
            partition_by="month",
            if_exists="replace_overlapping_partitions",
        )
    """

    class Config:
        extra = "allow"

    if_exists: FileDFExistBehavior = FileDFExistBehavior.APPEND
    """Behavior for existing target directory.

    If target directory does not exist, it will be created.
    But if it does exist, then behavior is different for each value.

    .. versionchanged:: 0.13.0

        Default value was changed from ``error`` to ``append``

    Possible values:
        * ``error``
            If folder already exists, raise an exception.

            Same as Spark's ``df.write.mode("error").save()``.

        * ``skip_entire_directory``
            If folder already exists, left existing files intact and stop immediately without any errors.

            Same as Spark's ``df.write.mode("ignore").save()``.

        * ``append`` (default)
            Appends data into existing directory.

            .. dropdown:: Behavior in details

                * Directory does not exist
                    Directory is created using all the provided options (``format``, ``partition_by``, etc).

                * Directory exists, does not contain partitions, but :obj:`~partition_by` is set
                    Data is appended to a directory, but to partitioned directory structure.

                    .. warning::

                        Existing files still present in the root of directory, but Spark will ignore those files while reading,
                        unless using ``recursive=True``.

                * Directory exists and contains partitions, but :obj:`~partition_by` is not set
                    Data is appended to a directory, but to the root of directory instead of nested partition directories.

                    .. warning::

                        Spark will ignore such files while reading, unless using ``recursive=True``.

                * Directory exists and contains partitions, but with different partitioning schema than :obj:`~partition_by`
                    Data is appended to a directory with new partitioning schema.

                    .. warning::

                        Spark cannot read directory with multiple partitioning schemas,
                        unless using ``recursive=True`` to disable partition scanning.

                * Directory exists and partitioned according :obj:`~partition_by`, but partition is present only in dataframe
                    New partition directory is created.

                * Directory exists and partitioned according :obj:`~partition_by`, partition is present in both dataframe and directory
                    New files are added to existing partition directory, existing files are sill present.

                * Directory exists and partitioned according :obj:`~partition_by`, but partition is present only in directory, not dataframe
                    Existing partition is left intact.

        * ``replace_overlapping_partitions``
            If partitions from dataframe already exist in directory structure, they will be overwritten.

            Same as Spark's ``df.write.mode("overwrite").save()`` +
            ``spark.sql.sources.partitionOverwriteMode=dynamic``.

            .. DANGER::

                This mode does make sense **ONLY** if the directory is partitioned.
                **IF NOT, YOU'LL LOOSE YOUR DATA!**

            .. dropdown:: Behavior in details

                * Directory does not exist
                    Directory is created using all the provided options (``format``, ``partition_by``, etc).

                * Directory exists, does not contain partitions, but :obj:`~partition_by` is set
                    Directory **will be deleted**, and will be created with partitions.

                * Directory exists and contains partitions, but :obj:`~partition_by` is not set
                    Directory **will be deleted**, and will be created with partitions.

                * Directory exists and contains partitions, but with different partitioning schema than :obj:`~partition_by`
                    Data is appended to a directory with new partitioning schema.

                    .. warning::

                        Spark cannot read directory with multiple partitioning schemas,
                        unless using ``recursive=True`` to disable partition scanning.

                * Directory exists and partitioned according :obj:`~partition_by`, but partition is present only in dataframe
                    New partition directory is created.

                * Directory exists and partitioned according :obj:`~partition_by`, partition is present in both dataframe and directory
                    Partition directory **will be deleted**, and new one is created with files containing data from dataframe.

                * Directory exists and partitioned according :obj:`~partition_by`, but partition is present only in directory, not dataframe
                    Existing partition is left intact.

        * ``replace_entire_directory``
            Remove existing directory and create new one, **overwriting all existing data**.
            **All existing partitions are dropped.**

            Same as Spark's ``df.write.mode("overwrite").save()`` +
            ``spark.sql.sources.partitionOverwriteMode=static``.

    .. note::

        Unlike using pure Spark, config option ``spark.sql.sources.partitionOverwriteMode``
        does not affect behavior of any ``mode``
    """

    partition_by: Optional[Union[List[str], str]] = Field(default=None, alias="partitionBy")
    """
    List of columns should be used for data partitioning. ``None`` means partitioning is disabled.

    Each partition is a folder which contains only files with the specific column value,
    like ``some.csv/col1=value1``, ``some.csv/col1=value2``, and so on.

    Multiple partitions columns means nested folder structure, like ``some.csv/col1=val1/col2=val2``.

    If ``WHERE`` clause in the query contains expression like ``partition = value``,
    Spark will scan only files in a specific partition.

    Examples: ``reg_id`` or ``["reg_id", "business_dt"]``

    .. note::

        Values should be scalars (integers, strings),
        and either static (``countryId``) or incrementing (dates, years), with low
        number of distinct values.

        Columns like ``userId`` or ``datetime``/``timestamp`` should **NOT** be used for partitioning.
    """

    @slot
    @contextmanager
    def apply_to_writer(self, writer: DataFrameWriter) -> Iterator[DataFrameWriter]:
        """
        Apply provided format to :obj:`pyspark.sql.DataFrameWriter`. |support_hooks|

        Returns
        -------
        :obj:`pyspark.sql.DataFrameWriter` with options applied
        """
        from pyspark.sql import SparkSession

        for method, value in self.dict(by_alias=True, exclude_none=True, exclude={"if_exists"}).items():
            # <value> is the arguments that will be passed to the <method>
            # format orc, parquet methods and format simultaneously
            if hasattr(writer, method):
                if isinstance(value, Iterable) and not isinstance(value, str):
                    writer = getattr(writer, method)(*value)  # noqa: WPS220
                else:
                    writer = getattr(writer, method)(value)  # noqa: WPS220
            else:
                writer = writer.option(method, value)

        context: ContextManager
        spark: SparkSession
        if isinstance(writer._spark, SparkSession):  # noqa: WPS437
            spark = writer._spark  # noqa: WPS437
        else:
            # In Spark 2, `writer._spark` is not a SparkSession but SQLContext
            # Using some nasty hack to get current SparkSession
            spark = SparkSession._instantiatedSession  # noqa: WPS437 # type: ignore[error]

        context = nullcontext()

        if self.if_exists == FileDFExistBehavior.REPLACE_OVERLAPPING_PARTITIONS:
            context = inject_spark_param(spark.conf, PARTITION_OVERWRITE_MODE_PARAM, "dynamic")  # noqa: WPS437
        elif self.if_exists == FileDFExistBehavior.REPLACE_ENTIRE_DIRECTORY:
            context = inject_spark_param(spark.conf, PARTITION_OVERWRITE_MODE_PARAM, "static")  # noqa: WPS437

        mode = self.if_exists.value
        if self.if_exists in {
            FileDFExistBehavior.REPLACE_OVERLAPPING_PARTITIONS,
            FileDFExistBehavior.REPLACE_ENTIRE_DIRECTORY,
        }:
            mode = "overwrite"
        elif self.if_exists == FileDFExistBehavior.SKIP_ENTIRE_DIRECTORY:
            mode = "ignore"

        with context:
            yield writer.mode(mode)

    @root_validator(pre=True)
    def _mode_is_restricted(cls, values):
        if "mode" in values:
            raise ValueError("Parameter `mode` is not allowed. Please use `if_exists` parameter instead.")
        return values

    @root_validator
    def _partition_overwrite_mode_is_not_allowed(cls, values):
        partition_overwrite_mode = values.get("partitionOverwriteMode") or values.get("partition_overwrite_mode")
        if partition_overwrite_mode:
            if partition_overwrite_mode == "static":
                recommended_mode = "replace_entire_directory"
            else:
                recommended_mode = "replace_overlapping_partitions"

            raise ValueError(
                f"`partitionOverwriteMode` option should be replaced with if_exists='{recommended_mode}'",
            )
        return values
