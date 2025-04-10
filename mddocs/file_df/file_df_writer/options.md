<a id="file-df-writer-options"></a>

# Options

### *class* onetl.file.file_df_writer.options.FileDFWriterOptions(\*, if_exists: FileDFExistBehavior = FileDFExistBehavior.APPEND, partitionBy: List[str] | str | None = None, \*\*kwargs)

Options for [`FileDFWriter`](file_df_writer.md#onetl.file.file_df_writer.file_df_writer.FileDFWriter).

#### Versionadded
Added in version 0.9.0.

### Examples

#### NOTE
You can pass any value [supported by Spark](https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html),
even if it is not mentioned in this documentation. **Option names should be in** `camelCase`!

The set of supported options depends on Spark version.

```python
from onetl.file import FileDFWriter

options = FileDFWriter.Options(
    if_exists="replace_overlapping_partitions",
    partitionBy="month",
)
```

<!-- !! processed by numpydoc !! -->

#### *field* if_exists *: FileDFExistBehavior* *= FileDFExistBehavior.APPEND*

Behavior for existing target directory.

If target directory does not exist, it will be created.
But if it does exist, then behavior is different for each value.

#### Versionchanged
Changed in version 0.13.0: Default value was changed from `error` to `append`

Possible values:
: * `error`
    : If folder already exists, raise an exception.
      <br/>
      Same as Spark’s `df.write.mode("error").save()`.
  * `skip_entire_directory`
    : If folder already exists, left existing files intact and stop immediately without any errors.
      <br/>
      Same as Spark’s `df.write.mode("ignore").save()`.
  * `append` (default)
    : Appends data into existing directory.
      <br/>
      ### Behavior in details
      <br/>
      * Directory does not exist
        : Directory is created using all the provided options (`format`, `partition_by`, etc).
      * Directory exists, does not contain partitions, but [`partition_by`](#onetl.file.file_df_writer.options.FileDFWriterOptions.partition_by) is set
        : Data is appended to a directory, but to partitioned directory structure.
          <br/>
          #### WARNING
          Existing files still present in the root of directory, but Spark will ignore those files while reading,
          unless using `recursive=True`.
      * Directory exists and contains partitions, but [`partition_by`](#onetl.file.file_df_writer.options.FileDFWriterOptions.partition_by) is not set
        : Data is appended to a directory, but to the root of directory instead of nested partition directories.
          <br/>
          #### WARNING
          Spark will ignore such files while reading, unless using `recursive=True`.
      * Directory exists and contains partitions, but with different partitioning schema than [`partition_by`](#onetl.file.file_df_writer.options.FileDFWriterOptions.partition_by)
        : Data is appended to a directory with new partitioning schema.
          <br/>
          #### WARNING
          Spark cannot read directory with multiple partitioning schemas,
          unless using `recursive=True` to disable partition scanning.
      * Directory exists and partitioned according [`partition_by`](#onetl.file.file_df_writer.options.FileDFWriterOptions.partition_by), but partition is present only in dataframe
        : New partition directory is created.
      * Directory exists and partitioned according [`partition_by`](#onetl.file.file_df_writer.options.FileDFWriterOptions.partition_by), partition is present in both dataframe and directory
        : New files are added to existing partition directory, existing files are sill present.
      * Directory exists and partitioned according [`partition_by`](#onetl.file.file_df_writer.options.FileDFWriterOptions.partition_by), but partition is present only in directory, not dataframe
        : Existing partition is left intact.
  * `replace_overlapping_partitions`
    : If partitions from dataframe already exist in directory structure, they will be overwritten.
      <br/>
      Same as Spark’s `df.write.mode("overwrite").save()` +
      `spark.sql.sources.partitionOverwriteMode=dynamic`.
      <br/>
      ### Behavior in details
      <br/>
      * Directory does not exist
        : Directory is created using all the provided options (`format`, `partition_by`, etc).
      * Directory exists, does not contain partitions, but [`partition_by`](#onetl.file.file_df_writer.options.FileDFWriterOptions.partition_by) is set
        : Directory **will be deleted**, and will be created with partitions.
      * Directory exists and contains partitions, but [`partition_by`](#onetl.file.file_df_writer.options.FileDFWriterOptions.partition_by) is not set
        : Directory **will be deleted**, and will be created with partitions.
      * Directory exists and contains partitions, but with different partitioning schema than [`partition_by`](#onetl.file.file_df_writer.options.FileDFWriterOptions.partition_by)
        : Data is appended to a directory with new partitioning schema.
          <br/>
          #### WARNING
          Spark cannot read directory with multiple partitioning schemas,
          unless using `recursive=True` to disable partition scanning.
      * Directory exists and partitioned according [`partition_by`](#onetl.file.file_df_writer.options.FileDFWriterOptions.partition_by), but partition is present only in dataframe
        : New partition directory is created.
      * Directory exists and partitioned according [`partition_by`](#onetl.file.file_df_writer.options.FileDFWriterOptions.partition_by), partition is present in both dataframe and directory
        : Partition directory **will be deleted**, and new one is created with files containing data from dataframe.
      * Directory exists and partitioned according [`partition_by`](#onetl.file.file_df_writer.options.FileDFWriterOptions.partition_by), but partition is present only in directory, not dataframe
        : Existing partition is left intact.
  * `replace_entire_directory`
    : Remove existing directory and create new one, **overwriting all existing data**.
      **All existing partitions are dropped.**
      <br/>
      Same as Spark’s `df.write.mode("overwrite").save()` +
      `spark.sql.sources.partitionOverwriteMode=static`.

#### NOTE
Unlike using pure Spark, config option `spark.sql.sources.partitionOverwriteMode`
does not affect behavior of any `mode`

<!-- !! processed by numpydoc !! -->

#### *field* partition_by *: List[str] | str | None* *= None* *(alias 'partitionBy')*

List of columns should be used for data partitioning. `None` means partitioning is disabled.

Each partition is a folder which contains only files with the specific column value,
like `some.csv/col1=value1`, `some.csv/col1=value2`, and so on.

Multiple partitions columns means nested folder structure, like `some.csv/col1=val1/col2=val2`.

If `WHERE` clause in the query contains expression like `partition = value`,
Spark will scan only files in a specific partition.

Examples: `reg_id` or `["reg_id", "business_dt"]`

#### NOTE
Values should be scalars (integers, strings),
and either static (`countryId`) or incrementing (dates, years), with low
number of distinct values.

Columns like `userId` or `datetime`/`timestamp` should **NOT** be used for partitioning.

<!-- !! processed by numpydoc !! -->
