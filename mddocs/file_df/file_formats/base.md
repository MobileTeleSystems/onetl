<a id="base-file-format"></a>

# Base interface

### *class* onetl.base.base_file_format.BaseReadableFileFormat

Representation of readable file format.

#### Versionadded
Added in version 0.9.0.

<!-- !! processed by numpydoc !! -->

#### *abstract* check_if_supported(spark: SparkSession) → None

Check if Spark session does support this file format. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

#### Versionadded
Added in version 0.9.0.

* **Raises:**
  RuntimeError
  : If file format is not supported.

<!-- !! processed by numpydoc !! -->

#### *abstract* apply_to_reader(reader: DataFrameReader) → DataFrameReader

Apply provided format to `pyspark.sql.DataFrameReader`. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

#### Versionadded
Added in version 0.9.0.

* **Returns:**
  `pyspark.sql.DataFrameReader`
  : DataFrameReader with options applied.

<!-- !! processed by numpydoc !! -->

### *class* onetl.base.base_file_format.BaseWritableFileFormat

Representation of writable file format.

#### Versionadded
Added in version 0.9.0.

<!-- !! processed by numpydoc !! -->

#### *abstract* check_if_supported(spark: SparkSession) → None

Check if Spark session does support this file format. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

#### Versionadded
Added in version 0.9.0.

* **Raises:**
  RuntimeError
  : If file format is not supported.

<!-- !! processed by numpydoc !! -->

#### *abstract* apply_to_writer(writer: DataFrameWriter) → DataFrameWriter

Apply provided format to `pyspark.sql.DataFrameWriter`. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

#### Versionadded
Added in version 0.9.0.

* **Returns:**
  `pyspark.sql.DataFrameWriter`
  : DataFrameWriter with options applied.

<!-- !! processed by numpydoc !! -->
