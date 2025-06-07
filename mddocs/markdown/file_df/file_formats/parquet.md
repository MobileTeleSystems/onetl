<a id="parquet-file-format"></a>

# Parquet

### *class* onetl.file.format.parquet.Parquet(\*, mergeSchema: bool | None = None, compression: str | Literal['uncompressed', 'snappy', 'gzip', 'lzo', 'brotli', 'lz4', 'lz4raw', 'zstd'] | None = None, \*\*kwargs)

Parquet file format (columnar). [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

Based on [Spark Parquet Files](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html) file format.

Supports reading/writing files with `.parquet` extension.

#### Versionadded
Added in version 0.9.0.

### Examples

#### NOTE
You can pass any option mentioned in
[official documentation](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html).
**Option names should be in** `camelCase`!

The set of supported options depends on Spark version.

You may also set options mentioned [parquet-hadoop documentation](https://github.com/apache/parquet-java/blob/master/parquet-hadoop/README.md).
They are prefixed with `parquet.` with dots in names, so instead of calling constructor `Parquet(parquet.option=True)` (invalid in Python)
you should call method `Parquet.parse({"parquet.option": True})`.

Reading files

```py
from onetl.file.format import Parquet

parquet = Parquet(mergeSchema=True)
```

Writing files

```py
from onetl.file.format import Parquet

parquet = Parquet.parse(
    {
        "compression": "snappy",
        # Enable Bloom filter for columns 'id' and 'name'
        "parquet.bloom.filter.enabled#id": True,
        "parquet.bloom.filter.enabled#name": True,
        # Set expected number of distinct values for column 'id'
        "parquet.bloom.filter.expected.ndv#id": 10_000_000,
        # other options
    }
)
```

<!-- !! processed by numpydoc !! -->

#### *field* mergeSchema *: bool | None* *= None*

Merge schemas of all Parquet files being read into a single schema.
By default, Spark config option `spark.sql.parquet.mergeSchema` value is used (`false`).

#### NOTE
Used only for reading files.

<!-- !! processed by numpydoc !! -->

#### *field* compression *: str | Literal['uncompressed', 'snappy', 'gzip', 'lzo', 'brotli', 'lz4', 'lz4raw', 'zstd'] | None* *= None*

Compression codec of the Parquet files.
By default, Spark config option `spark.sql.parquet.compression.codec` value is used (`snappy`).

#### NOTE
Used only for writing files.

<!-- !! processed by numpydoc !! -->

#### \_\_init_\_(\*\*kwargs)

<!-- !! processed by numpydoc !! -->
