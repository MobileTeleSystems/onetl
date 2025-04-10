<a id="orc-file-format"></a>

# ORC

### *class* onetl.file.format.orc.ORC(\*, mergeSchema: bool | None = None, compression: str | Literal['uncompressed', 'snappy', 'zlib', 'lzo', 'zstd', 'lz4'] | None = None, \*\*kwargs)

ORC file format (columnar). [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

Based on [Spark ORC Files](https://spark.apache.org/docs/latest/sql-data-sources-orc.html) file format.

Supports reading/writing files with `.orc` extension.

#### Versionadded
Added in version 0.9.0.

### Examples

#### NOTE
You can pass any option mentioned in
[official documentation](https://spark.apache.org/docs/latest/sql-data-sources-orc.html).
**Option names should be in** `camelCase`!

The set of supported options depends on Spark version.

You may also set options mentioned [orc-java documentation](https://orc.apache.org/docs/core-java-config.html).
They are prefixed with `orc.` with dots in names, so instead of calling constructor `ORC(orc.option=True)` (invalid in Python)
you should call method `ORC.parse({"orc.option": True})`.

Reading files

```py
from onetl.file.format import ORC

orc = ORC(mergeSchema=True)
```

Writing files

```python
from onetl.file.format import ORC

orc = ORC.parse(
    {
        "compression": "snappy",
        # Enable Bloom filter for columns 'id' and 'name'
        "orc.bloom.filter.columns": "id,name",
        # Set Bloom filter false positive probability
        "orc.bloom.filter.fpp": 0.01,
        # Do not use dictionary for 'highly_selective_column'
        "orc.column.encoding.direct": "highly_selective_column",
        # other options
    }
)
```

<!-- !! processed by numpydoc !! -->

#### *field* mergeSchema *: bool | None* *= None*

Merge schemas of all ORC files being read into a single schema.
By default, Spark config option `spark.sql.orc.mergeSchema` value is used (`False`).

#### NOTE
Used only for reading files.

<!-- !! processed by numpydoc !! -->

#### *field* compression *: str | Literal['uncompressed', 'snappy', 'zlib', 'lzo', 'zstd', 'lz4'] | None* *= None*

Compression codec of the ORC files.
By default, Spark config option `spark.sql.orc.compression.codec` value is used (`snappy`).

#### NOTE
Used only for writing files.

<!-- !! processed by numpydoc !! -->

#### \_\_init_\_(\*\*kwargs)

<!-- !! processed by numpydoc !! -->
