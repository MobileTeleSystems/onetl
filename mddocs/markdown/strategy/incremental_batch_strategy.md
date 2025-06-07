<a id="incremental-batch-strategy"></a>

# Incremental Batch Strategy

### *class* onetl.strategy.incremental_strategy.IncrementalBatchStrategy(\*, hwm: HWM | None = None, step: Any = None, start: Any = None, stop: Any = None, offset: Any = None)

Incremental batch strategy for [DB Reader](../db/db_reader.md#db-reader).

#### NOTE
Cannot be used with [File Downloader](../file/file_downloader/file_downloader.md#file-downloader)

Same as [`IncrementalStrategy`](incremental_strategy.md#onetl.strategy.incremental_strategy.IncrementalStrategy),
but reads data from the source in sequential batches (1..N) like:

```sql
1: SELECT id, data
   FROM public.mydata
   WHERE id > 1000 AND id <= 1100; -- previous HWM value is 1000, step is 100

2: WHERE id > 1100 AND id <= 1200; -- + step
3: WHERE id > 1200 AND id <= 1200; -- + step
N: WHERE id > 1300 AND id <= 1400; -- until stop
```

This allows to use less CPU and RAM than reading all the data in the one batch,
but takes proportionally more time.

#### WARNING
Unlike [`SnapshotBatchStrategy`](snapshot_batch_strategy.md#onetl.strategy.snapshot_strategy.SnapshotBatchStrategy),
it **saves** current HWM value after **each batch** into [HWM Store](../hwm_store/index.md#hwm).

So if code inside the context manager raised an exception, like:

```python
with IncrementalBatchStrategy() as batches:
    for _ in batches:
        df = reader.run()  # something went wrong here
        writer.run(df)  # or here
        # or here...
```

DBReader will **NOT** update HWM in HWM Store for the failed batch.

All of that allows to resume reading process from the *last successful batch*.

#### WARNING
Not every [DB connection](../connection/db_connection/index.md#db-connections)
supports batch strategy. For example, Kafka connection doesn’t support it.
Make sure the connection you use is compatible with the IncrementalBatchStrategy.

#### Versionadded
Added in version 0.1.0.

* **Parameters:**
  **step**
  : Step size used for generating batch SQL queries like:
    ```sql
    SELECT id, data
    FROM public.mydata
    WHERE id > 1000 AND id <= 1100; -- 1000 is previous HWM value, step is 100
    ```
    <br/>
    #### NOTE
    Step defines a range of values will be fetched by each batch. This is **not**
    a number of rows, it depends on a table content and value distribution across the rows.
    <br/>
    #### NOTE
    `step` value will be added to the HWM, so it should have a proper type.
    <br/>
    For example, for `TIMESTAMP` column `step` type should be `datetime.timedelta`, not `int`

  **stop**
  : If passed, the value will be used for generating WHERE clauses with `hwm.expression` filter,
    as a stop value for the last batch.
    <br/>
    If not set, the value is determined by a separated query:
    ```sql
    SELECT MAX(id) as stop
    FROM public.mydata
    WHERE id > 1000; -- 1000 is previous HWM value (if any)
    ```
    <br/>
    #### NOTE
    `stop` should be the same type as `hwm.expression` value,
    e.g. `datetime.datetime` for `TIMESTAMP` column, `datetime.date` for `DATE`, and so on

  **offset**
  : If passed, the offset value will be used to read rows which appeared in the source after the previous read.
    <br/>
    For example, previous incremental run returned rows:
    ```default
    898
    899
    900
    1000
    ```
    <br/>
    Current HWM value is 1000.
    <br/>
    But since then few more rows appeared in the source:
    ```default
    898
    899
    900
    901 # new
    902 # new
    ...
    999 # new
    1000
    ```
    <br/>
    and you need to read them too.
    <br/>
    So you can set `offset=100`, so the first batch of a next incremental run will look like:
    ```sql
    SELECT id, data
    FROM public.mydata
    WHERE id > 900 AND id <= 1000; -- 900 = 1000 - 100 = HWM - offset
    ```
    <br/>
    and return rows from 901 (**not** 900) to **1000** (duplicate).
    <br/>
    #### WARNING
    This can lead to reading duplicated values from the table.
    You probably need additional deduplication step to handle them
    <br/>
    #### NOTE
    `offset` value will be subtracted from the HWM, so it should have a proper type.
    <br/>
    For example, for `TIMESTAMP` column `offset` type should be `datetime.timedelta`, not `int`

### Examples

IncrementalBatch run

```python
from onetl.db import DBReader, DBWriter
from onetl.strategy import IncrementalBatchStrategy

reader = DBReader(
    connection=postgres,
    source="public.mydata",
    columns=["id", "data"],
    hwm=DBReader.AutoDetectHWM(name="some_hwm_name", expression="id"),
)

writer = DBWriter(connection=hive, target="db.newtable")

with IncrementalBatchStrategy(step=100) as batches:
    for _ in batches:
        df = reader.run()
        writer.run(df)
```

```sql
-- previous HWM value was 1000
-- each batch (1..N) will perform a query which return some part of input data

1:  SELECT id, data
    FROM public.mydata
    WHERE id > 1100 AND id <= 1200; --- from HWM to HWM+step (EXCLUDING first row)

2:  WHERE id > 1200 AND id <= 1300; -- + step
N:  WHERE id > 1300 AND id <= 1400; -- until max value of HWM column
```

IncrementalBatch run with `stop` value

```python
...

with IncrementalBatchStrategy(step=100, stop=2000) as batches:
    for _ in batches:
        df = reader.run()
        writer.run(df)
```

```sql
-- previous HWM value was 1000
-- each batch (1..N) will perform a query which return some part of input data

1:  SELECT id, data
    FROM public.mydata
    WHERE id > 1000 AND id <= 1100; --- from HWM to HWM+step (EXCLUDING first row)

2:  WHERE id > 1100 AND id <= 1200; -- + step
...
N:  WHERE id > 1900 AND id <= 2000; -- until stop
```

IncrementalBatch run with `offset` value

```python
...

with IncrementalBatchStrategy(step=100, offset=100) as batches:
    for _ in batches:
        df = reader.run()
        writer.run(df)
```

```sql
-- previous HWM value was 1000
-- each batch (1..N) will perform a query which return some part of input data

1:  SELECT id, data
    FROM public.mydata
    WHERE id >  900 AND id <= 1000; --- from HWM-offset to HWM-offset+step (EXCLUDING first row)

2:  WHERE id > 1000 AND id <= 1100; -- + step
3:  WHERE id > 1100 AND id <= 1200; -- + step
...
N:  WHERE id > 1300 AND id <= 1400; -- until max value of HWM column
```

IncrementalBatch run with all possible options

```python
...

with IncrementalBatchStrategy(
    step=100,
    stop=2000,
    offset=100,
) as batches:
    for _ in batches:
        df = reader.run()
        writer.run(df)
```

```sql
-- previous HWM value was 1000
-- each batch (1..N) will perform a query which return some part of input data

1:  SELECT id, data
    FROM public.mydata
    WHERE id > 900 AND id <= 1000; --- from HWM-offset to HWM-offset+step (EXCLUDING first row)

2:  WHERE id > 1000 AND id <= 1100; -- + step
3:  WHERE id > 1100 AND id <= 1200; -- + step
...
N:  WHERE id > 1900 AND id <= 2000; -- until stop
```

IncrementalBatch run over non-integer column

`hwm.expression`, `offset` and `stop` can be a date or datetime, not only integer:

```python
from onetl.db import DBReader, DBWriter
from datetime import date, timedelta

reader = DBReader(
    connection=postgres,
    source="public.mydata",
    columns=["business_dt", "data"],
    hwm=DBReader.AutoDetectHWM(name="some_hwm_name", expression="business_dt"),
)

writer = DBWriter(connection=hive, target="db.newtable")

with IncrementalBatchStrategy(
    step=timedelta(days=5),
    stop=date("2021-01-31"),
    offset=timedelta(days=1),
) as batches:
    for _ in batches:
        df = reader.run()
        writer.run(df)
```

```sql
-- previous HWM value was '2021-01-10'
-- each batch (1..N) will perform a query which return some part of input data

1:  SELECT business_dt, data
    FROM public.mydata
    WHERE business_dt  > CAST('2021-01-09' AS DATE)  -- from HWM-offset (EXCLUDING first row)
    AND   business_dt <= CAST('2021-01-14' AS DATE); -- to HWM-offset+step

2:  WHERE business_dt  > CAST('2021-01-14' AS DATE) -- + step
    AND   business_dt <= CAST('2021-01-19' AS DATE);

3:  WHERE business_dt  > CAST('2021-01-19' AS DATE) -- + step
    AND   business_dt <= CAST('2021-01-24' AS DATE);

...

N:  WHERE business_dt  > CAST('2021-01-29' AS DATE)
    AND   business_dt <= CAST('2021-01-31' AS DATE); -- until stop
```

<!-- !! processed by numpydoc !! -->

#### \_\_init_\_(\*\*kwargs)

Create a new model by parsing and validating input data from keyword arguments.

Raises ValidationError if the input data cannot be parsed to form a valid model.

<!-- !! processed by numpydoc !! -->
