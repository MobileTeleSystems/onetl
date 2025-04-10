<a id="snapshot-batch-strategy"></a>

# Snapshot Batch Strategy

### *class* onetl.strategy.snapshot_strategy.SnapshotBatchStrategy(\*, hwm: HWM | None = None, step: Any = None, start: Any = None, stop: Any = None)

Snapshot batch strategy for [DB Reader](../db/db_reader.md#db-reader).

#### NOTE
Cannot be used with [File Downloader](../file/file_downloader/file_downloader.md#file-downloader)

Same as [`SnapshotStrategy`](snapshot_strategy.md#onetl.strategy.snapshot_strategy.SnapshotStrategy),
but reads data from the source in sequential batches (1..N) like:

```sql
1:  SELECT id, data
    FROM public.mydata
    WHERE id >= 1000 AND id <= 1100; -- from start to start+step (INCLUDING first row)

2:  WHERE id > 1100 AND id <= 1200; -- + step
3:  WHERE id > 1200 AND id <= 1200; -- + step
N:  WHERE id > 1300 AND id <= 1400; -- until stop
```

This allows to use less CPU and RAM on Spark cluster than reading all the data in parallel,
but takes proportionally more time.

#### NOTE
This strategy uses HWM column value to filter data for each batch,
but does **NOT** save it into [HWM Store](../hwm_store/index.md#hwm).
So every run starts from the beginning, not from the previous HWM value.

#### NOTE
If you only need to reduce number of rows read by Spark from opened cursor,
use `onetl.connection.db_connection.postgres.Postgres.ReadOptions.fetchsize` instead

#### WARNING
Not every [DB connection](../connection/db_connection/index.md#db-connections)
supports batch strategy. For example, Kafka connection doesn’t support it.
Make sure the connection you use is compatible with the SnapshotBatchStrategy.

#### Versionadded
Added in version 0.1.0.

* **Parameters:**
  **step**
  : Step size used for generating batch SQL queries like:
    ```sql
    SELECT id, data
    FROM public.mydata
    WHERE id >= 1000 AND id <= 1100; -- 1000 is start value, step is 100
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

  **start**
  : If passed, the value will be used for generating WHERE clauses with `hwm.expression` filter,
    as a start value for the first batch.
    <br/>
    If not set, the value is determined by a separated query:
    ```sql
    SELECT MIN(id) as start
    FROM public.mydata
    WHERE id <= 1400; -- 1400 here is stop value (if set)
    ```
    <br/>
    #### NOTE
    `start` should be the same type as `hwm.expression` value,
    e.g. `datetime.datetime` for `TIMESTAMP` column, `datetime.date` for `DATE`, and so on

  **stop**
  : If passed, the value will be used for generating WHERE clauses with `hwm.expression` filter,
    as a stop value for the last batch.
    <br/>
    If not set, the value is determined by a separated query:
    ```sql
    SELECT MAX(id) as stop
    FROM public.mydata
    WHERE id >= 1000; -- 1000 here is start value (if set)
    ```
    <br/>
    #### NOTE
    `stop` should be the same type as `hwm.expression` value,
    e.g. `datetime.datetime` for `TIMESTAMP` column, `datetime.date` for `DATE`, and so on

### Examples

SnapshotBatch run

```python
from onetl.db import DBReader, DBWriter
from onetl.strategy import SnapshotBatchStrategy

reader = DBReader(
    connection=postgres,
    source="public.mydata",
    columns=["id", "data"],
    hwm=DBReader.AutoDetectHWM(name="some_hwm_name", expression="id"),
)

writer = DBWriter(connection=hive, target="db.newtable")

with SnapshotBatchStrategy(step=100) as batches:
    for _ in batches:
        df = reader.run()
        writer.run(df)
```

```sql
-- get start and stop values

    SELECT MIN(id) as start, MAX(id) as stop
    FROM public.mydata;

-- for example, start=1000 and stop=2345

-- when each batch (1..N) will perform a query which return some part of input data

1:  SELECT id, data
    FROM public.mydata
    WHERE id >= 1000 AND id <= 1100; -- from start to start+step (INCLUDING first row)

2:  WHERE id > 1100 AND id <= 1200; -- + step
3:  WHERE id > 1200 AND id <= 1300; -- + step
N:  WHERE id > 2300 AND id <= 2345; -- until stop
```

SnapshotBatch run with `stop` value

```python
...

with SnapshotBatchStrategy(step=100, stop=1234) as batches:
    for _ in batches:
        df = reader.run()
        writer.run(df)
```

```sql
-- stop value is set, so there is no need to fetch it from DB
-- get start value

    SELECT MIN(id) as start
    FROM public.mydata
    WHERE id <= 1234; -- until stop

-- for example, start=1000.
-- when each batch (1..N) will perform a query which return some part of input data

1:  SELECT id, data
    FROM public.mydata
    WHERE id >= 1000 AND id <= 1100; -- from start to start+step (INCLUDING first row)

2:  WHERE id >  1100 AND id <= 1200; -- + step
3:  WHERE id >  1200 AND id <= 1300; -- + step
N:  WHERE id >  1300 AND id <= 1234; -- until stop
```

SnapshotBatch run with `start` value

```python
...

with SnapshotBatchStrategy(step=100, start=500) as batches:
    for _ in batches:
        df = reader.run()
        writer.run(df)
```

```sql
-- start value is set, so there is no need to fetch it from DB
-- get only stop value

    SELECT MAX(id) as stop
    FROM public.mydata
    WHERE id >= 500; -- from start

-- for example, stop=2345.
-- when each batch (1..N) will perform a query which return some part of input data

1:  SELECT id, data
    FROM public.mydata
    WHERE id >= 500 AND id <=  600; -- from start to start+step (INCLUDING first row)

2:  WHERE id >  600 AND id <=  700; -- + step
3:  WHERE id >  700 AND id <=  800; -- + step
...
N:  WHERE id > 2300 AND id <= 2345; -- until stop
```

SnapshotBatch run with all options

```python
...

with SnapshotBatchStrategy(
    start=1000,
    step=100,
    stop=2000,
) as batches:
    for _ in batches:
        df = reader.run()
        writer.run(df)
```

```sql
-- start and stop values are set, so no need to fetch boundaries from DB
-- each batch (1..N) will perform a query which return some part of input data

1:    SELECT id, data
    FROM public.mydata
    WHERE id >= 1000 AND id <= 1100; -- from start to start+step (INCLUDING first row)

2:  WHERE id >  1100 AND id <= 1200; -- + step
3:  WHERE id >  1200 AND id <= 1300; -- + step
...
N:  WHERE id >  1900 AND id <= 2000; -- until stop
```

SnapshotBatch run over non-integer column

`hwm.expression`, `start` and `stop` can be a date or datetime, not only integer:

```python
from datetime import date, timedelta

reader = DBReader(
    connection=postgres,
    source="public.mydata",
    columns=["business_dt", "data"],
    hwm=DBReader.AutoDetectHWM(name="some_hwm_name", expression="business_dt"),
)

with SnapshotBatchStrategy(
    start=date("2021-01-01"),
    step=timedelta(days=5),
    stop=date("2021-01-31"),
) as batches:
    for _ in batches:
        df = reader.run()
        writer.run(df)
```

```sql
-- start and stop values are set, so no need to fetch boundaries from DB
-- each batch will perform a query which return some part of input data
-- HWM value will casted to match column type

1:  SELECT business_dt, data
    FROM public.mydata
    WHERE business_dt >= CAST('2020-01-01' AS DATE) -- from start to start+step (INCLUDING first row)
    AND   business_dt <= CAST('2021-01-05' AS DATE);

2:  WHERE business_dt >  CAST('2021-01-05' AS DATE) -- + step
    AND   business_dt <= CAST('2021-01-10' AS DATE);

3:  WHERE business_dt >  CAST('2021-01-10' AS DATE) -- + step
    AND   business_dt <= CAST('2021-01-15' AS DATE);

...

N:  WHERE business_dt >  CAST('2021-01-30' AS DATE)
    AND   business_dt <= CAST('2021-01-31' AS DATE); -- until stop
```

<!-- !! processed by numpydoc !! -->

#### \_\_init_\_(\*\*kwargs)

Create a new model by parsing and validating input data from keyword arguments.

Raises ValidationError if the input data cannot be parsed to form a valid model.

<!-- !! processed by numpydoc !! -->
