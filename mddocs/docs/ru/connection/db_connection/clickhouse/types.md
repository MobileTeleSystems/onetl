# Преобразование типов между Clickhouse и Spark { #clickhouse-types }

!!! note

    Результаты ниже актуальны для Spark 3.5.5 и могут отличаться для других версий Spark.

!!! note

    Рекомендуется использовать пакет [spark-dialect-extension](https://github.com/MobileTeleSystems/spark-dialect-extension), который реализует запись массивов из Spark в Clickhouse, исправляет потерю долей секунд в `TimestampType` и некоторые другие проблемы преобразования типов.

## Определение типов и приведение

DataFrame в Spark всегда имеют `schema` — список колонок с соответствующими типами Spark. Все операции над колонками выполняются с использованием типа колонки.

### Чтение из Clickhouse { #clickhouse-read-0 }

Коннектор Clickhouse выполняет следующие действия:

- Для каждой колонки в результате запроса (`SELECT column1, column2, ... FROM table ...`) получает имя колонки и тип Clickhouse.
- Находит соответствующую комбинацию `Тип Clickhouse (чтение)` → `Тип Spark` (см. ниже) для каждой колонки DataFrame. Если комбинация не найдена, выбрасывает исключение.
- Создает DataFrame из запроса с определенными именами колонок и типами Spark.

### Запись в существующую таблицу Clickhouse { #clickhouse-table }

Коннектор Clickhouse выполняет следующие действия:

- Получает имена колонок в DataFrame. [^1]
- Выполняет запрос `SELECT * FROM table LIMIT 0`.
- Берет только колонки, присутствующие в DataFrame (по имени, без учета регистра). Для каждой найденной колонки получает тип Clickhouse.
- **Находит соответствующую комбинацию** `Тип Clickhouse (чтение)` → `Тип Spark` (см. ниже) для каждой колонки DataFrame. Если комбинация не найдена, выбрасывает исключение. [^2]
- Находит соответствующую комбинацию `Тип Spark` → `Тип Clickhouse (запись)` (см. ниже) для каждой колонки DataFrame. Если комбинация не найдена, выбрасывает исключение.
- Если `Тип Clickhouse (запись)` соответствует `Тип Clickhouse (чтение)`, дополнительные преобразования не выполняются, колонка DataFrame записывается в Clickhouse как есть.
- Если `Тип Clickhouse (запись)` не соответствует `Тип Clickhouse (чтение)`, колонка DataFrame будет приведена к целевому типу колонки **на стороне Clickhouse**. Например, вы можете записать колонку с текстовыми данными в колонку `Int32`, если колонка содержит допустимые целочисленные значения в пределах поддерживаемого диапазона значений и точности.

[^1]: Это позволяет записывать данные в таблицы с колонками `DEFAULT` - если в DataFrame нет такой колонки, она будет заполнена Clickhouse.

[^2]: Да, это странно.

### Создание новой таблицы с помощью Spark

!!! warning

    АБСОЛЮТНО НЕ РЕКОМЕНДУЕТСЯ!

Коннектор Clickhouse выполняет следующие действия:

- Находит соответствующую комбинацию `Тип Spark` → `Тип Clickhouse (создание)` (см. ниже) для каждой колонки DataFrame. Если комбинация не найдена, выбрасывает исключение.
- Генерирует DDL для создания таблицы в Clickhouse, например `CREATE TABLE (col1 ...)`, и выполняет его.
- Записывает DataFrame в созданную таблицу как есть.

Но Spark не имеет специфического диалекта для Clickhouse, поэтому используется диалект Generic JDBC.
Общий диалект использует имена типов SQL ANSI при создании таблиц в целевой базе данных, а не типы, специфичные для базы данных.

В некоторых случаях это может привести к использованию неправильного типа колонки. Например, Spark создает колонку типа `TIMESTAMP`, которая соответствует типу Clickhouse `DateTime32` (точность до секунд), вместо более точного `DateTime64` (точность до наносекунд).
Это может привести к случайной потере точности, или иногда данные вообще невозможно записать в созданную таблицу.

Поэтому вместо того, чтобы полагаться на Spark для создания таблиц:

??? "Используйте пример"

    ```python

        writer = DBWriter(
            connection=clickhouse,
            target="default.target_tbl",
            options=Clickhouse.WriteOptions(
                if_exists="append",
                # ENGINE обязателен для Clickhouse
                createTableOptions="ENGINE = MergeTree() ORDER BY id",
            ),
        )
        writer.run(df)
    ```

Всегда предпочитайте создавать таблицы с конкретными типами **ПЕРЕД ЗАПИСЬЮ ДАННЫХ**:

??? "Пример"

    ```python

        clickhouse.execute(
            """
            CREATE TABLE default.target_tbl (
                id UInt8,
                value DateTime64(6) -- конкретный тип и точность
            )
            ENGINE = MergeTree()
            ORDER BY id
            """,
        )

        writer = DBWriter(
            connection=clickhouse,
            target="default.target_tbl",
            options=Clickhouse.WriteOptions(if_exists="append"),
        )
        writer.run(df)
    ```

### Ссылки

Здесь вы можете найти исходный код с преобразованиями типов:

- [Clickhouse -> JDBC](https://github.com/ClickHouse/clickhouse-java/blob/0.3.2/clickhouse-jdbc/src/main/java/com/clickhouse/jdbc/JdbcTypeMapping.java#L39-L176)
- [JDBC -> Spark](https://github.com/apache/spark/blob/v3.5.5/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/jdbc/JdbcUtils.scala#L307)
- [Spark -> JDBC](https://github.com/apache/spark/blob/v3.5.5/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/jdbc/JdbcUtils.scala#L141-L164)
- [JDBC -> Clickhouse](https://github.com/ClickHouse/clickhouse-java/blob/0.3.2/clickhouse-jdbc/src/main/java/com/clickhouse/jdbc/JdbcTypeMapping.java#L185-L311)

## Поддерживаемые типы

См. [официальную документацию](https://clickhouse.com/docs/en/sql-reference/data-types)

### Общие типы

- `LowCardinality(T)` то же самое, что и `T`
- `Nullable(T)` то же самое, что и `T`, но колонка Spark определяется как `nullable=True`

### Числовые типы

| Тип Clickhouse (чтение)         | Тип Spark                        | Тип Clickhouse (запись)       | Тип Clickhouse (создание)    
|--------------------------------|-----------------------------------|-------------------------------|-----------------------------
| `Bool`                         | `BooleanType()`                 | `Bool`                      | `UInt64`                   
| `Decimal`                      | `DecimalType(P=10, S=0)`        | `Decimal(P=10, S=0)`        | `Decimal(P=10, S=0)`       
| `Decimal(P=0..38)`             | `DecimalType(P=0..38, S=0)`     | `Decimal(P=0..38, S=0)`     | `Decimal(P=0..38, S=0)`    
| `Decimal(P=0..38, S=0..38)`    | `DecimalType(P=0..38, S=0..38)` | `Decimal(P=0..38, S=0..38)` | `Decimal(P=0..38, S=0..38)`
| `Decimal(P=39..76, S=0..76)`   | не поддерживается [^3]         |                             |                             
| `Decimal32(P=0..9)`            | `DecimalType(P=9, S=0..9)`      | `Decimal(P=9, S=0..9)`      | `Decimal(P=9, S=0..9)`     
| `Decimal64(S=0..18)`           | `DecimalType(P=18, S=0..18)`    | `Decimal(P=18, S=0..18)`    | `Decimal(P=18, S=0..18)`   
| `Decimal128(S=0..38)`          | `DecimalType(P=38, S=0..38)`    | `Decimal(P=38, S=0..38)`    | `Decimal(P=38, S=0..38)`   
| `Decimal256(S=0..76)`          | не поддерживается [^3]         |                             |                             
| `Float32`                      | `FloatType()`                   | `Float32`                   | `Float32`                  
| `Float64`                      | `DoubleType()`                  | `Float64`                   | `Float64`                  
| `Int8`<br/>`Int16`<br/>`Int32` | <br/>`IntegerType()`            | <br/>`Int32`                | <br/>`Int32`
| `Int64`                        | `LongType()`                    | `Int64`                     | `Int64`                    
| `Int128`<br/>`Int256`          | не поддерживается [^3]         |                             |                             
| `-`                            | `ByteType()`                    | `Int8`                      | `Int8`                     
| `-`                            | `ShortType()`                   | `Int32`                     | `Int32`                    
| `UInt8`                        | `IntegerType()`                 | `Int32`                     | `Int32`                    
| `UInt16`                       | `LongType()`                    | `Int64`                     | `Int64`                    
| `UInt32`<br/>`UInt64`          | `DecimalType(20,0)`             | `Decimal(20,0)`             | `Decimal(20,0)`            
| `UInt128`<br/>`UInt256`        | не поддерживается [^3]          |                             |                            

[^3]: Clickhouse поддерживает числовые типы до 256 бит - `Int256`, `UInt256`, `Decimal256(S)`, `Decimal(P=39..76, S=0..76)`.

    Но `DecimalType(P, S)` в Spark поддерживает максимум `P=38` (128 бит). Невозможно читать, записывать или работать со значениями большей точности,
    это приводит к исключению.

### Временные типы

Примечания:

- Дата-время с часовым поясом имеет ту же точность, что и без часового пояса
- `DateTime` является псевдонимом для `DateTime32`
- `TIMESTAMP` является псевдонимом для `DateTime32`, но `TIMESTAMP(N)` является псевдонимом для `DateTime64(N)`

| Тип Clickhouse (чтение)            | Тип Spark                           | Тип Clickhouse (запись)          | Тип Clickhouse (создание)      |
|-----------------------------------|--------------------------------------|----------------------------------|-------------------------------|
| `Date`                          | `DateType()`                       | `Date`                         | `Date`                      |
| `Date32`                        | `DateType()`                       | `Date`                         | `Date`, **невозможно вставить данные** [^4]                    |
|                                   |                                      |                                  | _   |
| `DateTime32`, секунды           | `TimestampType()`, микросекунды    | `DateTime64(6)`, микросекунды  | `DateTime32`, секунды       |
| `DateTime64(3)`, миллисекунды   | `TimestampType()`, микросекунды    | `DateTime64(6)`, микросекунды  | `DateTime32`, секунды, **потеря точности** [^5]     |
| `DateTime64(6)`, микросекунды   | `TimestampType()`, микросекунды    |                                  | `DateTime32`, секунды, **потеря точности** [^7]      |
| `DateTime64(7..9)`, наносекунды | `TimestampType()`, микросекунды, **потеря точности** [^6]   |                                  |                               |
| `-`                             | `TimestampNTZType()`, микросекунды |                                  |                               |
| `DateTime32(TZ)`<br/>`DateTime64(P, TZ)`                | не поддерживается [^7]_                     |                                  |                               |
| `IntervalNanosecond`<br/>`IntervalMicrosecond`<br/>`IntervalMillisecond`<br/>`IntervalSecond`<br/>`IntervalMinute`<br/>`IntervalHour`<br/>`IntervalDay`<br/>`IntervalMonth`<br/>`IntervalQuarter`<br/>`IntervalWeek`<br/>`IntervalYear`            | <br/><br/><br/><br/><br/><br/>`LongType()`                       | <br/><br/><br/><br/><br/><br/>`Int64`                        |  <br/><br/><br/><br/><br/><br/>`Int64`                    |

!!! warning

    Обратите внимание, что типы в Clickhouse и Spark имеют разные диапазоны значений:

    | Тип Clickhouse        | Минимальное значение                | Максимальное значение                | Тип Spark          | Минимальное значение               | Максимальное значение               | 
    |------------------------|-----------------------------------|-----------------------------------|---------------------|--------------------------------|--------------------------------| 
    | `Date`                 | `1970-01-01`                      | `2149-06-06`                      | <br/><br/>`DateType()` {: rowspan=3}        | <br/><br/>`0001-01-01 00:00:00.000000` {: rowspan=3}   | <br/><br/>`9999-12-31 23:59:59.999999` {:  rowspan=3} |   
    | `DateTime64(P=0..8)`   | `1900-01-01 00:00:00.00000000`    | `2299-12-31 23:59:59.99999999`  | &#8288 {: style="padding:0"} | &#8288 {: style="padding:0"} | &#8288 {: style="padding:0"} | 
    | `DateTime64(P=9)`      | `1900-01-01 00:00:00.000000000`   | `2262-04-11 23:47:16.999999999` | &#8288 {: style="padding:0"} | &#8288 {: style="padding:0"} | &#8288 {: style="padding:0"} | 
        
    Таким образом, не все значения в DataFrame Spark могут быть записаны в Clickhouse.
        
    Ссылки:
         
    * [Документация Clickhouse по типу Date](https://clickhouse.com/docs/en/sql-reference/data-types/date)
    * [Документация Clickhouse по типу Datetime32](https://clickhouse.com/docs/en/sql-reference/data-types/datetime)
    * [Документация Clickhouse по типу Datetime64](https://clickhouse.com/docs/en/sql-reference/data-types/datetime64)
    * [Документация Spark по типу DateType](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/types/DateType.html)
    * [Документация Spark по типу TimestampType](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/types/TimestampType.html)

[^4]: `Date32` имеет другое байтовое представление, чем `Date`, и вставка значения типа `Date32` в колонку `Date`
    приводит к ошибкам на стороне Clickhouse, например, `Date(106617) should be between 0 and 65535 inclusive of both values`.
    Хотя Spark правильно читает колонку `Date32` как `DateType()`, и никакой разницы быть не должно.
    Вероятно, это какой-то баг в драйвере Clickhouse.

[^5]: Общий диалект JDBC генерирует DDL с типом Clickhouse `TIMESTAMP`, который является псевдонимом для `DateTime32` с точностью до секунд (`23:59:59`).
    Вставка данных с точностью до миллисекунд (`23:59:59.999`) приведет к **отбрасыванию миллисекунд**.
    Решение: создайте таблицу вручную, с правильным типом колонки.

[^6]: Clickhouse поддерживает дату-время с точностью до наносекунд (`23:59:59.999999999`),
    но `TimestampType()` в Spark поддерживает дату-время с точностью до микросекунд (`23:59:59.999999`).
    Наносекунды будут потеряны во время операций чтения или записи.
    Решение: создайте таблицу вручную, с правильным типом колонки.

[^7]: Clickhouse выбросит исключение, что данные в формате `2001-01-01 23:59:59.999999` имеют данные `.999999`, которые не соответствуют формату `YYYY-MM-DD hh:mm:ss`
    типа колонки `DateTime32` (см. [^5]).
    Таким образом, Spark может создать таблицу Clickhouse, но не может записать данные в колонку этого типа.
    Решение: создайте таблицу вручную, с правильным типом колонки.

### Строковые типы

| Тип Clickhouse (чтение)               | Тип Spark       | Тип Clickhouse (запись) | Тип Clickhouse (создание) |
|--------------------------------------|------------------|------------------------|--------------------------|
| `FixedString(N)`<br/>`String`<br/>`Enum8`<br/>`Enum16`<br/>`IPv4`<br/>`IPv6`<br/>`UUID`                   | <br/><br/><br/>`StringType()` | <br/><br/><br/>`String`             | <br/><br/><br/>`String`               |
| `-`                                | `BinaryType()` |                        |                          |

## Неподдерживаемые типы

Колонки следующих типов Clickhouse не могут быть прочитаны Spark:

- `AggregateFunction(func, T)`
- `Array(T)`
- `JSON`
- `Map(K, V)`
- `MultiPolygon`
- `Nested(field1 T1, ...)`
- `Nothing`
- `Point`
- `Polygon`
- `Ring`
- `SimpleAggregateFunction(func, T)`
- `Tuple(T1, T2, ...)`

DataFrame со следующими типами Spark не могут быть записаны в Clickhouse:

- `ArrayType(T)`
- `BinaryType()`
- `CharType(N)`
- `DayTimeIntervalType(P, S)`
- `MapType(K, V)`
- `NullType()`
- `StructType([...])`
- `TimestampNTZType()`
- `VarcharType(N)`

Это происходит потому, что Spark не имеет специального диалекта для Clickhouse и использует вместо этого общий диалект JDBC.
Этот диалект не имеет преобразования типов между некоторыми типами, такими как Clickhouse `Array` -> Spark `ArrayType()` и наоборот.

Есть способ избежать этого - просто приводить всё к типу `String`.

## Явное приведение типов

### `DBReader`

Используйте `CAST` или `toJSONString` для получения данных колонки в виде строки в формате JSON,

Для разбора JSON-колонок в ClickHouse используйте метод [JSON.parse_column][onetl.file.format.json.JSON.parse_column].

    ```python
    from pyspark.sql.types import ArrayType, IntegerType

    from onetl.file.format import JSON
    from onetl.connection import ClickHouse
    from onetl.db import DBReader

    reader = DBReader(
        connection=clickhouse,
        target="default.source_tbl",
        columns=[
            "id",
            "toJSONString(array_column) array_column",
        ],
    )
    df = reader.run()

    # Spark требует, чтобы все колонки имели определенный тип, опишите его
    column_type = ArrayType(IntegerType())

    json = JSON()
    df = df.select(
        df.id,
        json.parse_column("array_column", column_type),
    ) 
    ```

### `DBWriter`

Для записи JSON-данных в ClickHouse используйте метод [JSON.serialize_column][onetl.file.format.json.JSON.serialize_column] для эффективного преобразования колонки DataFrame в формат JSON и записи ее как колонки типа `String` в Clickhouse.

    ```python
    from onetl.file.format import JSON
    from onetl.connection import ClickHouse
    from onetl.db import DBWriter

    clickhouse = ClickHouse(...)

    clickhouse.execute(
        """
        CREATE TABLE default.target_tbl (
            id Int32,
            array_column_json String,
        )
        ENGINE = MergeTree()
        ORDER BY id
        """,
    )

    json = JSON()
    df = df.select(
        df.id,
        json.serialize_column(df.array_column).alias("array_column_json"),
    )

    writer.run(df) 
    ```

Затем вы можете разобрать эту колонку на стороне Clickhouse - например, создав представление:

    ```sql
    SELECT
        id,
        JSONExtract(json_column, 'Array(String)') AS array_column
    FROM target_tbl 
    ```

Вы также можете использовать колонки [ALIAS](https://clickhouse.com/docs/en/sql-reference/statements/create/table#alias) или [MATERIALIZED](https://clickhouse.com/docs/en/sql-reference/statements/create/table#materialized), чтобы избежать написания такого выражения в каждом запросе `SELECT`:

    ```sql
    CREATE TABLE default.target_tbl (
        id Int32,
        array_column_json String,
        -- вычисляемая колонка
        array_column Array(String) ALIAS JSONExtract(json_column, 'Array(String)')
        -- или материализованная колонка
        -- array_column Array(String) MATERIALIZED JSONExtract(json_column, 'Array(String)')
    )
    ENGINE = MergeTree()
    ORDER BY id 
    ```

Недостатки:

- Использование `SELECT JSONExtract(...)` или колонки `ALIAS` может быть дорогостоящим, поскольку значение вычисляется при каждом доступе к строке. Это может быть особенно вредным, если такая колонка используется в условии `WHERE`.
- Колонки `ALIAS` и `MATERIALIZED` не включаются в выражение `SELECT *`, их следует добавлять явно: `SELECT *, calculated_column FROM table`.

!!! warning

    Колонки [EPHEMERAL](https://clickhouse.com/docs/en/sql-reference/statements/create/table#ephemeral) не поддерживаются Spark,
    поскольку их нельзя выбрать для определения типа целевой колонки.