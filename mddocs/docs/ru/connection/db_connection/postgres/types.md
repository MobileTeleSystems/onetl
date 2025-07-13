# Соответствие типов Postgres <-> Spark { #postgres-types }

!!! note

    Результаты ниже действительны для Spark 3.5.5 и могут отличаться в других версиях Spark.

## Определение типов и приведение

DataFrame в Spark всегда имеют `schema`, которая представляет собой список столбцов с соответствующими типами Spark. Все операции со столбцом выполняются с использованием типа столбца.

### Чтение из Postgres

Вот как коннектор Postgres выполняет это:

- Для каждого столбца в результате запроса (`SELECT column1, column2, ... FROM table ...`) получает имя столбца и тип Postgres.
- Находит соответствующую комбинацию `Тип Postgres (чтение)` → `Тип Spark` (см. ниже) для каждого столбца DataFrame [^1]. Если комбинация не найдена, вызывает исключение.
- Создает DataFrame из запроса с указанными именами столбцов и типами Spark.

### Запись в существующую таблицу Postgres

Коннектор Postgres выполняет это следующим образом:

- Получает имена столбцов в DataFrame. [^1]
- Выполняет запрос `SELECT * FROM table LIMIT 0`.
- Выбирает только столбцы, присутствующие в DataFrame (по имени, без учета регистра) [^2]. Для каждого найденного столбца получает тип Postgres.
- Находит соответствующую комбинацию `Тип Spark` → `Тип Postgres (запись)` (см. ниже) для каждого столбца DataFrame. Если комбинация не найдена, вызывает исключение.
- Если `Тип Postgres (запись)` соответствует `Тип Postgres (чтение)`, дополнительные преобразования не выполняются, столбец DataFrame будет записан в Postgres как есть.
- Если `Тип Postgres (запись)` не соответствует `Тип Postgres (чтение)`, столбец DataFrame будет приведен к целевому типу столбца **на стороне Postgres**.
  Например, можно записать столбец с текстовыми данными в столбец `int`, если столбец содержит допустимые целочисленные значения в пределах поддерживаемого диапазона значений и точности [^3].

[^1]: Все типы Postgres, которые не имеют соответствующего типа Java, преобразуются в `String`.

[^2]: Это позволяет записывать данные в таблицы со столбцами `DEFAULT` и `GENERATED` - если в DataFrame нет такого столбца, он будет заполнен Postgres.

[^3]: Это верно, только если столбец DataFrame имеет тип `StringType()` или целевой столбец имеет тип `text`.

    Но другие типы не могут быть преобразованы автоматически, например, `bytea -> bit(N)`. Это требует явного приведения, см. [Ручное преобразование в строку].

### Создание новой таблицы с помощью Spark

!!! warning

    КАТЕГОРИЧЕСКИ НЕ РЕКОМЕНДУЕТСЯ!

Вот как коннектор Postgres выполняет это:

- Находит соответствующую комбинацию `Тип Spark` → `Тип Postgres (создание)` (см. ниже) для каждого столбца DataFrame. Если комбинация не найдена, вызывает исключение.
- Генерирует DDL для создания таблицы в Postgres, например `CREATE TABLE (col1 ...)`, и выполняет его.
- Записывает DataFrame в созданную таблицу как есть.

Но коннектор Postgres поддерживает ограниченное количество типов и почти не поддерживает пользовательские выражения (такие как `PARTITION BY`, `INDEX` и т.д.).
Поэтому вместо того, чтобы полагаться на Spark для создания таблиц:

??? note "Смотрите пример"

    ```python

    writer = DBWriter(
        connection=postgres,
        target="public.table",
        options=Postgres.WriteOptions(
            if_exists="append",
            createTableOptions="PARTITION BY RANGE (id)",
        ),
    )
    writer.run(df)
    ```

Всегда предпочтительнее создавать таблицу с нужным DDL **ПЕРЕД ЗАПИСЬЮ ДАННЫХ**:

??? note "Смотрите пример"

    ```python
        postgres.execute(
            """
            CREATE TABLE public.table (
                id bigint,
                business_dt timestamp(6),
                value json
            )
            PARTITION BY RANGE (Id)
            """,
        )

        writer = DBWriter(
            connection=postgres,
            target="public.table",
            options=Postgres.WriteOptions(if_exists="append"),
        )
        writer.run(df)
    ```

См. документацию Postgres [CREATE TABLE](https://www.postgresql.org/docs/current/sql-createtable.html).

## Поддерживаемые типы

### Ссылки

См. [Список типов Postgres](https://www.postgresql.org/docs/current/datatype.html).

Здесь вы можете найти исходный код с преобразованиями типов:

- [Postgres <-> JDBC](https://github.com/pgjdbc/pgjdbc/blob/REL42.6.0/pgjdbc/src/main/java/org/postgresql/jdbc/TypeInfoCache.java#L78-L112)
- [JDBC -> Spark](https://github.com/apache/spark/blob/v3.5.5/sql/core/src/main/scala/org/apache/spark/sql/jdbc/PostgresDialect.scala#L52-L108)
- [Spark -> JDBC](https://github.com/apache/spark/blob/v3.5.5/sql/core/src/main/scala/org/apache/spark/sql/jdbc/PostgresDialect.scala#L118-L132)

### Числовые типы

| Тип Postgres (чтение)           | Тип Spark                         | Тип Postgres (запись)        | Тип Postgres (создание)                        |
|--------------------------------|------------------------------------|------------------------------|-----------------------------------------------|
| `decimal`                      | `DecimalType(P=38, S=18)`          | `decimal(P=38, S=18)`        | <br/><br/>`decimal` (без ограничений) {: rowspan=3} |
| `decimal(P=0..38)`             | `DecimalType(P=0..38, S=0)`        | `decimal(P=0..38, S=0)`      | &#8288 {: style="padding:0"}                  |
| `decimal(P=0..38, S=0..38)`    | `DecimalType(P=0..38, S=0..38)`    | `decimal(P=0..38, S=0..38)`  | &#8288 {: style="padding:0"}                  |
| `decimal(P=39.., S=0..)`       | не поддерживается [^4]                   |                              |                                               |
| `decimal(P=.., S=..-1)`        | не поддерживается [^5]                   |                              |                                               |
| `real`                         | `FloatType()`                      | `real`                       | `real`                                        |
| `double precision`             | `DoubleType()`                     | `double precision`           | `double precision`                            |
| `smallint`                     | `ShortType()`                      | `smallint`                   | `smallint`                                    |
| `-`                            | `ByteType()`                       |                              |                                               |
| `integer`                      | `IntegerType()`                    | `integer`                    | `integer`                                     |
| `bigint`                       | `LongType()`                       | `bigint`                     | `bigint`                                      |
| `money`                        | <br/><br/><br/><br/>`StringType()` [^1]  {: rowspan=5} | <br/><br/><br/><br/>`text`   {: rowspan=5}       | <br/><br/><br/><br/>`text`    {: rowspan=5}                       |
| `int4range`                    |  &#8288 {: style="padding:0"}      | &#8288 {: style="padding:0"} | &#8288 {: style="padding:0"}                  |
| `int8range`                    |  &#8288 {: style="padding:0"}      | &#8288 {: style="padding:0"} | &#8288 {: style="padding:0"}                  |
| `numrange`                     |  &#8288 {: style="padding:0"}      | &#8288 {: style="padding:0"} | &#8288 {: style="padding:0"}                  |
| `int2vector`                   |  &#8288 {: style="padding:0"}      | &#8288 {: style="padding:0"} | &#8288 {: style="padding:0"}                  |

[^4]: Postgres поддерживает типы decimal с неограниченной точностью.

    Но `DecimalType(P, S)` в Spark поддерживает максимум `P=38` (128 бит). Невозможно читать, записывать или работать со значениями большей точности,
    это приводит к ошибке.

[^5]: Postgres поддерживает типы decimal с отрицательным масштабом, например `decimal(38, -10)`. Spark - нет.

### Временные типы

| Тип Postgres (чтение)               | Тип Spark                   | Тип Postgres (запись) | Тип Postgres (создание)  |
|------------------------------------|------------------------------|-----------------------|-------------------------|
| `date`                           | `DateType()`               | `date`              | `date`                |
| `time`<br/>`time(0..6)`<br/>`time with time zone`<br/>`time(0..6) with time zone` | <br/>`TimestampType()`,<br/>с особенностями форматирования времени [^6]        | <br/><br/>`timestamp(6)`      | <br/><br/>`timestamp(6)`        |
| `timestamp`<br/>`timestamp(0..6)`<br/>`timestamp with time zone`<br/>`timestamp(0..6) with time zone`  | <br/><br/>`TimestampType()`          | <br/><br/>`timestamp(6)`      | <br/><br/>`timestamp(6)`        |
| `-`                              | `TimestampNTZType()`       | `timestamp(6)`      | `timestamp(6)`        |
| `interval` любой точности      | `StringType()` [^1]        | `text`              | `text`                |
| `-`                              | `DayTimeIntervalType()`    | не поддерживается           | не поддерживается             |
| `-`                              | `YearMonthIntervalType()`  | не поддерживается           | не поддерживается             |
| `daterange`<br/>`tsrange`<br/>`tstzrange`                      | <br/>`StringType()` [^1]        | <br/><br/>`text`              | <br/><br/>`text`                |

!!! warning "Предупреждение"

    Обратите внимание, что типы в Postgres и Spark имеют разные диапазоны значений:

    
    | Тип Postgres | Минимальное значение                       | Максимальное значение                        | Тип Spark          | Минимальное значение                      | Максимальное значение                      |
    |---------------|---------------------------------|----------------------------------|---------------------|--------------------------------|--------------------------------|
    | `date`      | `-4713-01-01`                 | `5874897-01-01`                | `DateType()`      | `0001-01-01`                 | `9999-12-31`                 |
    | `timestamp` | `-4713-01-01 00:00:00.000000` | `294276-12-31 23:59:59.999999` | <br/><br/>`TimestampType()` {: rowspan=2} | <br/><br/>`0001-01-01 00:00:00.000000` {: rowspan=2} | <br/><br/>`9999-12-31 23:59:59.999999` {: rowspan=2} |
    | `time`      | `00:00:00.000000`             | `24:00:00.000000`              | &#8288 {: style="padding:0"}   |  &#8288 {: style="padding:0"}      |  &#8288 {: style="padding:0"}     |


    Таким образом, не все значения могут быть прочитаны из Postgres в Spark.

    Ссылки:

    * [Документация по типам даты/времени Postgres](https://www.postgresql.org/docs/current/datatype-datetime.html)
    * [Документация по DateType в Spark](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/types/DateType.html)
    * [Документация по TimestampType в Spark](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/types/TimestampType.html)

[^6]: Тип `time` аналогичен `timestamp` с датой `1970-01-01`. Поэтому вместо чтения данных из Postgres как `23:59:59`
    фактически читается `1970-01-01 23:59:59`, и наоборот.

### Строковые типы

| Тип Postgres (чтение)        | Тип Spark            | Тип Postgres (запись) | Тип Postgres (создание)  |
|-----------------------------|-----------------------|-----------------------|-------------------------|
| `character`<br/>`character(N)`<br/>`character varying`<br/>`character varying(N)`<br/>`text`<br/>`json`<br/>`jsonb`<br/>`xml`               | <br/><br/><br/><br/>`StringType()`      | <br/><br/><br/><br/><br/>`text` {: rowspan=2}              | <br/><br/><br/><br/><br/>`text` {: rowspan=2}               |
| `CREATE TYPE ... AS ENUM`<br/>`tsvector`<br/>`tsquery` | <br/>`StringType()`[^1] |   &#8288 {: style="padding:0"}    |    &#8288 {: style="padding:0"}    |
| `-`                       | `CharType()`        | `не поддерживается`       | `не поддерживается`         |
| `-`                       | `VarcharType()`     | `не поддерживается`       | `не поддерживается`         |

### Бинарные типы

| Тип Postgres (чтение)     | Тип Spark            | Тип Postgres (запись)       | Тип Postgres (создание)  |
|--------------------------|-----------------------|-----------------------------|-------------------------|
| `boolean`              | `BooleanType()`     | `boolean`                 | `boolean`             |
| `bit`<br/>`bit(N=1)`                  | `BooleanType()`      | `bool`,<br/>**невозможно вставить данные** [^3]                   | `bool`                |
| `bit(N=2..)`           | `ByteType()`        | `bytea`,<br/>**невозможно вставить данные** [^3]                  | `bytea`               |
| `bit varying`<br/> `bit varying(N)`         | `StringType()` [^1] | `text`                    | `text`                |
| `bytea`                | `BinaryType()`      | `bytea`                   | `bytea`               |

### Структурные типы

| Тип Postgres (чтение)           | Тип Spark            | Тип Postgres (запись) | Тип Postgres (создание)  |
|--------------------------------|-----------------------|-----------------------|-------------------------|
| `T[]`                        | `ArrayType(T)`      | `T[]`               | `T[]`                 |
| `T[][]`                      | не поддерживается           |                       |                         |
| `CREATE TYPE sometype (...)` | `StringType()` [^1] | `text`              | `text`                |
| `-`                          | `StructType()`<br/> `MapType()`     | не поддерживается           |                         |

### Сетевые типы

| Тип Postgres (чтение) | Тип Spark            | Тип Postgres (запись) | Тип Postgres (создание)  |
|----------------------|-----------------------|-----------------------|-------------------------|
| `cidr`<br/>`inet`<br/>`macaddr`<br/>`macaddr8`             | `StringType()` [^1] | <br/><br/>`text`              | <br/><br/>`text`                |

### Географические типы

| Тип Postgres (чтение) | Тип Spark            | Тип Postgres (запись) | Тип Postgres (создание)  |
|----------------------|-----------------------|-----------------------|-------------------------|
| `circle`<br/>`box`<br/>`line`<br/>`lseg`<br/>`path`<br/>`point`<br/>`polygon`           | <br/><br/><br/>`StringType()` [^1] | <br/><br/><br/>`text`              | <br/><br/><br/>`text`                |

## Явное приведение типов

### `DBReader`

Можно явно привести столбец неподдерживаемого типа с помощью синтаксиса `DBReader(columns=...)`.

Например, вы можете использовать `CAST(column AS text)` для преобразования данных в строковое представление на стороне Postgres, и тогда они будут прочитаны как `StringType()` в Spark.

Также возможно использовать функцию [to_json](https://www.postgresql.org/docs/current/functions-json.html) в Postgres для преобразования столбца любого типа в строковое представление, а затем разобрать этот столбец на стороне Spark с помощью метода [JSON.parse_column][onetl.file.format.json.JSON.parse_column]:

    ```python
        from pyspark.sql.types import IntegerType

        from onetl.connection import Postgres
        from onetl.db import DBReader
        from onetl.file.format import JSON

        postgres = Postgres(...)

        DBReader(
            connection=postgres,
            columns=[
                "id",
                "supported_column",
                "CAST(unsupported_column AS text) unsupported_column_str",
                # или
                "to_json(unsupported_column) array_column_json",
            ],
        )
        df = reader.run()

        json_schema = StructType(
            [
                StructField("id", IntegerType(), nullable=True),
                StructField("name", StringType(), nullable=True),
                ...,
            ]
        )
        df = df.select(
            df.id,
            df.supported_column,
            # явное приведение
            df.unsupported_column_str.cast("integer").alias("parsed_integer"),
            JSON().parse_column("array_column_json", json_schema).alias("json_string"),
        )
    ```

### `DBWriter`

Всегда возможно преобразовать данные на стороне Spark в строку, а затем записать их в текстовый столбец в таблице Postgres.

#### Использование JSON.serialize_column

Вы можете использовать метод [JSON.serialize_column][onetl.file.format.json.JSON.serialize_column] для сериализации данных:

    ```python
        from onetl.file.format import JSON
        from pyspark.sql.functions import col

        from onetl.connection import Postgres
        from onetl.db import DBWriter

        postgres = Postgres(...)

        postgres.execute(
            """
            CREATE TABLE schema.target_table (
                id int,
                supported_column timestamp,
                array_column_json jsonb -- любой тип столбца на самом деле
            )
            """,
        )

        write_df = df.select(
            df.id,
            df.supported_column,
            JSON().serialize_column(df.unsupported_column).alias("array_column_json"),
        )

        writer = DBWriter(
            connection=postgres,
            target="schema.target_table",
        )
        writer.run(write_df)
    ```

Затем вы можете разобрать этот столбец на стороне Postgres (например, создав представление):

    ```sql
        SELECT
            id,
            supported_column,
            array_column_json->'0' AS array_item_0
        FROM
            schema.target_table
    ```

Чтобы избежать приведения значения при каждом чтении таблицы, вы можете использовать столбец [GENERATED ALWAYS STORED](https://www.postgresql.org/docs/current/ddl-generated-columns.html), но это требует в 2 раза больше места (для исходного и разобранного значения).

#### Ручное преобразование в строку

Коннектор Postgres также поддерживает прямое преобразование текстового значения в целевой тип столбца, если это значение имеет правильный формат.

Например, вы можете записать данные вида `[123, 345]` в тип `int8range`, поскольку Postgres позволяет выполнить приведение `'[123, 345]'::int8range'`:

    ```python
        from pyspark.sql.ftypes import StringType
        from pyspark.sql.functions import udf

        from onetl.connection import Postgres
        from onetl.db import DBReader

        postgres = Postgres(...)

        postgres.execute(
            """
            CREATE TABLE schema.target_table (
                id int,
                range_column int8range -- любой тип столбца на самом деле
            )
            """,
        )


        @udf(returnType=StringType())
        def array_to_range(value: tuple):
            """Эта UDF позволяет преобразовать tuple[start, end] в формат диапазона Postgres"""
            start, end = value
            return f"[{start},{end}]"


        write_df = df.select(
            df.id,
            array_to_range(df.range_column).alias("range_column"),
        )

        writer = DBWriter(
            connection=postgres,
            target="schema.target_table",
        )
        writer.run(write_df)
    ```

Это может быть сложно реализовать и может привести к более длительному процессу записи.
Но это не требует дополнительного места на стороне Postgres и позволяет избежать явного приведения значения при каждом чтении таблицы.
