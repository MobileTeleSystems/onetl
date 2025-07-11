# Сопоставление типов MSSQL <-> Spark { #mssql-types }

!!! note

    Результаты ниже действительны для Spark 3.5.5 и могут отличаться в других версиях Spark.

## Определение типов и приведение

DataFrame в Spark всегда имеют `schema`, которая представляет собой список столбцов с соответствующими типами Spark. Все операции со столбцом выполняются с использованием типа столбца.

### Чтение из MSSQL

Коннектор MSSQL выполняет это следующим образом:

- Для каждого столбца в результате запроса (`SELECT column1, column2, ... FROM table ...`) получает имя столбца и тип MSSQL.
- Находит соответствующую комбинацию `MSSQL тип (чтение)` → `Spark тип` (см. ниже) для каждого столбца DataFrame. Если комбинация не найдена, вызывает исключение.
- Создает DataFrame из запроса с определенными именами столбцов и типами Spark.

### Запись в существующую таблицу MSSQL

Коннектор MSSQL выполняет это следующим образом:

- Получает имена столбцов в DataFrame. [^1]
- Выполняет запрос `SELECT * FROM table LIMIT 0`.
- Берет только столбцы, присутствующие в DataFrame (по имени, без учета регистра). Для каждого найденного столбца получает тип MSSQL.
- Находит соответствующую комбинацию `Spark тип` → `MSSQL тип (запись)` (см. ниже) для каждого столбца DataFrame. Если комбинация не найдена, вызывает исключение.
- Если `MSSQL тип (запись)` соответствует `MSSQL тип (чтение)`, дополнительные преобразования не выполняются, столбец DataFrame будет записан в MSSQL как есть.
- Если `MSSQL тип (запись)` не соответствует `MSSQL тип (чтение)`, столбец DataFrame будет приведен к целевому типу столбца **на стороне MSSQL**.
  Например, вы можете записать столбец с текстовыми данными в столбец `int`, если столбец содержит допустимые целочисленные значения в пределах поддерживаемого диапазона значений и точности [^2].

[^1]: Это позволяет записывать данные в таблицы со столбцами `DEFAULT` и `GENERATED` - если в DataFrame нет такого столбца, он будет заполнен MSSQL.

[^2]: Это верно только если столбец DataFrame имеет тип `StringType()`, поскольку текстовое значение автоматически анализируется для целевого типа столбца.

    Но другие типы не могут быть неявно преобразованы, например, `int -> text`. Это требует явного приведения, см. [DBWriter].

### Создание новой таблицы с помощью Spark

!!! warning

    КАТЕГОРИЧЕСКИ НЕ РЕКОМЕНДУЕТСЯ!

Коннектор MSSQL выполняет это следующим образом:

- Находит соответствующую комбинацию `Spark тип` → `MSSQL тип (создание)` (см. ниже) для каждого столбца DataFrame. Если комбинация не найдена, вызывает исключение.
- Генерирует DDL для создания таблицы в MSSQL, например `CREATE TABLE (col1 ...)`, и выполняет его.
- Записывает DataFrame в созданную таблицу как есть.

Но в некоторых случаях это может привести к использованию неправильного типа столбца. Например, Spark создает столбец типа `timestamp`, который соответствует типу MSSQL `timestamp(0)` (точность до секунд) вместо более точного `timestamp(6)` (точность до наносекунд).
Это может привести к случайной потере точности, или иногда данные вообще не могут быть записаны в созданную таблицу.

Поэтому вместо того, чтобы полагаться на Spark для создания таблиц:

??? note "Смотрите пример"

    ```python

        writer = DBWriter(
            connection=mssql,
            target="myschema.target_tbl",
            options=MSSQL.WriteOptions(
                if_exists="append",
            ),
        )
        writer.run(df)
    ```

Всегда предпочтительнее создавать таблицы с конкретными типами **ПЕРЕД ЗАПИСЬЮ ДАННЫХ**:

??? note "Смотрите пример"

    ```python

        mssql.execute(
            """
            CREATE TABLE schema.table (
                id bigint,
                key text,
                value datetime2(6) -- конкретный тип и точность
            )
            """,
        )

        writer = DBWriter(
            connection=mssql,
            target="myschema.target_tbl",
            options=MSSQL.WriteOptions(if_exists="append"),
        )
        writer.run(df)
    ```

### Ссылки

Здесь вы можете найти исходный код с преобразованиями типов:

- [MSSQL -> JDBC](https://github.com/microsoft/mssql-jdbc/blob/v12.2.0/src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResultSetMetaData.java#L117-L170)
- [JDBC -> Spark](https://github.com/apache/spark/blob/v3.5.5/sql/core/src/main/scala/org/apache/spark/sql/jdbc/MsSqlServerDialect.scala#L135-L152)
- [Spark -> JDBC](https://github.com/apache/spark/blob/v3.5.5/sql/core/src/main/scala/org/apache/spark/sql/jdbc/MsSqlServerDialect.scala#L154-L163)
- [JDBC -> MSSQL](https://github.com/microsoft/mssql-jdbc/blob/v12.2.0/src/main/java/com/microsoft/sqlserver/jdbc/DataTypes.java#L625-L676)

## Поддерживаемые типы

См. [официальную документацию](https://learn.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql)

### Числовые типы

| MSSQL тип (чтение)           | Spark тип                        | MSSQL тип (запись)           | MSSQL тип (создание)         |
|-------------------------------|-----------------------------------|-------------------------------|-------------------------------|
| `decimal`                   | `DecimalType(P=18, S=0)`        | `decimal(P=18, S=0)`        | `decimal(P=18, S=0)`        |
| `decimal(P=0..38)`          | `DecimalType(P=0..38, S=0)`     | `decimal(P=0..38, S=0)`     | `decimal(P=0..38, S=0)`     |
| `decimal(P=0..38, S=0..38)` | `DecimalType(P=0..38, S=0..38)` | `decimal(P=0..38, S=0..38)` | `decimal(P=0..38, S=0..38)` |
| `real`                      | `FloatType()`                   | `real`                      | `real`                      |
| `float`                     | `DoubleType()`                  | `float`                     | `float`                     |
| `smallint`                  | `ShortType()`                   | `smallint`                  | `smallint`                  |
| `tinyint`<br/>`int`                   | `IntegerType()`                 | `int`                       | `int`                       |
| `bigint`                    | `LongType()`                    | `bigint`                    | `bigint`                    |

### Временные типы

!!! note

    Тип MSSQL `timestamp` является псевдонимом для `rowversion` (см. `Специальные типы`). Это не временной тип!

| MSSQL тип (чтение)                      | Spark тип                           | MSSQL тип (запись)                | MSSQL тип (создание)           |
|------------------------------------------|--------------------------------------|-----------------------------------|-------------------------------|
| `date`                                 | `DateType()`                       | `date`                          | `date`                      |
| `smalldatetime`, минуты<br/>`datetime`, миллисекунды<br/>`datetime2(0)`, секунды<br/>`datetime2(3)`, миллисекунды               | <br/><br/>`TimestampType()`, микросекунды    | <br/><br/>`datetime2(6)`, микросекунды    | <br/><br/>`datetime`, миллисекунды    |
| `datetime2(6)`, микросекунды           | `TimestampType()`, микросекунды    | `datetime2(6)`, микросекунды    | `datetime`, миллисекунды, **потеря точности** [3]  |
| `datetime2(7)`, 100-е наносекунды    | `TimestampType()`, микросекунды, **потеря точности** [^4]  | `datetime2(6)`, микросекунды, **потеря точности** [^4]  |                               |
| `time(0)`, секунды<br/>`time(3)`, миллисекунды                     | `TimestampType()`, микросекунды, с особенностями формата времени [^5]  | `datetime2(6)`, микросекунды    | `datetime`, миллисекунды    |
| `time(6)`, микросекунды                | `TimestampType()`, микросекунды, с особенностями формата времени [^5]  | `datetime2(6)`, микросекунды    | `datetime`, миллисекунды, **потеря точности** [^3]  |
| `time`, 100-е наносекунды            | `TimestampType()`, микросекунды, **потеря точности** [^4]  | `datetime2(6)`, микросекунды **потеря точности** [^3]   |                               |
| `time(7)`, 100-е наносекунды         | с особенностями формата времени [^5]         |                                   |                               |
| `datetimeoffset`                       | `StringType()`                     | `nvarchar`                      | `nvarchar`                  |

!!! warning

    Обратите внимание, что типы в MSSQL и Spark имеют разные диапазоны значений:

    | MySQL тип        | Минимальное значение              | Максимальное значение              | Spark тип          | Минимальное значение              | Максимальное значение              |
    |-------------------|--------------------------------|--------------------------------|---------------------|--------------------------------|--------------------------------|
    | `smalldatetime`<br/>`datetime`<br/>`datetime2`<br/>`time` | `1900-01-01 00:00:00`<br/>`1753-01-01 00:00:00.000`<br/>`0001-01-01 00:00:00.000000`<br/>`00:00:00.0000000`        | `2079-06-06 23:59:00`<br/>`9999-12-31 23:59:59.997`<br/>`9999-12-31 23:59:59.999999`<br/>`23:59:59.9999999`        | `TimestampType()`   | `0001-01-01 00:00:00.000000`   | `9999-12-31 23:59:59.999999`   |

    Так что не все значения в DataFrame Spark могут быть записаны в MSSQL.

    Ссылки:

    * [Документация по типам даты и времени MSSQL](https://learn.microsoft.com/en-us/sql/t-sql/data-types/date-and-time-types)
    * [Документация по DateType в Spark](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/types/DateType.html)
    * [Документация по TimestampType в Spark](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/types/TimestampType.html)

[^3]: Диалект MSSQL для Spark генерирует DDL с типом `datetime`, который имеет точность до миллисекунд (`23:59:59.999`, 10{superscript}`-3` секунд). Вставка данных с точностью микросекунд и выше (`23:59:59.999999` .. `23.59:59.9999999`, 10{superscript}`-6` .. 10{superscript}`-7` секунд) приведет к **отбрасыванию микросекунд**.

[^4]: MSSQL поддерживает отметки времени с точностью до 100-х наносекунд (`23:59:59.9999999999`, 10{superscript}`-7` секунд), но Spark `TimestampType()` поддерживает datetime с точностью до микросекунд (`23:59:59.999999`, 10{superscript}`-6` секунд). Последняя цифра будет потеряна во время операций чтения или записи.

[^5]: Тип `time` такой же, как `datetime2` с датой `1970-01-01`. Поэтому вместо чтения данных из MSSQL как `23:59:59.999999` фактически читается `1970-01-01 23:59:59.999999`, и наоборот.

### Строковые типы

| MSSQL тип (чтение) | Spark тип       | MSSQL тип (запись) | MSSQL тип (создание) |
|-------------------|------------------|--------------------|---------------------|
| `char`<br/>`char(N)`<br/>`nchar`<br/>`nchar(N)`<br/>`varchar`<br/>`varchar(N)`<br/>`nvarchar`<br/>`nvarchar(N)`<br/>`mediumtext`<br/>`text`<br/>`ntext`<br/>`xml`          | <br/><br/><br/><br/>`StringType()` | <br/><br/><br/><br/>`nvarchar`       | <br/><br/><br/><br/>`nvarchar`        |

### Бинарные типы

| MSSQL тип (чтение)  | Spark тип        | MSSQL тип (запись) | MSSQL тип (создание) |
|--------------------|-------------------|--------------------|---------------------|
| `bit`            | `BooleanType()` | `bit`            | `bit`             |
| `binary`<br/>`binary(N)`<br/>`varbinary`<br/>`varbinary(N)`<br/>`image`         | <br/><br/>`BinaryType()`  | <br/><br/>`varbinary`      | <br/><br/>`varbinary`       |

### Специальные типы

| MSSQL тип (чтение)         | Spark тип       | MSSQL тип (запись) | MSSQL тип (создание) |
|---------------------------|------------------|--------------------|---------------------|
| `geography`<br/>`geometry`<br/>`hierarchyid`<br/>`rowversion`             | <br/><br/>`BinaryType()` | <br/><br/>`varbinary`      | <br/><br/>`varbinary`       |
| `sql_variant`           | не поддерживается      |                    |                     |
| `sysname`<br/> `uniqueidentifier`              | `StringType()` | `nvarchar`       | `nvarchar`        |

## Явное приведение типов

### `DBReader`

Возможно явно привести тип столбца с использованием синтаксиса `DBReader(columns=...)`.

Например, вы можете использовать `CAST(column AS text)` для преобразования данных в строковое представление на стороне MSSQL, и так они будут прочитаны как `StringType()` Spark:

    ```python
        from onetl.connection import MSSQL
        from onetl.db import DBReader

        mssql = MSSQL(...)

        DBReader(
            connection=mssql,
            columns=[
                "id",
                "supported_column",
                "CAST(unsupported_column AS text) unsupported_column_str",
            ],
        )
        df = reader.run()

        # приведение содержимого столбца к правильному типу Spark
        df = df.select(
            df.id,
            df.supported_column,
            # явное приведение
            df.unsupported_column_str.cast("integer").alias("parsed_integer"),
        )
    ```

### `DBWriter`

Преобразуйте столбец dataframe в JSON с помощью [to_json](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_json.html), и запишите его как столбец `text` в MSSQL:

    ```python
        mssql.execute(
            """
            CREATE TABLE schema.target_tbl (
                id bigint,
                struct_column_json text -- любой строковый тип, на самом деле
            )
            """,
        )

        from pyspark.sql.functions import to_json

        df = df.select(
            df.id,
            to_json(df.struct_column).alias("struct_column_json"),
        )

        writer.run(df)
    ```

Затем вы можете разобрать этот столбец на стороне MSSQL - например, создав представление:

    ```sql
        SELECT
            id,
            JSON_VALUE(struct_column_json, "$.nested.field") AS nested_field
        FROM target_tbl
    ```

Или с помощью [вычисляемого столбца](https://learn.microsoft.com/en-us/sql/relational-databases/tables/specify-computed-columns-in-a-table):

    ```sql
        CREATE TABLE schema.target_table (
            id bigint,
            supported_column datetime2(6),
            struct_column_json text, -- любой строковый тип, на самом деле
            -- вычисляемый столбец
            nested_field AS (JSON_VALUE(struct_column_json, "$.nested.field"))
            -- или постоянный столбец
            -- nested_field AS (JSON_VALUE(struct_column_json, "$.nested.field")) PERSISTED
        )
    ```

По умолчанию значение столбца вычисляется при каждом чтении таблицы.
Столбец, помеченный как `PERSISTED`, вычисляется во время вставки, но для этого требуется дополнительное пространство.
