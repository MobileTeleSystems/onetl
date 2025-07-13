# Соответствие типов Oracle <-> Spark { #oracle-types }

!!! note

    Результаты ниже действительны для Spark 3.5.5 и могут отличаться в других версиях Spark.

## Определение типов и преобразование

DataFrame в Spark всегда имеют `schema`, которая представляет собой список столбцов с соответствующими типами Spark. Все операции со столбцом выполняются с использованием типа столбца.

### Чтение из Oracle

Коннектор Oracle выполняет это следующим образом:

- Для каждого столбца в результате запроса (`SELECT column1, column2, ... FROM table ...`) получает имя столбца и тип Oracle.
- Находит соответствующую комбинацию `Тип Oracle (чтение)` → `Тип Spark` (см. ниже) для каждого столбца DataFrame. Если комбинация не найдена, вызывается исключение.
- Создает DataFrame из запроса с указанными именами столбцов и типами Spark.

### Запись в существующую таблицу Oracle

Вот как коннектор Oracle выполняет это:

- Получает имена столбцов в DataFrame. [^1]
- Выполняет запрос `SELECT * FROM table LIMIT 0`.
- Выбирает только столбцы, присутствующие в DataFrame (по имени, без учета регистра). Для каждого найденного столбца получает тип Clickhouse.
- **Находит соответствующую комбинацию** `Тип Oracle (чтение)` → `Тип Spark` (см. ниже) для каждого столбца DataFrame. Если комбинация не найдена, вызывается исключение. [^2]
- Находит соответствующую комбинацию `Тип Spark` → `Тип Oracle (запись)` (см. ниже) для каждого столбца DataFrame. Если комбинация не найдена, вызывается исключение.
- Если `Тип Oracle (запись)` соответствует `Тип Oracle (чтение)`, дополнительные преобразования не выполняются, столбец DataFrame будет записан в Oracle как есть.
- Если `Тип Oracle (запись)` не соответствует `Тип Oracle (чтение)`, столбец DataFrame будет преобразован в целевой тип столбца **на стороне Oracle**.
  Например, вы можете записать столбец с текстовыми данными в столбец `int`, если столбец содержит допустимые целочисленные значения в пределах поддерживаемого диапазона значений и точности.

[^1]: Это позволяет записывать данные в таблицы со столбцами `DEFAULT` и `GENERATED` - если в DataFrame нет такого столбца, он будет заполнен Oracle.

[^2]: Да, это странно.

### Создание новой таблицы с помощью Spark

!!! warning

    КАТЕГОРИЧЕСКИ НЕ РЕКОМЕНДУЕТСЯ!

Вот как коннектор Oracle выполняет это:

- Находит соответствующую комбинацию `Тип Spark` → `Тип Oracle (создание)` (см. ниже) для каждого столбца DataFrame. Если комбинация не найдена, вызывается исключение.
- Генерирует DDL для создания таблицы в Oracle, например `CREATE TABLE (col1 ...)`, и выполняет его.
- Записывает DataFrame в созданную таблицу как есть.

Но коннектор Oracle поддерживает только ограниченное количество типов и почти не поддерживает пользовательские предложения (такие как `PARTITION BY`, `INDEX` и т.д.).
Поэтому вместо того, чтобы полагаться на Spark для создания таблиц:

??? note "Смотрите пример"

    ```python
        writer = DBWriter(
            connection=oracle,
            target="public.table",
            options=Oracle.WriteOptions(if_exists="append"),
        )
        writer.run(df)
    ```

Всегда предпочтительнее создавать таблицу с нужным DDL **ПЕРЕД ЗАПИСЬЮ ДАННЫХ**:

??? note "Смотрите пример"

    ```python
        oracle.execute(
            """
            CREATE TABLE username.table (
                id NUMBER,
                business_dt TIMESTAMP(6),
                value VARCHAR2(2000)
            )
            """,
        )

        writer = DBWriter(
            connection=oracle,
            target="public.table",
            options=Oracle.WriteOptions(if_exists="append"),
        )
        writer.run(df)
    ```

См. документацию Oracle [CREATE TABLE](https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/SELECT.html).

## Поддерживаемые типы

### Ссылки

См. [Список типов Oracle](https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/Data-Types.html).

Здесь вы можете найти исходный код с преобразованиями типов:

- [JDBC -> Spark](https://github.com/apache/spark/blob/v3.5.5/sql/core/src/main/scala/org/apache/spark/sql/jdbc/OracleDialect.scala#L83-L109)
- [Spark -> JDBC](https://github.com/apache/spark/blob/v3.5.5/sql/core/src/main/scala/org/apache/spark/sql/jdbc/OracleDialect.scala#L111-L123)

### Числовые типы

| Тип Oracle (чтение)              | Тип Spark                        | Тип Oracle (запись)           | Тип Oracle (создание)      |
|----------------------------------|-----------------------------------|-------------------------------|---------------------------|
| `NUMBER`                       | `DecimalType(P=38, S=10)`       | `NUMBER(P=38, S=10)`        | `NUMBER(P=38, S=10)`    |
| `NUMBER(P=0..38)`              | `DecimalType(P=0..38, S=0)`     | `NUMBER(P=0..38, S=0)`      | `NUMBER(P=38, S=0)`     |
| `NUMBER(P=0..38, S=0..38)`     | `DecimalType(P=0..38, S=0..38)` | `NUMBER(P=0..38, S=0..38)`  | `NUMBER(P=38, S=0..38)` |
| `NUMBER(P=..., S=-127..-1)`    | не поддерживается [^3]                  |                               |                           |
| `FLOAT`<br/>`FLOAT(N)`<br/>`REAL`<br/>`DOUBLE PRECISION` | <br/>`DecimalType(P=38, S=10)`       | <br/>`NUMBER(P=38, S=10)`        | <br/>`NUMBER(P=38, S=10)`    |
| `BINARY_FLOAT`<br/>`BINARY_DOUBLE`                 | `FloatType()`<br/>`DoubleType()`                   | `NUMBER(P=19, S=4)`         | `NUMBER(P=19, S=4)`     |
| `SMALLINT`<br/>`INTEGER`       | `DecimalType(P=38, S=0)`        | `NUMBER(P=38, S=0)`         | `NUMBER(P=38, S=0)`     |
| `LONG`                         | `StringType()`                  | `CLOB`                      | `CLOB`                  |

[^3]: Oracle поддерживает десятичные типы с отрицательной шкалой, например `NUMBER(38, -10)`. Spark не поддерживает.

### Временные типы

| Тип Oracle (чтение)                         | Тип Spark                         | Тип Oracle (запись)             | Тип Oracle (создание)            |
|--------------------------------------------|------------------------------------|---------------------------------|---------------------------------|
| `DATE`, дни                             | `TimestampType()`, микросекунды  | `TIMESTAMP(6)`, микросекунды  | `TIMESTAMP(6)`, микросекунды  |
| `TIMESTAMP`, микросекунды<br/>`TIMESTAMP(0)`, секунды<br/>`TIMESTAMP(3)`, миллисекунды<br/>`TIMESTAMP(6)`, микросекунды               | <br/><br/>`TimestampType()`, микросекунды  | <br/><br/>`TIMESTAMP(6)`, микросекунды  | <br/><br/>`TIMESTAMP(6)`, микросекунды **потеря точности** |
| `TIMESTAMP(9)`, наносекунды              | `TimestampType()`, микросекунды, **потеря точности** [^4] | `TIMESTAMP(6)`, микросекунды, **потеря точности** | `TIMESTAMP(6)`, микросекунды, |
| `TIMESTAMP WITH TIME ZONE`<br/>`TIMESTAMP(N) WITH TIME ZONE`<br/>`TIMESTAMP WITH LOCAL TIME ZONE`<br/>`TIMESTAMP(N) WITH LOCAL TIME ZONE`<br/>`INTERVAL YEAR TO MONTH`<br/>`INTERVAL DAY TO SECOND`               | <br/><br/><br/>не поддерживается                        |                                 |                                 |

!!! warning

    Обратите внимание, что типы в Oracle и Spark имеют разные диапазоны значений:

    | Тип Oracle   | Минимальное значение                          | Максимальное значение                         | Тип Spark          | Минимальное значение                      | Максимальное значение                      |
    |---------------|------------------------------------|-----------------------------------|---------------------|--------------------------------|--------------------------------|
    | `date`      | `-4712-01-01`                    | `9999-01-01`                    | `DateType()`      | `0001-01-01`                 | `9999-12-31`                 |
    | `timestamp` | `-4712-01-01 00:00:00.000000000` | `9999-12-31 23:59:59.999999999` | `TimestampType()` | `0001-01-01 00:00:00.000000` | `9999-12-31 23:59:59.999999` |

    Таким образом, не все значения могут быть прочитаны из Oracle в Spark.

    Ссылки:

    * [Документация Oracle по date, timestamp и интервалам](https://oracle-base.com/articles/misc/oracle-dates-timestamps-and-intervals#DATE)
    * [Документация Spark DateType](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/types/DateType.html)
    * [Документация Spark TimestampType](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/types/TimestampType.html)

[^4]: Oracle поддерживает timestamp с точностью до наносекунд (`23:59:59.999999999`), но `TimestampType()` в Spark поддерживает datetime с точностью до микросекунд (`23:59:59.999999`). Наносекунды будут потеряны во время операций чтения или записи.

### Строковые типы

| Тип Oracle (чтение)          | Тип Spark       | Тип Oracle (запись) | Тип Oracle (создание) |
|-----------------------------|------------------|---------------------|----------------------|
| `CHAR`<br/>`CHAR(N CHAR)`<br/>`CHAR(N BYTE)`<br/>`NCHAR`<br/>`NCHAR(N)`<br/>`VARCHAR(N)`<br/>`LONG VARCHAR`<br/>`VARCHAR2(N CHAR)`<br/>`VARCHAR2(N BYTE)`<br/>`NVARCHAR2(N)`<br/>`CLOB`<br/>`NCLOB`                    | <br/><br/><br/><br/><br/>`StringType()` | <br/><br/><br/><br/><br/>`CLOB`            | <br/><br/><br/><br/><br/>`CLOB`             |

### Бинарные типы

| Тип Oracle (чтение)       | Тип Spark       | Тип Oracle (запись) | Тип Oracle (создание) |
|--------------------------|------------------|---------------------|----------------------|
| `RAW(N)`<br/>`LONG RAW`<br/>`BLOB`               | <br/>`BinaryType()` | <br/>`BLOB`            | <br/>`BLOB`             |
| `BFILE`                | не поддерживается      |                     |                      |

### Структурные типы

| Тип Oracle (чтение)                  | Тип Spark       | Тип Oracle (запись) | Тип Oracle (создание) |
|-------------------------------------|------------------|---------------------|----------------------|
| `XMLType`<br/>`URIType`<br/>`DBURIType`<br/>`XDBURIType`<br/>`HTTPURIType`<br/>`CREATE TYPE ... AS OBJECT (...)`                         | `StringType()` | `CLOB`            | `CLOB`             |
| `JSON`<br/>`CREATE TYPE ... AS VARRAY ...`<br/>`CREATE TYPE ... AS TABLE OF ...`                            | <br/>не поддерживается      |                     |                      |

### Специальные типы

| Тип Oracle (чтение) | Тип Spark        | Тип Oracle (запись) | Тип Oracle (создание) |
|--------------------|-------------------|---------------------|----------------------|
| `BOOLEAN`        | `BooleanType()` | `BOOLEAN`         | `NUMBER(P=1, S=0)` |
| `ROWID`<br/>`UROWID`<br/>`UROWID(N)`          | <br/>`StringType()`  | <br/>`CLOB`            | <br/>`CLOB`             |
| `ANYTYPE`<br/>`ANYDATA`<br/>`ANYDATASET`        | <br/>не поддерживается       |                     |                      |

## Явное преобразование типов

### `DBReader`

Возможно явно преобразовать столбец неподдерживаемого типа с помощью синтаксиса `DBReader(columns=...)`.

Например, вы можете использовать `CAST(column AS CLOB)` для преобразования данных в строковое представление на стороне Oracle, и тогда они будут прочитаны как `StringType()` в Spark.

Также возможно использовать функции Oracle [JSON_ARRAY](https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/JSON_ARRAY.html) или [JSON_OBJECT](https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/JSON_OBJECT.html) для преобразования столбца любого типа в строковое представление. Затем эта строка JSON может быть эффективно разобрана с помощью метода [JSON.parse_column][onetl.file.format.json.JSON.parse_column].

    ```python
        from onetl.file.format import JSON
        from pyspark.sql.types import IntegerType, StructType, StructField

        from onetl.connection import Oracle
        from onetl.db import DBReader

        oracle = Oracle(...)

        DBReader(
            connection=oracle,
            columns=[
                "id",
                "supported_column",
                "CAST(unsupported_column AS VARCHAR2(4000)) unsupported_column_str",
                # или
                "JSON_ARRAY(array_column) array_column_json",
            ],
        )
        df = reader.run()

        json_scheme = StructType([StructField("key", IntegerType())])

        df = df.select(
            df.id,
            df.supported_column,
            df.unsupported_column_str.cast("integer").alias("parsed_integer"),
            JSON().parse_column("array_column_json", json_scheme).alias("array_column"),
        )
    ```

### `DBWriter`

Всегда возможно преобразовать данные на стороне Spark в строку, а затем записать их в текстовый столбец в таблице Oracle.

Для сериализации и записи данных JSON в столбец `text` или `json` в таблице Oracle используйте метод [JSON.serialize_column][onetl.file.format.json.JSON.serialize_column].

    ```python
        from onetl.connection import Oracle
        from onetl.db import DBWriter
        from onetl.file.format import JSON

        oracle = Oracle(...)

        oracle.execute(
            """
            CREATE TABLE schema.target_table (
                id INTEGER,
                supported_column TIMESTAMP,
                array_column_json VARCHAR2(4000) -- любой строковый тип, на самом деле
            )
            """,
        )

        write_df = df.select(
            df.id,
            df.supported_column,
            JSON().serialize_column(df.unsupported_column).alias("array_column_json"),
        )

        writer = DBWriter(
            connection=oracle,
            target="schema.target_table",
        )
        writer.run(write_df)
    ```

Затем вы можете разобрать этот столбец на стороне Oracle - например, создав представление:

    ```sql
        SELECT
            id,
            supported_column,
            JSON_VALUE(array_column_json, '$[0]' RETURNING NUMBER) AS array_item_0
        FROM
            schema.target_table
    ```

Или используя [VIRTUAL столбец](https://oracle-base.com/articles/11g/virtual-columns-11gr1):

    ```sql
        CREATE TABLE schema.target_table (
            id INTEGER,
            supported_column TIMESTAMP,
            array_column_json VARCHAR2(4000), -- любой строковый тип, на самом деле
            array_item_0 GENERATED ALWAYS AS (JSON_VALUE(array_column_json, '$[0]' RETURNING NUMBER)) VIRTUAL
        )
    ```

Но данные будут разбираться при каждом чтении таблицы в любом случае, так как Oracle не поддерживает столбцы `GENERATED ALWAYS AS (...) STORED`.
