# Сопоставление типов MySQL <-> Spark { #mysql-types }

!!! note

    Результаты ниже действительны для Spark 3.5.5 и могут отличаться в других версиях Spark.

## Определение типов и приведение

DataFrame в Spark всегда имеют `schema`, которая представляет собой список столбцов с соответствующими типами Spark. Все операции со столбцом выполняются с использованием типа столбца.

### Чтение из MySQL { #mysql-read-0 }

Вот как коннектор MySQL выполняет это:

- Для каждого столбца в результате запроса (`SELECT column1, column2, ... FROM table ...`) получает имя столбца и тип MySQL.
- Находит соответствующую комбинацию `MySQL тип (чтение)` → `Spark тип` (см. ниже) для каждого столбца DataFrame. Если комбинация не найдена, вызывает исключение.
- Создает DataFrame из запроса с определенными именами столбцов и типами Spark.

### Запись в существующую таблицу MySQL { #mysql-table }

Вот как коннектор MySQL выполняет это:

- Получает имена столбцов в DataFrame. [^1]
- Выполняет запрос `SELECT * FROM table LIMIT 0`.
- Выбирает только столбцы, присутствующие в DataFrame (по имени, без учета регистра). Для каждого найденного столбца получает тип MySQL.
- Находит соответствующую комбинацию `Spark тип` → `MySQL тип (запись)` (см. ниже) для каждого столбца DataFrame. Если комбинация не найдена, вызывает исключение.
- Если `MySQL тип (запись)` соответствует `MySQL тип (чтение)`, дополнительные приведения не выполняются, столбец DataFrame будет записан в MySQL как есть.
- Если `MySQL тип (запись)` не соответствует `MySQL тип (чтение)`, столбец DataFrame будет приведен к целевому типу столбца **на стороне MySQL**. Например, вы можете записать столбец с текстовыми данными в столбец `int`, если столбец содержит допустимые целочисленные значения в пределах поддерживаемого диапазона значений и точности.

[^1]: Это позволяет записывать данные в таблицы со столбцами `DEFAULT` и `GENERATED` - если в DataFrame нет такого столбца, он будет заполнен MySQL.

### Создание новой таблицы с помощью Spark

!!! warning

    АБСОЛЮТНО НЕ РЕКОМЕНДУЕТСЯ!

Вот как коннектор MySQL выполняет это:

- Находит соответствующую комбинацию `Spark тип` → `MySQL тип (создание)` (см. ниже) для каждого столбца DataFrame. Если комбинация не найдена, вызывает исключение.
- Генерирует DDL для создания таблицы в MySQL, например `CREATE TABLE (col1 ...)`, и запускает его.
- Записывает DataFrame в созданную таблицу как есть.

Но в некоторых случаях это может привести к использованию неправильного типа столбца. Например, Spark создает столбец типа `timestamp`, который соответствует типу MySQL `timestamp(0)` (точность до секунд) вместо более точного `timestamp(6)` (точность до наносекунд).
Это может привести к случайной потере точности, или иногда данные вообще не могут быть записаны в созданную таблицу.

Поэтому вместо того, чтобы полагаться на Spark для создания таблиц:

??? note "Смотрите пример"

    ```python

        writer = DBWriter(
            connection=mysql,
            target="myschema.target_tbl",
            options=MySQL.WriteOptions(
                if_exists="append",
                createTableOptions="ENGINE = InnoDB",
            ),
        )
        writer.run(df)
    ```

Всегда предпочтительнее создавать таблицы с конкретными типами **ДО ЗАПИСИ ДАННЫХ**:

??? note "Смотрите пример"

    ```python

        mysql.execute(
            """
            CREATE TABLE schema.table (
                id bigint,
                key text,
                value timestamp(6) -- конкретный тип и точность
            )
            ENGINE = InnoDB
            """,
        )

        writer = DBWriter(
            connection=mysql,
            target="myschema.target_tbl",
            options=MySQL.WriteOptions(if_exists="append"),
        )
        writer.run(df)  
    ```

### Ссылки

Здесь вы можете найти исходный код с преобразованиями типов:

- [MySQL -> JDBC](https://github.com/mysql/mysql-connector-j/blob/8.0.33/src/main/core-api/java/com/mysql/cj/MysqlType.java#L44-L623)
- [JDBC -> Spark](https://github.com/apache/spark/blob/v3.5.5/sql/core/src/main/scala/org/apache/spark/sql/jdbc/MySQLDialect.scala#L104-L132)
- [Spark -> JDBC](https://github.com/apache/spark/blob/v3.5.5/sql/core/src/main/scala/org/apache/spark/sql/jdbc/MySQLDialect.scala#L204-L211)
- [JDBC -> MySQL](https://github.com/mysql/mysql-connector-j/blob/8.0.33/src/main/core-api/java/com/mysql/cj/MysqlType.java#L625-L867)

## Поддерживаемые типы

См. [официальную документацию](https://dev.mysql.com/doc/refman/en/data-types.html)

### Числовые типы

| MySQL тип (чтение)            | Spark тип                        | MySQL тип (запись)            | MySQL тип (создание)           |
|-------------------------------|-----------------------------------|-------------------------------|-------------------------------|
| `decimal`                   | `DecimalType(P=10, S=0)`        | `decimal(P=10, S=0)`        | `decimal(P=10, S=0)`        |
| `decimal(P=0..38)`          | `DecimalType(P=0..38, S=0)`     | `decimal(P=0..38, S=0)`     | `decimal(P=0..38, S=0)`     |
| `decimal(P=0..38, S=0..30)` | `DecimalType(P=0..38, S=0..30)` | `decimal(P=0..38, S=0..30)` | `decimal(P=0..38, S=0..30)` |
| `decimal(P=39..65, S=...)`  | не поддерживается [^2]                  |                               |                               |
| `float`<br/>`double`                     | `DoubleType()`                  | `double`                    | `double`                    |
| `tinyint`<br/>`smallint`<br/>`mediumint`<br/>`int`                   | <br/><br/>`IntegerType()`                 | <br/><br/>`int`                       | <br/><br/>`int`                       |
| `bigint`                    | `LongType()`                    | `bigint`                    | `bigint`                    |

[^2]: MySQL поддерживает десятичные типы с точностью `P` до 65.

    Но `DecimalType(P, S)` в Spark поддерживает максимум `P=38`. Невозможно читать, записывать или работать со значениями большей точности,
    это приводит к исключению.

### Временные типы

| MySQL тип (чтение)                 | Spark тип                           | MySQL тип (запись)                | MySQL тип (создание)           |
|-----------------------------------|--------------------------------------|-----------------------------------|-------------------------------|
| `year`<br/>`date`                          | `DateType()`                       | `date`                          | `date`                      |
| `datetime`, секунды<br/>`timestamp`, секунды<br/>`datetime(0)`, секунды<br/>`timestamp(0)`, секунды             | <br/><br/>`TimestampType()`, микросекунды    | <br/><br/>`timestamp(6)`, микросекунды    | <br/><br/>`timestamp(0)`, секунды     |
| `datetime(3)`, миллисекунды<br/>`timestamp(3)`, миллисекунды<br/>`datetime(6)`, микросекунды<br/>`timestamp(6)`, микросекунды     | <br/><br/>`TimestampType()`, микросекунды    | <br/><br/>`timestamp(6)`, микросекунды    | <br/><br/>`timestamp(0)`, секунды, **потеря точности** [^3],   |
| `time`, секунды<br/>`time(0)`, секунды                 | `TimestampType()`, микросекунды, с особенностями формата времени [^4]  | `timestamp(6)`, микросекунды    | `timestamp(0)`, секунды     |
| `time(3)`, миллисекунды<br/>`time(6)`, микросекунды         | `TimestampType()`, микросекунды, с особенностями формата времени [^4]    | `timestamp(6)`, микросекунды    | `timestamp(0)`, секунды, **потеря точности** [^3],   |

!!! warning

    Обратите внимание, что типы в MySQL и Spark имеют разные диапазоны значений:

    
    | MySQL тип    | Минимальное значение                      | Максимальное значение                      | Spark тип          | Минимальное значение                      | Максимальное значение                      |
    |---------------|--------------------------------|--------------------------------|---------------------|--------------------------------|--------------------------------|
    | `year`<br/>`date`      | `1901`<br/>`1000-01-01`                       | `2155`<br/>`9999-12-31`                       | `DateType()`      | `0001-01-01`                 | `9999-12-31`                 |
    | `datetime`<br/>`timestamp`<br/>`time`  | `1000-01-01 00:00:00.000000`<br/>`1970-01-01 00:00:01.000000`<br/>`-838:59:59.000000` | `9999-12-31 23:59:59.499999`<br/>`9999-12-31 23:59:59.499999`<br/>`838:59:59.000000` | `TimestampType()` | `0001-01-01 00:00:00.000000` | `9999-12-31 23:59:59.999999` |

    Таким образом, Spark может прочитать все значения из MySQL, но не все значения в DataFrame Spark могут быть записаны в MySQL.

    Ссылки:

    * [Документация MySQL year](https://dev.mysql.com/doc/refman/en/year.html)
    * [Документация MySQL date, datetime и timestamp](https://dev.mysql.com/doc/refman/en/datetime.html)
    * [Документация MySQL time](https://dev.mysql.com/doc/refman/en/time.html)
    * [Документация Spark DateType](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/types/DateType.html)
    * [Документация Spark TimestampType](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/types/TimestampType.html)

[^3]: Диалект MySQL генерирует DDL с типом MySQL `timestamp`, который является псевдонимом для `timestamp(0)` с точностью до секунд (`23:59:59`). Вставка данных с точностью до микросекунд (`23:59:59.999999`) приведет к **отбрасыванию микросекунд**.

[^4]: Тип `time` такой же, как `timestamp` с датой `1970-01-01`. Поэтому вместо чтения данных из MySQL вида `23:59:59` фактически читается `1970-01-01 23:59:59`, и наоборот.

### Строковые типы

| MySQL тип (чтение)             | Spark тип       | MySQL тип (запись) | MySQL тип (создание) |
|-------------------------------|------------------|--------------------|---------------------|
| `char`<br/>`char(N)`<br/>`varchar(N)`<br/>`mediumtext`<br/>`text`<br/>`longtext`<br/>`json`<br/>`enum("val1", "val2", ...)`<br/>`set("val1", "val2", ...)`  | <br/><br/><br/><br/>`StringType()` | <br/><br/><br/><br/>`longtext`       | <br/><br/><br/><br/>`longtext`        |

### Бинарные типы

| MySQL тип (чтение) | Spark тип       | MySQL тип (запись) | MySQL тип (создание) |
|-------------------|------------------|--------------------|---------------------|
| `binary`<br/>`binary(N)`<br/>`varbinary(N)`<br/>`mediumblob`<br/>`blob`<br/>`longblob`        | <br/><br/><br/>`BinaryType()` | <br/><br/><br/>`blob`           | <br/><br/><br/>`blob`            |

### Геометрические типы

| MySQL тип (чтение)      | Spark тип       | MySQL тип (запись) | MySQL тип (создание) |
|------------------------|------------------|--------------------|---------------------|
| `point`<br/>`linestring`<br/>`polygon`<br/>`geometry`<br/>`multipoint`<br/>`multilinestring`<br/>`multipolygon`<br/>`geometrycollection`              | <br/><br/><br/><br/>`BinaryType()` | <br/><br/><br/><br/>`blob`           | <br/><br/><br/><br/>`blob`            |

## Явное приведение типов

### `DBReader`

Возможно явно приводить тип столбца, используя синтаксис `DBReader(columns=...)`.

Например, вы можете использовать `CAST(column AS text)` для преобразования данных в строковое представление на стороне MySQL, и тогда они будут прочитаны как `StringType()` в Spark.

Также возможно использовать функцию MySQL [JSON_OBJECT](https://dev.mysql.com/doc/refman/en/json.html) и парсить JSON-столбцы в MySQL с помощью метода [JSON.parse_column][onetl.file.format.json.JSON.parse_column].

    ```python
        from pyspark.sql.types import IntegerType, StructType, StructField

        from onetl.connection import MySQL
        from onetl.db import DBReader
        from onetl.file.format import JSON

        mysql = MySQL(...)

        DBReader(
            connection=mysql,
            columns=[
                "id",
                "supported_column",
                "CAST(unsupported_column AS text) unsupported_column_str",
                # или
                "JSON_OBJECT('key', value_column) json_column",
            ],
        )
        df = reader.run()

        json_scheme = StructType([StructField("key", IntegerType())])

        df = df.select(
            df.id,
            df.supported_column,
            # явное приведение
            df.unsupported_column_str.cast("integer").alias("parsed_integer"),
            JSON().parse_column("json_column", json_scheme).alias("struct_column"),
        )  
    ```

### `DBWriter`

Для записи данных JSON в столбец `json` или `text` в таблице MySQL используйте метод [JSON.serialize_column][onetl.file.format.json.JSON.serialize_column].

    ```python
        from onetl.connection import MySQL
        from onetl.db import DBWriter
        from onetl.file.format import JSON

        mysql.execute(
            """
            CREATE TABLE schema.target_tbl (
                id bigint,
                array_column_json json -- на самом деле, любой строковый тип
            )
            ENGINE = InnoDB
            """,
        )

        df = df.select(
            df.id,
            JSON().serialize_column(df.array_column).alias("array_column_json"),
        )

        writer.run(df)  
    ```

Затем вы можете парсить этот столбец на стороне MySQL - например, создав представление:

    ```sql
        SELECT
            id,
            array_column_json->"$[0]" AS array_item
        FROM target_tbl
    ```

Или используя [GENERATED column](https://dev.mysql.com/doc/refman/en/create-table-generated-columns.html):

    ```sql
        CREATE TABLE schema.target_table (
            id bigint,
            supported_column timestamp,
            array_column_json json, -- на самом деле, любой строковый тип
            -- виртуальный столбец
            array_item_0 GENERATED ALWAYS AS (array_column_json->"$[0]")) VIRTUAL
            -- или сохраняемый столбец
            -- array_item_0 GENERATED ALWAYS AS (array_column_json->"$[0]")) STORED
        )
    ```

Значение столбца `VIRTUAL` вычисляется при каждом чтении таблицы.
Значение столбца `STORED` вычисляется при вставке, но это требует дополнительного пространства.
