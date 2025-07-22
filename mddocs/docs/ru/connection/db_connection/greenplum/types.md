# Соответствие типов Greenplum <-> Spark { #greenplum-types }

!!! note

    Результаты ниже действительны для Spark 3.2.4 и могут отличаться в других версиях Spark.

## Определение типов и приведение

DataFrame в Spark всегда имеют `schema`, которая представляет собой список столбцов с соответствующими типами Spark. Все операции над столбцом выполняются с использованием типа столбца.

### Чтение из Greenplum { #greenplum-read-0 }

Коннектор Greenplum выполняет следующие действия:

- Выполняет запрос `SELECT * FROM table LIMIT 0` [^1].
- Для каждого столбца в результате запроса получает имя столбца и тип Greenplum.
- Находит соответствующую комбинацию `Тип Greenplum (чтение)` → `Тип Spark` (см. ниже) для каждого столбца DataFrame. Если комбинация не найдена, выбрасывает исключение.
- Использует функции проекции столбцов Spark и предикаты pushdown для построения финального запроса.
- Создает DataFrame из сгенерированного запроса с выведенной схемой.

[^1]: Да, **все столбцы таблицы**, а не только выбранные.
    Это означает, что если исходная таблица **содержит** столбцы с неподдерживаемым типом, всю таблицу невозможно прочитать.

### Запись в существующую таблицу Greenplum { #greenplum-table }

Коннектор Greenplum выполняет следующие действия:

- Получает имена столбцов в DataFrame.
- Выполняет запрос `SELECT * FROM table LIMIT 0`.
- Для каждого столбца в результате запроса получает имя столбца и тип Greenplum.
- Сопоставляет столбцы таблицы со столбцами DataFrame (по имени, без учета регистра).
  Если какой-то столбец присутствует только в целевой таблице, но не в DataFrame (например, столбцы с `DEFAULT` или `SERIAL`), и наоборот, выбрасывает исключение.
  См. [Явное приведение типов][explicit-type-cast].
- Находит соответствующую комбинацию `Тип Spark` → `Тип Greenplum (запись)` (см. ниже) для каждого столбца DataFrame. Если комбинация не найдена, выбрасывает исключение.
- Если `Тип Greenplum (запись)` совпадает с `Тип Greenplum (чтение)`, дополнительные преобразования не выполняются, столбец DataFrame записывается в Greenplum как есть.
- Если `Тип Greenplum (запись)` не совпадает с `Тип Greenplum (чтение)`, столбец DataFrame будет приведен к типу целевого столбца **на стороне Greenplum**. Например, вы можете записать столбец с текстовыми данными в столбец `json`, который коннектор Greenplum в настоящее время не поддерживает.

### Создание новой таблицы с помощью Spark

!!! warning

    КАТЕГОРИЧЕСКИ НЕ РЕКОМЕНДУЕТСЯ!

Коннектор Greenplum выполняет следующие действия:

- Находит соответствующую комбинацию `Тип Spark` → `Тип Greenplum (создание)` (см. ниже) для каждого столбца DataFrame. Если комбинация не найдена, выбрасывает исключение.
- Генерирует DDL для создания таблицы в Greenplum, например `CREATE TABLE (col1 ...)`, и выполняет его.
- Записывает DataFrame в созданную таблицу как есть.

Более подробную информацию [можно найти здесь](https://docs.vmware.com/en/VMware-Greenplum-Connector-for-Apache-Spark/2.3/greenplum-connector-spark/write_to_gpdb.html).

Но коннектор Greenplum поддерживает только ограниченное количество типов и почти не поддерживает пользовательские выражения (такие как `PARTITION BY`).
Поэтому вместо того, чтобы полагаться на Spark для создания таблиц:

??? note "Посмотреть пример"

    ```python

        writer = DBWriter(
            connection=greenplum,
            target="public.table",
            options=Greenplum.WriteOptions(
                if_exists="append",
                # по умолчанию распределение случайное
                distributedBy="id",
                # partitionBy не поддерживается
            ),
        )
        writer.run(df) 
    ```

Всегда предпочитайте создавать таблицу с нужным DDL **ПЕРЕД ЗАПИСЬЮ ДАННЫХ**:

??? note "Посмотреть пример"

    ```python

        greenplum.execute(
            """
            CREATE TABLE public.table (
                id int32,
                business_dt timestamp(6),
                value json
            )
            PARTITION BY RANGE (business_dt)
            DISTRIBUTED BY id
            """,
        )

        writer = DBWriter(
            connection=greenplum,
            target="public.table",
            options=Greenplum.WriteOptions(if_exists="append"),
        )
        writer.run(df)
    ```

См. документацию Greenplum [CREATE TABLE](https://docs.vmware.com/en/VMware-Greenplum/7/greenplum-database/ref_guide-sql_commands-CREATE_TABLE.html).

## Поддерживаемые типы

См.:
- [официальную документацию коннектора](https://docs.vmware.com/en/VMware-Greenplum-Connector-for-Apache-Spark/2.3/greenplum-connector-sparkreference-datatype_mapping.html)
- [список типов Greenplum](https://docs.vmware.com/en/VMware-Greenplum/7/greenplum-database/ref_guide-data_types.html)

### Числовые типы

| Тип Greenplum (чтение)           | Тип Spark                       | Тип Greenplum (запись)        | Тип Greenplum (создание) |
|---------------------------------- |---------------------------------- |------------------------------- |------------------------- |
| `decimal`<br/>`decimal(P=0..38)`<br/>`decimal(P=0..38, S=0..38)`                      | `DecimalType(P=38, S=18)`<br/>`DecimalType(P=0..38, S=0)`<br/>`DecimalType(P=0..38, S=0..38)`       | `decimal(P=38, S=18)`<br/>`decimal(P=0..38, S=0)`<br/>`decimal(P=0..38, S=0..38)`       | <br/>`decimal` (неограниченный) |
| `decimal(P=39.., S=0..)`       | не поддерживается [^2]           |                               |                         |
| `real`                         | `FloatType()`                   | `real`                      | `real`                |
| `double precision`             | `DoubleType()`                  | `double precision`          | `double precision`    |
| `-`                            | `ByteType()`                    | не поддерживается              | не поддерживается        |
| `smallint`                     | `ShortType()`                   | `smallint`                  | `smallint`            |
| `integer`                      | `IntegerType()`                 | `integer`                   | `integer`             |
| `bigint`                       | `LongType()`                    | `bigint`                    | `bigint`              |
| `money`<br/>`int4range`<br/>`int8range`<br/>`numrange`<br/>`int2vector`                        | <br/><br/><br/>не поддерживается            |                               |                         |

[^2]: Greenplum поддерживает десятичные типы с неограниченной точностью.

    Но `DecimalType(P, S)` в Spark поддерживает максимум `P=38` (128 бит). Невозможно читать, записывать или оперировать значениями большей точности, это приводит к исключению.

### Временные типы

| Тип Greenplum (чтение)             | Тип Spark              | Тип Greenplum (запись) | Тип Greenplum (создание) |
|------------------------------------ |------------------------- |----------------------- |------------------------- |
| `date`                           | `DateType()`          | `date`              | `date`                |
| `time`<br/>`time(0..6)`<br/>`time with time zone`<br/>`time(0..6) with time zone`                           | <br/><br/>`TimestampType()`, особенности форматирования времени [^3]   | <br/><br/>`timestamp`         | <br/><br/>`timestamp`           |
| `timestamp`<br/>`timestamp(0..6)`<br/>`timestamp with time zone`<br/>`timestamp(0..6) with time zone`                      | <br/><br/>`TimestampType()`     | <br/><br/>`timestamp`         | <br/><br/>`timestamp`           |
| `interval` или любой точности<br/>`daterange`<br/>`tsrange`<br/>`tstzrange`      | <br/><br/>не поддерживается      |                       |                         |

!!! warning

    Обратите внимание, что типы в Greenplum и Spark имеют разные диапазоны значений:

    
    | Тип Greenplum | Минимальное значение             | Максимальное значение             | Тип Spark          | Минимальное значение            | Максимальное значение           |
    |----------------|---------------------------------|----------------------------------|---------------------|--------------------------------|--------------------------------|
    | `date`       | `-4713-01-01`                 | `5874897-01-01`                | `DateType()`      | `0001-01-01`                 | `9999-12-31`                 |
    | `timestamp`<br/>`time`  | `-4713-01-01 00:00:00.000000`<br/>`00:00:00.000000` | `294276-12-31 23:59:59.999999`<br/>`24:00:00.000000` | `TimestampType()` | `0001-01-01 00:00:00.000000` | `9999-12-31 23:59:59.999999` |

    Таким образом, не все значения могут быть считаны из Greenplum в Spark.

    Ссылки:

    * [Документация по типам Greenplum](https://docs.vmware.com/en/VMware-Greenplum/7/greenplum-database/ref_guide-data_types.html)
    * [Документация по DateType в Spark](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/types/DateType.html)
    * [Документация по TimestampType в Spark](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/types/TimestampType.html)

[^3]: Тип `time` аналогичен `timestamp` с датой `1970-01-01`. Поэтому вместо чтения данных из Postgres как `23:59:59`
    фактически читается `1970-01-01 23:59:59`, и наоборот.

### Строковые типы

| Тип Greenplum (чтение)      | Тип Spark       | Тип Greenplum (запись) | Тип Greenplum (создание) |
|----------------------------- |------------------ |----------------------- |------------------------- |
| `character`<br/>`character(N)`<br/>`character varying`<br/>`character varying(N)`<br/>`text`<br/>`xml`<br/>`CREATE TYPE ... AS ENUM`               | <br/><br/><br/><br/>`StringType()` | <br/><br/><br/><br/>`text`              | <br/><br/><br/><br/>`text`                |
| `json`<br/>`jsonb`                    | <br/>не поддерживается |                       |                         |

### Бинарные типы

| Тип Greenplum (чтение)   | Тип Spark        | Тип Greenplum (запись) | Тип Greenplum (создание) |
 |-------------------------- |------------------- |----------------------- |------------------------- |
| `boolean`              | `BooleanType()` | `boolean`           | `boolean`             |
| `bit`<br/>`bit(N)`<br/>`bit varying`<br/>`bit varying(N)`                  | <br/><br/>не поддерживается |                       |                         |
| `bytea`                | не поддерживается [^4] |                       |                         |
| `-`                    | `BinaryType()`  | `bytea`             | `bytea`               |

[^4]: Да, это странно.

### Структурные типы

| Тип Greenplum (чтение)         | Тип Spark       | Тип Greenplum (запись) | Тип Greenplum (создание) |
|--------------------------------|------------------|-----------------------|-------------------------|
| `T[]`                        | не поддерживается |                       |                         |
| `-`                          | `ArrayType()`  | не поддерживается      |                         |
| `CREATE TYPE sometype (...)` | `StringType()` | `text`              | `text`                |
| `-`                          | `StructType()`<br/>`MapType()` | не поддерживается      |                         |

## Неподдерживаемые типы

Столбцы этих типов не могут быть прочитаны/записаны в Spark:

- `cidr`
- `inet`
- `macaddr`
- `macaddr8`
- `circle`
- `box`
- `line`
- `lseg`
- `path`
- `point`
- `polygon`
- `tsvector`
- `tsquery`
- `uuid`

Есть способ избежать этого - просто привести неподдерживаемые типы к `text`. Но способ, которым это можно сделать, не так прост.

## Явное приведение типов { #explicit-type-cast }

### `DBReader`

Прямое приведение типов Greenplum не поддерживается DBReader из-за особенностей реализации коннектора.

    ```python
        reader = DBReader(
            connection=greenplum,
            # вызовет ошибку
            columns=["CAST(unsupported_column AS text)"],
        ) 
    ```

Но есть обходной путь - создать представление (view) с приведением неподдерживаемого столбца к тексту (или любому другому поддерживаемому типу).
Например, вы можете использовать функцию Postgres [to_json](https://www.postgresql.org/docs/current/functions-json.html) для преобразования столбца любого типа в строковое представление, а затем анализировать этот столбец на стороне Spark с помощью метода [JSON.parse_column][onetl.file.format.json.JSON.parse_column].

    ```python
            from pyspark.sql.types import ArrayType, IntegerType

            from onetl.connection import Greenplum
            from onetl.db import DBReader
            from onetl.file.format import JSON

            greenplum = Greenplum(...)

            greenplum.execute(
                """
                CREATE VIEW schema.view_with_json_column AS
                SELECT
                    id,
                    supported_column,
                    to_json(array_column) array_column_as_json,
                    gp_segment_id  -- ! важно !
                FROM
                    schema.table_with_unsupported_columns
                """,
            )

            # создаем dataframe используя это представление
            reader = DBReader(
                connection=greenplum,
                source="schema.view_with_json_column",
            )
            df = reader.run()

            # Определяем схему для JSON-данных
            json_scheme = ArrayType(IntegerType())

            df = df.select(
                df.id,
                df.supported_column,
                JSON().parse_column(df.array_column_as_json, json_scheme).alias("array_column"),
            )
    ```

### `DBWriter`

Для записи данных в столбец `text` или `json` в таблице Greenplum используйте метод [JSON.serialize_column][onetl.file.format.json.JSON.serialize_column].

    ```python
        from onetl.connection import Greenplum
        from onetl.db import DBWriter
        from onetl.file.format import JSON

        greenplum = Greenplum(...)

        greenplum.execute(
            """
            CREATE TABLE schema.target_table (
                id int,
                supported_column timestamp,
                array_column_as_json jsonb, -- или text
            )
            DISTRIBUTED BY id
            """,
        )

        write_df = df.select(
            df.id,
            df.supported_column,
            JSON().serialize_column(df.array_column).alias("array_column_json"),
        )

        writer = DBWriter(
            connection=greenplum,
            target="schema.target_table",
        )
        writer.run(write_df) 
    ```

Затем вы можете анализировать этот столбец на стороне Greenplum:

    ```sql
        SELECT
            id,
            supported_column,
            -- доступ к первому элементу массива
            array_column_as_json->0
        FROM
            schema.target_table
    ```
