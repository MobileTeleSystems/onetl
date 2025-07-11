# Сопоставление типов MongoDB <-> Spark { #mongodb-types }

!!! note

    Результаты ниже действительны для Spark 3.5.5 и могут отличаться в других версиях Spark.

## Определение типов и приведение

DataFrame в Spark всегда имеют `schema` — список полей с соответствующими типами Spark. Все операции с полем выполняются с использованием типа поля.

MongoDB по своей природе является \_\_бесхемной\_\_ (schemaless). Поэтому есть 2 способа решения этой проблемы:

- Пользователь явно указывает схему DataFrame:

??? note "Смотрите пример"

    ```python

        from onetl.connection import MongoDB
        from onetl.db import DBReader

        from pyspark.sql.types import (
            StructType,
            StructField,
            IntegerType,
            StringType,
            TimestampType,
        )

        mongodb = MongoDB(...)

        df_schema = StructType(
            [
                StructField("_id", StringType()),
                StructField("some", StringType()),
                StructField(
                    "field",
                    StructType(
                        [
                            StructField("nested", IntegerType()),
                        ]
                    ),
                ),
            ]
        )

        reader = DBReader(
            connection=mongodb,
            source="some_collection",
            df_schema=df_schema,
        )
        df = reader.run()

        # или

        df = mongodb.pipeline(
            collection="some_collection",
            df_schema=df_schema,
        )
    ```

- Положиться на автоматическое определение схемы коннектором MongoDB:

        ```python
            df = mongodb.pipeline(collection="some_collection")
        ```

  В этом случае коннектор MongoDB читает образец документов коллекции и создает схему DataFrame на основе полей и значений документов.

Настоятельно рекомендуется явно передавать `df_schema`, чтобы избежать проблем с преобразованием типов.

### Ссылки

Здесь вы можете найти исходный код с преобразованиями типов:

- [MongoDB -> Spark](https://github.com/mongodb/mongo-spark/blob/r10.4.1/src/main/java/com/mongodb/spark/sql/connector/schema/InferSchema.java#L214-L260)
- [Spark -> MongoDB](https://github.com/mongodb/mongo-spark/blob/r10.4.1/src/main/java/com/mongodb/spark/sql/connector/schema/RowToBsonDocumentConverter.java#L157-L260)

## Поддерживаемые типы

См. [официальную документацию](https://www.mongodb.com/docs/manual/reference/bson-types/)

### Числовые типы

| Тип MongoDB (чтение) | Тип Spark                   | Тип MongoDB (запись) |
|----------------------|-----------------------------|-----------------------|
| `Decimal128`        | `DecimalType(P=34, S=32)`  | `Decimal128`         |
| `-`<br/>`Double`    | `FloatType()`<br/>`DoubleType()` | `Double`           |
| `-`<br/>`-`<br/>`Int32` | `ByteType()`<br/>`ShortType()`<br/>`IntegerType()` | `Int32`            |
| `Int64`             | `LongType()`              | `Int64`              |

### Временные типы

| Тип MongoDB (чтение)     | Тип Spark                         | Тип MongoDB (запись)    |
|--------------------------|-----------------------------------|-------------------------|
| `-`                      | `DateType()`, дни               | `Date`, миллисекунды  |
| `Date`, миллисекунды   | `TimestampType()`, микросекунды | `Date`, миллисекунды, **потеря точности** [^1]|
| `Timestamp`, секунды   | `TimestampType()`, микросекунды | `Date`, миллисекунды  |
| `-`<br/>`-`              | `TimestampNTZType()`<br/>`DayTimeIntervalType()`            | не поддерживается      |

!!! warning

    Обратите внимание, что типы в MongoDB и Spark имеют различные диапазоны значений:

    
    | Тип MongoDB   | Минимальное значение                 | Максимальное значение                | Тип Spark          | Минимальное значение              | Максимальное значение               |
    |---------------|--------------------------------------|-------------------------------------|---------------------|-----------------------------------|--------------------------------------|
    | `Date`<br/>`Timestamp`      | -290 миллионов лет<br/>`1970-01-01 00:00:00`             | 290 миллионов лет<br/>`2106-02-07 09:28:16`              | `TimestampType()` | `0001-01-01 00:00:00.000000` | `9999-12-31 23:59:59.999999` |

    Таким образом, не все значения могут быть прочитаны из MongoDB в Spark или записаны из Spark DataFrame в MongoDB.

    Ссылки:

    * [Документация по типу Date в MongoDB](https://www.mongodb.com/docs/manual/reference/bson-types/#date)
    * [Документация по Timestamp в MongoDB](https://www.mongodb.com/docs/manual/reference/bson-types/#timestamps)
    * [Документация по DateType в Spark](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/types/DateType.html)
    * [Документация по TimestampType в Spark](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/types/TimestampType.html)

[^1]: Тип `Date` в MongoDB имеет точность до миллисекунд (`23:59:59.999`).
    Вставка данных с точностью до микросекунд (`23:59:59.999999`)
    приведет к **отбрасыванию микросекунд**.

### Строковые типы

Примечание: поля устаревшего типа MongoDB `Symbol` исключаются при чтении.

| Тип MongoDB (чтение) | Тип Spark       | Тип MongoDB (запись) |
|----------------------|-----------------|----------------------|
| `String`<br/>`Code`<br/>`RegExp` | `StringType()` | `String`           |

### Бинарные типы

| Тип MongoDB (чтение) | Тип Spark      | Тип MongoDB (запись) |
| -------------------- | --------------- | -------------------- |
| `Boolean`            | `BooleanType()` | `Boolean`            |
| `Binary`             | `BinaryType()`  | `Binary`             |

### Структурные типы

| Тип MongoDB (чтение) | Тип Spark            | Тип MongoDB (запись) |
|----------------------|-----------------------|----------------------|
| `Array[T]`          | `ArrayType(T)`      | `Array[T]`         |
| `Object[...]`<br/>`-`     | `StructType([...])`<br/>`MapType(...)` | <br/>`Object[...]`      |

### Специальные типы

| Тип MongoDB (чтение) | Тип Spark                                              | Тип MongoDB (запись)                  |
|----------------------|---------------------------------------------------------|---------------------------------------|
| `ObjectId`<br/>`MaxKey`<br/>`MinKey`        | <br/><br/>`StringType()`                                        | <br/><br/>`String`                            |
| `Null`<br/>`Undefined`            | `NullType()`                                          | `Null`                              |
| `DBRef`           | `StructType([$ref: StringType(), $id: StringType()])` | `Object[$ref: String, $id: String]` |

## Явное приведение типов

### `DBReader`

В настоящее время невозможно приводить типы полей с помощью `DBReader`. Но это можно сделать с помощью `MongoDB.pipeline`.

### `MongoDB.pipeline`

Вы можете использовать агрегацию `$project` для приведения типов полей:

    ```python
        from pyspark.sql.types import IntegerType, StructField, StructType

        from onetl.connection import MongoDB
        from onetl.db import DBReader

        mongodb = MongoDB(...)

        df = mongodb.pipeline(
            collection="my_collection",
            pipeline=[
                {
                    "$project": {
                        # преобразование unsupported_field в строку
                        "unsupported_field_str": {
                            "$convert": {
                                "input": "$unsupported_field",
                                "to": "string",
                            },
                        },
                        # исключение unsupported_field из результата
                        "unsupported_field": 0,
                    }
                }
            ],
        )

        # приведение содержимого поля к правильному типу Spark
        df = df.select(
            df.id,
            df.supported_field,
            # явное приведение
            df.unsupported_field_str.cast("integer").alias("parsed_integer"),
        )
    ```

### `DBWriter`

Преобразование поля dataframe в строку на стороне Spark, а затем запись в MongoDB:

    ```python
        df = df.select(
            df.id,
            df.unsupported_field.cast("string").alias("array_field_json"),
        )

        writer.run(df)
    ```
