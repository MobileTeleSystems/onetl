# Запись в Teradata с использованием `DBWriter` { #teradata-write }

Для записи данных в Teradata используйте [DBWriter][db-writer].

!!! warning "Предупреждение"

    Всегда рекомендуется создавать таблицу явно с помощью [Teradata.execute][teradata-execute] вместо того, чтобы полагаться на автоматическую генерацию DDL в Spark.

    Это связано с тем, что генератор DDL в Spark может создавать столбцы с точностью и типами, отличными от ожидаемых, что может привести к потере точности и другим проблемам.

## Примеры

    ```python
        from onetl.connection import Teradata
        from onetl.db import DBWriter

        teradata = Teradata(
            ...,
            extra={"TYPE": "FASTLOAD", "TMODE": "TERA"},
        )

        df = ...  # данные здесь

        writer = DBWriter(
            connection=teradata,
            target="database.table",
            options=Teradata.WriteOptions(
                if_exists="append",
                # избегаем создания таблицы SET, используем MULTISET
                createTableOptions="NO PRIMARY INDEX",
            ),
        )

        writer.run(df.repartition(1))
    ```

## Рекомендации

### Количество соединений

Teradata не основан на MVCC, поэтому операции записи блокируют всю таблицу эксклюзивно.
Таким образом, **невозможно записывать данные в таблицу Teradata через несколько параллельных соединений**, без исключений.

Единственный способ записи в Teradata без создания взаимоблокировок - использовать датафрейм с ровно 1 разделом.

Это можно реализовать с помощью `df.repartition(1)`:

    ```python
        # НЕ используйте df.coalesce(1), так как это может привести к зависанию
        writer.run(df.repartition(1))
    ```

Это перемещает все данные на один рабочий узел Spark, что может потребовать много оперативной памяти. Обычно требуется увеличить параметр `spark.executor.memory` для обработки этого.

Другой способ - записывать разделы датафрейма один за другим:

    ```python
        from pyspark.sql.functions import spark_partition_id

        # получаем список всех разделов в датафрейме
        partitions = sorted(df.select(spark_partition_id()).distinct().collect())

        for partition in partitions:
            # получаем только часть данных в этом конкретном разделе
            part_df = df.where(**partition.asDict()).coalesce(1)

            writer.run(part_df)
    ```

Этот метод требует равномерного распределения данных по всем разделам, чтобы избежать перекоса данных и скачков потребления памяти.

### Выбор типа соединения

Teradata поддерживает несколько [различных типов соединений](https://teradata-docs.s3.amazonaws.com/doc/connectivity/jdbc/reference/current/jdbcug_chapter_2.html#BABFGFAF):

- `TYPE=DEFAULT` - выполняет обычные запросы `INSERT`
- `TYPE=FASTLOAD` - использует специальный протокол FastLoad для запросов вставки

Всегда рекомендуется использовать `TYPE=FASTLOAD`, потому что:

- Он обеспечивает более высокую производительность
- Он правильно обрабатывает вставку значений `NULL` (`TYPE=DEFAULT` вызывает исключение)

Однако его можно использовать только для записи, но не для чтения.

### Выбор режима транзакций

Teradata поддерживает [2 различных режима транзакций](https://teradata-docs.s3.amazonaws.com/doc/connectivity/jdbc/reference/current/jdbcug_chapter_2.html#TMODESEC):

- `TMODE=ANSI`
- `TMODE=TERA`

Выбор одного из режимов может изменить поведение коннектора. Например:

- При вставке данных, превышающих длину столбца таблицы, например, вставка `CHAR(25)` в столбец с типом `CHAR(24)`:
  - `TMODE=ANSI` - вызывает исключение
  - `TMODE=TERA` - обрезает входную строку до 24 символов
- При создании таблицы с помощью Spark:
  - `TMODE=ANSI` - создает таблицу `MULTISET`
  - `TMODE=TERA` - создает таблицу `SET` с `PRIMARY KEY` в качестве первого столбца датафрейма.
      Это может привести к замедлению вставки, так как каждая строка будет проверяться на соответствие уникальному индексу.
      К счастью, это можно отключить, передав пользовательские `createTableOptions`.

## Опции { #teradata-write-options }

Метод выше принимает [Teradata.WriteOptions][onetl.connection.db_connection.teradata.options.TeradataWriteOptions]

::: onetl.connection.db_connection.teradata.options.TeradataWriteOptions
    options:
        inherited_members: true
        heading_level: 3
        show_root_heading: true
