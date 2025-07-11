# Чтение из Hive с помощью `Hive.sql` { #hive-sql }

`Hive.sql` позволяет передавать пользовательский SQL-запрос, но не поддерживает инкрементальные стратегии.

## Поддержка синтаксиса

Поддерживаются только запросы со следующим синтаксисом:

- ✅︎ `SELECT ... FROM ...`
- ✅︎ `WITH alias AS (...) SELECT ...`
- ❌ `SET ...; SELECT ...;` - несколько операторов не поддерживаются

!!! warning

    Фактически, запрос должен быть написан с использованием синтаксиса [SparkSQL](https://spark.apache.org/docs/latest/sql-ref-syntax.html#data-retrieval-statements), а не HiveQL.

## Примеры

    ```python
    from onetl.connection import Hive

    hive = Hive(...)
    df = hive.sql(
        """
        SELECT
            id,
            key,
            CAST(value AS text) value,
            updated_at
        FROM
            some.mytable
        WHERE
            key = 'something'
        """
    )   
    ```

## Рекомендации

### Используйте столбцовые форматы записи

Предпочтительны следующие форматы записи:

- [ORC](https://spark.apache.org/docs/latest/sql-data-sources-orc.html)
- [Parquet](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html)
- [Iceberg](https://iceberg.apache.org/spark-quickstart/)
- [Hudi](https://hudi.apache.org/docs/quick-start-guide/)
- [Delta](https://docs.delta.io/latest/quick-start.html#set-up-apache-spark-with-delta-lake)

Для столбцовых форматов записи каждый файл содержит отдельные секции, где хранятся данные столбцов. В нижнем колонтитуле файла содержится информация о расположении каждой секции/группы столбцов. Spark может использовать эту информацию для загрузки только тех секций, которые требуются для конкретного запроса, например, только выбранных столбцов, что значительно ускоряет выполнение запроса.

Еще одно преимущество — высокий коэффициент сжатия, например, в 10-100 раз по сравнению с JSON или CSV.

### Выбирайте только необходимые столбцы

Вместо использования `SELECT * FROM ...` предпочтительнее указывать точные имена столбцов `SELECT col1, col2, ...`.
Это значительно снижает объем данных, считываемых Spark, **если используются столбцовые форматы файлов**.

### Используйте столбцы разделов в условии `where`

Запросы должны включать условие `WHERE` с фильтрами по столбцам секционирования Hive.
Это позволяет Spark читать только небольшой набор файлов (*отсечение разделов*) вместо сканирования всей таблицы, что значительно повышает производительность.

Поддерживаемые операторы: `=`, `>`, `<` и `BETWEEN`, и только по отношению к некоторым **статическим** значениям.

## Подробнее

<!-- 
    ```{eval-rst}
    .. currentmodule:: onetl.connection.db_connection.hive.connection
    ```

    ```{eval-rst}
    .. automethod:: Hive.sql
    ```
 -->

::: onetl.connection.db_connection.hive.connection.Hive.sql
    options:
        members:
            - sql
