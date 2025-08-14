# Решение проблем со Spark { #troubleshooting-spark }

## Перезапуск сессии Spark

Иногда требуется остановить текущую сессию Spark и запустить новую, например, чтобы добавить некоторые .jar пакеты или изменить конфигурацию сессии. Но PySpark не только запускает сессию Spark, но и запускает процесс виртуальной машины Java (JVM) в фоновом режиме. Поэтому вызов `sparkSession.stop()` [не завершает работу JVM](https://issues.apache.org/jira/browse/SPARK-47740), и это может вызвать некоторые проблемы.

Кроме того, помимо свойств JVM, остановка сессии Spark не очищает контекст Spark, который является глобальным объектом. Таким образом, новые сессии Spark создаются с использованием того же объекта контекста и, следовательно, с использованием тех же параметров конфигурации Spark.

Для правильной остановки сессии Spark **необходимо**:

* Остановить сессию Spark, вызвав `sparkSession.stop()`.
* **ОСТАНОВИТЬ ИНТЕРПРЕТАТОР PYTHON**, например, вызвав `sys.exit()`.
* Запустить новый интерпретатор Python.
* Запустить новую сессию Spark с нужными параметрами конфигурации.

Пропуск некоторых из этих шагов может привести к проблемам с созданием новой сессии Spark.

## Уровень логирования драйвера

Уровень логирования по умолчанию для сессии Spark - `WARN`. Чтобы показать более подробные логи, используйте:

```python
spark.sparkContext.setLogLevel("INFO")
```

или увеличьте подробность ещё больше:

```python
spark.sparkContext.setLogLevel("DEBUG")
```

После получения всей необходимой информации вы можете вернуться к предыдущему уровню логирования:

```python
spark.sparkContext.setLogLevel("WARN")
```

## Уровень логирования исполнителей

`sparkContext.setLogLevel` изменяет только уровень логирования сессии Spark на **драйвере** Spark. Чтобы сделать логи исполнителей Spark более подробными, выполните следующие шаги:

* Создайте файл `log4j.properties` со следующим содержимым:

  ```jproperties
  log4j.rootCategory=DEBUG, console

  log4j.appender.console=org.apache.log4j.ConsoleAppender
  log4j.appender.console.target=System.err
  log4j.appender.console.layout=org.apache.log4j.PatternLayout
  log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n
  ```

* Остановите существующую сессию Spark и создайте новую со следующими параметрами:

  ```python
  from pyspark.sql import SparkSession

  spark = (
      SparkSesion.builder.config("spark.files", "file:log4j.properties").config(
          "spark.executor.extraJavaOptions", "-Dlog4j.configuration=file:log4j.properties"
      )
      # вы можете применить те же настройки логирования к драйверу Spark, раскомментировав строку ниже
      # .config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:log4j.properties")
      .getOrCreate()
  )
  ```

Каждый исполнитель Spark получит копию файла `log4j.properties` при запуске и загрузит его для изменения собственного уровня логирования. Тот же подход может быть использован и для драйвера Spark, чтобы исследовать проблему, когда сессия Spark не может правильно запуститься.
