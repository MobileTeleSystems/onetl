.. _troubleshooting-spark:

Spark Troubleshooting
=====================

Restarting Spark session
------------------------

Sometimes it is required to stop current Spark session and start a new one, e.g. to add some .jar packages, or change session config.
But PySpark not only starts Spark session, but also starts Java virtual machine (JVM) process in the background,
which. So calling ``sparkSession.stop()`` `does not shutdown JVM <https://issues.apache.org/jira/browse/SPARK-47740>`_,
and this can cause some issue.

Also apart from JVM properties, stopping Spark session does not clear Spark context, which is a global object. So new
Spark sessions are created using the same context object, and thus using the same Spark config options.

To properly stop Spark session, it is **required** to:
* Stop Spark session by calling ``sparkSession.stop()``.
* **STOP PYTHON INTERPRETER**, e.g. by calling ``sys.exit()``.
* Start new Python interpreter.
* Start new Spark session with config options you need.

Skipping some of these steps can lead to issues with creating new Spark session.

Driver log level
----------------

Default logging level for Spark session is ``WARN``. To show more verbose logs, use:

.. code:: python

    spark.sparkContext.setLogLevel("INFO")

or increase verbosity even more:

.. code:: python

    spark.sparkContext.setLogLevel("DEBUG")

After getting all information you need, you can return back the previous log level:

.. code:: python

    spark.sparkContext.setLogLevel("WARN")

Executors log level
-------------------

``sparkContext.setLogLevel`` changes only log level of Spark session on Spark **driver**.
To make Spark executor logs more verbose, perform following steps:

* Create ``log4j.properties`` file with content like this:

  .. code-block:: jproperties

    log4j.rootCategory=DEBUG, console

    log4j.appender.console=org.apache.log4j.ConsoleAppender
    log4j.appender.console.target=System.err
    log4j.appender.console.layout=org.apache.log4j.PatternLayout
    log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n

* Stop existing Spark session and create a new one with following options:

  .. code-block:: python

    from pyspark.sql import SparkSession

    spark = (
        SparkSesion.builder.config("spark.files", "file:log4j.properties").config(
            "spark.executor.extraJavaOptions", "-Dlog4j.configuration=file:log4j.properties"
        )
        # you can apply the same logging settings to Spark driver, by uncommenting the line below
        # .config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:log4j.properties")
        .getOrCreate()
    )

Each Spark executor will receive a copy of ``log4j.properties`` file during start, and load it to change own log level.
Same approach can be used for Spark driver as well, to investigate issue when Spark session cannot properly start.
