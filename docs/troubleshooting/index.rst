.. _troubleshooting:

Troubleshooting
===============

In case of error please follow instructions below:

* Read the logs or exception messages you've faced with.
    * If Python logs are note verbose enough, :ref:`increase the log level <logging>`.
    * If Spark logs are note verbose enough, :ref:`increase the log level <troubleshooting-spark>`.
* Read documentation related to a class or method you are using.
* `Google <https://google.com>`_ the error message, and carefully read the search result:
    * `StackOverflow <https://stackoverflow.com/>`_ answers.
    * `Spark <https://spark.apache.org/docs/latest/>`_ documentation.
    * Documentation of database or filesystem you are connecting to.
    * Documentation of underlying connector.
* Search for known `issues <https://github.com/MobileTeleSystems/onetl/issues>`_, or create a new one.
* Always use the most resent versions of onETL, PySpark and connector packages, :ref:`compatible with your environment <install-spark>`.

.. toctree::
    :maxdepth: 3
    :caption: Troubleshooting
    :hidden:

    spark
