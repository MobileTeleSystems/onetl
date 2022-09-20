.. title

onETL
=======

|Build Status| |Quality Gate Status| |Maintainability Rating| |Coverage|
|Documentation| |PyPI|

.. |Build Status| image:: https://gitlab.services.mts.ru/bigdata/platform/onetools/onetl/badges/develop/pipeline.svg
    :target: https://gitlab.services.mts.ru/bigdata/platform/onetools/onetl/-/pipelines
.. |Quality Gate Status| image:: https://sonar.bd.msk.mts.ru/api/project_badges/measure?project=onetl&metric=alert_status
    :target: https://sonar.bd.msk.mts.ru/dashboard?id=onetl
.. |Maintainability Rating| image:: https://sonar.bd.msk.mts.ru/api/project_badges/measure?project=onetl&metric=sqale_rating
    :target: https://sonar.bd.msk.mts.ru/dashboard?id=onetl
.. |Coverage| image:: https://sonar.bd.msk.mts.ru/api/project_badges/measure?project=onetl&metric=coverage
    :target: https://sonar.bd.msk.mts.ru/dashboard?id=onetl
.. |Documentation| image:: https://img.shields.io/badge/docs-latest-success
    :target: https://bigdata.pages.mts.ru/platform/onetools/onetl/
.. |PyPI| image:: https://img.shields.io/badge/pypi-download-orange
    :target: http://rep.msk.mts.ru/ui/packages/pypi:%2F%2Fonetl?name=onetl&type=packages

What is onETL?
--------------

* ``onETL`` is a Python ETL/ELT framework powered by `Apache Spark <https://spark.apache.org/>`_ & other open-source tools
* ``onETL`` provides unified classes to extract data from (**E**) & load data to (**L**) various stores
* ``onETL`` relies on `Spark DataFrame API <https://spark.apache.org/docs/3.2.0/api/python/reference/api/pyspark.sql.DataFrame.html>`_ for performing transformations (**T**) in terms of *ETL*
* ``onETL`` provides direct assess to database, allowing to execute SQL queries, as well as DDL, DML, and call functions/procedures. This can be used for building up *ELT* pipelines
* ``onETL`` supports different `read strategies <https://bigdata.pages.mts.ru/platform/onetools/onetl/strategy/index.html>`_ for incremental and batch data fetching
* (TBD) onETL can send data lineage to data catalog (`Apache Atlas <https://atlas.apache.org/#/>`_)

Requirements
------------
* **Python 3.7+**
* PySpark 2.3+
* Java 8+ (required by Spark)
* Kerberos system libs (for HDFS access)
* ``smbclient`` system package (for Samba access)

Storage Compatibility
---------------------

+---------------------------------------+----------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------+
| Storage                               | Documentation                                            | Notes                                                                                                                 |
+=======================================+==========================================================+=======================================================================================================================+
| Clickhouse                            | `Clickhouse connection <db_connection/teradata.html>`_   | Powered by Apache Spark `JDBC Data Source <https://spark.apache.org/docs/2.4.8/sql-data-sources-jdbc.html>`_          |
+---------------------------------------+----------------------------------------------------------+                                                                                                                       |
| MSSQL                                 | `MSSQL connection <db_connection/mssql.html>`_           |                                                                                                                       |
+---------------------------------------+----------------------------------------------------------+                                                                                                                       |
| MySQL                                 | `MySQL connection <db_connection/mysql.html>`_           |                                                                                                                       |
+---------------------------------------+----------------------------------------------------------+                                                                                                                       |
| Postgres                              | `Postgres connection <db_connection/postgres.html>`_     |                                                                                                                       |
+---------------------------------------+----------------------------------------------------------+                                                                                                                       |
| Oracle                                | `Oracle connection <db_connection/oracle.html>`_         |                                                                                                                       |
+---------------------------------------+----------------------------------------------------------+                                                                                                                       |
| Teradata                              | `Teradata connection <db_connection/teradata.html>`_     |                                                                                                                       |
+---------------------------------------+----------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------+
| Hive                                  | `Hive connection <db_connection/hive.html>`_             | Powered by Apache Spark `Hive integration <https://spark.apache.org/docs/2.4.8/sql-data-sources-hive-tables.html>`_   |
+---------------------------------------+----------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------+
| Greenplum                             | `Greenplum connection <db_connection/greenplum.html>`_   | Powered by Pivotal `Greenplum Spark connector <https://network.tanzu.vmware.com/products/vmware-tanzu-greenplum>`_    |
+---------------------------------------+----------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------+
| HDFS                                  | `HDFS connection <file_connection/hdfs.html>`_           | Powered `HDFS client <https://pypi.org/project/hdfs/>`_                                                               |
+---------------------------------------+----------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------+
| Samba                                 | `Samba connection <file_connection/hdfs.html>`_          | Powered `Samba client <https://pypi.org/project/PySmbClient/>`_                                                       |
+---------------------------------------+----------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------+
| SFTP                                  | `SFTP connection <file_connection/hdfs.html>`_           | Powered `Paramiko library <https://pypi.org/project/paramiko/>`_                                                      |
+---------------------------------------+----------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------+
| FTP                                   | `FTP connection <file_connection/hdfs.html>`_            | Powered `FTP client <https://pypi.org/project/ftputil/>`_                                                             |
+---------------------------------------+----------------------------------------------------------+                                                                                                                       |
| FTPS                                  | `FTPS connection <file_connection/hdfs.html>`_           |                                                                                                                       |
+---------------------------------------+----------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------+


.. documentation

Documentation
-------------

See https://bigdata.pages.mts.ru/platform/onetools/onetl/

.. wiki

Wiki page
-------------

See https://wiki.bd.msk.mts.ru/display/ONE/onETL

.. contribution

Contribution guide
-------------------

See https://wiki.bd.msk.mts.ru/display/DAT/Contribution+guide


.. install

Installation
---------------

Stable release
~~~~~~~~~~~~~~~
Stable version is released on every tag to ``master`` branch. Please use stable releases on production environment.
Version example: ``1.1.2``

.. code:: bash

    pip install onetl==1.1.2 # exact version

    pip install onetl # latest release

Development release
~~~~~~~~~~~~~~~~~~~~
# TDB

.. develops

Development
---------------


Building
~~~~~~~~

Set up your Docker using the link below:

(https://wiki.bd.msk.mts.ru/pages/viewpage.action?pageId=42960827).


Build:

.. code-block:: bash

    docker build -t onetl -f ./docker/Dockerfile .

    docker system prune --volumes

Now you have Docker Image **onetl**.

Testing
~~~~~~~~

Up services for integration tests:

.. code-block:: bash

    docker-compose down

    docker system prune --volumes

    docker-compose up -d

You can start a specific service using ``docker-compose up -d servicename`` command


IDE (PyCharm)
^^^^^^^^^^^^^^

Settings:

Project Interpreter -> Add -> Docker -> Image name: ``onetl:latest``


Run -> Edit Configurations -> New -> ``pytest``:
1. Name **Test All**.

2. Script path **tests**.

3. Additional Arguments **--verbose -s -c pytest.ini**.

4. Python interpreter **Project Default** (``onetl:latest``). **You should write Python interpreter path:** ``python3``.

5. Working directory ``/opt/project``

6. ``Add content roots`` and ``source roots`` - **remove these buttons**

7. Docker container settings:

    1. Network mode **onetl** (network from ``docker-compose.yml``) or  Add ``--net onetl`` into ``Run options``

    2. Add ``--env-file $(absolute path to)/onetl_local.default.env`` into docker ``Run options``

    3. Volume bindings (container -> local): **/opt/project -> (absolute path to)/onetl**
        PyCharm will do it for you, but check it one more time!!!

Run -> Edit Configurations -> Copy Configuration **Test All**:

Now you can run tests with configuration **Test All**.

Console
^^^^^^^^

1. Set ``SPARK_EXTERNAL_IP`` environment variable to IP address of ``docker0`` network interface, e.g. ``172.17.0.1``

2. Set all environment variables from ``onetl_local.default.env``,
    but change all ``*_HOST`` variables to ``localhost``,
    and ``*_PORT`` variables to external ports from ``docker-compose.yml``

3. Run ``pytest``

.. usage
