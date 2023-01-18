.. _readme:

onETL
=====

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

Requirements
------------
* **Python 3.7...3.10**
* PySpark 2.3...3.3
* Java 8+ (required by Spark, see below)
* Kerberos libs & GCC (required by ``Hive`` and ``HDFS`` connectors)

Storage Compatibility
---------------------

+------------+----------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| Storage    | Documentation                                            | Notes                                                                                                               |
+============+==========================================================+=====================================================================================================================+
| Clickhouse | `Clickhouse connection <db_connection/clickhouse.html>`_ | Powered by Apache Spark `JDBC Data Source <https://spark.apache.org/docs/2.4.8/sql-data-sources-jdbc.html>`_        |
+------------+----------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| MSSQL      | `MSSQL connection <db_connection/mssql.html>`_           |                                                                                                                     |
+------------+----------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| MySQL      | `MySQL connection <db_connection/mysql.html>`_           |                                                                                                                     |
+------------+----------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| Postgres   | `Postgres connection <db_connection/postgres.html>`_     |                                                                                                                     |
+------------+----------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| Oracle     | `Oracle connection <db_connection/oracle.html>`_         |                                                                                                                     |
+------------+----------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| Teradata   | `Teradata connection <db_connection/teradata.html>`_     |                                                                                                                     |
+------------+----------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| Hive       | `Hive connection <db_connection/hive.html>`_             | Powered by Apache Spark `Hive integration <https://spark.apache.org/docs/2.4.8/sql-data-sources-hive-tables.html>`_ |
+------------+----------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| Greenplum  | `Greenplum connection <db_connection/greenplum.html>`_   | Powered by Pivotal `Greenplum Spark connector <https://network.tanzu.vmware.com/products/vmware-tanzu-greenplum>`_  |
+------------+----------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| HDFS       | `HDFS connection <file_connection/hdfs.html>`_           | Powered by `HDFS Python client <https://pypi.org/project/hdfs/>`_                                                   |
+------------+----------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| S3         | `S3 connection <file_connection/s3.html>`_               | Powered by `minio-py client <https://pypi.org/project/minio/>`_                                                     |
+------------+----------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| SFTP       | `SFTP connection <file_connection/sftp.html>`_           | Powered by `Paramiko library <https://pypi.org/project/paramiko/>`_                                                 |
+------------+----------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| FTP        | `FTP connection <file_connection/ftp.html>`_             | Powered by `FTPUtil library <https://pypi.org/project/ftputil/>`_                                                   |
+------------+----------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| FTPS       | `FTPS connection <file_connection/ftps.html>`_           |                                                                                                                     |
+------------+----------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------+
| WebDAV     | `WebDAV connection <file_connection/webdav.html>`_       | Powered by `WebdavClient3 library <https://pypi.org/project/webdavclient3/>`_                                       |
+------------+----------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------+


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

See `<CONTRIBUTING.rst>`__

.. security

Security
-------------------

See `<SECURITY.rst>`__


.. install

How to install
---------------

.. _minimal-install:

Minimal installation
~~~~~~~~~~~~~~~~~~~~

Base ``onetl`` package contains:

    * ``DBReader``, ``DBWriter`` and related classes
    * ``FileDownloader``, ``FileUploader``, ``FileFilter``, ``FileLimit`` and related classes
    * Read Strategies & HWM classes
    * Plugins support

It can be installed via:

.. code:: bash

    pip install onetl

.. warning::

    This method does NOT include any connections.

    This method is recommended for use in third-party libraries which require for ``onetl`` to be installed,
    but do not use its connection classes.

.. _spark-install:

With DB connections
~~~~~~~~~~~~~~~~~~~

All DB connection classes (``Clickhouse``, ``Greenplum``, ``Hive`` and others) requires PySpark to be installed.

Firstly, you should install JDK. The exact installation instruction depends on your OS, here are some examples:

.. code:: bash

    yum install java-1.8.0-openjdk-devel  # CentOS 7 + Spark 2
    dnf install java-11-openjdk-devel  # CentOS 8 + Spark 3
    apt-get install openjdk-11-jdk  # Debian-based + Spark 3

Compatibility matrix (`see also <https://spark.apache.org/docs/latest/>`_):

+--------+------------+------------+
| Spark  | Java       | Python     |
+========+============+============+
| 2.3.x  | 8 only     | 2.7..3.7   |
+--------+------------+------------+
| 2.4.x  | 8 only     | 2.7..3.7   |
+--------+------------+------------+
| 3.2.x  | 8u201..11  | 3.7..3.10  |
+--------+------------+------------+
| 3.3.x  | 8u201..17  | 3.7..3.11  |
+--------+------------+------------+

Then you should install PySpark via passing ``spark`` to ``extras``:

.. code:: bash

    pip install onetl[spark]  # install latest PySpark

or install PySpark explicitly:

.. code:: bash

    pip install onetl pyspark==3.3.1  # install a specific PySpark version

or inject PySpark to ``sys.path`` in some other way BEFORE creating a class instance.
**Otherwise class import will fail.**


.. _files-install:

With file connections
~~~~~~~~~~~~~~~~~~~~~

All file connection classes (``FTP``,  ``SFTP``, ``HDFS`` and so on) requires specific Python clients to be installed.

Each client can be installed explicitly by passing connector name (in lowercase) to ``extras``:

.. code:: bash

    pip install onetl[ftp]  # specific connector
    pip install onetl[ftp, ftps, sftp, hdfs, s3, webdav]  # multiple connectors

To install all file connectors at once you can pass ``files`` to ``extras``:

.. code:: bash

    pip install onetl[files]

**Otherwise class import will fail.**


.. _kerberos-install:

With Kerberos support
~~~~~~~~~~~~~~~~~~~~~

Most of Hadoop instances set up with Kerberos support,
so some connections require additional setup to work properly.

* ``HDFS``
    Uses `requests-kerberos <https://pypi.org/project/requests-kerberos/>`_ and
    `GSSApi <https://pypi.org/project/gssapi/>`_ for authentication in WebHDFS.
    It also uses ``kinit`` executable to generate Kerberos ticket.

* ``Hive``
    Requires Kerberos ticket to exist before creating Spark session.

So you need to install OS packages with:

    * ``krb5`` libs
    * Headers for ``krb5``
    * ``gcc`` or other compiler for C sources

The exact installation instruction depends on your OS, here are some examples:

.. code:: bash

    dnf install krb5-devel gcc  # CentOS, OracleLinux
    apt install libkrb5-dev gcc  # Debian-based

Also you should pass ``kerberos`` to ``extras`` to install required Python packages:

.. code:: bash

    pip install onetl[kerberos]


.. _full-install:

Full bundle
~~~~~~~~~~~

To install all connectors and dependencies, you can pass ``all`` into ``extras``:

.. code:: bash

    pip install onetl[all]

    # this is just the same as
    pip install onetl[spark, files, kerberos]

.. warning::

    This method consumes a lot of disk space, and requires for Java & Kerberos libraries to be installed into your OS.

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
