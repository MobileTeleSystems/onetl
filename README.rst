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

Python ETL/ELT framework powered by `Apache Spark <https://spark.apache.org/>`_ & other open-source tools.

* Provides unified classes to extract data from (**E**) & load data to (**L**) various stores.
* Relies on `Spark DataFrame API <https://spark.apache.org/docs/3.2.0/api/python/reference/api/pyspark.sql.DataFrame.html>`_ for performing transformations (**T**) in terms of *ETL*.
* Provides direct assess to database, allowing to execute SQL queries, as well as DDL, DML, and call functions/procedures. This can be used for building up *ELT* pipelines.
* Supports different `read strategies <https://bigdata.pages.mts.ru/platform/onetools/onetl/strategy/index.html>`_ for incremental and batch data fetching.
* Provides hooks & plugins mechanism for altering behavior of internal classes.

Requirements
------------
* **Python 3.7 - 3.10**
* PySpark 2.3 - 3.3 (depends on used connector)
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
| MongoDB    | `MongoDB connection <db_connection/mongodb.html>`_       | Powered by `MongoDB Spark connector <https://www.mongodb.com/docs/spark-connector/master/>`_                        |
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

+--------+-------------+-------------+
| Spark  | Java        | Python      |
+========+=============+=============+
| 2.3.x  | 8 only      | 2.7 - 3.7   |
+--------+-------------+-------------+
| 2.4.x  | 8 only      | 2.7 - 3.7   |
+--------+-------------+-------------+
| 3.2.x  | 8u201 - 11  | 3.7 - 3.10  |
+--------+-------------+-------------+
| 3.3.x  | 8u201 - 17  | 3.7 - 3.10  |
+--------+-------------+-------------+

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

Develop
-------

Clone repo
~~~~~~~~~~

Clone repo:

.. code:: bash

    git clone git@gitlab.services.mts.ru:bigdata/platform/onetools/onetl.git -b develop

    cd onetl

Setup environment
~~~~~~~~~~~~~~~~~

Create virtualenv and install dependencies:

.. code:: bash

    python -m venv venv
    source venv/bin/activate
    pip install -U wheel
    pip install -U pip setuptools
    pip install -U \
        -r requirements/requirements.txt \
        -r requirements/requirements-ftp.txt \
        -r requirements/requirements-hdfs.txt \
        -r requirements/requirements-kerberos.txt \
        -r requirements/requirements-s3.txt \
        -r requirements/requirements-sftp.txt \
        -r requirements/requirements-spark.txt \
        -r requirements/requirements-webdav.txt \
        -r requirements/requirements-dev.txt \
        -r requirements/requirements-docs.txt \
        -r requirements/requirements-test.txt

Enable pre-commit hooks
~~~~~~~~~~~~~~~~~~~~~~~

Install pre-commit hooks:

.. code:: bash

    pre-commit install --install-hooks

Test pre-commit hooks run:

.. code:: bash

    pre-commit run

.. tests

Tests
~~~~~

Using docker-compose
^^^^^^^^^^^^^^^^^^^^

Build image for running tests:

.. code:: bash

    docker-compose build

Start all containers with dependencies:

.. code:: bash

    docker-compose up -d

You can run limited set of dependencies:

.. code:: bash

    docker-compose up -d mongodb

Run tests:

.. code:: bash

    docker-compose run --rm onetl ./run_tests.sh

You can pass additional arguments, they will be passed to pytest:

.. code:: bash

    docker-compose run --rm onetl ./run_tests.sh -m mongodb -lsx -vvvv --log-cli-level=INFO

You can run interactive bash session and use it:

.. code:: bash

    docker-compose run --rm onetl bash

    ./run_tests.sh -m mongodb -lsx -vvvv --log-cli-level=INFO

See logs of test container:

.. code:: bash

    docker-compose logs -f onetl

Stop all containers and remove created volumes:

.. code:: bash

    docker-compose down -v

Run tests locally
^^^^^^^^^^^^^^^^^

.. warning::

    To run HDFS and Hive tests locally you should add the following line to your ``/etc/hosts`` (file path depends on OS):

    .. code::

        127.0.0.1 hive2

.. note::

    To run Oracle tests you need to install `Oracle instantclient <https://www.oracle.com/database/technologies/instant-client.html>`__,
    and pass its path to ``ONETL_ORA_CLIENT_PATH`` environment variable, e.g. ``ONETL_ORA_CLIENT_PATH=/path/to/client64/lib``.

    It may also require to add the same path into ``LD_LIBRARY_PATH`` environment variable

Build image for running tests:

.. code:: bash

    docker-compose build

Start all containers with dependencies:

.. code:: bash

    docker-compose up -d

You can run limited set of dependencies:

.. code:: bash

    docker-compose up -d mongodb

Load environment variables with connection properties:

.. code:: bash

    source .env.local

Run tests:

.. code:: bash

    ./run_tests.sh

You can pass additional arguments, they will be passed to pytest:

.. code:: bash

    ./run_tests.sh -m mongodb -lsx -vvvv --log-cli-level=INFO

Stop all containers and remove created volumes:

.. code:: bash

    docker-compose down -v
