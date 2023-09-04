.. _greenplum-prerequisites:

Prerequisites
=============

Version Compatibility
---------------------

* Greenplum server versions: 5.x, 6.x
* Spark versions: 2.3.x - 3.2.x (Spark 3.3.x is not supported yet)
* Java versions: 8 - 11

See `official documentation <https://docs.vmware.com/en/VMware-Tanzu-Greenplum-Connector-for-Apache-Spark/2.1/tanzu-greenplum-connector-spark/GUID-release_notes.html>`_.

Installing PySpark
------------------

To use Greenplum connector you should have PySpark installed (or injected to ``sys.path``)
BEFORE creating the connector instance.

You can install PySpark as follows:

.. code:: bash

    pip install onetl pyspark=3.2.3  # pass specific PySpark version

See :ref:`spark-install` instruction for more details.

Downloading Pivotal package
---------------------------

To use Greenplum connector you should download connector ``.jar`` file from
`Pivotal website <https://network.tanzu.vmware.com/products/vmware-greenplum#/releases/1341690/file_groups/14993>`_
and then pass it to Spark session.

There are several ways to do that.

.. note::

    Please pay attention to Spark <-> Scala version compatibility. See :ref:`spark-compatibility-matrix`.

Using ``spark.jars``
~~~~~~~~~~~~~~~~~~~~

The most simple solution, but this requires to store/deploy ``.jar`` file in the local environment.

* Download ``greenplum-connector-apache-spark-scala_2.12-2.1.4.jar`` file.
* Create Spark session with passing ``.jar`` absolute file path to ``spark.jars`` Spark config option, e.g.

.. code:: python

    # no need to use spark.jars.packages
    spark = (
        SparkSession.builder.config("spark.app.name", "onetl")
        .config("spark.jars", "/path/to/downloaded.jar")
        .getOrCreate()
    )

Using ``spark.jars.repositories``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Can be used if you have access both to public repos (like Maven) and a private Artifactory/Nexus repo.

* Setup private Maven repository in `JFrog Artifactory <https://jfrog.com/artifactory/>`_ or `Sonatype Nexus <https://www.sonatype.com/products/sonatype-nexus-repository>`_.
* Download ``greenplum-connector-apache-spark-scala_2.12-2.1.4.jar`` file.
* Upload ``.jar`` file to private repository (with ``groupId=io.pivotal``, ``artifactoryId=greenplum-spark_2.12``).
* Pass repo URL to ``spark.jars.repositories`` Spark config option
* Create Spark session with passing Greenplum package name to ``spark.jars.packages`` Spark config option.


Example
^^^^^^^

.. code:: python

    maven_packages = Greenplum.get_packages(spark_version="3.2")
    spark = (
        SparkSession.builder.config("spark.app.name", "onetl")
        .config("spark.jars.repositories", "http://nexus.domain.com/example-repo/")
        .config("spark.jars.packages", ",".join(maven_packages))
        .getOrCreate()
    )


Using ``spark.jars.ivySettings``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Same as above, but can be used even if there is no network access to public repos like Maven.

* Setup private Maven repository in `JFrog Artifactory <https://jfrog.com/artifactory/>`_ or `Sonatype Nexus <https://www.sonatype.com/products/sonatype-nexus-repository>`_.
* Download ``greenplum-connector-apache-spark-scala_2.12-2.1.4.jar`` file.
* Upload ``.jar`` file to private repository (with ``groupId=io.pivotal``, ``artifactoryId=greenplum-spark_2.12``).
* Create `ivysettings.xml <https://github.com/MobileTeleSystems/onetl/blob/develop/tests/ivysettings.xml>`_ file.
* Add here a resolver with repository URL (and credentials, if required).
* Pass ``ivysettings.xml`` absolute path to ``spark.jars.ivySettings`` Spark config option.
* Create Spark session with passing Greenplum package name to ``spark.jars.packages`` Spark config option.

Example
^^^^^^^

.. code-block:: xml
    :caption: ivysettings.xml

    <ivysettings>
        <settings defaultResolver="main"/>
        <resolvers>
            <chain name="main" returnFirst="true">
                <!-- Use Maven cache -->
                <ibiblio name="local-maven-cache" m2compatible="true" root="file://${user.home}/.m2/repository"/>
                <!-- Use ~/.ivy2/jars/*.jar files -->
                <ibiblio name="local-ivy2-cache" m2compatible="false" root="file://${user.home}/.ivy2/jars"/>
                <!-- Download packages from Maven, remove if no network access -->
                <ibiblio name="central" m2compatible="true" />
                <!-- Download packages from SparkPackages, remove if no network access -->
                <ibiblio name="spark-packages" m2compatible="true" root="https://repos.spark-packages.org/" />
                <!-- Nexus repo-->
                <ibiblio name="nexus-private" m2compatible="true" root="http://nexus.domain.com/example-repo/" />
            </chain>
        </resolvers>
    </ivysettings>


.. code-block:: python
    :caption: script.py

    maven_packages = Greenplum.get_packages(spark_version="3.2")
    spark = (
        SparkSession.builder.config("spark.app.name", "onetl")
        .config("spark.jars.ivySettings", "/path/to/ivysettings.xml")
        .config("spark.jars.packages", ",".join(maven_packages))
        .getOrCreate()
    )

Moving ``.jar`` file to ``~/.ivy2/jars/``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Can be used to pass already downloaded file to Ivy, and skip resolving package from Maven.

* Download ``greenplum-connector-apache-spark-scala_2.12-2.1.4.jar`` file.
* Move it to ``~/.ivy2/jars/`` folder
* Create Spark session with passing Greenplum package name to ``spark.jars.packages`` Spark config option.

Example
^^^^^^^

.. code:: python

    maven_packages = Greenplum.get_packages(spark_version="3.2")
    spark = (
        SparkSession.builder.config("spark.app.name", "onetl")
        .config("spark.jars.packages", ",".join(maven_packages))
        .getOrCreate()
    )

Inserting ``.jar`` file to Spark jars folder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Can be used to embed ``.jar`` files to a default Spark classpath.

* Download ``greenplum-connector-apache-spark-scala_2.12-2.1.4.jar`` file.
* Move it to ``$SPARK_HOME/jars/`` folder, e.g. ``~/.local/lib/python3.7/site-packages/pyspark/jars/`` or ``/opt/spark/3.2.3/jars/``.
* Create Spark session **WITHOUT** passing Greenplum package name to ``spark.jars.packages``


Manually adding ``.jar`` files to ``CLASSPATH``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Can be used to embed ``.jar`` files to a default Java classpath.

* Download ``greenplum-connector-apache-spark-scala_2.12-2.1.4.jar`` file.
* Set environment variable ``CLASSPATH`` to ``/path/to/downloader.jar``
* Create Spark session **WITHOUT** passing Greenplum package name to ``spark.jars.packages``
