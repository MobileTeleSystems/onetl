.. _install-spark:

Spark
=====

.. include:: ../../README.rst
    :start-after: .. _spark-install:
    :end-before: .. _java-install:

Installing Java
---------------

.. include:: ../../README.rst
    :start-after: .. _java-install:
    :end-before: .. _pyspark-install:

Installing PySpark
------------------

.. include:: ../../README.rst
    :start-after: .. _pyspark-install:
    :end-before: With File connections

.. _java-packages:

Injecting Java packages
-----------------------

Some DB and FileDF connection classes require specific packages to be inserted to ``CLASSPATH`` of Spark session,
like JDBC drivers.

This is usually done by setting up ``spark.jars.packages`` option while creating Spark session:

.. code:: python

    # here is a list of packages to be downloaded:
    maven_packages = (
        Greenplum.get_packages(spark_version="3.2")
        + MySQL.get_packages()
        + Postgres.get_packages()
    )

    spark = (
        SparkSession.builder.config("spark.app.name", "onetl")
        .config("spark.jars.packages", ",".join(maven_packages))
        .getOrCreate()
    )


Spark automatically resolves package and all its dependencies, download them and inject to Spark session
(both driver and all executors).

This requires internet access, because package metadata and ``.jar`` files are fetched from `Maven Repository <https://mvnrepository.com/>`_.

But sometimes it is required to:

* Install package without direct internet access (isolated network)
* Install package which is not available in Maven

There are several ways to do that.

Using ``spark.jars``
^^^^^^^^^^^^^^^^^^^^

The most simple solution, but this requires to store raw ``.jar`` files somewhere on filesystem or web server.

* Download ``package.jar`` files (it's usually something like ``some-package_1.0.0.jar``). Local file name does not matter, but it should be unique.
* (For ``spark.submit.deployMode=cluster``) place downloaded files to HDFS or deploy to any HTTP web server serving static files. See `official documentation <https://spark.apache.org/docs/latest/submitting-applications.html#advanced-dependency-management>`_ for more details.
* Create Spark session with passing ``.jar`` absolute file path to ``spark.jars`` Spark config option:

.. tabs::

    .. code-tab:: py for spark.submit.deployMode=client (default)

        jar_files = ["/path/to/package.jar"]

        # do not pass spark.jars.packages
        spark = (
            SparkSession.builder.config("spark.app.name", "onetl")
            .config("spark.jars", ",".join(jar_files))
            .getOrCreate()
        )

    .. code-tab:: py for spark.submit.deployMode=cluster

        # you can also pass URLs like http://domain.com/path/to/downloadable/package.jar
        jar_files = ["hdfs:///path/to/package.jar"]

        # do not pass spark.jars.packages
        spark = (
            SparkSession.builder.config("spark.app.name", "onetl")
            .config("spark.jars", ",".join(jar_files))
            .getOrCreate()
        )

Using ``spark.jars.repositories``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. note::

    In this case Spark still will try to fetch packages from the internet, so if you don't have internet access,
    Spark session will be created with significant delay because of all attempts to fetch packages.

Can be used if you have access both to public repos (like Maven) and a private Artifactory/Nexus repo.

* Setup private Maven repository in `JFrog Artifactory <https://jfrog.com/artifactory/>`_ or `Sonatype Nexus <https://www.sonatype.com/products/sonatype-nexus-repository>`_.
* Download ``package.jar`` file (it's usually something like ``some-package_1.0.0.jar``). Local file name does not matter.
* Upload ``package.jar`` file to private repository (with same ``groupId`` and ``artifactoryId`` as in source package in Maven).
* Pass repo URL to ``spark.jars.repositories`` Spark config option.
* Create Spark session with passing Package name to ``spark.jars.packages`` Spark config option:

.. code:: python

    maven_packages = (
        Greenplum.get_packages(spark_version="3.2")
        + MySQL.get_packages()
        + Postgres.get_packages()
    )

    spark = (
        SparkSession.builder.config("spark.app.name", "onetl")
        .config("spark.jars.repositories", "http://nexus.mydomain.com/private-repo/")
        .config("spark.jars.packages", ",".join(maven_packages))
        .getOrCreate()
    )


Using ``spark.jars.ivySettings``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Same as above, but can be used even if there is no network access to public repos like Maven.

* Setup private Maven repository in `JFrog Artifactory <https://jfrog.com/artifactory/>`_ or `Sonatype Nexus <https://www.sonatype.com/products/sonatype-nexus-repository>`_.
* Download ``package.jar`` file (it's usually something like ``some-package_1.0.0.jar``). Local file name does not matter.
* Upload ``package.jar`` file to `private repository <https://help.sonatype.com/repomanager3/nexus-repository-administration/repository-management#RepositoryManagement-HostedRepository>`_ (with same ``groupId`` and ``artifactoryId`` as in source package in Maven).
* Create ``ivysettings.xml`` file (see below).
* Add here a resolver with repository URL (and credentials, if required).
* Pass ``ivysettings.xml`` absolute path to ``spark.jars.ivySettings`` Spark config option.
* Create Spark session with passing package name to ``spark.jars.packages`` Spark config option:

.. tabs::

    .. code-tab:: xml ivysettings-all-packages-uploaded-to-nexus.xml

        <ivysettings>
            <settings defaultResolver="main"/>
            <resolvers>
                <chain name="main" returnFirst="true">
                    <!-- Use Maven cache -->
                    <ibiblio name="local-maven-cache" m2compatible="true" root="file://${user.home}/.m2/repository"/>
                    <!-- Use -/.ivy2/jars/*.jar files -->
                    <ibiblio name="local-ivy2-cache" m2compatible="false" root="file://${user.home}/.ivy2/jars"/>
                    <!-- Download all packages from own Nexus instance -->
                    <ibiblio name="nexus-private" m2compatible="true" root="http://nexus.mydomain.com/private-repo/" />
                </chain>
            </resolvers>
        </ivysettings>

    .. code-tab:: xml ivysettings-private-packages-in-nexus-public-in-maven.xml

        <ivysettings>
            <settings defaultResolver="main"/>
            <resolvers>
                <chain name="main" returnFirst="true">
                    <!-- Use Maven cache -->
                    <ibiblio name="local-maven-cache" m2compatible="true" root="file://${user.home}/.m2/repository"/>
                    <!-- Use -/.ivy2/jars/*.jar files -->
                    <ibiblio name="local-ivy2-cache" m2compatible="false" root="file://${user.home}/.ivy2/jars"/>
                    <!-- Download private packages from own Nexus instance -->
                    <ibiblio name="nexus-private" m2compatible="true" root="http://nexus.mydomain.com/private-repo/" />
                    <!-- Download other packages from Maven -->
                    <ibiblio name="central" m2compatible="true" />
                    <!-- Download other packages from SparkPackages -->
                    <ibiblio name="spark-packages" m2compatible="true" root="https://repos.spark-packages.org/" />
                </chain>
            </resolvers>
        </ivysettings>

    .. code-tab:: xml ivysettings-private-packages-in-nexus-public-fetched-using-proxy-repo.xml

        <ivysettings>
            <settings defaultResolver="main"/>
            <resolvers>
                <chain name="main" returnFirst="true">
                    <!-- Use Maven cache -->
                    <ibiblio name="local-maven-cache" m2compatible="true" root="file://${user.home}/.m2/repository"/>
                    <!-- Use -/.ivy2/jars/*.jar files -->
                    <ibiblio name="local-ivy2-cache" m2compatible="false" root="file://${user.home}/.ivy2/jars"/>
                    <!-- Download private packages from own Nexus instance -->
                    <ibiblio name="nexus-private" m2compatible="true" root="http://nexus.mydomain.com/private-repo/" />
                    <!-- Download public packages from same Nexus instance using Proxy Repo
                    See https://help.sonatype.com/repomanager3/nexus-repository-administration/repository-management#RepositoryManagement-ProxyRepository
                    -->
                    <ibiblio name="nexus-proxy" m2compatible="true" root="http://nexus.mydomain.com/proxy-repo/" />
                </chain>
            </resolvers>
        </ivysettings>

    .. code-tab:: xml ivysettings-nexus-with-auth-required.xml

        <ivysettings>
            <settings defaultResolver="main"/>
            <properties environment="env"/>
            <!-- use environment variables NEXUS_USER and NEXUS_PASSWORD as credentials to auth in Nexus -->
            <property name="repo.username" value="${env.NEXUS_USER}"/>
            <property name="repo.pass" value="${env.NEXUS_PASSWORD}"/>
            <!-- realm value is described
            - here https://stackoverflow.com/a/38019000
            - here https://github.com/sonatype/nexus-book-examples/blob/master/ant-ivy/simple-project/ivysettings.xml
            - here https://support.sonatype.com/hc/en-us/articles/213465388-How-do-I-configure-my-Ivy-build-to-deploy-artifacts-to-Nexus-Repository-2-
            -->
            <credentials host="nexus.mydomain.com" username="${repo.username}" passwd="${repo.pass}" realm="Sonatype Nexus Repository Manager" />
            <resolvers>
                <chain name="main" returnFirst="true">
                    <!-- Use Maven cache -->
                    <ibiblio name="local-maven-cache" m2compatible="true" root="file://${user.home}/.m2/repository"/>
                    <!-- Use -/.ivy2/jars/*.jar files -->
                    <ibiblio name="local-ivy2-cache" m2compatible="false" root="file://${user.home}/.ivy2/jars"/>
                    <!-- Download all packages from own Nexus instance, using credentials for domain above -->
                    <ibiblio name="nexus-private" m2compatible="true" root="http://nexus.mydomain.com/private-repo/" />
                </chain>
            </resolvers>
        </ivysettings>


.. code-block:: python
    :caption: script.py

    maven_packages = (
        Greenplum.get_packages(spark_version="3.2")
        + MySQL.get_packages()
        + Postgres.get_packages()
    )

    spark = (
        SparkSession.builder.config("spark.app.name", "onetl")
        .config("spark.jars.ivySettings", "/path/to/ivysettings.xml")
        .config("spark.jars.packages", ",".join(maven_packages))
        .getOrCreate()
    )

Place ``.jar`` file to ``-/.ivy2/jars/``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Can be used to pass already downloaded file to Ivy, and skip resolving package from Maven.

* Download ``package.jar`` file (it's usually something like ``some-package_1.0.0.jar``). Local file name does not matter, but it should be unique.
* Move it to ``-/.ivy2/jars/`` folder.
* Create Spark session with passing package name to ``spark.jars.packages`` Spark config option:

.. code:: python

    maven_packages = (
        Greenplum.get_packages(spark_version="3.2")
        + MySQL.get_packages()
        + Postgres.get_packages()
    )

    spark = (
        SparkSession.builder.config("spark.app.name", "onetl")
        .config("spark.jars.packages", ",".join(maven_packages))
        .getOrCreate()
    )

Place ``.jar`` file to Spark jars folder
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. note::

    Package file should be placed on all hosts/containers Spark is running,
    both driver and all executors.

    Usually this is used only with either:
        * ``spark.master=local`` (driver and executors are running on the same host),
        * ``spark.master=k8s://...`` (``.jar`` files are added to image or to volume mounted to all pods).

Can be used to embed ``.jar`` files to a default Spark classpath.

* Download ``package.jar`` file (it's usually something like ``some-package_1.0.0.jar``). Local file name does not matter, but it should be unique.
* Move it to ``$SPARK_HOME/jars/`` folder, e.g. ``^/.local/lib/python3.7/site-packages/pyspark/jars/`` or ``/opt/spark/3.2.3/jars/``.
* Create Spark session **WITHOUT** passing Package name to ``spark.jars.packages``

.. code:: python

    # no need to set spark.jars.packages or any other spark.jars.* option
    # all jars already present in CLASSPATH, and loaded automatically

    spark = SparkSession.builder.config("spark.app.name", "onetl").getOrCreate()


Manually adding ``.jar`` files to ``CLASSPATH``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. note::

    Package file should be placed on all hosts/containers Spark is running,
    both driver and all executors.

    Usually this is used only with either:
        * ``spark.master=local`` (driver and executors are running on the same host),
        * ``spark.master=k8s://...`` (``.jar`` files are added to image or to volume mounted to all pods).

Can be used to embed ``.jar`` files to a default Java classpath.

* Download ``package.jar`` file (it's usually something like ``some-package_1.0.0.jar``). Local file name does not matter.
* Set environment variable ``CLASSPATH`` to ``/path/to/package.jar``. You can set multiple file paths
* Create Spark session **WITHOUT** passing Package name to ``spark.jars.packages``

.. code:: python

    # no need to set spark.jars.packages or any other spark.jars.* option
    # all jars already present in CLASSPATH, and loaded automatically

    import os

    jar_files = ["/path/to/package.jar"]
    # different delimiters for Windows and Linux
    delimiter = ";" if os.name == "nt" else ":"
    spark = (
        SparkSession.builder.config("spark.app.name", "onetl")
        .config("spark.driver.extraClassPath", delimiter.join(jar_files))
        .config("spark.executor.extraClassPath", delimiter.join(jar_files))
        .getOrCreate()
    )
