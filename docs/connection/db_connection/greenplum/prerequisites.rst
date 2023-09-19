.. _greenplum-prerequisites:

Prerequisites
=============

Version Compatibility
---------------------

* Greenplum server versions: 5.x, 6.x
* Spark versions: 2.3.x - 3.2.x (Spark 3.3+ is not supported yet)
* Java versions: 8 - 11

See `official documentation <https://docs.vmware.com/en/VMware-Tanzu-Greenplum-Connector-for-Apache-Spark/2.1/tanzu-greenplum-connector-spark/GUID-release_notes.html>`_.

Installing PySpark
------------------

To use Greenplum connector you should have PySpark installed (or injected to ``sys.path``)
BEFORE creating the connector instance.

You can install PySpark as follows:

.. code:: bash

    pip install onetl pyspark=3.2.4  # pass specific PySpark version

See :ref:`spark-install` instruction for more details.

Downloading Pivotal package
---------------------------

To use Greenplum connector you should download connector ``.jar`` file from
`Pivotal website <https://network.tanzu.vmware.com/products/vmware-greenplum#/releases/1341690/file_groups/14993>`_
and then pass it to Spark session.

.. warning::

    Please pay attention to :ref:`Spark <-> Scala version compatibility <spark-compatibility-matrix>`.

There are several ways to do that. See :ref:`java-packages` for details.

.. note::

    If you're uploading package to private package repo, use ``groupId=io.pivotal`` and ``artifactoryId=greenplum-spark_2.12``
    (``2.12`` is Scala version) to give uploaded package a proper name.
