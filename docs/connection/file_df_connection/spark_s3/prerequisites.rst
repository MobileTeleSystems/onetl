.. _spark-s3-prerequisites:

Prerequisites
=============

Version Compatibility
---------------------

* Spark versions: 3.2.x - 3.5.x
* Java versions: 8 - 20

Installing PySpark
------------------

To use SparkS3 connector you should have PySpark installed (or injected to ``sys.path``)
BEFORE creating the connector instance.

See :ref:`install-spark` installation instruction for more details.

Connecting to S3
----------------

Bucket access style
~~~~~~~~~~~~~~~~~~~

AWS and some other S3 cloud providers allows bucket access using domain style only, e.g. ``https://mybucket.s3provider.com``.

Other implementations, like Minio, by default allows path style access only, e.g. ``https://s3provider.com/mybucket``
(see `MINIO_DOMAIN <https://min.io/docs/minio/linux/reference/minio-server/minio-server.html#envvar.MINIO_DOMAIN>`_).

You should set ``path_style_access`` to ``True`` or ``False``, to choose the preferred style.

Authentication
~~~~~~~~~~~~~~

Different S3 instances can use different authentication methods, like:
  * ``access_key + secret_key`` (or username + password)
  * ``access_key + secret_key + session_token``

Usually these are just passed to SparkS3 constructor:

.. code:: python

    SparkS3(
        access_key=...,
        secret_key=...,
        session_token=...,
    )

But some S3 cloud providers, like AWS, may require custom credential providers. You can pass them like:

.. code:: python

    SparkS3(
        extra={
            # provider class
            "aws.credentials.provider": "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider",
            # other options, if needed
            "assumed.role.arn": "arn:aws:iam::90066806600238:role/s3-restricted",
        },
    )

See `Hadoop-AWS <https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html#Changing_Authentication_Providers>`_ documentation.

Troubleshooting
---------------

See :ref:`spark-s3-troubleshooting`.
