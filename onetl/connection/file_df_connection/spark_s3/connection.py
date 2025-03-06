# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
import os
from contextlib import suppress
from typing import TYPE_CHECKING, ClassVar, List, Optional

from etl_entities.instance import Host

try:
    from pydantic.v1 import SecretStr, root_validator, validator
except (ImportError, AttributeError):
    from pydantic import SecretStr, root_validator, validator  # type: ignore[no-redef, assignment]

from typing_extensions import Literal

from onetl._util.hadoop import get_hadoop_config, get_hadoop_version
from onetl._util.java import try_import_java_class
from onetl._util.scala import get_default_scala_version
from onetl._util.spark import get_client_info, get_spark_version, stringify
from onetl._util.version import Version
from onetl.base import (
    BaseReadableFileFormat,
    BaseWritableFileFormat,
    FileDFReadOptions,
    FileDFWriteOptions,
    PurePathProtocol,
)
from onetl.connection.file_df_connection.spark_file_df_connection import (
    SparkFileDFConnection,
)
from onetl.connection.file_df_connection.spark_s3.extra import SparkS3Extra
from onetl.exception import MISSING_JVM_CLASS_MSG
from onetl.hooks import slot, support_hooks
from onetl.impl import RemotePath
from onetl.log import log_options

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import StructType

log = logging.getLogger(__name__)


@support_hooks
class SparkS3(SparkFileDFConnection):
    """
    Spark connection to S3 filesystem. |support_hooks|

    Based on `Hadoop-AWS module <https://hadoop.apache.org/docs/current3/hadoop-aws/tools/hadoop-aws/index.html>`_
    and `Spark integration with Cloud Infrastructures <https://spark.apache.org/docs/latest/cloud-integration.html>`_.

    .. seealso::

        Before using this connector please take into account :ref:`spark-s3-prerequisites`

    .. note::

        Supports only reading files as Spark DataFrame and writing DataFrame to files.

        Does NOT support file operations, like create, delete, rename, etc. For these operations,
        use :obj:`S3 <onetl.connection.file_connection.s3.S3>` connection.

    .. versionadded:: 0.9.0

    Parameters
    ----------
    host : str
        Host of S3 source. For example: ``domain.com``

    port : int, optional
        Port of S3 source

    bucket : str
        Bucket name in the S3 file source

    protocol : str, default : ``https``
        Connection protocol. Allowed values: ``https`` or ``http``

    access_key : str, optional
        Access key (aka user ID) of an account in the S3 service

    secret_key : str, optional
        Secret key (aka password) of an account in the S3 service

    session_token : str, optional
        Session token of your account in S3 service

    region : str, optional
        Region name of bucket in S3 service

    extra : dict, optional
        A dictionary of additional properties to be used when connecting to S3.

        These are Hadoop AWS specific properties, see links below:

        * `Hadoop AWS <https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html#General_S3A_Client_configuration>`_
        * `Hadoop AWS committers options <https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/committers.html>`_

        Options are passed without prefixes ``spark.hadoop.``, ``fs.s3a.`` and ``fs.s3a.bucket.$BUCKET.``, for example:

        .. code:: python

            extra = {
                "path.style.access": True,
                "committer.magic.enabled": True,
                "committer.name": "magic",
                "connection.timeout": 300000,
            }

        .. warning::

            Options that populated from connection
            attributes (like ``endpoint``, ``access.key``) are not allowed to override.

            But you may override ``aws.credentials.provider`` and pass custom credential options.

    spark : :class:`pyspark.sql.SparkSession`
        Spark session

    Examples
    --------

    .. tabs::

        .. code-tab:: py Create S3 connection with bucket as subdomain (``my-bucket.domain.com``):

            from onetl.connection import SparkS3
            from pyspark.sql import SparkSession

            # Create Spark session with Hadoop AWS libraries loaded
            maven_packages = SparkS3.get_packages(spark_version="3.5.5")
            # Some packages are not used, but downloading takes a lot of time. Skipping them.
            excluded_packages = SparkS3.get_exclude_packages()
            spark = (
                SparkSession.builder.appName("spark-app-name")
                .config("spark.jars.packages", ",".join(maven_packages))
                .config("spark.jars.excludes", ",".join(excluded_packages))
                .config("spark.hadoop.fs.s3a.committer.magic.enabled", "true")
                .config("spark.hadoop.fs.s3a.committer.name", "magic")
                .config(
                    "spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a",
                    "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory",
                )
                .config(
                    "spark.sql.parquet.output.committer.class",
                    "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter",
                )
                .config(
                    "spark.sql.sources.commitProtocolClass",
                    "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol",
                )
                .getOrCreate()
            )

            # Create connection
            s3 = SparkS3(
                host="domain.com",
                protocol="http",
                bucket="my-bucket",
                access_key="ACCESS_KEY",
                secret_key="SECRET_KEY",
                spark=spark,
            ).check()

        .. code-tab:: py Create S3 connection with bucket as subpath (``domain.com/my-bucket``)

            # Create Spark session with Hadoop AWS libraries loaded
            ...

            # Create connection
            s3 = SparkS3(
                host="domain.com",
                protocol="http",
                bucket="my-bucket",
                access_key="ACCESS_KEY",
                secret_key="SECRET_KEY",
                extra={
                    "path.style.access": True,  # <---
                },
                spark=spark,
            ).check()
    """

    Extra = SparkS3Extra

    host: Host
    port: Optional[int] = None
    bucket: str
    protocol: Literal["http", "https"] = "https"
    access_key: Optional[str] = None
    secret_key: Optional[SecretStr] = None
    session_token: Optional[SecretStr] = None
    region: Optional[str] = None
    extra: SparkS3Extra = SparkS3Extra()

    _ROOT_CONFIG_KEYS: ClassVar[List[str]] = [
        "committer.magic.enabled",
        "committer.name",
        "user.agent.prefix",
    ]

    @slot
    @classmethod
    def get_packages(
        cls,
        spark_version: str,
        scala_version: str | None = None,
    ) -> list[str]:
        """
        Get package names to be downloaded by Spark. |support_hooks|

        .. versionadded:: 0.9.0

        Parameters
        ----------
        spark_version : str
            Spark version in format ``major.minor.patch``.

        scala_version : str, optional
            Scala version in format ``major.minor``.

            If ``None``, ``spark_version`` is used to determine Scala version.

        Examples
        --------

        .. code:: python

            from onetl.connection import SparkS3

            SparkS3.get_packages(spark_version="3.5.5")
            SparkS3.get_packages(spark_version="3.5.5", scala_version="2.12")

        """

        spark_ver = Version(spark_version).min_digits(3)
        if spark_ver.major < 3:
            # https://issues.apache.org/jira/browse/SPARK-23977
            raise ValueError(f"Spark version must be at least 3.x, got {spark_ver}")

        scala_ver = Version(scala_version).min_digits(2) if scala_version else get_default_scala_version(spark_ver)
        # https://mvnrepository.com/artifact/org.apache.spark/spark-hadoop-cloud
        return [f"org.apache.spark:spark-hadoop-cloud_{scala_ver.format('{0}.{1}')}:{spark_ver.format('{0}.{1}.{2}')}"]

    @slot
    @classmethod
    def get_exclude_packages(cls) -> list[str]:
        """
        Get package names to be excluded by Spark. |support_hooks|

        .. versionadded:: 0.13.0

        Examples
        --------

        .. code:: python

            from onetl.connection import SparkS3

            SparkS3.get_exclude_packages()

        """

        return [
            # heavy and not used
            "com.google.cloud.bigdataoss:gcs-connector",
            "org.apache.hadoop:hadoop-aliyun",
            "org.apache.hadoop:hadoop-azure-datalake",
            "org.apache.hadoop:hadoop-azure",
            "org.apache.hadoop:hadoop-cos",
            "org.apache.hadoop:hadoop-openstack",
            "org.apache.hadoop:hadoop-huaweicloud",
            # already a part of Spark bundle
            "org.apache.hadoop:hadoop-client-api",
            "org.apache.hadoop:hadoop-client-runtime",
            "com.fasterxml.jackson.core:jackson-annotations",
            "com.fasterxml.jackson.core:jackson-core",
            "com.fasterxml.jackson.core:jackson-databind",
            "com.google.code.findbugs:jsr305",
            "commons-codec:commons-codec",
            "commons-logging:commons-logging",
            "joda-time:joda-time",
            "org.apache.httpcomponents:httpclient",
            "org.apache.httpcomponents:httpcore",
            "org.slf4j:slf4j-api",
            "org.xerial.snappy:snappy-java",
        ]

    @slot
    def path_from_string(self, path: os.PathLike | str) -> RemotePath:
        return RemotePath(os.fspath(path))

    @property
    def instance_url(self):
        return f"s3://{self.host}:{self.port}/{self.bucket}"

    def __str__(self):
        return f"S3[{self.host}:{self.port}/{self.bucket}]"

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @slot
    def close(self):
        """
        Close all connections created to S3. |support_hooks|

        Also resets all ``fs.s3a.bucket.$BUCKET.*`` properties of Hadoop configuration.

        .. note::

            Connection can be used again after it was closed.

        Returns
        -------
        Connection itself

        Examples
        --------

        Close connection automatically:

        .. code:: python

            with connection:
                ...

        Close connection manually:

        .. code:: python

            connection.close()

        """
        with suppress(Exception):
            self._reset_hadoop_conf()
        return self

    # Do not all __del__ with calling .close(), like other connections,
    # because this can influence dataframes created by this connection

    @slot
    def check(self):
        self._patch_hadoop_conf()
        return super().check()

    @slot
    def read_files_as_df(
        self,
        paths: list[PurePathProtocol],
        format: BaseReadableFileFormat,  # noqa: WPS125
        root: PurePathProtocol | None = None,
        df_schema: StructType | None = None,
        options: FileDFReadOptions | None = None,
    ) -> DataFrame:
        self._patch_hadoop_conf()
        return super().read_files_as_df(paths, format=format, root=root, df_schema=df_schema, options=options)

    @slot
    def write_df_as_files(
        self,
        df: DataFrame,
        path: PurePathProtocol,
        format: BaseWritableFileFormat,  # noqa: WPS125
        options: FileDFWriteOptions | None = None,
    ) -> None:
        self._patch_hadoop_conf()
        return super().write_df_as_files(df, path, format=format, options=options)

    @root_validator
    def _validate_port(cls, values):
        if values["port"] is not None:
            return values

        values["port"] = 443 if values["protocol"] == "https" else 80
        return values

    @validator("spark")
    def _check_java_class_imported(cls, spark: SparkSession) -> SparkSession:
        java_class = "org.apache.hadoop.fs.s3a.S3AFileSystem"

        try:
            try_import_java_class(spark, java_class)
        except Exception as e:
            spark_version = get_spark_version(spark).format("{major}.{minor}.{patch}")
            msg = MISSING_JVM_CLASS_MSG.format(
                java_class=java_class,
                package_source=cls.__name__,
                args=f"spark_version='{spark_version}'",
            )
            if log.isEnabledFor(logging.DEBUG):
                log.debug("Missing Java class", exc_info=e, stack_info=True)
            raise ValueError(msg) from e

        return spark

    @validator("spark")
    def _validate_hadoop_version(cls, spark: SparkSession) -> SparkSession:
        hadoop_version = get_hadoop_version(spark)
        if hadoop_version.major < 3:
            raise ValueError(f"Only Hadoop 3.x libraries are supported, got {hadoop_version}")
        return spark

    def _get_hadoop_config_prefix(self) -> str:
        return f"fs.s3a.bucket.{self.bucket}"

    def _fix_root_conf(self, conf: dict, prefix: str) -> None:
        """
        Some keys are read only from Hadoop ``fs.s3a.*`` config instead of ``fs.s3a.bucket.$BUCKET.*``, like
        ``fs.s3a.committer.name``. Move them from bucket config to root.
        """
        for key in self._ROOT_CONFIG_KEYS:
            prefixed_key = f"{prefix}.{key}"
            if prefixed_key in conf:
                conf[f"fs.s3a.{key}"] = conf.pop(prefixed_key)

    def _get_expected_hadoop_config(self, prefix: str) -> dict:
        conf = {
            f"{prefix}.endpoint": f"{self.protocol}://{self.host}:{self.port}",
            f"{prefix}.connection.ssl.enabled": self.protocol == "https",
            f"{prefix}.aws.credentials.provider": "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider",
        }

        if self.access_key and self.secret_key:
            conf[f"{prefix}.aws.credentials.provider"] = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
            conf[f"{prefix}.access.key"] = self.access_key
            conf[f"{prefix}.secret.key"] = self.secret_key.get_secret_value()

        if self.session_token:
            conf[f"{prefix}.aws.credentials.provider"] = "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider"
            conf[f"{prefix}.session.token"] = self.session_token.get_secret_value()

        if self.region:
            conf[f"{prefix}.endpoint.region"] = self.region

        for key, value in self.extra.dict(by_alias=True, exclude_none=True).items():
            conf[f"{prefix}.{key}"] = value

        # https://hadoop.apache.org/docs/r3.4.1/hadoop-aws/tools/hadoop-aws/index.html
        user_agent = conf.get(f"{prefix}.user.agent.prefix")
        client_info = get_client_info(self.spark)
        if user_agent:
            conf[f"{prefix}.user.agent.prefix"] = f"{user_agent} {client_info}"
        else:
            conf[f"{prefix}.user.agent.prefix"] = client_info

        self._fix_root_conf(conf, prefix)
        return stringify(conf)

    def _patch_hadoop_conf(self) -> None:
        """
        Spark relies on Hadoop AWS library to access S3 as FileSystem.
        It uses Hadoop configuration with prefix ``fs.s3a.`` and ``fs.s3a.bucket.$BUCKET.``.

        Instead of passing them in Spark config session with additional ``spark.hadoop.`` prefix
        (which is inconvenient and not consistent with other Connection classes),
        we can change these options in runtime by altering Hadoop configuration object.

        But if someone already accessed path like ``s3a://$BUCKET/...``, Spark will use existing configuration,
        because Hadoop FileSystem properties are cached after creating a FileSystem object.
        So we need to both write new options to Hadoop configuration and call FileSystem.close() to clear cache.

        If some other connection uses the same prefix (e.g. working with bucket of the same name, or
        user created connection with wrong options before), we also need to reset config.
        Warn user that something may be wrong.
        """
        prefix = self._get_hadoop_config_prefix()
        hadoop_config = get_hadoop_config(self.spark)
        expected_values = self._get_expected_hadoop_config(prefix)
        real_values = {
            item.getKey(): item.getValue()
            for item in hadoop_config.iterator()
            if item.getKey().startswith(prefix) or item.getKey() in expected_values
        }

        if real_values != expected_values:
            if real_values:
                log.warning(
                    "|%s| Spark hadoop configuration is different from expected, it will be reset",
                    self.__class__.__name__,
                )
                if log.isEnabledFor(logging.DEBUG):
                    log_options(log, expected_values, "expected", level=logging.DEBUG)
                    log_options(log, real_values, "real", level=logging.DEBUG)

            log.debug("Reset FileSystem cache")
            # Reset FileSystem cache to apply new config
            # Doing that before creating new FileSystem instance
            self._get_spark_fs().close()

            log.debug("Set Hadoop configuration")
            for key in real_values.keys():
                hadoop_config.unset(key)

            for key, value in expected_values.items():
                hadoop_config.set(key, value)

            # Create FileSystem with new config
            self._get_spark_fs()

    def _reset_hadoop_conf(self):
        """
        Reset changes made by :obj:`~_patch_hadoop_conf`.

        We cannot return default values which were set before patching because it requires rereading .xml files,
        undoing all customization made by Spark's ``spark.hadoop.`` options and by classes like SparkS3.

        So just call ``unset`` on specific keys. Hadoop source code are already contains default values for
        these options, this this should not be an issue.
        """
        log.debug("Unset Hadoop configuration")
        prefix = self._get_hadoop_config_prefix()
        hadoop_config = get_hadoop_config(self.spark)
        expected_values = self._get_expected_hadoop_config(prefix)
        for item in hadoop_config.iterator():
            key = item.getKey()
            if key.startswith(prefix) or key in expected_values:
                hadoop_config.unset(key)

        log.debug("Reset FileSystem cache")
        self._get_spark_fs().close()

    def _convert_to_url(self, path: PurePathProtocol) -> str:
        # Path looks like "s3a://bucket/absolute/path"
        # relative paths cannot be passed
        return "s3a://" + self.bucket + "/" + path.as_posix().lstrip("/")

    def _get_default_path(self):
        # Accessing s3://bucket will not fail even if user passed wrong connection options
        # Trying to access some path in bucket to trigger a request
        return RemotePath("/check")
