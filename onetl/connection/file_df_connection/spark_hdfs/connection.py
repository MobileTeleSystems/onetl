# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import getpass
import logging
import os
from contextlib import suppress
from pathlib import Path
from typing import TYPE_CHECKING, Optional

from etl_entities.instance import Cluster, Host

try:
    from pydantic.v1 import Field, PrivateAttr, validator
except (ImportError, AttributeError):
    from pydantic import Field, PrivateAttr, validator  # type: ignore[no-redef, assignment]

from onetl.base import PurePathProtocol
from onetl.connection.file_df_connection.spark_file_df_connection import (
    SparkFileDFConnection,
)
from onetl.connection.file_df_connection.spark_hdfs.slots import SparkHDFSSlots
from onetl.hooks import slot, support_hooks
from onetl.impl import RemotePath

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

log = logging.getLogger(__name__)


@support_hooks
class SparkHDFS(SparkFileDFConnection):
    """
    Spark connection to HDFS. |support_hooks|

    Based on `Spark Generic File Data Source <https://spark.apache.org/docs/latest/sql-data-sources-generic-options.html>`_.

    .. seealso::

        Before using this connector please take into account :ref:`spark-hdfs-prerequisites`

    .. note::

        Supports only reading files as Spark DataFrame and writing DataFrame to files.

        Does NOT support file operations, like create, delete, rename, etc. For these operations,
        use :obj:`HDFS <onetl.connection.file_connection.hdfs.connection.HDFS>` connection.

    .. versionadded:: 0.9.0

    Parameters
    ----------
    cluster : str
        Cluster name.

        Used for:
            * HWM and lineage (as instance name for file paths)
            * Validation of ``host`` value,
                if latter is passed and if some hooks are bound to
                :obj:`Slots.get_cluster_namenodes <onetl.connection.file_df_connection.spark_hdfs.slots.SparkHDFSSlots.get_cluster_namenodes>`.

    host : str, optional
        Hadoop namenode host. For example: ``namenode1.domain.com``.

        Should be an active namenode (NOT standby).

        If value is not set, but there are some hooks bound to
        :obj:`Slots.get_cluster_namenodes <onetl.connection.file_df_connection.spark_hdfs.slots.SparkHDFSSlots.get_cluster_namenodes>`
        and
        :obj:`Slots.is_namenode_active <onetl.connection.file_df_connection.spark_hdfs.slots.SparkHDFSSlots.is_namenode_active>`,
        onETL will iterate over cluster namenodes to detect which one is active.

    ipc_port : int, default: ``8020``
        Port of Hadoop namenode (IPC protocol).

        If omitted, but there are some hooks bound to
        :obj:`Slots.get_ipc_port <onetl.connection.file_df_connection.spark_hdfs.slots.SparkHDFSSlots.get_ipc_port>`,
        onETL will try to detect port number for a specific ``cluster``.

    spark : :class:`pyspark.sql.SparkSession`
        Spark session

    Examples
    --------

    .. tabs::

        .. tab:: Create SparkHDFS connection with Kerberos auth

            Execute ``kinit`` consome command before creating Spark Session

            .. code:: bash

                $ kinit -kt /path/to/keytab user

            .. code:: python

                from onetl.connection import SparkHDFS
                from pyspark.sql import SparkSession

                # Create Spark session.
                # Use names "spark.yarn.access.hadoopFileSystems", "spark.yarn.principal"
                # and "spark.yarn.keytab" for Spark 2

                spark = (
                    SparkSession.builder.appName("spark-app-name")
                    .option(
                        "spark.kerberos.access.hadoopFileSystems",
                        "hdfs://namenode1.domain.com:8020",
                    )
                    .option("spark.kerberos.principal", "user")
                    .option("spark.kerberos.keytab", "/path/to/keytab")
                    .enableHiveSupport()
                    .getOrCreate()
                )

                # Create connection
                hdfs = SparkHDFS(
                    host="namenode1.domain.com",
                    cluster="rnd-dwh",
                    spark=spark,
                ).check()

        .. code-tab:: py Create SparkHDFS connection with anonymous auth

            from onetl.connection import SparkHDFS
            from pyspark.sql import SparkSession

            # Create Spark session
            spark = SparkSession.builder.master("local").appName("spark-app-name").getOrCreate()

            # Create connection
            hdfs = SparkHDFS(
                host="namenode1.domain.com",
                cluster="rnd-dwh",
                spark=spark,
            ).check()

        .. tab:: Use cluster name to detect active namenode

            Can be used only if some third-party plugin provides :ref:`spark-hdfs-slots` implementation

            .. code:: python

                # Create Spark session
                ...

                # Create connection
                hdfs = SparkHDFS(cluster="rnd-dwh", spark=spark).check()
    """

    Slots = SparkHDFSSlots

    cluster: Cluster
    host: Optional[Host] = None
    ipc_port: int = Field(default=8020, alias="port")

    _active_host: Optional[Host] = PrivateAttr(default=None)

    @slot
    def path_from_string(self, path: os.PathLike | str) -> Path:
        return RemotePath(os.fspath(path))

    @property
    def instance_url(self):
        return self.cluster

    def __str__(self):
        return f"HDFS[{self.cluster}]"

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @slot
    def close(self):
        """
        Close all connections created to HDFS. |support_hooks|

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
        log.debug("Reset FileSystem cache")
        with suppress(Exception):
            self._get_spark_fs().close()

        with suppress(Exception):
            self._active_host = None
        return self

    # Do not all __del__ with calling .close(), like other connections,
    # because this can influence dataframes created by this connection

    @slot
    @classmethod
    def get_current(cls, spark: SparkSession):
        """
        Create connection for current cluster. |support_hooks|

        Automatically sets up current cluster name as ``cluster``.

        .. note::

            Can be used only if there are a some hooks bound to
            :obj:`Slots.get_current_cluster <onetl.connection.file_df_connection.spark_hdfs.slots.SparkHDFSSlots.get_current_cluster>`.

        .. versionadded:: 0.9.0

        Parameters
        ----------
        spark : SparkSession

            See :obj:`~SparkHDFS` constructor documentation.

        Examples
        --------

        .. code:: python

            from onetl.connection import SparkHDFS

            # injecting current cluster name via hooks mechanism
            hdfs = SparkHDFS.get_current(spark=spark)
        """

        log.info("|%s| Detecting current cluster...", cls.__name__)
        current_cluster = cls.Slots.get_current_cluster()
        if not current_cluster:
            raise RuntimeError(
                f"{cls.__name__}.get_current() can be used only if there are "
                f"some hooks bound to {cls.__name__}.Slots.get_current_cluster",
            )

        log.info("|%s|   Got %r", cls.__name__, current_cluster)
        return cls(cluster=current_cluster, spark=spark)

    @validator("cluster")
    def _validate_cluster_name(cls, cluster):
        log.debug("|%s| Normalizing cluster %r name...", cls.__name__, cluster)
        validated_cluster = cls.Slots.normalize_cluster_name(cluster) or cluster
        if validated_cluster != cluster:
            log.debug("|%s|   Got %r", cls.__name__, validated_cluster)

        log.debug("|%s| Checking if cluster %r is a known cluster...", cls.__name__, validated_cluster)
        known_clusters = cls.Slots.get_known_clusters()
        if known_clusters and validated_cluster not in known_clusters:
            raise ValueError(
                f"Cluster {validated_cluster!r} is not in the known clusters list: {sorted(known_clusters)!r}",
            )

        return validated_cluster

    @validator("host")
    def _validate_host_name(cls, host, values):
        cluster = values.get("cluster")

        log.debug("|%s| Normalizing namenode %r host...", cls.__name__, host)
        namenode = cls.Slots.normalize_namenode_host(host, cluster) or host
        if namenode != host:
            log.debug("|%s|   Got %r", cls.__name__, namenode)

        log.debug("|%s| Checking if %r is a known namenode of cluster %r ...", cls.__name__, namenode, cluster)
        known_namenodes = cls.Slots.get_cluster_namenodes(cluster)
        if known_namenodes and namenode not in known_namenodes:
            raise ValueError(
                f"Namenode {namenode!r} is not in the known nodes list of cluster {cluster!r}: "
                f"{sorted(known_namenodes)!r}",
            )

        return namenode

    @validator("ipc_port", always=True)
    def _validate_port_number(cls, port, values):
        cluster = values.get("cluster")
        if cluster:
            log.debug("|%s| Getting IPC port of cluster %r ...", cls.__name__, cluster)
            result = cls.Slots.get_ipc_port(cluster) or port
            if result != port:
                log.debug("|%s|   Got %r", cls.__name__, result)
            return result

        return port

    def _get_active_namenode(self) -> str:
        class_name = self.__class__.__name__
        log.info("|%s| Detecting active namenode of cluster %r ...", class_name, self.cluster)

        namenodes = self.Slots.get_cluster_namenodes(self.cluster)
        if not namenodes:
            raise RuntimeError(f"Cannot get list of namenodes for a cluster {self.cluster!r}")

        nodes_len = len(namenodes)
        for i, namenode in enumerate(namenodes, start=1):
            log.debug("|%s|   Trying namenode %r (%d of %d) ...", class_name, namenode, i, nodes_len)
            if self.Slots.is_namenode_active(namenode, self.cluster):
                log.info("|%s|     Node %r is active!", class_name, namenode)
                return namenode
            log.debug("|%s|     Node %r is not active, skipping", class_name, namenode)

        raise RuntimeError(f"Cannot detect active namenode for cluster {self.cluster!r}")

    def _get_host(self) -> str:
        if not self.host:
            return self._get_active_namenode()

        # host is passed explicitly
        class_name = self.__class__.__name__
        log.info("|%s| Detecting if namenode %r of cluster %r is active...", class_name, self.host, self.cluster)

        is_active = self.Slots.is_namenode_active(self.host, self.cluster)
        if is_active:
            log.info("|%s|   Namenode %r is active!", class_name, self.host)
            return self.host

        if is_active is None:
            log.debug("|%s|   No hooks, skip validation", class_name)
            return self.host

        raise RuntimeError(f"Host {self.host!r} is not an active namenode of cluster {self.cluster!r}")

    def _get_conn_str(self) -> str:
        # cache active host to reduce number of requests.
        if not self._active_host:
            self._active_host = self._get_host()
        return f"hdfs://{self._active_host}:{self.ipc_port}"

    def _convert_to_url(self, path: PurePathProtocol) -> str:
        # example "hdfs://namenode:8020/absolute/path"
        return self._get_conn_str() + path.as_posix()

    def _get_default_path(self):
        return RemotePath("/user") / getpass.getuser()
