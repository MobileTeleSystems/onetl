#  Copyright 2023 MTS (Mobile Telesystems)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from __future__ import annotations

import os
import stat
import textwrap
from logging import getLogger
from typing import TYPE_CHECKING, Optional, Tuple

try:
    from hdfs import InsecureClient

    if TYPE_CHECKING:
        from hdfs.ext.kerberos import KerberosClient
except (ImportError, NameError) as err:
    raise ImportError(
        textwrap.dedent(
            """
            Cannot import module "hdfs".

            Since onETL v0.7.0 you should install package as follows:
                pip install onetl[hdfs]

            or
                pip install onetl[files]
            """,
        ).strip(),
    ) from err

from etl_entities.instance import Cluster, Host
from pydantic import Field, FilePath, SecretStr, root_validator, validator

from onetl.base import PathStatProtocol
from onetl.connection.file_connection.file_connection import FileConnection
from onetl.connection.kerberos_helpers import kinit
from onetl.hooks import slot, support_hooks
from onetl.impl import LocalPath, RemotePath, RemotePathStat

log = getLogger(__name__)
ENTRY_TYPE = Tuple[str, dict]


class HDFS(FileConnection):
    """HDFS file connection.

    Powered by `HDFS Python client <https://pypi.org/project/hdfs/>`_.

    .. warning::

        Since onETL v0.7.0 to use HDFS connector you should install package as follows:

        .. code:: bash

            pip install onetl[hdfs]

            # or
            pip install onetl[files]

        See :ref:`files-install` instruction for more details.

    .. note::

        To access Hadoop cluster with Kerberos installed, you should have ``kinit`` executable
        in some path in ``PATH`` environment variable.

        See onETL :ref:`kerberos-install` instruction for more details.

    Parameters
    ----------
    cluster : str, optional
        Hadoop cluster name. For example: ``rnd-dwh``.

        Used for:
            * HWM and lineage (as instance name for file paths), if set.
            * Validation of ``host`` value,
                if latter is passed and if some hooks are bound to :obj:`~slots.get_cluster_namenodes`.

        .. warning:

            You should pass at least one of these arguments: ``cluster``, ``host``.

    host : str, optional
        Hadoop namenode host. For example: ``namenode1.domain.com``.

        Should be an active namenode (NOT standby).

        If value is not set, but there are some hooks bound to
        :obj:`~slots.get_cluster_namenodes` and :obj:`~slots.is_namenode_active`,
        onETL will iterate over cluster namenodes to detect which one is active.

        .. warning:

            You should pass at least one of these arguments: ``cluster``, ``host``.

    webhdfs_port : int, default: ``50070``
        Port of Hadoop namenode (WebHDFS protocol).

        If omitted, but there are some hooks bound to :obj:`~slots.get_webhdfs_port` slot,
        onETL will try to detect port number for a specific ``cluster``.

    user : str, optional
        User, which have access to the file source. For example: ``someuser``.

        If set, Kerberos auth will be used. Otherwise an anonymous connection is created.

    password : str, default: ``None``
        User password.

        Used for generating Kerberos ticket.

        .. warning ::

            You can provide only one of the parameters: ``password`` or ``kinit``.
            If you provide both, an exception will be raised.

    keytab : str, default: ``None``
        LocalPath to keytab file.

        Used for generating Kerberos ticket.

        .. warning ::

            You can provide only one of the parameters: ``password`` or ``kinit``.
            If you provide both, an exception will be raised.

    timeout : int, default: ``10``
        Connection timeout.

    Examples
    --------

    HDFS connection with password:

    .. code:: python

        from onetl.connection import HDFS

        hdfs = HDFS(
            host="namenode1.domain.com",
            user="someuser",
            password="*****",
        ).check()

    HDFS connection with keytab:

    .. code:: python

        from onetl.connection import HDFS

        hdfs = HDFS(
            host="namenode1.domain.com",
            user="someuser",
            keytab="/path/to/keytab",
        ).check()

    HDFS file connection initialization without auth (HDFS without Kerberos support)

    .. code:: python

        from onetl.connection import HDFS

        hdfs = HDFS(host="namenode1.domain.com").check()

    HDFS connection with both cluster and host names:

    .. code:: python

        from onetl.connection import HDFS

        hdfs = HDFS(
            cluster="rnd-dwh",
            host="namenode1.domain.com",
            user="someuser",
            password="*****",
        ).check()

    HDFS connection with cluster name only:

    .. code:: python

        from onetl.connection import HDFS

        hdfs = HDFS(
            cluster="rnd-dwh",
            user="someuser",
            password="*****",
        ).check()
    """

    @support_hooks
    class slots:  # noqa: N801
        """Slots that could be implemented by third-party plugins"""

        @slot
        @staticmethod
        def normalize_cluster_name(cluster: str) -> str | None:
            """
            Normalize cluster name passed into HDFS constructor.

            If hooks didn't return anything, cluster name is left intact.

            Parameters
            ----------
            cluster : :obj:`str`
                Cluster name

            Returns
            -------
            str | None
                Normalized cluster name.

                If hook cannot be applied to a specific cluster, it should return ``None``.

            Examples
            --------

            .. code:: python

                from onetl.connection import HDFS
                from onetl.hooks import hook


                @HDFS.slots.normalize_cluster_name.bind
                @hook
                def normalize_cluster_name(cluster: str) -> str:
                    return cluster.lower()
            """

        @slot
        @staticmethod
        def normalize_namenode_host(host: str, cluster: str | None) -> str | None:
            """
            Normalize namenode host passed into HDFS constructor.

            If hooks didn't return anything, host is left intact.

            Parameters
            ----------
            host : :obj:`str`
                Namenode host (raw)

            cluster : :obj:`str` or :obj:`None`
                Cluster name (normalized), if set

            Returns
            -------
            str | None
                Normalized namenode host name.

                If hook cannot be applied to a specific host name, it should return ``None``.

            Examples
            --------

            .. code:: python

                from onetl.connection import HDFS
                from onetl.hooks import hook


                @HDFS.slots.normalize_namenode_host.bind
                @hook
                def normalize_namenode_host(host: str, cluster: str) -> str | None:
                    if cluster == "rnd-dwh":
                        if not host.endswith(".domain.com"):
                            # fix missing domain name
                            host += ".domain.com"
                        return host

                    return None
            """

        @slot
        @staticmethod
        def get_known_clusters() -> set[str] | None:
            """
            Return collection of known clusters.

            Cluster passed into HDFS constructor should be present in this list.
            If hooks didn't return anything, no validation will be performed.

            Returns
            -------
            set[str] | None
                Collection of cluster names (in normalized form).

                If hook cannot be applied, it should return ``None``.

            Examples
            --------

            .. code:: python

                from onetl.connection import HDFS
                from onetl.hooks import hook


                @HDFS.slots.get_known_clusters.bind
                @hook
                def get_known_clusters() -> str[str]:
                    return {"rnd-dwh", "rnd-prod"}
            """

        @slot
        @staticmethod
        def get_cluster_namenodes(cluster: str) -> set[str] | None:
            """
            Return collection of known namenodes for the cluster.

            Namenode host passed into HDFS constructor should be present in this list.
            If hooks didn't return anything, no validation will be performed.

            Parameters
            ----------
            cluster : :obj:`str`
                Cluster name (normalized)

            Returns
            -------
            set[str] | None
                Collection of host names (in normalized form).

                If hook cannot be applied, it should return ``None``.

            Examples
            --------

            .. code:: python

                from onetl.connection import HDFS
                from onetl.hooks import hook


                @HDFS.slots.get_cluster_namenodes.bind
                @hook
                def get_cluster_namenodes(cluster: str) -> str[str] | None:
                    if cluster == "rnd-dwh":
                        return {"namenode1.domain.com", "namenode2.domain.com"}
                    return None
            """

        @slot
        @staticmethod
        def get_current_cluster() -> str | None:
            """
            Get current cluster name.

            Used in :obj:`~get_current_cluster` to  automatically fill up ``cluster`` attribute of a connection.
            If hooks didn't return anything, calling the method above will raise an exception.

            Returns
            -------
            str | None
                Current cluster name (in normalized form).

                If hook cannot be applied, it should return ``None``.

            Examples
            --------

            .. code:: python

                from onetl.connection import HDFS
                from onetl.hooks import hook


                @HDFS.slots.get_current_cluster.bind
                @hook
                def get_current_cluster() -> str:
                    # some magic here
                    return "rnd-dwh"
            """

        @slot
        @staticmethod
        def get_webhdfs_port(cluster: str) -> int | None:
            """
            Get WebHDFS port number for a specific cluster.

            Used by constructor to automatically set port number if omitted.

            Parameters
            ----------
            cluster : :obj:`str`
                Cluster name (normalized)

            Returns
            -------
            int | None
                WebHDFS port number.

                If hook cannot be applied, it should return ``None``.

            Examples
            --------

            .. code:: python

                from onetl.connection import HDFS
                from onetl.hooks import hook


                @HDFS.slots.get_webhdfs_port.bind
                @hook
                def get_webhdfs_port(cluster: str) -> int | None:
                    if cluster == "rnd-dwh":
                        return 50007  # Cloudera
                    return None
            """

        @slot
        @staticmethod
        def is_namenode_active(host: str, cluster: str | None) -> bool | None:
            """
            Check whether a namenode of a specified cluster is active (=not standby) or not.

            Used for:
                * If HDFS connection is created without ``host``

                    Connector will iterate over :obj:`~get_cluster_namenodes` of a cluster to get active namenode,
                    and then use it instead of ``host`` attribute.

                * If HDFS connection is created with ``host``

                    :obj:`~check` will determine whether this host is active.

            Parameters
            ----------
            host : :obj:`str`
                Namenode host (normalized)

            cluster : :obj:`str` or :obj:`None`
                Cluster name (normalized), if set

            Returns
            -------
            bool | None
                ``True`` if namenode is active, ``False`` if not.

                If hook cannot be applied, it should return ``None``.

            Examples
            --------

            .. code:: python

                from onetl.connection import HDFS
                from onetl.hooks import hook


                @HDFS.slots.is_namenode_active.bind
                @hook
                def is_namenode_active(host: str, cluster: str | None) -> bool:
                    # some magic here
                    return True
            """

    cluster: Optional[Cluster] = None
    host: Optional[Host] = None
    webhdfs_port: int = Field(alias="port", default=50070)
    user: Optional[str] = None
    password: Optional[SecretStr] = None
    keytab: Optional[FilePath] = None
    timeout: int = 10

    @validator("user", pre=True)
    def validate_packages(cls, user):
        if user:
            try:
                from hdfs.ext.kerberos import KerberosClient as CheckForKerberosSupport
            except (ImportError, NameError) as e:
                raise ImportError(
                    textwrap.dedent(
                        """
                        Cannot import module "hdfs.ext.kerberos".

                        Since onETL v0.7.0 you should install package as follows:
                            pip install onetl[hdfs,kerberos]

                        or
                            pip install onetl[all]

                        You should also have Kerberos libraries installed to OS,
                        specifically ``kinit`` executable.
                        """,
                    ).strip(),
                ) from e

        return user

    @root_validator(pre=True)
    def validate_cluster_or_hostname_set(cls, values):
        host = values.get("host")
        cluster = values.get("cluster")

        if not cluster and not host:
            raise ValueError("You should pass either host or cluster name")

        return values

    @validator("cluster")
    def validate_cluster_name(cls, cluster):
        log.debug("|%s| Normalizing cluster %r name ...", cls.__name__, cluster)
        validated_cluster = cls.slots.normalize_cluster_name(cluster) or cluster
        if validated_cluster != cluster:
            log.debug("|%s|   Got %r", cls.__name__, validated_cluster)

        log.debug("|%s| Checking if cluster %r is a known cluster ...", cls.__name__, validated_cluster)
        known_clusters = cls.slots.get_known_clusters()
        if known_clusters and validated_cluster not in known_clusters:
            raise ValueError(
                f"Cluster {validated_cluster!r} is not in the known clusters list: {sorted(known_clusters)!r}",
            )

        return validated_cluster

    @validator("host")
    def validate_host_name(cls, host, values):
        cluster = values.get("cluster")

        log.debug("|%s| Normalizing namenode %r ...", cls.__name__, host)
        namenode = cls.slots.normalize_namenode_host(host, cluster) or host
        if namenode != host:
            log.debug("|%s|   Got %r", cls.__name__, namenode)

        if cluster:
            log.debug("|%s| Checking if %r is a known namenode of cluster %r ...", cls.__name__, namenode, cluster)
            known_namenodes = cls.slots.get_cluster_namenodes(cluster)
            if known_namenodes and namenode not in known_namenodes:
                raise ValueError(
                    f"Namenode {namenode!r} is not in the known nodes list of cluster {cluster!r}: "
                    f"{sorted(known_namenodes)!r}",
                )

        return namenode

    @validator("webhdfs_port", always=True)
    def validate_port_number(cls, port, values):
        cluster = values.get("cluster")
        if cluster:
            log.debug("|%s| Getting WebHDFS port of cluster %r ...", cls.__name__, cluster)
            result = cls.slots.get_webhdfs_port(cluster) or port
            if result != port:
                log.debug("|%s|   Got %r", cls.__name__, result)
            return result

        return port

    @root_validator
    def validate_credentials(cls, values):
        user = values.get("user")
        password = values.get("password")
        keytab = values.get("keytab")
        if password and keytab:
            raise ValueError("Please provide either `keytab` or `password` for kinit, not both")

        if (password or keytab) and not user:
            raise ValueError("`keytab` or `password` should be used only with `user`")

        return values

    @classmethod
    def get_current(cls, **kwargs):
        """
        Create connection for current cluster.

        Automatically sets up current cluster name as ``cluster``.

        .. note::

            Can be used only if there are a some hooks bound to slot :obj:`~slots.get_current_cluster`.

        Parameters
        ----------
        user : str
        password : str | None
        keytab : str | None
        timeout : int

            See :obj:`~HDFS` constructor documentation.

        Examples
        --------

        .. code:: python

            from onetl.connection import HDFS

            # injecting current cluster name via hooks mechanism
            hdfs = HDFS.get_current(user="me", password="pass")
        """

        log.info("|%s| Detecting current cluster...", cls.__name__)
        current_cluster = cls.slots.get_current_cluster()
        if not current_cluster:
            raise RuntimeError(
                f"{cls.__name__}.get_current() can be used only if there are "
                f"some hooks bound to {cls.__name__}.slots.get_current_cluster",
            )

        log.info("|%s|   Got %r", cls.__name__, current_cluster)
        return cls(cluster=current_cluster, **kwargs)

    @property
    def instance_url(self) -> str:
        if self.cluster:
            return self.cluster
        return f"hdfs://{self.host}:{self.webhdfs_port}"

    def path_exists(self, path: os.PathLike | str) -> bool:
        return self.client.status(os.fspath(path), strict=False)

    def _get_active_namenode(self) -> str:
        class_name = self.__class__.__name__
        log.info("|%s| Detecting active namenode of cluster %r ...", class_name, self.cluster)

        host: str | None = None
        namenodes = self.slots.get_cluster_namenodes(self.cluster)

        if not namenodes:
            raise RuntimeError(f"Cannot get list of namenodes for a cluster {self.cluster!r}")

        nodes_len = len(namenodes)
        for i, namenode in enumerate(namenodes):
            log.debug("|%s|   Trying namenode %r (%d of %d) ...", class_name, namenode, i, nodes_len)
            if self.slots.is_namenode_active(namenode, self.cluster):
                log.info("|%s|     Node %r is active!", class_name, namenode)
                host = namenode
                break
            log.debug("|%s|     Node %r is not active, skipping", class_name, namenode)

        if not host:
            raise RuntimeError(f"Cannot detect active namenode for cluster {self.cluster!r}")

        return host

    def _get_host(self) -> str:
        if not self.host and self.cluster:
            return self._get_active_namenode()

        # host is passed explicitly or cluster not set
        class_name = self.__class__.__name__
        if self.cluster:
            log.info("|%s| Detecting if namenode %r of cluster %r is active...", class_name, self.host, self.cluster)
        else:
            log.info("|%s| Detecting if namenode %r is active...", class_name, self.host)

        is_active = self.slots.is_namenode_active(self.host, self.cluster)
        if is_active:
            log.info("|%s|   Namenode %r is active!", class_name, self.host)
            return self.host

        if is_active is None:
            log.debug("|%s|   No hooks, skip validation", class_name)
            return self.host

        if self.cluster:
            raise RuntimeError(f"Host {self.host!r} is not an active namenode of cluster {self.cluster!r}")

        raise RuntimeError(f"Host {self.host!r} is not an active namenode")

    def _get_client(self) -> KerberosClient | InsecureClient:
        host = self._get_host()
        conn_str = f"http://{host}:{self.webhdfs_port}"  # NOSONAR

        if self.user and (self.keytab or self.password):
            from hdfs.ext.kerberos import KerberosClient

            kinit(
                self.user,
                keytab=self.keytab,
                password=self.password.get_secret_value() if self.password else None,
            )
            client = KerberosClient(conn_str, timeout=self.timeout)
        else:
            from hdfs import InsecureClient  # noqa: F401, WPS442

            client = InsecureClient(conn_str, user=self.user)

        return client

    def _is_client_closed(self) -> bool:
        # Underlying client does not support closing
        return False

    def _close_client(self) -> None:  # NOSONAR
        # Underlying client does not support closing
        pass

    def _rmdir(self, path: RemotePath) -> None:
        self.client.delete(os.fspath(path), recursive=False)

    def _mkdir(self, path: RemotePath) -> None:
        self.client.makedirs(os.fspath(path))

    def _upload_file(self, local_file_path: LocalPath, remote_file_path: RemotePath) -> None:
        self.client.upload(os.fspath(remote_file_path), os.fspath(local_file_path))

    def _rename(self, source: RemotePath, target: RemotePath) -> None:
        self.client.rename(os.fspath(source), os.fspath(target))

    def _download_file(self, remote_file_path: RemotePath, local_file_path: LocalPath) -> None:
        self.client.download(os.fspath(remote_file_path), os.fspath(local_file_path))

    def _remove_file(self, remote_file_path: RemotePath) -> None:
        self.client.delete(os.fspath(remote_file_path), recursive=False)

    def _scan_entries(self, path: RemotePath) -> list[ENTRY_TYPE]:
        return self.client.list(os.fspath(path), status=True)

    def _is_file(self, path: RemotePath) -> bool:
        return self.client.status(os.fspath(path))["type"] == "FILE"

    def _is_dir(self, path: RemotePath) -> bool:
        return self.client.status(os.fspath(path))["type"] == "DIRECTORY"

    def _get_stat(self, path: RemotePath) -> RemotePathStat:
        status = self.client.status(os.fspath(path))

        # Status examples:
        # {
        #   "accessTime"      : 1320171722771,
        #   "blockSize"       : 33554432,
        #   "group"           : "supergroup",
        #   "length"          : 24930,
        #   "modificationTime": 1320171722771,
        #   "owner"           : "webuser",
        #   "pathSuffix"      : "a.patch",
        #   "permission"      : "644",
        #   "replication"     : 1,
        #   "type"            : "FILE"
        # }
        #
        # {
        #   "accessTime"      : 0,
        #   "blockSize"       : 0,
        #   "group"           : "supergroup",
        #   "length"          : 0,
        #   "modificationTime": 1320895981256,
        #   "owner"           : "szetszwo",
        #   "pathSuffix"      : "bar",
        #   "permission"      : "711",
        #   "replication"     : 0,
        #   "type"            : "DIRECTORY"
        # }

        path_type = stat.S_IFDIR if status["type"] == "DIRECTORY" else stat.S_IFREG

        return RemotePathStat(
            st_size=status["length"],
            st_mtime=status["modificationTime"] / 1000,  # HDFS uses timestamps with milliseconds
            st_uid=status["owner"],
            st_gid=status["group"],
            st_mode=int(status["permission"], 8) | path_type,
        )

    def _read_text(self, path: RemotePath, encoding: str, **kwargs) -> str:
        with self.client.read(os.fspath(path), encoding=encoding, **kwargs) as file:
            return file.read()

    def _read_bytes(self, path: RemotePath, **kwargs) -> bytes:
        with self.client.read(os.fspath(path), **kwargs) as file:
            return file.read()

    def _write_text(self, path: RemotePath, content: str, encoding: str, **kwargs) -> None:
        self.client.write(os.fspath(path), data=content, encoding=encoding, overwrite=True, **kwargs)

    def _write_bytes(self, path: RemotePath, content: bytes, **kwargs) -> None:
        self.client.write(os.fspath(path), data=content, overwrite=True, **kwargs)

    def _extract_name_from_entry(self, entry: ENTRY_TYPE) -> str:
        return entry[0]

    def _is_dir_entry(self, top: RemotePath, entry: ENTRY_TYPE) -> bool:
        entry_stat = entry[1]

        return entry_stat["type"] == "DIRECTORY"

    def _is_file_entry(self, top: RemotePath, entry: ENTRY_TYPE) -> bool:
        entry_stat = entry[1]

        return entry_stat["type"] == "FILE"

    def _extract_stat_from_entry(self, top: RemotePath, entry: ENTRY_TYPE) -> PathStatProtocol:
        entry_stat = entry[1]

        return RemotePathStat(
            st_size=entry_stat["length"],
            st_mtime=entry_stat["modificationTime"] / 1000,  # HDFS uses timestamps with milliseconds
            st_uid=entry_stat["owner"],
            st_gid=entry_stat["group"],
            st_mode=int(entry_stat["permission"], 8) | stat.S_IFDIR
            if entry_stat["type"] == "DIRECTORY"
            else stat.S_IFREG,
        )
