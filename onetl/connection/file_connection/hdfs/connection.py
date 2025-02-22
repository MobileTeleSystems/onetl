# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import os
import stat
import textwrap
from contextlib import suppress
from logging import getLogger
from typing import TYPE_CHECKING, Optional, Tuple

from etl_entities.instance import Cluster, Host

try:
    from pydantic.v1 import (
        Field,
        FilePath,
        PrivateAttr,
        SecretStr,
        root_validator,
        validator,
    )
except (ImportError, AttributeError):
    from pydantic import Field, FilePath, SecretStr, PrivateAttr, root_validator, validator  # type: ignore[no-redef, assignment]

from onetl.base import PathStatProtocol
from onetl.connection.file_connection.file_connection import FileConnection
from onetl.connection.file_connection.hdfs.slots import HDFSSlots
from onetl.connection.file_connection.mixins.rename_dir_mixin import RenameDirMixin
from onetl.connection.kerberos_helpers import kinit
from onetl.hooks import slot, support_hooks
from onetl.impl import LocalPath, RemotePath, RemotePathStat

try:
    from hdfs import Client, InsecureClient

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

log = getLogger(__name__)
ENTRY_TYPE = Tuple[str, dict]


@support_hooks
class HDFS(FileConnection, RenameDirMixin):
    """HDFS file connection. |support_hooks|

    Powered by `HDFS Python client <https://pypi.org/project/hdfs/>`_.

    .. warning::

        Since onETL v0.7.0 to use HDFS connector you should install package as follows:

        .. code:: bash

            pip install onetl[hdfs]

            # or
            pip install onetl[files]

        See :ref:`install-files` installation instruction for more details.

    .. note::

        To access Hadoop cluster with Kerberos installed, you should have ``kinit`` executable
        in some path in ``PATH`` environment variable.

        See :ref:`install-kerberos` instruction for more details.

    Parameters
    ----------
    cluster : str, optional
        Hadoop cluster name. For example: ``rnd-dwh``.

        Used for:
            * HWM and lineage (as instance name for file paths), if set.
            * Validation of ``host`` value,
                if latter is passed and if some hooks are bound to
                :obj:`Slots.get_cluster_namenodes <onetl.connection.file_connection.hdfs.slots.HDFSSlots.get_cluster_namenodes>`

        .. warning:

            You should pass at least one of these arguments: ``cluster``, ``host``.

        .. versionadded:: 0.7.0

    host : str, optional
        Hadoop namenode host. For example: ``namenode1.domain.com``.

        Should be an active namenode (NOT standby).

        If value is not set, but there are some hooks bound to
        :obj:`Slots.get_cluster_namenodes <onetl.connection.file_connection.hdfs.slots.HDFSSlots.get_cluster_namenodes>`
        and :obj:`Slots.is_namenode_active <onetl.connection.file_connection.hdfs.slots.HDFSSlots.is_namenode_active>`,
        onETL will iterate over cluster namenodes to detect which one is active.

        .. warning:

            You should pass at least one of these arguments: ``cluster``, ``host``.

    webhdfs_port : int, default: ``50070``
        Port of Hadoop namenode (WebHDFS protocol).

        If omitted, but there are some hooks bound to
        :obj:`Slots.get_webhdfs_port <onetl.connection.file_connection.hdfs.slots.HDFSSlots.get_webhdfs_port>` slot,
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

    .. tabs::

        .. code-tab:: py Create HDFS connection with user+password

            from onetl.connection import HDFS

            hdfs = HDFS(
                host="namenode1.domain.com",
                user="someuser",
                password="*****",
            ).check()

        .. code-tab:: py Create HDFS connection with user+keytab

            from onetl.connection import HDFS

            hdfs = HDFS(
                host="namenode1.domain.com",
                user="someuser",
                keytab="/path/to/keytab",
            ).check()

        .. code-tab:: py Create HDFS connection without auth

            from onetl.connection import HDFS

            hdfs = HDFS(host="namenode1.domain.com").check()

        .. tab:: Use cluster name to detect active namenode

            Can be used only if some third-party plugin provides :ref:`hdfs-slots` implementation

            .. code:: python

                from onetl.connection import HDFS

                hdfs = HDFS(
                    cluster="rnd-dwh",
                    user="someuser",
                    password="*****",
                ).check()
    """

    cluster: Optional[Cluster] = None
    host: Optional[Host] = None
    webhdfs_port: int = Field(alias="port", default=50070)
    user: Optional[str] = None
    password: Optional[SecretStr] = None
    keytab: Optional[FilePath] = None
    timeout: int = 10

    Slots = HDFSSlots
    # TODO: remove in v1.0.0
    slots = Slots

    _active_host: Optional[Host] = PrivateAttr(default=None)

    @slot
    @classmethod
    def get_current(cls, **kwargs):
        """
        Create connection for current cluster. |support_hooks|

        Automatically sets up current cluster name as ``cluster``.

        .. note::

            Can be used only if there are a some hooks bound to slot
            :obj:`Slots.get_current_cluster <onetl.connection.file_connection.hdfs.slots.HDFSSlots.get_current_cluster>`

        .. versionadded:: 0.7.0

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
        current_cluster = cls.Slots.get_current_cluster()
        if not current_cluster:
            raise RuntimeError(
                f"{cls.__name__}.get_current() can be used only if there are "
                f"some hooks bound to {cls.__name__}.Slots.get_current_cluster",
            )

        log.info("|%s|   Got %r", cls.__name__, current_cluster)
        return cls(cluster=current_cluster, **kwargs)

    @property
    def instance_url(self) -> str:
        if self.cluster:
            return self.cluster
        return f"hdfs://{self.host}:{self.webhdfs_port}"

    def __str__(self):
        if self.cluster:
            return f"{self.__class__.__name__}[{self.cluster}]"
        return f"{self.__class__.__name__}[{self.host}:{self.webhdfs_port}]"

    @slot
    def path_exists(self, path: os.PathLike | str) -> bool:
        return self.client.status(os.fspath(path), strict=False)

    @slot
    def close(self):
        super().close()

        with suppress(Exception):
            self._active_host = None
        return self

    @validator("user", pre=True)
    def _validate_packages(cls, user):
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

    @root_validator
    def _validate_cluster_or_hostname_set(cls, values):
        host = values.get("host")
        cluster = values.get("cluster")

        if not cluster and not host:
            raise ValueError("You should pass either host or cluster name")

        return values

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

        if cluster:
            log.debug("|%s| Checking if %r is a known namenode of cluster %r ...", cls.__name__, namenode, cluster)
            known_namenodes = cls.Slots.get_cluster_namenodes(cluster)
            if known_namenodes and namenode not in known_namenodes:
                raise ValueError(
                    f"Namenode {namenode!r} is not in the known nodes list of cluster {cluster!r}: "
                    f"{sorted(known_namenodes)!r}",
                )

        return namenode

    @validator("webhdfs_port", always=True)
    def _validate_port_number(cls, port, values):
        cluster = values.get("cluster")
        if cluster:
            log.debug("|%s| Getting WebHDFS port of cluster %r ...", cls.__name__, cluster)
            result = cls.Slots.get_webhdfs_port(cluster) or port
            if result != port:
                log.debug("|%s|   Got %r", cls.__name__, result)
            return result

        return port

    @root_validator
    def _validate_credentials(cls, values):
        user = values.get("user")
        password = values.get("password")
        keytab = values.get("keytab")
        if password and keytab:
            raise ValueError("Please provide either `keytab` or `password` for kinit, not both")

        if (password or keytab) and not user:
            raise ValueError("`keytab` or `password` should be used only with `user`")

        return values

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
        if not self.host and self.cluster:
            return self._get_active_namenode()

        # host is passed explicitly or cluster not set
        class_name = self.__class__.__name__
        if self.cluster:
            log.info("|%s| Detecting if namenode %r of cluster %r is active...", class_name, self.host, self.cluster)
        else:
            log.info("|%s| Detecting if namenode %r is active...", class_name, self.host)

        is_active = self.Slots.is_namenode_active(self.host, self.cluster)
        if is_active:
            log.info("|%s|   Namenode %r is active!", class_name, self.host)
            return self.host

        if is_active is None:
            log.debug("|%s|   No hooks, skip validation", class_name)
            return self.host

        if self.cluster:
            raise RuntimeError(f"Host {self.host!r} is not an active namenode of cluster {self.cluster!r}")

        raise RuntimeError(f"Host {self.host!r} is not an active namenode")

    def _get_conn_str(self) -> str:
        # cache active host to reduce number of requests.
        if not self._active_host:
            self._active_host = self._get_host()
        return f"http://{self._active_host}:{self.webhdfs_port}"

    def _get_client(self) -> Client:
        if self.user and (self.keytab or self.password):
            from hdfs.ext.kerberos import KerberosClient  # noqa: F811

            kinit(
                self.user,
                keytab=self.keytab,
                password=self.password.get_secret_value() if self.password else None,
            )
            # checking if namenode is active requires a Kerberos ticket
            conn_str = self._get_conn_str()
            client = KerberosClient(conn_str, timeout=self.timeout)
        else:
            from hdfs import InsecureClient  # noqa: F401, WPS442, F811

            conn_str = self._get_conn_str()
            client = InsecureClient(conn_str, user=self.user)

        return client

    def _is_client_closed(self, client: Client):
        return False

    def _close_client(self, client: Client) -> None:  # NOSONAR
        pass

    def _remove_dir(self, path: RemotePath) -> None:
        self.client.delete(os.fspath(path), recursive=False)

    def _create_dir(self, path: RemotePath) -> None:
        self.client.makedirs(os.fspath(path))

    def _upload_file(self, local_file_path: LocalPath, remote_file_path: RemotePath) -> None:
        self.client.upload(os.fspath(remote_file_path), os.fspath(local_file_path))

    def _rename_file(self, source: RemotePath, target: RemotePath) -> None:
        self.client.rename(os.fspath(source), os.fspath(target))

    _rename_dir = _rename_file

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
            st_mode=(
                int(entry_stat["permission"], 8) | stat.S_IFDIR if entry_stat["type"] == "DIRECTORY" else stat.S_IFREG
            ),
        )
