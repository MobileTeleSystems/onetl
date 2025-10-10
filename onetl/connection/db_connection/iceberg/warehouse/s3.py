# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import os
from typing import Dict, List, Optional

from typing_extensions import Literal

from onetl._util.version import Version
from onetl.hooks import slot, support_hooks
from onetl.impl.remote_path import RemotePath

try:
    from pydantic.v1 import Field, SecretStr, validator
except (ImportError, AttributeError):
    from pydantic import validator, Field, SecretStr  # type: ignore[no-redef, assignment]

from onetl._util.spark import stringify
from onetl.base import PurePathProtocol
from onetl.connection.db_connection.iceberg.warehouse import IcebergWarehouse
from onetl.impl.frozen_model import FrozenModel


@support_hooks
class IcebergS3Warehouse(IcebergWarehouse, FrozenModel):
    """Iceberg S3 Warehouse. |support_hooks|

    .. versionadded:: 0.14.1

    .. note::

        This warehouse uses a **dedicated Iceberg S3 client** to access the warehouse data.
        It does **not rely on SparkS3 implementation**.

    Parameters
    ----------
    path : str
        Warehouse path

    host : str
        S3 endpoint hostname

    port : int
        S3 endpoint port

    protocol : Literal["http", "https"], default: "https"
        Protocol to use for S3 connections

    bucket : str
        S3 bucket name

    path_style_access : bool, default: False
        Whether to use path-style access

    access_key : str, optional
        S3 access key

    secret_key : str, optional
        S3 secret key

    session_token : str, optional
        S3 session token for temporary credentials

    region : str
        S3 region

    extra : Dict[str, str], default: {}
        Additional S3 configuration parameters

    Examples
    --------

    .. code:: python

        from onetl.connection import Iceberg

        warehouse = Iceberg.S3Warehouse(
            path="/warehouse",
            host="s3.domain.com",
            protocol="http",
            bucket="my-bucket",
            region="us-east-1",
            path_style_access=True,
            access_key="my_access_key",
            secret_key="my_secret_key",
        )
    """

    path: PurePathProtocol
    host: str
    port: int
    protocol: Literal["http", "https"] = "https"
    bucket: str
    path_style_access: bool = False
    access_key: Optional[str] = None
    secret_key: Optional[SecretStr] = None
    session_token: Optional[SecretStr] = None
    region: str
    extra: Dict[str, str] = Field(default_factory=dict)

    def get_config(self) -> Dict[str, str]:
        config = {
            "warehouse": "s3a://" + self.bucket + "/" + self.path.as_posix().lstrip("/"),
            "io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
            "s3.endpoint": f"{self.protocol}://{self.host}:{self.port}",
            "s3.access-key-id": self.access_key,
            "s3.secret-access-key": self.secret_key.get_secret_value() if self.secret_key else None,
            "s3.session-token": self.session_token.get_secret_value() if self.session_token else None,
            "s3.region": self.region,
            "s3.path-style-access": stringify(self.path_style_access),
            **self.extra,
        }
        return {k: v for k, v in config.items() if v is not None}

    @slot
    @classmethod
    def get_packages(cls, package_version: str) -> List[str]:
        """
        Get package names to be downloaded by Spark. |support_hooks|

        See `Maven package index <https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-aws-bundle>`_
        for all available packages.

        Parameters
        ----------
        package_version : str
            Iceberg package version in format ``major.minor.patch``.

        Returns
        -------
        list[str]
            List of Maven coordinates.

        Examples
        --------
        .. code:: python

            from onetl.connection import Iceberg

            # Note: Iceberg 1.10.0 requires Java 11+
            Iceberg.S3Warehouse.get_packages(package_version="1.10.0")
        """
        version = Version(package_version).min_digits(3)
        return [f"org.apache.iceberg:iceberg-aws-bundle:{version}"]

    @validator("path", pre=True)
    def _validate_path(cls, path, values):
        return RemotePath(os.fspath(path))
