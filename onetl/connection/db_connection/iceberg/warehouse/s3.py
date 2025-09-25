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
        version = Version(package_version).min_digits(3)
        return [f"org.apache.iceberg:iceberg-aws-bundle:{version}"]

    @validator("path", pre=True)
    def _validate_path(cls, path, values):
        return RemotePath(os.fspath(path))
