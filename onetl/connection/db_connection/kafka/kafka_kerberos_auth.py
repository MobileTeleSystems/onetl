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
import shutil
from textwrap import dedent
from typing import TYPE_CHECKING

from pydantic import Field, validator

from onetl._internal import to_camel  # noqa: WPS436
from onetl.connection.db_connection.kafka.i_kafka_auth import IKafkaAuth
from onetl.impl import GenericOptions, LocalPath, path_repr

if TYPE_CHECKING:
    from onetl.connection import Kafka


class KafkaKerberosAuth(IKafkaAuth, GenericOptions):
    """
    A class designed to generate a Kafka connection configuration using K8S.

    https://kafka.apache.org/documentation/#security_sasl_kerberos_clientconfig
    https://docs.oracle.com/javase/8/docs/jre/api/security/jaas/spec/com/sun/security/auth/module/Krb5LoginModule.html
    """

    principal: str
    keytab: LocalPath = Field(alias="keyTab")
    deploy_keytab: bool = True
    service_name: str = Field(default="kafka", alias="serviceName")
    renew_ticket: bool = Field(default=True, alias="renewTicket")
    store_key: bool = Field(default=True, alias="storeKey")
    use_keytab: bool = Field(default=True, alias="useKeyTab")
    debug: bool = False

    class Config:
        prohibited_options = {"use_ticket_cache", "useTicketCache"}
        extra = "allow"
        alias_generator = to_camel

    def get_jaas_conf(self) -> str:
        options = {}

        class_options = self.dict(
            by_alias=True,
            exclude_none=True,
            exclude={"deploy_keytab", "keytab"},
        ).items()

        for class_option, value in class_options:
            if isinstance(value, bool):
                options[class_option] = str(value).lower()
            else:
                options[class_option] = f'"{value}"'
        conf = "\n".join(f"{k}={v}" for k, v in options.items())
        return (
            dedent(
                """\
                com.sun.security.auth.module.Krb5LoginModule required
                useTicketCache=false
                """,
            )
            + conf
            + ";"
        )

    def get_options(self, kafka: Kafka) -> dict:
        keytab_processed = self._move_keytab_if_deploy(kafka)

        if keytab_processed is None:
            keytab_path = self.keytab
        else:
            keytab_path = keytab_processed

        return {
            "kafka.sasl.mechanism": "GSSAPI",
            "kafka.sasl.jaas.config": f'keyTab="{keytab_path}"\n{self.get_jaas_conf()}',
            "kafka.sasl.kerberos.service.name": self.service_name,
        }

    @validator("keytab")
    def _validate_keytab(cls, value):  # noqa: N805
        if not os.path.exists(value):
            raise ValueError(
                f"File '{os.fspath(value)}' is missing",
            )

        if not os.access(value, os.R_OK):
            raise ValueError(
                f"No access to file {path_repr(value)}",
            )

        return value

    def _move_keytab_if_deploy(self, kafka: Kafka):
        if self.deploy_keytab:
            kafka.spark.sparkContext.addFile(os.fspath(self.keytab))
            return self._move_keytab(self.keytab)

    @staticmethod
    def _move_keytab(keytab: LocalPath) -> LocalPath:
        cwd = LocalPath(os.getcwd())
        copy_path = LocalPath(shutil.copy2(keytab, cwd))

        return copy_path.relative_to(cwd)
