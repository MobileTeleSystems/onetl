# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
import os
import shutil
from typing import TYPE_CHECKING, Optional

try:
    from pydantic.v1 import Field, PrivateAttr, root_validator, validator
except (ImportError, AttributeError):
    from pydantic import Field, PrivateAttr, root_validator, validator  # type: ignore[no-redef, assignment]

from onetl._util.file import get_file_hash, is_file_readable
from onetl._util.spark import stringify
from onetl.connection.db_connection.kafka.kafka_auth import KafkaAuth
from onetl.impl import GenericOptions, LocalPath, path_repr

if TYPE_CHECKING:
    from onetl.connection import Kafka

log = logging.getLogger(__name__)


KNOWN_OPTIONS = frozenset(
    (
        "clearPass",
        "debug",
        "doNotPrompt",
        "isInitiator",
        "refreshKrb5Config",
        "renewTGT",
        "storePass",
        "ticketCache",
        "tryFirstPass",
        "useFirstPass",
        "sasl.kerberos.*",
    ),
)


PROHIBITED_OPTIONS = frozenset(
    (
        # Already set by class itself
        "sasl.kerberos.service.name",
        "sasl.jaas.config",
        "sasl.mechanism",
    ),
)


class KafkaKerberosAuth(KafkaAuth, GenericOptions):
    """
    Connect to Kafka using ``sasl.mechanism="GSSAPI"``.

    For more details see:

    * `Kafka Documentation <https://kafka.apache.org/documentation/#security_sasl_kerberos_clientconfig>`_
    * `Krb5LoginModule documentation <https://docs.oracle.com/javase/8/docs/jre/api/security/jaas/spec/com/sun/security/auth/module/Krb5LoginModule.html>`_

    .. versionadded:: 0.9.0

    Examples
    --------

    Auth in Kafka with keytab, automatically deploy keytab files to all Spark hosts (driver and executors):

    .. code:: python

        from onetl.connection import Kafka

        auth = Kafka.KerberosAuth(
            principal="user",
            keytab="/path/to/keytab",
            deploy_keytab=True,
        )

    Auth in Kafka with keytab, keytab is **already deployed** on all Spark hosts (driver and executors):

    .. code:: python

        from onetl.connection import Kafka

        auth = Kafka.KerberosAuth(
            principal="user",
            keytab="/path/to/keytab",
            deploy_keytab=False,
        )

    Auth in Kafka with existing Kerberos ticket (only Spark session created with ``master=local``):

    .. code:: python

        from onetl.connection import Kafka

        auth = Kafka.KerberosAuth(
            principal="user",
            use_keytab=False,
            use_ticket_cache=True,
        )

    Pass custom options for JAAS config and Kafka SASL:

    .. code:: python

        from onetl.connection import Kafka

        auth = Kafka.KerberosAuth.parse(
            {
                "principal": "user",
                "keytab": "/path/to/keytab",
                # options without sasl.kerberos. prefix are passed to JAAS config
                # names are in camel case!
                "isInitiator": True,
                # options with `sasl.kerberos.` prefix are passed to Kafka client config as-is
                "sasl.kerberos.kinit.cmd": "/usr/bin/kinit",
            }
        )
    """

    principal: str
    keytab: Optional[LocalPath] = Field(default=None, alias="keyTab")
    deploy_keytab: bool = True
    service_name: str = Field(default="kafka", alias="serviceName")
    renew_ticket: bool = Field(default=True, alias="renewTicket")
    store_key: bool = Field(default=True, alias="storeKey")
    use_keytab: bool = Field(default=True, alias="useKeyTab")
    use_ticket_cache: bool = Field(default=False, alias="useTicketCache")

    _keytab_path: Optional[LocalPath] = PrivateAttr(default=None)

    class Config:
        prohibited_options = PROHIBITED_OPTIONS
        known_options = KNOWN_OPTIONS
        strip_prefixes = ["kafka."]
        extra = "allow"

    def get_jaas_conf(self, kafka: Kafka) -> str:
        options = self.dict(
            by_alias=True,
            exclude_none=True,
            exclude={"deploy_keytab"},
        )
        if self.keytab:
            options["keyTab"] = self._prepare_keytab(kafka)

        jaas_conf = stringify({key: value for key, value in options.items() if not key.startswith("sasl.")}, quote=True)
        jaas_conf_items = [f"{key}={value}" for key, value in jaas_conf.items()]
        return "com.sun.security.auth.module.Krb5LoginModule required " + " ".join(jaas_conf_items) + ";"

    def get_options(self, kafka: Kafka) -> dict:
        result = {
            key: value for key, value in self.dict(by_alias=True, exclude_none=True).items() if key.startswith("sasl.")
        }
        result.update(
            {
                "sasl.mechanism": "GSSAPI",
                "sasl.jaas.config": self.get_jaas_conf(kafka),
                "sasl.kerberos.service.name": self.service_name,
            },
        )
        return stringify(result)

    def cleanup(self, kafka: Kafka) -> None:
        if self._keytab_path and self._keytab_path.exists():
            log.debug("Removing keytab from %s", path_repr(self._keytab_path))
            try:
                self._keytab_path.unlink()
            except OSError:
                log.exception("Failed to remove keytab file '%s'", self._keytab_path)
        self._keytab_path = None

    @validator("keytab")
    def _validate_keytab(cls, value):
        return is_file_readable(value)

    @root_validator
    def _use_keytab(cls, values):
        keytab = values.get("keytab")
        use_keytab = values.get("use_keytab")
        if use_keytab and not keytab:
            raise ValueError("keytab is required if useKeytab is True")
        return values

    def _prepare_keytab(self, kafka: Kafka) -> str:
        keytab: LocalPath = self.keytab  # type: ignore[assignment]
        if not self.deploy_keytab:
            return os.fspath(keytab)

        self._keytab_path = self._generate_keytab_path(keytab, self.principal)
        log.debug("Moving keytab from %s to %s", path_repr(keytab), path_repr(self._keytab_path))
        shutil.copy2(keytab, self._keytab_path)
        kafka.spark.sparkContext.addFile(os.fspath(self._keytab_path))
        return os.fspath(self._keytab_path.name)

    @staticmethod
    def _generate_keytab_path(keytab: LocalPath, principal: str) -> LocalPath:
        # Using hash in keytab name prevents collisions if there are several Kafka instances with
        # keytab paths like `/some/kafka.keytab` and `/another/kafka.keytab`.
        # Not using random values here to avoid generating too much garbage in current directory.
        # Also Spark copies all files to the executor working directory ignoring their paths,
        # so if there is some other connector deploy keytab file, these files will have different names.
        keytab_hash = get_file_hash(keytab, "md5")
        return LocalPath().joinpath(f"kafka_{principal}_{keytab_hash.hexdigest()}.keytab").resolve()
