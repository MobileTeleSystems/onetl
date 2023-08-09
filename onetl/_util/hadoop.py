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

from typing import TYPE_CHECKING

from onetl._util.java import set_java_object_field
from onetl._util.version import Version

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def get_hadoop_version(spark_session: SparkSession) -> Version:
    """
    Get version of Hadoop libraries embedded to Spark
    """
    jvm = spark_session._jvm  # noqa: WPS437
    version_info = jvm.org.apache.hadoop.util.VersionInfo  # type: ignore[union-attr]
    hadoop_version: str = version_info.getVersion()
    return Version.parse(hadoop_version)


def get_hadoop_config(spark_session: SparkSession):
    """
    Get ``org.apache.hadoop.conf.Configuration`` object
    """
    return spark_session.sparkContext._jsc.hadoopConfiguration()


def fix_hadoop_config_get_class(spark_session: SparkSession):
    """
    Fix ``org.apache.hadoop.conf.Configuration.getClass(...)``
    raising ``java.lang.ClassNotFoundException`` if class was added to JVM after it was started
    (e.g. by adding Ivy package in runtime).

    This exception is caused by implementation of ``Configuration.getClassByNameOrNull`` - it caches
    result of class searching, including negative results:
    * https://github.com/apache/hadoop/blob/b6edcb9a/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/conf/Configuration.java#L2653
    * https://github.com/apache/hadoop/blob/b6edcb9a/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/conf/Configuration.java#L2661

    There is no public method to reset this cache (like ``FileSystem.close()`` for ``FileSystem`` objects cache).
    Fortunately, ``CACHE_CLASSES`` is a map where key is ``configuration.classLoader`` object:
    * https://github.com/apache/hadoop/blob/b6edcb9a/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/conf/Configuration.java#L2636

    So we can set ``configuration.classLoader`` to point on different classLoader object,
    just to bypass the cache.
    """
    hadoop_conf = get_hadoop_config(spark_session)
    jvm = spark_session._jvm  # noqa: WPS437

    # Get old classLoader. Just copies logic from:
    # https://github.com/apache/hadoop/blob/b6edcb9a/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/conf/Configuration.java#L810-L813
    thread = jvm.Thread.currentThread()  # type: ignore[union-attr]
    old_class_loader = thread.getContextClassLoader()  # type: ignore[union-attr]
    if old_class_loader is None:
        config_class = hadoop_conf.getClass()
        old_class_loader = config_class._java_lang_class.getClassLoader()

    # Wrap old classLoader with new no-op classLoader, just to make new object:
    # https://github.com/apache/spark/blob/v3.2.0/core/src/main/java/org/apache/spark/util/ParentClassLoader.java
    new_class_loader = jvm.org.apache.spark.util.ParentClassLoader(old_class_loader)  # type: ignore[union-attr]

    # Set new classLoader
    set_java_object_field(hadoop_conf, "classLoader", new_class_loader)
    thread.setContextClassLoader(new_class_loader)  # type: ignore[union-attr]
