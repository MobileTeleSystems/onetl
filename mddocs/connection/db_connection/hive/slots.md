<a id="hive-slots"></a>

# Hive Slots

### *class* onetl.connection.db_connection.hive.slots.HiveSlots

[Slots](../../../hooks/slot.md#slot-decorator) that could be implemented by third-party plugins.

#### Versionadded
Added in version 0.7.0.

<!-- !! processed by numpydoc !! -->

#### *static* normalize_cluster_name(cluster: str) → str | None

Normalize cluster name passed into Hive constructor. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

If hooks didn’t return anything, cluster name is left intact.

#### Versionadded
Added in version 0.7.0.

* **Parameters:**
  **cluster**
  : Cluster name (raw)
* **Returns:**
  str | None
  : Normalized cluster name.
    <br/>
    If hook cannot be applied to a specific cluster, it should return `None`.

### Examples

```python
from onetl.connection import Hive
from onetl.hooks import hook

@Hive.Slots.normalize_cluster_name.bind
@hook
def normalize_cluster_name(cluster: str) -> str:
    return cluster.lower()
```

<!-- !! processed by numpydoc !! -->

#### *static* get_known_clusters() → set[str] | None

Return collection of known clusters. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

Cluster passed into Hive constructor should be present in this list.
If hooks didn’t return anything, no validation will be performed.

#### Versionadded
Added in version 0.7.0.

* **Returns:**
  set[str] | None
  : Collection of cluster names (normalized).
    <br/>
    If hook cannot be applied, it should return `None`.

### Examples

```python
from onetl.connection import Hive
from onetl.hooks import hook

@Hive.Slots.get_known_clusters.bind
@hook
def get_known_clusters() -> str[str]:
    return {"rnd-dwh", "rnd-prod"}
```

<!-- !! processed by numpydoc !! -->

#### *static* get_current_cluster() → str | None

Get current cluster name. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

Used in `check` method to verify that connection is created only from the same cluster.
If hooks didn’t return anything, no validation will be performed.

#### Versionadded
Added in version 0.7.0.

* **Returns:**
  str | None
  : Current cluster name (normalized).
    <br/>
    If hook cannot be applied, it should return `None`.

### Examples

```python
from onetl.connection import Hive
from onetl.hooks import hook

@Hive.Slots.get_current_cluster.bind
@hook
def get_current_cluster() -> str:
    # some magic here
    return "rnd-dwh"
```

<!-- !! processed by numpydoc !! -->
