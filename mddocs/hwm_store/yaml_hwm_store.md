<a id="yaml-hwm-store"></a>

# YAMLHWMStore

### *class* onetl.hwm.store.yaml_hwm_store.YAMLHWMStore(\*, path: LocalPath = LocalPosixPath('/home/sattar/.local/share/onETL/yml_hwm_store'), encoding: str = 'utf-8')

YAML **local store** for HWM values. Used as default HWM store. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

* **Parameters:**
  **path**
  : Folder name there HWM value files will be stored.
    <br/>
    Default:
    * `~/.local/share/onETL/yml_hwm_store` on Linux
    * `C:\Documents and Settings\<User>\Application Data\oneTools\onETL\yml_hwm_store` on Windows
    * `~/Library/Application Support/onETL/yml_hwm_store` on MacOS

  **encoding**
  : Encoding of files with HWM value

### Examples

Default parameters

```python
from onetl.connection import Hive, Postgres
from onetl.db import DBReader, DBWriter
from onetl.strategy import IncrementalStrategy
from onetl.hwm.store import YAMLHWMStore

postgres = Postgres(...)
hive = Hive(...)

reader = DBReader(
    connection=postgres,
    source="public.mydata",
    columns=["id", "data"],
    hwm=DBReader.AutoDetectHWM(name="some_unique_name", expression="id"),
)

writer = DBWriter(connection=hive, target="db.newtable")

with YAMLHWMStore():
    with IncrementalStrategy():
        df = reader.run()
        writer.run(df)

# will create file
# "~/.local/share/onETL/id__public.mydata__postgres_postgres.domain.com_5432__myprocess__myhostname.yml"
# with encoding="utf-8" and save a serialized HWM values to this file
```

With all options

```python
with YAMLHWMStore(path="/my/store", encoding="utf-8"):
    with IncrementalStrategy():
        df = reader.run()
        writer.run(df)

# will create file
# "/my/store/id__public.mydata__postgres_postgres.domain.com_5432__myprocess__myhostname.yml"
# with encoding="utf-8" and save a serialized HWM values to this file
```

File content example:

```yaml
- column:
    name: id
    partition: {}
  modified_time: '2023-02-11T17:10:49.659019'
  process:
      dag: ''
      host: myhostname
      name: myprocess
      task: ''
  source:
      db: public
      instance: postgres://postgres.domain.com:5432/target_database
      name: mydata
  type: int
  value: '1500'
- column:
      name: id
      partition: {}
  modified_time: '2023-02-11T16:00:31.962150'
  process:
      dag: ''
      host: myhostname
      name: myprocess
      task: ''
  source:
      db: public
      instance: postgres://postgres.domain.com:5432/target_database
      name: mydata
  type: int
  value: '1000'
```

<!-- !! processed by numpydoc !! -->

#### \_\_enter_\_()

HWM store context manager.

Enter this context to use this HWM store instance as current one (instead default).

### Examples

```pycon
>>> from etl_entities.hwm_store import HWMStoreStackManager
>>> with SomeHWMStore(...) as hwm_store:
...     print(HWMStoreStackManager.get_current())
SomeHWMStore(...)
>>> HWMStoreStackManager.get_current()
DefaultHWMStore()
```

<!-- !! processed by numpydoc !! -->

#### get_hwm(name: str) → HWM | None

Get HWM by name from HWM store.

* **Parameters:**
  **name**
  : HWM unique name
* **Returns:**
  `HWM`
  : HWM object, if it exists in HWM store, or None

### Examples

```python
hwm = hwm_store.get_hwm(hwm_unique_name)
```

<!-- !! processed by numpydoc !! -->

#### set_hwm(hwm: HWM) → LocalPath

Save HWM object to HWM Store.

* **Parameters:**
  **hwm**
  : HWM object
* **Returns:**
  Any
  : HWM location, like URL of file path. Result type is implementation-specific.

### Examples

```python
from etl_entities.hwm import ColumnIntHWM

hwm = ColumnIntHWM(name=..., value=...)
hwm_location = hwm_store.set_hwm(hwm)
```

<!-- !! processed by numpydoc !! -->
