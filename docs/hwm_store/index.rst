.. _hwm-store:

HWM
=========

Since ``onetl>=0.10.0`` version, the HWM Store and HWM classes have been moved to a separate library :etl-entities:`etl-entities <>`.

|
| **HWM Store**
- :etl-entities:`Base HWM Store <hwm_store/base_hwm_store.html>` This class facilitates the creation of custom hwm stores.
- :etl-entities:`Memory HWM Store <hwm_store/memory_hwm_store.html>` This class serving as an example for storing hwms in memory.
|
| **HWM**
Moreover, HWM classes that operate with HWM stores have also been relocated to the :etl-entities:`etl-entities <>` library. These include:

- :etl-entities:`Column Int HWM <hwm/column/int_hwm.html>`
- :etl-entities:`Column Date HWM <hwm/column/date_hwm.html>`
- :etl-entities:`Column DateTime HWM <hwm/column/datetime_hwm.html>`
- :etl-entities:`File List HWM <hwm/file/file_list_hwm.html>`

For detailed insights into the various HWM classes available, refer to the documentation on:

- :etl-entities:`What is HWM </hwm/column/index.html#what-is-hwm>`

Below is the **default HWM store** in onETL - YAML HWM Store:

.. toctree::
    :maxdepth: 2

    yaml_hwm_store
