```{eval-rst}
.. include:: ../README.rst
    :end-before: |Logo|
```

```{image} _static/logo_wide.svg
:alt: onETL logo
```

```{eval-rst}
.. include:: ../README.rst
    :start-after: |Logo|
    :end-before: documentation
```

```{toctree}
:caption: How to
:hidden: true
:maxdepth: 2

self
install/index
quickstart
concepts
logging
troubleshooting/index
```

```{toctree}
:caption: Connection
:hidden: true
:maxdepth: 3

connection/index
```

```{toctree}
:caption: DB classes
:hidden: true
:maxdepth: 3

db/index
```

```{toctree}
:caption: File classes
:hidden: true
:maxdepth: 3

file/index
```

```{toctree}
:caption: File DataFrame classes
:hidden: true
:maxdepth: 3

file_df/index
```

```{toctree}
:caption: Read strategies and HWM
:hidden: true
:maxdepth: 2

strategy/index
hwm_store/index
```

```{toctree}
:caption: Hooks & plugins
:hidden: true
:maxdepth: 2

hooks/index
plugins
```

```{toctree}
:caption: Development
:hidden: true
:maxdepth: 2

changelog
contributing
security
```
