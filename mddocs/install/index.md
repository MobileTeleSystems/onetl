<a id="install"></a>

# How to install

Base `onetl` package contains:

* `DBReader`, `DBWriter` and related classes
* `FileDownloader`, `FileUploader`, `FileMover` and related classes, like file filters & limits
* `FileDFReader`, `FileDFWriter` and related classes, like file formats
* Read Strategies & HWM classes
* Plugins support

It can be installed via:

```bash
pip install onetl
```

```{admonition} warning

:class: warning

This method does NOT include any connections.

This method is recommended for use in third-party libraries which require for `onetl` to be installed, but do not use its connection classes.
```

## Installation in details

## How to install

* [How to install]()
* [Spark](spark.md)
* [File connections](files.md)
* [Kerberos support](kerberos.md)
* [Full bundle](full.md)
