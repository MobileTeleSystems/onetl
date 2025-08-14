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

!!! warning

    This method does NOT include any connections.

    This method is recommended for use in third-party libraries which require for `onetl` to be installed,
    but do not use its connection classes.
