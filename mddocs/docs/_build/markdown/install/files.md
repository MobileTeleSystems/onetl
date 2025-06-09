<a id="install-files"></a>

# File connections

All File (but not *FileDF*) connection classes (`FTP`,  `SFTP`, `HDFS` and so on) requires specific Python clients to be installed.

Each client can be installed explicitly by passing connector name (in lowercase) to `extras`:

```bash
pip install onetl[ftp]  # specific connector
pip install onetl[ftp,ftps,sftp,hdfs,s3,webdav,samba]  # multiple connectors
```

To install all file connectors at once you can pass `files` to `extras`:

```bash
pip install onetl[files]
```

**Otherwise class import will fail.**
