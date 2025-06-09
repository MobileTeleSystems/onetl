<a id="install-full"></a>

# Full bundle

To install all connectors and dependencies, you can pass `all` into `extras`:

```bash
pip install onetl[all]

# this is just the same as
pip install onetl[spark,files,kerberos]
```

#### WARNING
This method consumes a lot of disk space, and requires for Java & Kerberos libraries to be installed into your OS.
