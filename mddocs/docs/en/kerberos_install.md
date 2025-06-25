Most of Hadoop instances set up with Kerberos support,
so some connections require additional setup to work properly.

* `HDFS`
  Uses [requests-kerberos](https://pypi.org/project/requests-kerberos/) and
  [GSSApi](https://pypi.org/project/gssapi/) for authentication.
  It also uses `kinit` executable to generate Kerberos ticket.
* `Hive` and `SparkHDFS`
  require Kerberos ticket to exist before creating Spark session.

So you need to install OS packages with:

* `krb5` libs
* Headers for `krb5`
* `gcc` or other compiler for C sources

The exact installation instruction depends on your OS, here are some examples:

```bash
apt install libkrb5-dev krb5-user gcc  # Debian-based
dnf install krb5-devel krb5-libs krb5-workstation gcc  # CentOS, OracleLinux
```

Also you should pass `kerberos` to `extras` to install required Python packages:

```bash
pip install onetl[kerberos]
```
