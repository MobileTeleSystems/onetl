# SFTP connection { #DBR-onetl-connection-file-connection-sftp-connection }


::: onetl.connection.file_connection.sftp.SFTP
    options:
        members:
            - check
            - path_exists
            - is_file
            - is_dir
            - get_stat
            - resolve_dir
            - resolve_file
            - create_dir
            - remove_file
            - remove_dir
            - rename_dir
            - rename_file
            - list_dir
            - walk
            - download_file
            - upload_file

::: onetl.connection.file_connection.sftp.SFTPExtra
    options:
        members:
          - host_key_check
          - timeout
          - banner_timeout
          - auth_timeout
          - channel_timeout
          - compress
