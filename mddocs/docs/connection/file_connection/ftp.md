# FTP connection { #DBR-onetl-connection-file-connection-ftp-connection }


::: onetl.connection.file_connection.ftp.FTP
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

::: onetl.connection.file_connection.ftp.FTPExtra
    options:
        members:
          - use_passive_mode
          - encoding
