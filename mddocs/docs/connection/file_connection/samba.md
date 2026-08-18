# Samba connection { #DBR-onetl-connection-file-connection-samba-connection }


::: onetl.connection.file_connection.samba.Samba
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
            - rename_file
            - list_dir
            - download_file
            - upload_file

::: onetl.connection.file_connection.samba.SambaExtra
    options:
        members:
          - connect_timeout
          - operation_timeout
          - my_name
          - sign_options
