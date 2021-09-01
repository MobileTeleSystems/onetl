import os
import json

from logging import getLogger

from requests import Session
from requests_ntlm import HttpNtlmAuth

from onetl.connection.file_connection.file_connection import FileConnection

log = getLogger(__name__)


class Sharepoint(FileConnection):
    def get_client(self):
        auth = HttpNtlmAuth(self.login, self.password)
        conn = Session()
        conn.verify = False
        conn.auth = auth
        conn.headers.update({'Accept': 'application/json;odata=verbose'})
        return conn

    def is_dir(self, top, item):
        all_folders = [item['ServerRelativeUrl'] for item in self.get_folders_list(top)]
        return item['ServerRelativeUrl'] in all_folders

    def get_name(self, item):
        return item['Name']

    def download_file(self, remote_file_path, local_file_path):
        with open(local_file_path, 'wb') as f:
            f.write(self.get_file_content(remote_file_path))
        log.info(f'Successfully download _file {remote_file_path} from sharepoint to {local_file_path}')

    def remove_file(self, remote_file_path):
        self.remove_file_http(remote_file_path)
        log.info(f'Successfully removed file {remote_file_path}')

    def get_files_list(self, folder):
        list_files_url = f'https://{self.host}/_api/web/GetFolderByServerRelativeUrl("{folder}")/Files'
        response = self.client.get(list_files_url)
        response.raise_for_status()
        return json.loads(response.text)['d']['results']

    def get_folders_list(self, folder):
        list_dirs_url = f'https://{self.host}/_api/web/GetFolderByServerRelativeUrl("{folder}")/Folders'
        response = self.client.get(list_dirs_url)
        response.raise_for_status()
        return json.loads(response.text)['d']['results']

    def get_file_content(self, filepath):
        folder, filename = os.path.split(filepath)
        file_content_url = (
            f'https://{self.host}/_api/web/GetFolderByServerRelativeUrl("{folder}")/Files("{filename}")/$value'
        )
        response = self.client.get(file_content_url)
        response.raise_for_status()
        return response.content

    def remove_file_http(self, filepath):
        folder, filename = os.path.split(filepath)
        file_content_url = f'https://{self.host}/_api/web/GetFolderByServerRelativeUrl("{folder}")/Files("{filename}")'
        response = self.client.delete(file_content_url)
        response.raise_for_status()
        return response.content

    def _listdir(self, path):
        return self.get_files_list(path) + self.get_folders_list(path)
