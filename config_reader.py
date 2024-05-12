import configparser
from utils import find_project_root_path
from typing import Optional


class ConfigReader:
    def __init__(self,
                 config_file_path: Optional[str] = f'''{find_project_root_path()}/config.ini'''):
        self.config_file_path = config_file_path
        self.config = configparser.ConfigParser()
        self.config.read(self.config_file_path)

    def get_database_config(self):
        db = {
            'cluster': self.config['database']['db_cluster'],
            'user': self.config['database']['db_user'],
            'pwd': self.config['database']['db_pwd']
        }
        return db

    def get_tiendanube_config(self):
        api = {
            'tiendanube_api_key': self.config['tiendanube']['api_key'],
            'tiendanube_host': self.config['tiendanube']['host']
        }
        return api


if __name__ == "__main__":

    config_reader = ConfigReader()

    # Get database configuration
    db_config = config_reader.get_database_config()
    print("Database Config:", db_config)

    # Get Tiendanube configuration
    tiendanube_config = config_reader.get_tiendanube_config()
    print("Tiendanube Config:", tiendanube_config)

    print(db_config['cluster'])
    print(db_config['user'])
    print(db_config['pwd'])
