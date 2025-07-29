import os
import logging
import yaml
import pymongo

# Import custom packages
from ..global_variable import connection_string,database_name


class ClsMongoDBInit():
    """ Initialize MongoDB Database connection"""

    global connection_string, database_name
    def get_ew_db_client() -> None:
        """ Returns a db client """
        client = pymongo.MongoClient(connection_string)
        ew_db = client[database_name]
        return ew_db

    def get_cl_db_client(cl_db) -> None:
        """ Returns a db client """
        client = pymongo.MongoClient(connection_string)
        cl_db = client[cl_db]
        return cl_db







# with open(os.path.dirname(__file__) + '/../conf/logging.yaml', 'r') as f:
#     read_config = yaml.safe_load(f.read())
#     logging.read_config.dictread_config(read_config)
# LOG = logging.getLogger('dbConnectionLog')


"""
class ConnectionError(Exception):
    pass

class CredentialsError(Exception):
    pass

class ClsUseDatabase:
    def __init__(self):
        pass

    def __enter__(self, config = connection_string):
        try:
            self.client = pymongo.MongoClient(config)
            self.database = self.client[database_name]
            return self

        except:
            print("Check connection settings")
            raise ConnectionError

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.client.close()
"""
