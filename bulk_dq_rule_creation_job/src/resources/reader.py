import pandas as pd
from bulk_dq_rule_creation_job.src.resources.constants import *
import psycopg2

from dotenv import load_dotenv
import os
load_dotenv()
from google.cloud import bigquery, storage

class Reader():
    def __init__(self, source, metadata_identifier):
        self.source = source
        self.metadata_identifier = metadata_identifier

    def read_file(self, file_path, bucket_name):
        if platform == 'GCP':
            file_suffix = '/'.join(file_path.split('\\')[1:])
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(file_suffix)
            data_bytes = blob.download_as_bytes()
            df = pd.read_excel(data_bytes, sheet_name= self.metadata_identifier)
        elif platform == 'AWS':
            pass
        elif platform == 'AZURE':
            pass
        return df

    def jdbc_connection(self):

        host= os.environ['host']
        user= os.environ['user']
        port= os.environ['port']
        password= os.environ['password']
        database= os.environ['database']
        if self.source == 'postgres':
            conn = psycopg2.connect(host= host, user= user, password= password, database= database)
        elif self.source == 'mysql':
            pass
        elif self.source == 'oracle':
            pass

        conn.autocommit= True

        return conn
    
    def query_from_db(self, table_name, source):
        if source == 'postgres':
            conn = self.jdbc_connection()
            df = pd.read_sql(f"select * from {table_name}", conn)
            return df
        elif source == 'bigquery':
            client = bigquery.Client(project = project_id)
            result = client.query(f"select * from {project_id}.{dataset_id}.{table_name}")
            return result.to_dataframe()
        else:
            return 'Not a valid source or DQ app does not support'
    
    def convert_to_df(self, **args):
        db_source= ['postgres', 'bigquery', 'mysql', 'oracle', 'redshift']

        if platform == 'GCP' and 'bucket_name' in args.keys():
            df = self.read_file(args.get('file_path', ''), args.get('bucket_name', ''))
            return df
        elif platform == 'AWS' and 'bucket_name' in args.keys():
            df = self.read_file(args.get('file_path', ''), args.get('bucket_name', ''))
            return df
        elif platform == 'AZURE' and 'bucket_name' in args.keys():
            df = self.read_file(args.get('file_path', ''), args.get('bucket_name', ''))
            return df
        # DB is common for all platforms
        elif self.source in db_source:
            df = self.query_from_db(args.get('table_name', ''), self.source)
            return df
        else:
            raise Exception(f'Does not support this platform: {platform} or source:{self.source}')
        