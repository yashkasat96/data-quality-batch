import random
import string
import pandas as pd
from bulk_dq_rule_creation_job.src.resources.constants import *
import psycopg2

from dotenv import load_dotenv
import os
load_dotenv()

import io
import csv
from google.cloud import bigquery, storage

class Writer():
    def __init__(self, source):
        self.source = source

    # def insert_into_bq(self, df, table_name):
    #     client = bigquery.Client(project=project_id)
    #     job = client.load_table_from_dataframe(df, f'{project_id}.{dataset_id}.{table_name}')
    #     job.result()

    def jdbc_connection(self):

        host= os.environ['host']
        user= os.environ['user']
        port= os.environ['port']
        password= os.environ['password']
        database= os.environ['database']
        if self.source == 'postgres':
            conn = psycopg2.connect(host= host, user= user, password= password, database= database)
            conn.autocommit= True
        elif self.source == 'mysql':
            pass
        elif self.source == 'oracle':
            pass

        return conn

    def insert_into_db(self, table, df):

        _db_columns = db_columns[table]
        _df_columns = df_columns[table]

        if self.source == 'postgres':
            conn = self.jdbc_connection()
            csv_buffer = io.StringIO()
            writer = csv.writer(csv_buffer)
            for index, row in df.iterrows():
                record = [row[i] for i in _df_columns]
                writer.writerow(record)
            csv_buffer.seek(0)

            with conn.cursor() as cur:
                postgres_insert_query = f"COPY {table} ({_db_columns}) FROM STDIN WITH(FORMAT CSV, DELIMITER ',',NULL '')"
                cur.copy_expert(postgres_insert_query, csv_buffer)
        elif self.source == 'mysql':
            pass
        elif self.source == 'oracle':
            pass
        elif self.source == 'bigquery':
            client = bigquery.Client(project=project_id)
            job = client.load_table_from_dataframe(df[_df_columns], f'{project_id}.{dataset_id}.{table}')
            job.result()

    def write_to_file(self, table, bucket_name, df):
        file_suffix = datetime.today().strftime('%Y-%m-%d %H:%M:%S').\
                                replace('-', '').\
                                replace(' ', '').\
                                replace(':', '')
        file_name = f'output/{table}_{file_suffix}.csv'

        if platform == 'GCP':
            client = storage.Client()
            bucket = client.get_bucket(bucket_name)
            bucket.blob(file_name).upload_from_string(df.to_csv(), 'text/csv')
        elif platform == 'AWS':
            pass
        elif platform == 'AZURE':
            pass
        else:
            pass
        preview_file= f'gs://dq-bulk-upload/{file_name}'

        return preview_file
    
    def write_data(self, **args):
        db_source= ['postgres', 'bigquery']

        if platform == 'GCP' and 'bucket_name' in args.keys():
            return self.write_to_file(args.get('table', ''), \
                                        args.get('bucket_name', ''), \
                                        args.get('df', ''))
        # DB is common for all platforms
        elif self.source in db_source and 'bucket_name' not in args.keys():
            self.insert_into_db(args.get('table', ''), args.get('df', ''))
        else:
            raise Exception(f'Does not support this platform: {platform} or source:{self.source}')
        