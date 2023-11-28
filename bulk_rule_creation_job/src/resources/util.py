import random
import string
import pandas as pd
from resources.constants import *
import psycopg2

from dotenv import load_dotenv
import os
load_dotenv()

import io
import csv
from google.cloud import bigquery


def get_unique_id():
    while True:
        number = str(''.join(random.choices(string.digits, k=8)))
        if not number.startswith('0'):
            return number

def get_new_ids(df):
    # original_ids = df[type+'_unique_identifier'].unique()
    new_ids= []
    for i in range(0, len(df)):
        new_ids.append(get_unique_id())
    return new_ids

# def isNaN(string):
#     return string == string

def isNaN(string):
    if type(string) == float:
        if pd.isnull(string):
            return False
        else:
            return True
    else:
        if string.strip() == '' or pd.isnull(string):
            return False
        else:
            return True

def read_file(file, sheet_name):
    df = pd.read_excel(file, sheet_name= sheet_name)
    return df
        
def id_mapper(source):
    # mapper = {row[name]: row[id] for index, row in df.iterrows()}
    ruleset = query_from_db('ruleset', source)
    ruleset_mapper = {row['ruleset_name']: row['ruleset_id'] for index, row in ruleset.iterrows()}

    entity = query_from_db('entity', source)
    entity_mapper = {row['entity_physical_name']: row['entity_id'] for index, row in entity.iterrows()}
    # print(mapper)
    # for index, row in db_df.iterrows():
    #     mapper[row[name]] = str(row[id])
    # print(mapper)
    return ruleset_mapper, entity_mapper

def rename_columns(df):
    df.columns = df.columns.str.strip().str.replace(' ', '_').str.lower()
    return df

def add_who_columns(df):
    df['created_by'] = user
    df['created_date'] = current_date
    df['updated_by'] = user
    df['updated_date'] = current_date

    return df

def jdbc_connection():

    host= os.environ['host']
    user= os.environ['user']
    port= os.environ['port']
    password= os.environ['password']
    database= os.environ['database']

    conn = psycopg2.connect(host= host, user= user, password= password, database= database)
    conn.autocommit= True

    return conn

def insert_into_db(table_name, db_columns, df, df_columns):
    conn = jdbc_connection()

    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)

    for index, row in df.iterrows():
        record = [row[i] for i in df_columns]
        writer.writerow(record)

    csv_buffer.seek(0)
    # print(csv_buffer.getvalue())

    with conn.cursor() as cur:
        postgres_insert_query = f"COPY {table_name} ({db_columns}) FROM STDIN WITH(FORMAT CSV, DELIMITER ',',NULL '')"
        cur.copy_expert(postgres_insert_query, csv_buffer)

# def insert_into_bq(client, bq_table_name, df, job_config):
#     exec("job = client.load_table_from_dataframe(%s, '%s.%s.%s', job_config=job_config)"
#          % (df, project_id, dataset_id, bq_table_name))
#     job.result()

def insert_into_bq(df, bq_table_name):
    client = bigquery.Client(project=project_id)
    job = client.load_table_from_dataframe(df, f'{project_id}.{dataset_id}.{bq_table_name}')
    job.result()


def query_from_db(table_name, source):
    if source == 'postgres':
        conn = jdbc_connection()
        df = pd.read_sql(f"select * from {table_name}", conn)
        return df
    elif source == 'bigquery':
        client = bigquery.Client(project = project_id)
        result = client.query(f"select * from {project_id}.{dataset_id}.{table_name}")
        return result.to_dataframe()
    else:
        pass

def get_timestamp_columns(df):
    for column in timestamp_columns:
        df[column] = pd.to_datetime(df[column], format="%Y-%m-%d %H:%M:%S.%f")
    return df