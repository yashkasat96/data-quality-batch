
import sys
import os
import pandas as pd
import random
import string
sys.path.append('/'.join(os.path.abspath(__file__).split('\\')[:-1]) + '/resources')
from google.cloud import bigquery
# from google.oauth2 import service_account
import psycopg2

from dotenv import load_dotenv
import os
load_dotenv()

import io
import csv
from datetime import datetime

user = 'sys'
current_date = datetime.today().strftime('%Y-%m-%d %H:%M:%S')

def add_who_columns(df):
    df['created_by'] = user
    df['created_date'] = current_date
    df['updated_by'] = user
    df['updated_date'] = current_date

    return df

def create_dfs(file_path):
    for table in template_table_list:
        exec(f"%s = read_file('%s', '%s')" % (table, file_path, table), globals())

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

def insert_into_bq(df, bq_table_name):
    client = bigquery.Client(project=project_id)
    job = client.load_table_from_dataframe(df, f'{project_id}.{dataset_id}.{bq_table_name}')
    job.result()

def read_file(file, sheet_name):
    df = pd.read_excel(file, sheet_name= sheet_name)
    return df

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

source = 'bigquery'
file_path_prefix = '/bulk-upload'
# Used for loading template metadata in insert_template_data.py
template_table_list = ['entity_template', 'entity_template_properties','rule_template', 'rule_template_properties']
# BQ
bq_query_prefix = 'playground-375318.data_quality_ds.dq_metadata_schema'
project_id = 'playground-375318'
dataset_id = 'dq_metadata'

# reading the excel template file and creating df's
file_path = f'{file_path_prefix}/data_files/DQ_Metadata.xlsx'
create_dfs(file_path)

client = bigquery.Client(project=project_id)

rule_template['rule_template_id'] = get_new_ids(rule_template)
rule_template_properties['rule_template_prop_id'] = get_new_ids(rule_template_properties)
entity_template['entity_template_id'] = get_new_ids(entity_template)
entity_template_properties['entity_template_prop_id'] = get_new_ids(entity_template_properties)

for table in template_table_list:
    exec(f'%s = add_who_columns(%s)' % (table, table), globals())

for index, row in rule_template_properties.iterrows():
    rule_template_properties.at[index, 'rule_template_id'] = rule_template[(rule_template['rule_template_name'] == row['rule_template_name']) & 
                                                                        (rule_template['dq_metric'] == row['dq_metric'])]['rule_template_id'].values[0]

for index, row in entity_template_properties.iterrows():
    entity_template_properties.at[index, 'entity_template_id'] = entity_template[(entity_template['entity_template_type'] == row['entity_template_type']) & 
                                                                        (entity_template['entity_template_subtype'] == row['entity_template_subtype'])]['entity_template_id'].values[0]

rule_template_properties = rule_template_properties.drop(columns=['rule_template_name', 'dq_metric'])
entity_template_properties = entity_template_properties.drop(columns=['entity_template_type', 'entity_template_subtype'])

if source == 'postgres':
    for table in template_table_list:
        exec(f"insert_into_db('%s', %s_db_columns, %s, %s_df_columns)" % (table, table, table, table))
elif source == 'bigquery':
    for table in template_table_list:
        exec(f"insert_into_bq(%s, '%s')" % (table, table))
    # insert_into_bq(client, entity_template , 'entity_template')