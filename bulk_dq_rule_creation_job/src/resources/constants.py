from datetime import datetime

platform = 'GCP'
source = 'bigquery'
file_path_prefix = '/bulk-upload'

# used for generating who columns
user = 'sys'
current_date = datetime.today()
# current_date = datetime.today().strftime('%Y-%m-%d %H:%M:%S')

entity_columns = ['entity_id', 'entity_template_id', 'entity_name', 'entity_physical_name', 'entity_primary_key']
entity_props_columns=['entity_prop_id', 'entity_id', 'entity_prop_key', 'entity_prop_value']
rule_columns = ['rule_id', 'ruleset_id', 'rule_template_id', 'rule_name', 'rule_desc', 'created_by', 'created_date', 'updated_by', 'updated_date']


# parameters for loading df to DB/Bigquery
df_columns= {'ruleset': ['ruleset_id', 'ruleset_name', 'ruleset_desc', 'notification_email', 'created_by', 'created_date', 'updated_by', 'updated_date'],
             'entity': ['entity_id','entity_template_id','entity_name','entity_physical_name','entity_primary_key','created_by','created_date','updated_by','updated_date'],
             'entity_properties': ['entity_prop_id','entity_id','entity_prop_key','entity_prop_value','created_by','created_date','updated_by','updated_date'],
             'rule': ['rule_id','ruleset_id','rule_template_id','rule_name','rule_desc','created_by','created_date','updated_by','updated_date'],
             'rule_properties': ['rule_prop_id','rule_id','rule_prop_key','rule_prop_value','created_by','created_date','updated_by','updated_date'],
             'rule_entity': ['rule_entity_map_id','rule_id','entity_id','entity_behaviour','is_primary','created_by','created_date','updated_by','updated_date'],
             'entity_template': ['entity_template_id', 'entity_template_type', 'entity_template_subtype', 'created_by', 'created_date', 'updated_by', 'updated_date'],
             'entity_template_properties': ['entity_template_prop_id', 'entity_template_prop_key', 'entity_template_prop_type', 'entity_template_prop_desc', 'is_mandatory', 'entity_template_id', 'created_by', 'created_date', 'updated_by', 'updated_date'],
             'rule_template': ['rule_template_id', 'dq_metric', 'rule_template_name', 'rule_template_desc', 'created_by', 'created_date', 'updated_by', 'updated_date'],
             'rule_template_properties': ['rule_template_prop_id', 'rule_template_prop_key', 'rule_template_prop_type', 'rule_template_prop_value', 'rule_template_prop_desc', 'is_mandatory', 'rule_template_id', 'created_by', 'created_date', 'updated_by', 'updated_date']}

db_columns= {'ruleset': 'ruleset_id,ruleset_name,ruleset_desc,notification_email,created_by,created_date,updated_by,updated_date',
             'entity': 'entity_id,entity_template_id,entity_name,entity_physical_name,entity_primary_key,created_by,created_date,updated_by,updated_date',
             'entity_properties': 'entity_prop_id,entity_id,entity_prop_key,entity_prop_value,created_by,created_date,updated_by,updated_date',
             'rule': 'rule_id,ruleset_id,rule_template_id,rule_name,rule_desc,created_by,created_date,updated_by,updated_date',
             'rule_properties': 'rule_prop_id,rule_id,rule_prop_key,rule_prop_value,created_by,created_date,updated_by,updated_date',
             'rule_entity': 'rule_entity_map_id,rule_id,entity_id,entity_behaviour,is_primary,created_by,created_date,updated_by,updated_date',
             'entity_template': 'entity_template_id, entity_template_type, entity_template_subtype, created_by, created_date, updated_by, updated_date',
             'entity_template_properties': 'entity_template_prop_id, entity_template_prop_key, entity_template_prop_type, entity_template_prop_desc, is_mandatory, entity_template_id, created_by, created_date, updated_by, updated_date',
             'rule_template': 'rule_template_id, dq_metric, rule_template_name, rule_template_desc, created_by, created_date, updated_by, updated_date',
             'rule_template_properties': 'rule_template_prop_id, rule_template_prop_key, rule_template_prop_type, rule_template_prop_value, rule_template_prop_desc, is_mandatory, rule_template_id, created_by, created_date, updated_by, updated_date'}

# used in insert_template_data.py & json creation
table_list = ['entity', 'entity_properties', 'entity_template', 'entity_template_properties', 'rule', 'rule_entity_map', 'rule_properties', 'rule_template', 'rule_template_properties', 'ruleset']
# Used for loading template metadata in insert_template_data.py
template_table_list = ['entity_template', 'entity_template_properties','rule_template', 'rule_template_properties']


# BQ config
project_id = 'playground-375318'
# dataset_id = 'dq_metadata'
dataset_id = 'dq_experimental_ds'

timestamp_columns = ['created_date', 'updated_date']


mandatory_columns = {'ruleset': ['ruleset_name', 'ruleset_desc', 'notification_email'],
                     'entity': ['entity_name','entity_physical_name','entity_type', 'entity_subtype','entity_primary_key'],
                     'rule': ['rule_name', 'rule_desc', 'rule_template_name', 'dq_metric',
                              'source_entity', 'ruleset_name']
                     }