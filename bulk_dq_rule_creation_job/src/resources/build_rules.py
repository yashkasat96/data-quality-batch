import pandas as pd
from resources.util import *
from resources.map_templates import MapTemplates
from resources.constants import *
import numpy as np
from google.cloud import storage
from resources.writer import Writer


class BuildRules():
    def __init__(self, source, submit, uploaded_files):
        self.source = source
        self.submit = submit
        self.uploaded_files = uploaded_files
        
        self.writer = Writer(source)

    def build_rule_entity_map(self, rule_df):
        rule_entity_df = pd.DataFrame(columns= ['rule_entity_map_id', 'rule_id', 'entity_id', 'entity_behaviour', 'is_primary'])
        for index, row in rule_df.iterrows():
            for i in ['source', 'target']:
                if not pd.isna(row[i+'_entity_id']):
                    rule_entity_df.loc[len(rule_entity_df)] = [get_unique_id(), row['rule_id'],
                                                                row[i+'_entity_id'], i, 'True']
            for i in ['source', 'target']:
                if not pd.isna(row[i+'_secondary_entity_ids']):
                    entities_split= row[i+'_secondary_entity_ids'].split(',')
                    for j in entities_split:
                        rule_entity_df.loc[len(rule_entity_df)] = [get_unique_id(), row['rule_id'],
                                                                    j.strip(), i, 'False']

        rule_entity_df = add_who_columns(rule_entity_df)
        return rule_entity_df

    def map_secondary_entity(self, rule_df, entity_mapper):
        rule_df['source_secondary_entity_ids'] = np.NaN
        rule_df['target_secondary_entity_ids'] = np.NaN
        for index, row in rule_df.iterrows():
            source_secondary_entity = row['source_secondary_entity']
            target_secondary_entity = row['target_secondary_entity']
            if not pd.isna(source_secondary_entity):
                if ',' in source_secondary_entity:
                    source_secondary_entity = row['source_secondary_entity'].split(',')
                    source_secondary_entity_ids = [entity_mapper[x.strip()] for x in source_secondary_entity]
                    source_secondary_entity_ids_string = ', '.join(source_secondary_entity_ids)
                    rule_df.loc[index, 'source_secondary_entity_ids'] = source_secondary_entity_ids_string
                else:
                    source_secondary_entity_id = entity_mapper[row['source_secondary_entity']]
                    rule_df.loc[index, 'source_secondary_entity_ids'] = source_secondary_entity_id
            if not pd.isna(target_secondary_entity):
                if ',' in target_secondary_entity:
                    target_secondary_entity = row['target_secondary_entity'].split(',')
                    target_secondary_entity_ids = [entity_mapper[x.strip()] for x in target_secondary_entity]
                    target_secondary_entity_ids_string = ', '.join(target_secondary_entity_ids)
                    rule_df.loc[index, 'target_secondary_entity_ids'] = target_secondary_entity_ids_string
                else:
                    target_secondary_entity_id = entity_mapper[row['target_secondary_entity']]
                    rule_df.loc[index, 'target_secondary_entity_ids'] = target_secondary_entity_id
        return rule_df

    def build_rule_properties(self, rule_df):
        rule_props_dict = {}

        rule_props_columns = [x for x in rule_df.columns if x.startswith('property') or x.startswith('value') or x == 'rule_id']
        rule_props_df = rule_df[rule_props_columns]
        rule_props_df1 = pd.DataFrame(columns=['rule_prop_id', 'rule_id', 'rule_prop_key', 'rule_prop_value'])

        for index, row in rule_props_df.iterrows():
            for i in range(1,11):
                try:
                    if isNaN(row['property_'+str(i)]):
                        rule_props_df1.loc[len(rule_props_df1)] = [get_unique_id(), row['rule_id'],
                                                                    row['property_'+str(i)],
                                                                    row['value_'+str(i)]
                                                                    ]
                except KeyError:
                    break
    
        rule_props_df1 = add_who_columns(rule_props_df1)
        return rule_props_df1

        
    def build(self, rule_df):
        if self.submit == 'false':
            new_ids = get_new_ids(rule_df)
            rule_df['rule_id'] = new_ids

            ruleset_mapper, entity_mapper = id_mapper(self.source)

            # rule_df = rule_df[[x for x in rule_df.columns if not(x.startswith('prop_'))]]
            # mapping entities and ruleset ids
            rule_df['ruleset_id'] = rule_df['ruleset_name'].map(ruleset_mapper)
            rule_df['source_entity_id'] = rule_df['source_entity'].map(entity_mapper)
            rule_df['target_entity_id'] = rule_df['target_entity'].map(entity_mapper)
            # mapping secondary entities with ids
            rule_df = self.map_secondary_entity(rule_df, entity_mapper)

            # mapping template ids to rule records
            # rule_template_df = read_file('bulk-upload/data_files/Bulk_upload_template_HSBC.xlsx', 'rule_template')
            rule_template_df = query_from_db('rule_template', self.source)
            map_templates = MapTemplates()
            rule_df = map_templates.map_rule_template(rule_template_df, rule_df)
            rule_df = rule_df.drop_duplicates()

            rule_df = add_who_columns(rule_df)

            # creating rule entity map table
            rule_entity_df = self.build_rule_entity_map(rule_df)

            # creating rule properties table - only variable properties
            rule_properties_df = self.build_rule_properties(rule_df)

            preview_datafiles = {}
            # client = storage.Client()
            # bucket = client.get_bucket('dq-bulk-upload')
            # file_suffix = datetime.today().strftime('%Y-%m-%d %H:%M:%S').\
            #                             replace('-', '').\
            #                             replace(' ', '').\
            #                             replace(':', '')
            if rule_df.shape[0] > 0:
                # file_name = f'output/rule_{file_suffix}.csv'
                # print(file_name)
                # bucket.blob(file_name).upload_from_string(rule_df.to_csv(), 'text/csv')
                # preview_datafiles['rule'] = f'gs://dq-bulk-upload/{file_name}'

                preview_file= self.writer.write_data(table = 'rule', \
                                              bucket_name = 'dq-bulk-upload', \
                                              df = rule_df)
                preview_datafiles['rule'] = preview_file
            if rule_entity_df.shape[0] > 0:
                # file_name = f'output/rule_entity_{file_suffix}.csv'
                # print(file_name)
                # bucket.blob(file_name).upload_from_string(rule_entity_df.to_csv(), 'text/csv')
                # preview_datafiles['rule_entity'] = f'gs://dq-bulk-upload/{file_name}'

                preview_file= self.writer.write_data(table = 'rule_entity', \
                                              bucket_name = 'dq-bulk-upload', \
                                              df = rule_entity_df)
                preview_datafiles['rule_entity'] = preview_file
            if rule_properties_df.shape[0] > 0:
                # file_name = f'output/rule_properties_{file_suffix}.csv'
                # print(file_name)
                # bucket.blob(file_name).upload_from_string(rule_properties_df.to_csv(), 'text/csv')
                # preview_datafiles['rule_properties'] = f'gs://dq-bulk-upload/{file_name}'

                preview_file= self.writer.write_data(table = 'rule_properties', \
                                              bucket_name = 'dq-bulk-upload', \
                                              df = rule_properties_df)
                preview_datafiles['rule_properties'] = preview_file

        elif self.submit == 'true':
            gcs_file = self.uploaded_files['rule']
            rule_df = pd.read_csv(gcs_file)
            rule_df = get_timestamp_columns(rule_df)
            if self.source == 'postgres':
                # insert_into_db('rule', rule_db_columns, rule_df, rule_df_columns)
                self.writer.write_data(table = 'rule', \
                                        df = rule_df)
            elif self.source == 'bigquery':
                get_timestamp_columns(rule_df)
                # insert_into_bq(rule_df[rule_df_columns], 'rule')
                self.writer.write_data(table = 'rule', \
                                        df = rule_df)

            gcs_file = self.uploaded_files['rule_entity']
            rule_entity_df = pd.read_csv(gcs_file)
            rule_entity_df = get_timestamp_columns(rule_entity_df)
            if self.source == 'postgres':
                # insert_into_db('rule_entity_map', rule_entity_map_db_columns, rule_entity_df, rule_entity_map_df_columns)
                self.writer.write_data(table = 'rule_entity', \
                                        df = rule_entity_df)
            elif self.source == 'bigquery':
                # insert_into_bq(rule_entity_df[rule_entity_map_df_columns], 'rule_entity_map')
                self.writer.write_data(table = 'rule_entity', \
                                        df = rule_entity_df)

            gcs_file = self.uploaded_files['rule_properties']
            rule_properties_df = pd.read_csv(gcs_file)
            rule_properties_df = get_timestamp_columns(rule_properties_df)
            if self.source == 'postgres':
                # insert_into_db('rule_properties', rule_properties_db_columns, rule_properties_df, rule_properties_df_columns)
                self.writer.write_data(table = 'rule_properties', \
                                        df = rule_properties_df)
            elif self.source == 'bigquery':
                # insert_into_bq(rule_properties_df[rule_properties_df_columns], 'rule_properties')
                self.writer.write_data(table = 'rule_properties', \
                                        df = rule_properties_df)
        
        # if rule_entity_df.shape[0] > 0 and self.submit == 'false':
        #     if self.source == 'postgres':
        #         insert_into_db('rule_entity_map', rule_entity_map_db_columns, rule_entity_df, rule_entity_map_df_columns)
        #     elif self.source == 'bigquery':
        #         insert_into_bq(rule_entity_df[rule_entity_map_df_columns], 'rule_entity_map')
        # # rule_entity_df.to_csv(f'{file_path_prefix}/output/rule_entity_map.csv', index= False)
        
        # if rule_properties_df.shape[0] > 0 and self.submit == 'false':
        #     if self.source == 'postgres':
        #         insert_into_db('rule_properties', rule_properties_db_columns, rule_properties_df, rule_properties_df_columns)
        #     elif self.source == 'bigquery':
        #         temp_df = rule_properties_df[rule_properties_df_columns]
        #         print(temp_df.dtypes)
        #         insert_into_bq(rule_properties_df[rule_properties_df_columns], 'rule_properties')
        # # rule_properties_df.to_csv(f'{file_path_prefix}/output/rule_properties.csv', index= False)

        # if rule_df.shape[0] > 0 and self.submit == 'false':
        #     if self.source == 'postgres':
        #         insert_into_db('rule', rule_db_columns, rule_df, rule_df_columns)
        #     elif self.source == 'bigquery':
        #         insert_into_bq(rule_df[rule_df_columns], 'rule')
        # # rule_df.to_csv(f'{file_path_prefix}/output/rule.csv', index= False)

        return rule_df

