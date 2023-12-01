import pandas as pd
from util import *
from bulk_dq_rule_creation_job.src.resources.map_templates import MapTemplates
from bulk_dq_rule_creation_job.src.resources.constants import *
from google.cloud import storage
from bulk_dq_rule_creation_job.src.resources.writer import Writer


class BuildEntity:
    def __init__(self, df, source, submit, uploaded_files):
        self.df = df
        self.source = source
        self.submit = submit
        self.uploaded_files = uploaded_files
        
        self.writer = Writer(source)
        
    def build(self):
        if self.submit == 'false':
            new_ids = get_new_ids(self.df)
            self.df['entity_id'] = new_ids

            entity_df = self.df[[x for x in self.df.columns if not(x.startswith('property') or x.startswith('value'))]]
            
            # entity_template_df = read_file('data_files/Bulk_upload_template_HSBC.xlsx', 'entity_template')
            entity_template_df = query_from_db('entity_template', self.source)
            map_templates = MapTemplates()
            entity_df = map_templates.map_entity_template(entity_template_df, entity_df)

            entity_props_df = self.df[[x for x in self.df.columns if x.startswith('property') or x.startswith('value') or x == 'entity_id']]
            entity_props_df1 = pd.DataFrame(columns=entity_props_columns)

            for index, row in entity_props_df.iterrows():
                    for i in range(1,11):
                        try:
                            if isNaN(row['property_'+str(i)]):
                                entity_props_df1.loc[len(entity_props_df1)] = [get_unique_id(), row['entity_id'],
                                                                            row['property_'+str(i)], row['value_'+str(i)]
                                                                            ]
                        except KeyError:
                            break

            
            entity_df = entity_df[entity_columns]
            
            entity_df = add_who_columns(entity_df)
            entity_props_df1 = add_who_columns(entity_props_df1)

            preview_datafiles = {}
            # client = storage.Client()
            # bucket = client.get_bucket('dq-bulk-upload')
            # file_suffix = datetime.today().strftime('%Y-%m-%d %H:%M:%S').\
            #                             replace('-', '').\
            #                             replace(' ', '').\
            #                             replace(':', '')
            if entity_df.shape[0] > 0:
                # file_name = f'output/entity_{file_suffix}.csv'
                # bucket.blob(file_name).upload_from_string(entity_df.to_csv(), 'text/csv')
                # preview_datafiles['entity'] = f'gs://dq-bulk-upload/{file_name}'

                preview_file= self.writer.write_data(table = 'entity', \
                                              bucket_name = 'dq-bulk-upload', \
                                              df = entity_df)
                preview_datafiles['entity'] = preview_file
            if entity_props_df1.shape[0] > 0:
                # file_name = f'output/entity_properties_{file_suffix}.csv'
                # bucket.blob(file_name).upload_from_string(entity_props_df1.to_csv(), 'text/csv')
                # preview_datafiles['entity_properties'] = f'gs://dq-bulk-upload/{file_name}'

                preview_file= self.writer.write_data(table = 'entity_properties', \
                                              bucket_name = 'dq-bulk-upload', \
                                              df = entity_props_df1)
                preview_datafiles['entity_properties'] = preview_file
            print(preview_datafiles)

        elif self.submit == 'true':
            gcs_file = self.uploaded_files['entity']
            entity_df = pd.read_csv(gcs_file)
            entity_df = get_timestamp_columns(entity_df)
            if self.source == 'postgres':
                # insert_into_db('entity', entity_db_columns, entity_df, entity_df_columns)
                self.writer.write_data(table = 'entity', \
                                        df = entity_df)
            elif self.source == 'bigquery':
                # insert_into_bq(entity_df[entity_df_columns], 'entity')
                self.writer.write_data(table = 'entity', \
                                        df = entity_df)

            gcs_file = self.uploaded_files['entity_properties']
            entity_props_df1 = pd.read_csv(gcs_file)
            entity_props_df1 = get_timestamp_columns(entity_props_df1)
            if self.source == 'postgres':
                # insert_into_db('entity_properties', entity_properties_db_columns, entity_props_df1, entity_properties_df_columns)
                self.writer.write_data(table = 'entity_properties', \
                                        df = entity_props_df1)
            elif self.source == 'bigquery':
                # insert_into_bq(entity_props_df1[entity_properties_df_columns], 'entity_properties')
                self.writer.write_data(table = 'entity_properties', \
                                        df = entity_props_df1)

            # entity_df.to_csv(f'{file_path_prefix}/output/entity.csv', index= False)
            # entity_props_df1.to_csv(f'{file_path_prefix}/output/entity_properties.csv', index= False)