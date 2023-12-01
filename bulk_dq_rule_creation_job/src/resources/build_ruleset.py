from util import *
from constants import *
from google.cloud import storage
from bulk_dq_rule_creation_job.src.resources.writer import Writer


class BuildRuleset:
    def __init__(self, df, source, submit, uploaded_files):
        self.df = df
        self.source = source
        self.submit = submit
        self.uploaded_files = uploaded_files
        
        self.writer = Writer(source)
        
    def build(self):
        if self.submit == 'false':
            new_ids = get_new_ids(self.df)
            self.df['ruleset_id'] = new_ids
            ruleset_df = add_who_columns(self.df)
            
            if ruleset_df.shape[0] > 0:
                # ruleset_df.to_csv('dq-bulk-upload/output/ruleset.csv', index= False)
                # client = storage.Client()
                # bucket = client.get_bucket('dq-bulk-upload')
                # bucket.blob(file_name).upload_from_string(ruleset_df.to_csv(), 'text/csv')
                # file_suffix = datetime.today().strftime('%Y-%m-%d %H:%M:%S').\
                #                         replace('-', '').\
                #                         replace(' ', '').\
                #                         replace(':', '')
                # file_name = f'output/ruleset_{file_suffix}.csv'

                preview_file= self.writer.write_data(table = 'ruleset', \
                                              bucket_name = 'dq-bulk-upload', \
                                              df = ruleset_df)
                preview_datafiles= {}
                preview_datafiles['ruleset'] = preview_file
                
                
        elif self.submit == 'true':
            uploaded_file = self.uploaded_files['ruleset']
            ruleset_df = pd.read_csv(uploaded_file)
            ruleset_df = get_timestamp_columns(ruleset_df)
            # if self.source == 'postgres':
            #     insert_into_db('ruleset', ruleset_db_columns, ruleset_df, ruleset_df_columns)
            # elif self.source == 'bigquery':
            #     insert_into_bq(ruleset_df[ruleset_df_columns], 'ruleset')
            self.writer.write_data(table = 'ruleset', \
                                    df = ruleset_df)