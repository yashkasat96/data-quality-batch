
import sys
import os
sys.path.append('/'.join(os.path.abspath(__file__).split('\\')[:-1]) + '/resources')
from resources.util import *
from resources.build_ruleset import BuildRuleset
from resources.build_entity import BuildEntity
from resources.build_rules import BuildRules
from resources.reader import Reader
import argparse


parser= argparse.ArgumentParser()
parser.add_argument("--submit", required=False, type=str,
   help="Is it preview? Allowed entries: true or false")
parser.add_argument("--file_path", required=False, type=str,
   help="path of the file uploaded")
parser.add_argument("--metadata_identifier", required=True, type=str,
   help="Allowed entries: entity or ruleset or rule")
parser.add_argument("--uploaded_files", required=False, type=str,
   help="json of uploaded files")
parser.add_argument("--export", required=False, type=str,
   help="true or false")
args = vars(parser.parse_args())

uploaded_files = {}
if args['uploaded_files'] != None:
   uploaded_files_str = args['uploaded_files'].replace('{', '').replace('}', '')
   print(uploaded_files_str)
   if ',' in uploaded_files_str:
      uploaded_files_list = uploaded_files_str.split(',')
      for i in uploaded_files_list:
         uploaded_files[i.split(':', 1)[0].strip()] = i.split(':', 1)[1].strip()
   else:
      uploaded_files[uploaded_files_str.split(':', 1)[0].strip()] = uploaded_files_str.split(':', 1)[1].strip()
   print(uploaded_files)

reader = Reader(source, args['metadata_identifier'])
# data_bytes = reader.get_file(file_path= args['file_path'], bucket_name= 'dq-bulk-upload')

if args['metadata_identifier'] == 'ruleset':
   ruleset_df = reader.convert_to_df(file_path= args['file_path'], bucket_name= 'dq-bulk-upload')
   print(ruleset_df)
   # ruleset_df = read_file(f'{file_path_prefix}/data_files/{file_name}', args['metadata_identifier'])
   ruleset_df = rename_columns(ruleset_df)
   build_ruleset = BuildRuleset(ruleset_df, source, args['submit'], uploaded_files)
   response = build_ruleset.build()
   print(response)

if args['metadata_identifier'] == 'entity':
   # entity_df = read_file(f'{file_path_prefix}/data_files/{file_name}', args['metadata_identifier'])
   entity_df = reader.convert_to_df(file_path= args['file_path'], bucket_name= 'dq-bulk-upload')
   entity_df = rename_columns(entity_df)
   build_entity = BuildEntity(entity_df, source, args['submit'], uploaded_files)
   build_entity.build()

if args['metadata_identifier'] == 'rule':
   # rule_df = read_file(f'{file_path_prefix}/data_files/{file_name}', uploaded_files)
   rule_df = reader.convert_to_df(file_path= args['file_path'], bucket_name= 'dq-bulk-upload')
   rule_df = rename_columns(rule_df)
   build_rules = BuildRules(source, args['submit'], uploaded_files)
   rule_df = build_rules.build(rule_df)

# if args['export'] == 'true':
#    pass