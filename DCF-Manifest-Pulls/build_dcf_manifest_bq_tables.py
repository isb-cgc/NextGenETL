"""

Copyright 2019, Institute for Systems Biology

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""

import sys
import yaml
import io
from json import loads as json_loads
from common_etl.support import generic_bq_harness, csv_to_bq, update_description, update_schema_with_dict

'''
----------------------------------------------------------------------------------------------
The configuration reader. Parses the YAML configuration into dictionaries
'''
def load_config(yaml_config):
    yaml_dict = None
    config_stream = io.StringIO(yaml_config)
    try:
        yaml_dict = yaml.load(config_stream, Loader=yaml.FullLoader)
    except yaml.YAMLError as ex:
        print(ex)

    if yaml_dict is None:
        return None, None

    return yaml_dict['files_and_buckets_and_tables'], yaml_dict['steps']

'''
----------------------------------------------------------------------------------------------
Build the file mapping table
'''
def build_file_map(mani_table, target_dataset, dest_table, do_batch):
    sql = file_map_sql(mani_table)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)

'''
----------------------------------------------------------------------------------------------
SQL for the file mapping table
'''
def file_map_sql(manifest_table):
    return '''
      SELECT id as file_uuid,
             acl,
             gs_url as gcs_path
      FROM `{0}`
    '''.format(manifest_table)

'''
----------------------------------------------------------------------------------------------
Schema list to dict:
'''
def schema_list_to_dict(schema_list_file):
    with open(schema_list_file, mode='r') as schema_dict_list:
        schema_list = json_loads(schema_dict_list.read())

    full_schema_dict = {}
    for elem in schema_list:
        full_schema_dict[elem['name']] = elem

    return full_schema_dict

'''
----------------------------------------------------------------------------------------------
Main Control Flow
Note that the actual steps run are configured in the YAML input!
This allows you to e.g. skip previously run steps.
'''

def main(args):

    #if not confirm_google_vm():
    #    print('This job needs to run on a Google Cloud Compute Engine to avoid storage egress charges [EXITING]')
    #    return

    if len(args) != 2:
        print(" ")
        print(" Usage : {} <configuration_yaml>".format(args[0]))
        return

    print('job started')

    #
    # Get the YAML config loaded:
    #

    with open(args[1], mode='r') as yaml_file:
        params, steps = load_config(yaml_file.read())

    if params is None:
        print("Bad YAML load")
        return

    # Schema that describes DCF manifests:

    MANIFEST_SCHEMA_LIST = "SchemaFiles/dcf_manifest_schema.json"

    # Schema that describes our final map table:

    FILE_MAP_SCHEMA_LIST = "SchemaFiles/dcf_file_map_schema.json"

    #
    # Decide if we are doing active, legacy, or both manifests:
    #

    mani_dict = {}
    map_dict = {}
    if params['DO_ACTIVE']:
        mani_dict['ACTIVE_MANIFEST_TSV'] = 'ACTIVE_MANIFEST_BQ'
        map_dict['ACTIVE_MANIFEST_BQ'] = 'ACTIVE_FILE_MAP_BQ'

    if params['DO_LEGACY']:
        mani_dict['LEGACY_MANIFEST_TSV'] = 'LEGACY_MANIFEST_BQ'
        map_dict['LEGACY_MANIFEST_BQ'] = 'LEGACY_FILE_MAP_BQ'

    #
    # Create a manifest BQ table from a TSV:
    #

    if 'create_bq_manifest_from_tsv' in steps:
        with open(MANIFEST_SCHEMA_LIST, mode='r') as schema_hold_dict:
            typed_schema = json_loads(schema_hold_dict.read())

        for manikey in list(mani_dict.keys()):
            bucket_src_url = 'gs://{}/{}'.format(params['WORKING_BUCKET'], params[manikey])
            success = csv_to_bq(typed_schema, bucket_src_url, params['TARGET_DATASET'],
                                params[mani_dict[manikey]], params['BQ_AS_BATCH'])
            if not success:
                print("create_bq_manifest_from_tsv failed")
                return

    #
    # Create the file map tables:
    #

    if 'create_file_map_bq' in steps:

        for mapkey in list(map_dict.keys()):
            mani_table = '{}.{}.{}'.format(params['WORKING_PROJECT'], params['TARGET_DATASET'], params[mapkey])
            success = build_file_map(mani_table, params['TARGET_DATASET'], params[map_dict[mapkey]], params['BQ_AS_BATCH'])
            if not success:
                print("create_file_map_bq failed")
                return

            # Install a schema in the new table:
            schema_dict = schema_list_to_dict(FILE_MAP_SCHEMA_LIST)
            success = update_schema_with_dict(params['TARGET_DATASET'], params[map_dict[mapkey]], schema_dict)
            if not success:
                print("install file map schema failed")
                return

    #
    # Add descriptions
    #

    if 'add_table_descriptions' in steps:
        for mapkey in list(map_dict.keys()):
            success = update_description(params['TARGET_DATASET'], params[mapkey],
                                         params['DCF_MANIFEST_TABLE_DESCRIPTION'])
            if not success:
                print("install manifest description failed")
                return

            success = update_description(params['TARGET_DATASET'], params[map_dict[mapkey]],
                                         params['FILE_MAP_TABLE_DESCRIPTION'])
            if not success:
                print("install file map description failed")
                return

    print('job completed')


if __name__ == "__main__":
    main(sys.argv)
