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
from git import Repo
from json import loads as json_loads
from common_etl.support import generic_bq_harness, csv_to_bq, install_labels_and_desc, \
     update_schema_with_dict, create_clean_target, generate_table_detail_files, \
     publish_table, update_schema

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
Build a combined table to publish
'''
def build_combined_table(legacy_paths_table, active_paths_table, target_dataset, dest_table, do_batch):
    sql = combined_table_sql(legacy_paths_table, active_paths_table)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)

'''
----------------------------------------------------------------------------------------------
SQL for the combined table
'''
def combined_table_sql(legacy_paths_table, active_paths_table):
    return '''
      SELECT
        file_uuid as file_gdc_id,
        gcs_path as file_gdc_url
      FROM `{}`
        UNION DISTINCT
      SELECT
        file_uuid as file_gdc_id,
        gcs_path as file_gdc_url
      FROM `{}`
    '''.format(legacy_paths_table, active_paths_table)

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

    # Manifest files need to be copied over to the bucket gdc_manifests when DCF publishes them to their bucket:
    #


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

    # Schema that describes the file map table:

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
    # Schemas and table descriptions are maintained in the github repo:
    #

    if 'pull_table_info_from_git' in steps:
        try:
            create_clean_target(params['SCHEMA_REPO_LOCAL'])
            Repo.clone_from(params['SCHEMA_REPO_URL'], params['SCHEMA_REPO_LOCAL'])
        except Exception as ex:
            print("pull_table_info_from_git failed: {}".format(str(ex)))
            return

    if 'process_git_schemas' in steps:
        # Where do we dump the schema git repository?
        schema_file = "{}/{}".format(params['SCHEMA_REPO_LOCAL'], params['RAW_SCHEMA_JSON'])

        # Write out the details
        success = generate_table_detail_files(schema_file, params['PROX_DESC_PREFIX'])
        if not success:
            print("process_git_schemas failed")
            return


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

    if 'create_combined_table' in steps:
        legacy_paths_table = '{}.{}.{}'.format(params['WORKING_PROJECT'], params['TARGET_DATASET'],
                                               params['LEGACY_FILE_MAP_BQ'])
        active_paths_table = '{}.{}.{}'.format(params['WORKING_PROJECT'], params['TARGET_DATASET'],
                                               params['ACTIVE_FILE_MAP_BQ'])
        success = build_combined_table(legacy_paths_table, active_paths_table, params['TARGET_DATASET'],
                                       params['COMBINED_TABLE'], params['BQ_AS_BATCH'])
        if not success:
            print("create combined table failed")
            return

    if 'add_combined_desc' in steps:
        schema_dict_loc = "{}_schema.json".format(params['PROX_DESC_PREFIX'])
        schema_dict = {}
        with open(schema_dict_loc, mode='r') as schema_hold_dict:
            full_schema_list = json_loads(schema_hold_dict.read())
        for entry in full_schema_list:
            dict_for_entry = {'description': entry['description']}
            schema_dict[entry['name']] = dict_for_entry
        print(schema_dict)

        success = update_schema_with_dict(params['TARGET_DATASET'], params['COMBINED_TABLE'], schema_dict)
        if not success:
            print("add_combined_desc failed")
            return

    #
    # Add descriptions to the combined table:
    #

    if 'add_table_description' in steps:

        success = install_labels_and_desc(params['TARGET_DATASET'],
                                          params['COMBINED_TABLE'], params['PROX_DESC_PREFIX'])

        if not success:
            print("install file map description failed")
            return

    if 'publish' in steps:

        source_table = '{}.{}.{}'.format(params['WORKING_PROJECT'], params['TARGET_DATASET'],
                                         params['COMBINED_TABLE'])
        publication_dest = '{}.{}.{}'.format(params['PUBLICATION_PROJECT'], params['PUBLICATION_DATASET'],
                                             params['PUBLICATION_TABLE'])

        success = publish_table(source_table, publication_dest)

        if not success:
            print("install file map description failed")
            return

    print('job completed')


if __name__ == "__main__":
    main(sys.argv)
