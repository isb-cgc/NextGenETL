"""

Copyright 2019-2020, Institute for Systems Biology

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

'''
Pull in existing radiology files to our new format

'''

import yaml
import sys
import io
from git import Repo
from json import loads as json_loads

from common_etl.support import generic_bq_harness, \
                               delete_table_bq_job, \
                               bq_table_exists, bq_table_is_empty, create_clean_target, \
                               generate_table_detail_files, \
                               update_schema_with_dict, install_labels_and_desc, publish_table

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
        return None, None, None, None, None, None, None

    return (yaml_dict['files_and_buckets_and_tables'], yaml_dict['steps'],
            yaml_dict['builds'], yaml_dict['build_tags'], yaml_dict['path_tags'],
            yaml_dict['programs'], yaml_dict['schema_tags'])

'''
----------------------------------------------------------------------------------------------
Pull radiology from our previous collection
'''

def import_radiology(r14_table, target_dataset, dest_table, do_batch):

    sql = import_radiology_sql(r14_table)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)

'''
----------------------------------------------------------------------------------------------
SQL for above:
'''

def import_radiology_sql(r14_table):
    return '''

        SELECT
            file_gdc_id,
            case_gdc_id,
            case_barcode,
            sample_gdc_id,
            sample_barcode,
            CAST(null AS STRING) as sample_type_name,
            project_short_name,
            disease_code as project_short_name_suffix,
            program_name,
            data_type,
            data_category,
            experimental_strategy,
            `type` as file_type,
            file_size,
            data_format,
            platform,
            file_name_key,
            index_file_id,
            index_file_name_key,
            index_file_size,
            access,
            acl
        FROM `{0}` WHERE data_type = "Radiology image"'''.format(r14_table)

'''
----------------------------------------------------------------------------------------------
Main Control Flow
Note that the actual steps run are configured in the YAML input! This allows you to e.g. skip previously run steps.
'''

def main(args):

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

    #
    # Schemas and table descriptions are maintained in the github repo. Only do this once:
    #

    if 'pull_table_info_from_git' in steps:
        print('pull_table_info_from_git')
        try:
            create_clean_target(params['SCHEMA_REPO_LOCAL'])
            repo = Repo.clone_from(params['SCHEMA_REPO_URL'], params['SCHEMA_REPO_LOCAL'])
            repo.git.checkout(params['SCHEMA_REPO_BRANCH'])
        except Exception as ex:
            print("pull_table_info_from_git failed: {}".format(str(ex)))
            return

    #
    # Pull the radiology entries
    #

    if 'pull_radiology' in steps:
        success = import_radiology(params['RADIOLOGY_SOURCE'], params['TARGET_DATASET'],
                                   params['RADIOLOGY_TABLE_NAME'], params['BQ_AS_BATCH'])

        if not success:
            print("pull_radiology job failed")
            return

        if bq_table_is_empty(params['TARGET_DATASET'], params['RADIOLOGY_TABLE_NAME']):
            delete_table_bq_job(params['TARGET_DATASET'], params['RADIOLOGY_TABLE_NAME'])
            print("{} pull_slide table result was empty: table deleted".format(params['RADIOLOGY_TABLE_NAME']))

    #
    # Stage the schema metadata from the repo copy:
    #

    if 'process_git_schemas' in steps:
        print('process_git_schema')
        # Where do we dump the schema git repository?
        schema_file = "{}/{}/{}".format(params['SCHEMA_REPO_LOCAL'], params['RAW_SCHEMA_DIR'],
                                        params['SCHEMA_FILE_NAME'])
        full_file_prefix = "{}/{}".format(params['PROX_DESC_PREFIX'], params['RADIOLOGY_TABLE_NAME'])
        # Write out the details
        success = generate_table_detail_files(schema_file, full_file_prefix)
        if not success:
            print("process_git_schemas failed")
            return False

    #
    # Update the per-field descriptions:
    #

    if 'update_field_descriptions' in steps:
        print('update_field_descriptions')

        if bq_table_exists(params['TARGET_DATASET'], params['RADIOLOGY_TABLE_NAME']):
            full_file_prefix = "{}/{}".format(params['PROX_DESC_PREFIX'], params['RADIOLOGY_TABLE_NAME'])
            schema_dict_loc = "{}_schema.json".format(full_file_prefix)
            schema_dict = {}
            with open(schema_dict_loc, mode='r') as schema_hold_dict:
                full_schema_list = json_loads(schema_hold_dict.read())
            for entry in full_schema_list:
                schema_dict[entry['name']] = {'description': entry['description']}

            success = update_schema_with_dict(params['TARGET_DATASET'], params['RADIOLOGY_TABLE_NAME'],
                                              schema_dict, project=params['WORKING_PROJECT'])
            if not success:
                print("update_field_descriptions failed")
                return

    #
    # Add description and labels to the target table:
    #

    if 'update_table_description' in steps:
        print('update_table_description')

        if bq_table_exists(params['TARGET_DATASET'], params['RADIOLOGY_TABLE_NAME']):
            full_file_prefix = "{}/{}".format(params['PROX_DESC_PREFIX'], params['RADIOLOGY_TABLE_NAME'])
            success = install_labels_and_desc(params['TARGET_DATASET'], params['RADIOLOGY_TABLE_NAME'], full_file_prefix)
            if not success:
                print("update_table_description failed")
                return

    #
    # publish table:
    #

    if 'publish' in steps:

        if bq_table_exists(params['TARGET_DATASET'], params['RADIOLOGY_TABLE_NAME']):
            source_table = '{}.{}.{}'.format(params['WORKING_PROJECT'], params['TARGET_DATASET'],
                                             params['RADIOLOGY_TABLE_NAME'])
            publication_dest = '{}.{}.{}'.format(params['PUBLICATION_PROJECT'], params['PUBLICATION_DATASET'],
                                                 params['PUBLICATION_TABLE'])
            success = publish_table(source_table, publication_dest)
            if not success:
                print("publish table failed")
                return

    print('job completed')


if __name__ == "__main__":
    main(sys.argv)



