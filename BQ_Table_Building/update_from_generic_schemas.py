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
Make sure the VM has BigQuery and Storage Read/Write permissions!

Extract GDC Metadata into Per-Project/Build File BQ Tables
This is still a work in progress (01/18/2020)

'''

import yaml
import sys
import io
from git import Repo
from json import loads as json_loads

from common_etl.support import generic_bq_harness, confirm_google_vm, \
                               bq_harness_with_result, delete_table_bq_job, \
                               bq_table_exists, bq_table_is_empty, create_clean_target, \
                               generate_table_detail_files, customize_labels_and_desc, \
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
        return None, None, None

    return (yaml_dict['files_and_buckets_and_tables'], yaml_dict['steps'], 
            yaml_dict['tables_to_patch'])


'''
----------------------------------------------------------------------------------------------
Main Control Flow
Note that the actual steps run are configured in the YAML input! This allows you to e.g. skip previously run steps.
'''

def main(args):

    if not confirm_google_vm():
        print('This job needs to run on a Google Cloud Compute Engine to avoid storage egress charges [EXITING]')
        return

    if len(args) != 2:
        print(" ")
        print(" Usage : {} <configuration_yaml>".format(args[0]))
        return

    print('job started')

    #
    # Get the YAML config loaded:
    #

    with open(args[1], mode='r') as yaml_file:
        params, steps, tables_to_patch = load_config(yaml_file.read())

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

    for mydict in tables_to_patch:

        full_table, table_dict = next(iter(mydict.items()))

        #
        # Extract the project, dataset, and table name:
        #

        split_table = full_table.split('.')
        target_program = split_table[0]
        target_dataset = split_table[1]
        target_table = split_table[2]

        if 'process_git_schemas' in steps:
            print('process_git_schema')
            # Where do we dump the schema git repository?
            schema_file_name = table_dict["generic_schema_file"]
            schema_file = "{}/{}/{}".format(params['SCHEMA_REPO_LOCAL'], params['RAW_SCHEMA_DIR'],
                                            schema_file_name)
            full_file_prefix = "{}/{}".format(params['PROX_DESC_PREFIX'], full_table)
            # Write out the details
            success = generate_table_detail_files(schema_file, full_file_prefix)
            if not success:
                print("process_git_schemas failed")
                return False

        # Customize generic schema to this data program:

        if 'replace_schema_tags' in steps:
            print('replace_schema_tags')
            full_file_prefix = "{}/{}".format(params['PROX_DESC_PREFIX'], full_table)
            # Write out the details
            success = customize_labels_and_desc(full_file_prefix, table_dict["schema_tags"])
            if not success:
                print("replace_schema_tags failed")
                return False

        #
        # Update the per-field descriptions:
        #

        if 'install_field_descriptions' in steps:
            print('install_field_descriptions: {}'.format(full_table))
            full_file_prefix = "{}/{}".format(params['PROX_DESC_PREFIX'], full_table)
            schema_dict_loc = "{}_schema.json".format(full_file_prefix)
            schema_dict = {}
            with open(schema_dict_loc, mode='r') as schema_hold_dict:
                full_schema_list = json_loads(schema_hold_dict.read())
            for entry in full_schema_list:
                schema_dict[entry['name']] = {'description': entry['description']}
            success = update_schema_with_dict(params['TARGET_DATASET'], full_table, schema_dict, project=params['WORKING_PROJECT'])
            if not success:
                print("install_field_descriptions failed")
                return False

    #
    # Add description and labels to the target table:
    #

    if 'install_table_description' in steps:
        print('install_table_description: {}'.format(full_table))
        full_file_prefix = "{}/{}".format(params['PROX_DESC_PREFIX'], full_table)
        success = install_labels_and_desc(params['TARGET_DATASET'], full_table, full_file_prefix,
                                          project=params['WORKING_PROJECT'])
        if not success:
            print("install_table_description failed")
            return False

    print('job completed')

if __name__ == "__main__":
    main(sys.argv)

