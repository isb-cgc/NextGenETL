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
Main Control Flow
Note that the actual steps run are configured in the YAML input!
This allows you to e.g. skip previously run steps.
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


    if 'update_field_descriptions' in steps:
        schema_dict_loc = "{}_schema.json".format(params['PROX_DESC_PREFIX'])
        schema_dict = {}
        with open(schema_dict_loc, mode='r') as schema_hold_dict:
            full_schema_list = json_loads(schema_hold_dict.read())
        for entry in full_schema_list:
            schema_dict[entry['name']] = {'description': entry['description']}

        success = update_schema_with_dict(params['TARGET_DATASET'], params['TARGET_TABLE'], schema_dict)
        if not success:
            print("update_field_descriptions failed")
            return

    #
    # Add descriptions to the combined table:
    #

    if 'update_table_description' in steps:
        success = install_labels_and_desc(params['TARGET_DATASET'],
                                          params['TARGET_TABLE'], params['PROX_DESC_PREFIX'])
        if not success:
            print("update_table_description failed")
            return

    print('job completed')


if __name__ == "__main__":
    main(sys.argv)
