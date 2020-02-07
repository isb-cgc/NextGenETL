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
from common_etl.support import create_clean_target, generate_dataset_desc_file, install_dataset_desc

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
    # Dataset descriptions are maintained in the github repo:
    #

    if 'pull_dataset_info_from_git' in steps:
        print('pull_dataset_info_from_git')
        try:
            create_clean_target(params['SCHEMA_REPO_LOCAL'])
            repo = Repo.clone_from(params['SCHEMA_REPO_URL'], params['SCHEMA_REPO_LOCAL'])
            repo.git.checkout(params['SCHEMA_REPO_BRANCH'])
        except Exception as ex:
            print("pull_dataset_info_from_git failed: {}".format(str(ex)))
            return

    for mydict in params['FIX_LIST']:

        dataset, repo_file = next(iter(mydict.items()))

        if 'process_git_schemas' in steps:
            print('process_git_schemas: {}'.format(dataset))
            # Where do we dump the schema git repository?
            schema_file = "{}/{}/{}".format(params['SCHEMA_REPO_LOCAL'], params['RAW_SCHEMA_DIR'], repo_file)
            full_file_prefix = "{}/{}".format(params['PROX_DESC_PREFIX'], dataset)
            # Write out the details
            success = generate_dataset_desc_file(schema_file, full_file_prefix)
            if not success:
                print("process_git_schemas failed")
                return

        #
        # Add description and labels to the target table:
        #

        if 'update_dataset_descriptions' in steps:
            print('update_dataset_descriptions: {}'.format(dataset))
            full_file_prefix = "{}/{}".format(params['PROX_DESC_PREFIX'], dataset)
            full_dataset_id = "{}.{}".format(params['TARGET_PROJECT'], dataset)
            success = install_dataset_desc(full_dataset_id, full_file_prefix)
            if not success:
                print("update_dataset_descriptions failed")
                return

        print('job completed')


if __name__ == "__main__":
    main(sys.argv)
