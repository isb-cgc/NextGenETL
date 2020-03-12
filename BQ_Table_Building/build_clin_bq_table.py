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

'''
Make sure the VM has BigQuery and Storage Read/Write permissions!

'''

import sys
import os
import yaml
import io
# from json import loads as json_loads
from os.path import expanduser

# from createSchemaP3 import build_schema
# from common_etl.support import create_clean_target, build_file_list, generic_bq_harness, confirm_google_vm, \
#                               upload_to_bucket, csv_to_bq, concat_all_files, delete_table_bq_job, \
#                               build_pull_list_with_bq, update_schema, \
#                               update_description, build_combined_schema, get_the_bq_manifest, BucketPuller

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

    return yaml_dict['files_and_buckets_and_tables'], yaml_dict['bq_filters'], yaml_dict['steps']


'''
----------------------------------------------------------------------------------------------
Main Control Flow
Note that the actual steps run are configured in the YAML input! This allows you
to e.g. skip previously run steps.
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
        params, bq_filters, steps = load_config(yaml_file.read())
