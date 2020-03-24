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

    #
    # BQ does not like to be given paths that have "~". So make all local paths absolute:
    #

    home = expanduser("~")
    local_files_dir = "{}/{}".format(home, params['LOCAL_FILES_DIR'])
    one_big_tsv = "{}/{}".format(home, params['ONE_BIG_TSV'])
    manifest_file = "{}/{}".format(home, params['MANIFEST_FILE'])
    local_pull_list = "{}/{}".format(home, params['LOCAL_PULL_LIST'])
    file_traversal_list = "{}/{}".format(home, params['FILE_TRAVERSAL_LIST'])
    hold_schema_dict = "{}/{}".format(home, params['HOLD_SCHEMA_DICT'])
    hold_schema_list = "{}/{}".format(home, params['HOLD_SCHEMA_LIST'])

    if 'clear_target_directory' in steps:
        print('clear_target_directory')
        create_clean_target(local_files_dir)

    #
    # Use the filter set to build a manifest. Note that if a pull list is
    # provided, these steps can be omitted:
    #

    if 'build_manifest_from_filters' in steps:
        print('build_manifest_from_filters')
        max_files = params['MAX_FILES'] if 'MAX_FILES' in params else None

        manifest_success = get_the_bq_manifest(params['FILE_TABLE'], bq_filters, max_files,
                                               params['WORKING_PROJECT'], params['TARGET_DATASET'],
                                               params['BQ_MANIFEST_TABLE'], params['WORKING_BUCKET'],
                                               params['BUCKET_MANIFEST_TSV'], manifest_file,
                                               params['BQ_AS_BATCH'])
        if not manifest_success:
            print("Failure generating manifest")
            return

    #
    # We need to create a "pull list" of gs:// URLs to pull from GDC buckets. If you have already
    # created a pull list, just plunk it in 'LOCAL_PULL_LIST' and skip this step. If creating a pull
    # list, uses BQ as long as you have built the manifest using BQ (that route uses the BQ Manifest
    # table that was created).
    #

    if 'build_pull_list' in steps:
        print('build_pull_list')
        full_manifest = '{}.{}.{}'.format(params['WORKING_PROJECT'],
                                          params['TARGET_DATASET'],
                                          params['BQ_MANIFEST_TABLE'])
        success = build_pull_list_with_bq(full_manifest, params['INDEXD_BQ_TABLE'],
                                          params['WORKING_PROJECT'], params['TARGET_DATASET'],
                                          params['BQ_PULL_LIST_TABLE'],
                                          params['WORKING_BUCKET'],
                                          params['BUCKET_PULL_LIST'],
                                          local_pull_list, params['BQ_AS_BATCH'])

        if not success:
            print("Build pull list failed")
            return;

    #
    # Now hitting GDC cloud buckets. Get the files in the pull list:
    #

    if 'download_from_gdc' in steps:
        print('download_from_gdc')
        with open(local_pull_list, mode='r') as pull_list_file:
            pull_list = pull_list_file.read().splitlines()
        print("Preparing to download %s files from buckets\n" % len(pull_list))
        bp = BucketPuller(10)
        bp.pull_from_buckets(pull_list, local_files_dir)

    if 'build_file_list' in steps:
        print('build_file_list')
        all_files = build_file_list(local_files_dir)
        with open(file_traversal_list, mode='w') as traversal_list:
            for line in all_files:
                traversal_list.write("{}\n".format(line))
