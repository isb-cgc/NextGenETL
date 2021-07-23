"""

Copyright 2020, Institute for Systems Biology

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
import re
from os.path import expanduser
import yaml
import io
from git import Repo
from json import loads as json_loads
from createSchemaP3 import build_schema

from common_etl.support import confirm_google_vm, create_clean_target, \
                               build_file_list, upload_to_bucket, csv_to_bq, \
                               BucketPuller, build_combined_schema, \
                               install_labels_and_desc, update_schema_with_dict, \
                               generate_table_detail_files, publish_table, pull_from_buckets


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

    return yaml_dict['files_and_buckets_and_tables'], yaml_dict['no_data_values'], yaml_dict['steps']


'''
----------------------------------------------------------------------------------------------
Fix null values
'''
def fix_null_values(orig_file, fixed_file, na_values):
    print("processing {}".format(fixed_file))
    with open(fixed_file, 'w') as outfile:
        with open(orig_file, 'r') as readfile:
            for line in readfile:
                split_line = line.rstrip('\n').split("\t")
                write_line = []
                for i in range(len(split_line)):
                    write_line.append("" if split_line[i] in na_values else split_line[i])
                outfile.write('\t'.join(write_line))
                outfile.write('\n')
    return


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
        params, na_values, steps = load_config(yaml_file.read())

    #
    # BQ does not like to be given paths that have "~". So make all local paths absolute:
    #

    home = expanduser("~")
    local_files_dir = "{}/{}".format(home, params['LOCAL_FILES_DIR'])
    fixed_tsv = "{}/{}".format(home, params['FIXED_TSV'])
    local_pull_list = "{}/{}".format(home, params['LOCAL_PULL_LIST'])
    hold_schema_dict = "{}/{}".format(home, params['HOLD_SCHEMA_DICT'])
    hold_schema_list = "{}/{}".format(home, params['HOLD_SCHEMA_LIST'])

    print('local_files_dir: ', local_files_dir)
    print('fixed_tsv: ', fixed_tsv)
    print('local_pull_list: ', local_pull_list)
    print('hold_schema_dict: ', hold_schema_dict)
    print('hold_schema_list: ', hold_schema_list)

    if 'clear_target_directory' in steps:
        print('clear_target_directory')
        create_clean_target(local_files_dir)

    #
    # Download original TSV files in the local_pull_list from bucket:
    #

    if 'download_raw_data' in steps:
        print('download_raw_data')
        pull_from_buckets(
            ['gs://{}/{}/targetome_pull_list.tsv'.format(
                params['WORKING_BUCKET'], params['WORKING_BUCKET_DIR']
            )],
            os.path.dirname(local_files_dir)
        )
        with open(local_pull_list, mode='r') as pull_list_file:
            pull_list = pull_list_file.read().splitlines()
        print("Preparing to download %s files from buckets\n" % len(pull_list))
        bp = BucketPuller(10)
        bp.pull_from_buckets(pull_list, local_files_dir)

    file_base_names = {}
    if 'build_file_list' in steps:
        print('build_file_list')
        all_files = build_file_list(local_files_dir)
        # pre-calculate file base names
        for f in all_files:
            file_base_names[f] = os.path.splitext(os.path.basename(f))[0]

    if 'fix_null_values' in steps:
        print('fix_null_values')
        for f in all_files:
            fix_null_values(f, fixed_tsv.format(file_base_names[f]), set(na_values))

    #
    # Schemas and table descriptions are maintained in the github repo:
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

    if 'process_git_schemas' in steps:
        print('process_git_schemas')
        for f in all_files:
            # Where do we dump the schema git repository?
            schema_file = "{}/{}/{}.json".format(
                params['SCHEMA_REPO_LOCAL'], params['RAW_SCHEMA_DIR'], file_base_names[f]
            )
            full_file_prefix = "{}/{}".format(params['PROX_DESC_PREFIX'], file_base_names[f])
            # Write out the details
            success = generate_table_detail_files(schema_file, full_file_prefix)
            if not success:
                print("process_git_schemas failed")
                return

    if 'analyze_the_schema' in steps:
        print('analyze_the_schema')
        for f in all_files:
            typing_tups = build_schema(
                fixed_tsv.format(file_base_names[f]), params['SCHEMA_SAMPLE_SKIPS']
            )
            hold_schema_dict_for_file = hold_schema_dict.format(file_base_names[f])
            hold_schema_list_for_file = hold_schema_list.format(file_base_names[f])
            build_combined_schema(
                None, None, typing_tups, hold_schema_list_for_file, hold_schema_dict_for_file
            )

    bucket_target_blob = '{}/{}'.format(params['WORKING_BUCKET_DIR'], params['BUCKET_TSV'])

    if 'upload_to_bucket' in steps:
        print('upload_to_bucket')
        for f in all_files:
            upload_to_bucket(
                params['WORKING_BUCKET'],
                bucket_target_blob.format(file_base_names[f]),
                fixed_tsv.format(file_base_names[f])
            )

    if 'create_bq_from_tsv' in steps:
        print('create_bq_from_tsv')
        for f in all_files:
            bucket_src_url = 'gs://{}/{}'.format(
                params['WORKING_BUCKET'], bucket_target_blob.format(file_base_names[f])
            )
            schema_file = "{}/{}/{}.json".format(
                params['SCHEMA_REPO_LOCAL'], params['RAW_SCHEMA_DIR'], file_base_names[f]
            )
            hold_schema_list_for_file = hold_schema_list.format(file_base_names[f])
            with open(hold_schema_list_for_file, mode='r') as schema_fh:
                schema = json_loads(schema_fh.read())
            print('schema: ', hold_schema_list_for_file)
            print('table: ', file_base_names[f])
            csv_to_bq(
                schema,
                bucket_src_url,
                params['TARGET_DATASET'],
                file_base_names[f],
                params['BQ_AS_BATCH']
            )

    #
    # Update the per-field descriptions:
    #

    if 'update_field_descriptions' in steps:
        print('update_field_descriptions')
        for f in all_files:
            full_file_prefix = "{}/{}".format(params['PROX_DESC_PREFIX'], file_base_names[f])
            schema_dict_loc = "{}_schema.json".format(full_file_prefix)
            schema_dict = {}
            with open(schema_dict_loc, mode='r') as schema_hold_dict:
                full_schema_list = json_loads(schema_hold_dict.read())
            for entry in full_schema_list:
                schema_dict[entry['name']] = {'description': entry['description']}

            success = update_schema_with_dict(
                params['TARGET_DATASET'], file_base_names[f], schema_dict
            )
            if not success:
                print("update_field_descriptions failed")
                return

    #
    # Add description and labels to the target table:
    #

    if 'update_table_description' in steps:
        print('update_table_description')
        for f in all_files:
            full_file_prefix = "{}/{}".format(params['PROX_DESC_PREFIX'], file_base_names[f])
            success = install_labels_and_desc(
                params['TARGET_DATASET'], file_base_names[f], full_file_prefix
            )
            if not success:
                print("update_table_description failed")
                return

    #
    # publish table:
    #

    if 'publish' in steps:
        print('publish')
        for f in all_files:
            source_table = '{}.{}.{}'.format(
                params['WORKING_PROJECT'], params['TARGET_DATASET'], file_base_names[f]
            )
            publication_dest = '{}.{}.{}'.format(
                params['PUBLICATION_PROJECT'], params['PUBLICATION_DATASET'], file_base_names[f]
            )
            success = publish_table(source_table, publication_dest)

            if not success:
                print("publish table {} failed".format(file_base_names[f]))
                return

    print('job completed')

if __name__ == "__main__":
    main(sys.argv)
