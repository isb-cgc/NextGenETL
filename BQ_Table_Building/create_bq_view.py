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
import shutil
import os
import yaml
import io
from git import Repo
import zipfile
import gzip
from os.path import expanduser
from json import loads as json_loads
from createSchemaP3 import build_schema
from datetime import date
import re
from common_etl.support import create_clean_target, generic_bq_harness, upload_to_bucket, \
                               csv_to_bq_write_depo, delete_table_bq_job, confirm_google_vm, \
                               build_file_list, get_the_bq_manifest, BucketPuller, build_pull_list_with_bq, \
                               build_combined_schema, generic_bq_harness_write_depo, \
                               install_labels_and_desc, update_schema_with_dict, generate_table_detail_files, publish_table, \
                               bq_harness_with_result
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
Is the table that is replacing the view exactly the same?
'''

def compare_two_tables(old_table, new_table, do_batch):
    sql = compare_two_tables_sql(old_table, new_table)
    return bq_harness_with_result(sql, do_batch)

'''
----------------------------------------------------------------------------------------------
SQL for the compare_two_tables function
'''

def compare_two_tables_sql(old_table, new_table):
    return '''
        (
            SELECT * FROM `{0}`
            EXCEPT DISTINCT
            SELECT * from `{1}`
        )
        UNION ALL
        (
            SELECT * FROM `{1}`
            EXCEPT DISTINCT
            SELECT * from `{0}`
        )
    '''.format(old_table, new_table)

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
        params, steps = load_config(yaml_file.read())

    #
    # BQ does not like to be given paths that have "~". So make all local paths absolute:
    #

    home = expanduser("~")
    #local_files_dir = "{}/{}".format(home, params['LOCAL_FILES_DIR'])

    #
    # Create table variables
    #

    table_old = '{}.{}.{}'.format(params['PROJECT_OLD'], params['DATASET_OLD'], params['TABLE_OLD'])
    table_new = '{}.{}.{}'.format(params['PROJECT_NEW'], params['DATASET_NEW'], params['TABLE_NEW'])

    if 'compare_tables' in steps:
        print('Compare {} to {}'.format(table_old, table_new))

        success = compare_two_tables(table_old, table_new, params['BQ_AS_BATCH'])
        if success.total_rows == 0:
            print('the tables are the same')
        else:
            print('the tables are NOT the same')

        if not success:
            print('compare_tables failed')
            return

    if 'move_old_to_temp' in steps:
        print('Move old table to temp location')

        print('Deleting old table: {}').format(table_old)

    if 'remove_old_table' in steps:
        print('compare and remove old table')

    if 'create_view' in steps:

        print('job completed')

if __name__ == "__main__":
    main(sys.argv)