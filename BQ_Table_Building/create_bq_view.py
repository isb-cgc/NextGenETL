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
import yaml
import io
from git import Repo
from json import loads as json_loads
from google.cloud import bigquery
from common_etl.support import create_clean_target, confirm_google_vm, install_labels_and_desc, \
                               update_schema_with_dict, generate_table_detail_files, publish_table, \
                               compare_two_tables, delete_table_bq_job
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
Create a view in place of the old table
'''

def create_view(old_table_name, new_table, project_old, dataset_old):
    client = bigquery.Client()
    shared_dataset_ref = bigquery.DatasetReference(project_old, dataset_old)
    sql = create_view_sql(new_table)
    view_ref = shared_dataset_ref.table(old_table_name)
    view = bigquery.Table(view_ref)
    view.view_query = sql
    view = client.create_table(view)  # API request
    return view

'''
----------------------------------------------------------------------------------------------
SQL for the create_view function
'''

def create_view_sql(new_table):
    return '''
    SELECT *
    FROM `{}`
    '''.format(new_table)

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
    # Create table variables
    #

    table_old = '{}.{}.{}'.format(params['PROJECT_OLD'], params['DATASET_OLD'], params['TABLE_OLD'])
    table_new = '{}.{}.{}'.format(params['PROJECT_NEW'], params['DATASET_NEW'], params['TABLE_NEW'])
    table_temp = '{}.{}.{}'.format(params['PROJECT_TEMP'], params['DATASET_TEMP'], params['TABLE_OLD'])

    if 'compare_tables' in steps:
        print('Compare {} to {}'.format(table_old, table_new))

        success = compare_two_tables(table_old, table_new, params['BQ_AS_BATCH'])

        num_rows = success.total_rows

        if num_rows == 0:
            print('the tables are the same')
        else:
            print('the tables are NOT the same and differ by {} rows'.format(num_rows))

        if not success:
            print('compare_tables failed')
            return

    if 'copy_old_to_temp' in steps:
        print('Move old table to temp location')
        success = publish_table(table_old, table_temp)

        if not success:
            print('create temp table failed')
            return

    if 'remove_old_table_and_create_view' in steps:

        success = compare_two_tables(table_old, table_temp, params['BQ_AS_BATCH'])

        num_rows = success.total_rows

        if num_rows == 0:
            print('Deleting old table: {}'.format(table_old))
            deleted = delete_table_bq_job(params['DATASET_OLD'], params['TABLE_OLD'], params['PROJECT_OLD'])

            if not deleted:
                print('delete table failed')
                return

            else:
                print('create view')

                view_created = create_view(params['TABLE_OLD'], table_new, params['PROJECT_OLD'], params['DATASET_OLD'])

                if not view_created:
                    print('create view failed')
                    return
        else:
            print('the temp table is not the same as the old table and differs by {} rows'.format(num_rows))

        if not success:
            print('remove_old_table_and_create_view failed')
            return

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
        print('process_git_schemas: {}'.format(table_old))
        # Where do we dump the schema git repository?
        schema_file = "{}/{}/{}".format(params['SCHEMA_REPO_LOCAL'], params['RAW_SCHEMA_DIR'], params['SCHEMA_FILE_NAME'])
        full_file_prefix = "{}/{}".format(params['PROX_DESC_PREFIX'], table_old)
        # Write out the details
        success = generate_table_detail_files(schema_file, full_file_prefix)
        if not success:
            print("process_git_schemas failed")
            return
    #
    # Update the per-field descriptions:
    #

    if 'update_field_descriptions' in steps:
        print('update_field_descriptions: {}'.format(table_old))
        full_file_prefix = "{}/{}".format(params['PROX_DESC_PREFIX'], table_old)
        schema_dict_loc = "{}_schema.json".format(full_file_prefix)
        schema_dict = {}
        with open(schema_dict_loc, mode='r') as schema_hold_dict:
            full_schema_list = json_loads(schema_hold_dict.read())
        for entry in full_schema_list:
            schema_dict[entry['name']] = {'description': entry['description']}
        print(table_old)
        success = update_schema_with_dict(params['DATASET_OLD'], params['TABLE_OLD'], schema_dict,
                                          project=params['PROJECT_OLD'])
        if not success:
            print("update_field_descriptions failed")
            return

    #
    # Add description and labels to the target table:
    #

    if 'update_table_description' in steps:
        print('update_table_description: {}'.format(table_old))
        full_file_prefix = "{}/{}".format(params['PROX_DESC_PREFIX'], table_old)
        success = install_labels_and_desc(params['DATASET_OLD'], params['TABLE_OLD'], full_file_prefix,
                                          project=params['PROJECT_OLD'])
        if not success:
            print("update_table_description failed")
            return

    if 'remove_temp_table' in steps:
        success = compare_two_tables(table_old, table_temp, params['BQ_AS_BATCH'])

        num_rows = success.total_rows

        if num_rows == 0:
            print('removed temp table')
            delete_table_bq_job(params['DATASET_TEMP'], params['TABLE_OLD'], params['PROJECT_TEMP'])

        else:
            print('the temp table is not the view and differs by {} rows'.format(num_rows))

        if not success:
            print('remove_temp_table')
            return

    print('job completed')

if __name__ == "__main__":
    main(sys.argv)