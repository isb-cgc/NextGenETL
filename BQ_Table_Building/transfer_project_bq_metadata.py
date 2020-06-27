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

from google.cloud import bigquery
from common_etl.support import bq_harness_with_result, confirm_google_vm

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
Drop all BQ datasets in shadow project
'''
def clean_shadow_project(shadow_client, shadow_project):

    dataset_ids = []
    for dataset in shadow_client.list_datasets():
        dataset_ids.append(dataset.dataset_id)

    for did in dataset_ids:
        shadow_client.delete_dataset(did, delete_contents=True, not_found_ok=True)

    return True

'''
----------------------------------------------------------------------------------------------
Copy over the dataset structure:
'''

def shadow_datasets(source_client, shadow_client, shadow_project, empty_datasets):

    dataset_list = source_client.list_datasets()
    for src_dataset in dataset_list:
        table_list = list(source_client.list_tables(src_dataset.dataset_id))
        # If it is already empty in the source, we do not delete it later when tables are deleted
        if len(table_list) == 0:
            empty_datasets.append(src_dataset.dataset_id)

        src_dataset_obj =  source_client.get_dataset(src_dataset.dataset_id)
        copy_did_suffix = src_dataset.dataset_id.split(".")[-1]
        shadow_dataset_id = "{}.{}".format(shadow_project, copy_did_suffix)

        shadow_dataset = bigquery.Dataset(shadow_dataset_id)

        shadow_dataset.location = src_dataset_obj.location
        shadow_dataset.description = src_dataset_obj.description
        if src_dataset_obj.labels is not None:
            shadow_dataset.labels = src_dataset_obj.labels.copy()

        shadow_client.create_dataset(shadow_dataset)

    return True

'''
----------------------------------------------------------------------------------------------
Create all empty shadow tables
'''

def create_all_shadow_tables(source_client, shadow_client, source_project, target_project,
                             do_batch, shadow_prefix, do_tables):

    dataset_list = source_client.list_datasets()

    for dataset in dataset_list:
        table_list = list(source_client.list_tables(dataset.dataset_id))
        for tbl in table_list:
            tbl_obj = source_client.get_table(tbl)
            use_row_count = tbl_obj.num_rows
            use_query = None

            #
            # If we have a view, then we need to extract the row count through a query:
            #

            if tbl_obj.view_query is not None:
                if do_tables:
                    continue
                view_id = '{}.{}.{}'.format(source_project, dataset.dataset_id, tbl.table_id)
                sql = 'SELECT COUNT(*) as count FROM `{}`'.format(view_id)
                results = bq_harness_with_result(sql, do_batch)
                for row in results:
                    use_row_count = row.count
                    break
                use_query = tbl_obj.view_query.replace(source_project, target_project)

            if do_tables or (use_query is not None):

                table_id = '{}.{}.{}'.format(target_project, dataset.dataset_id, tbl.table_id)
                print(table_id)

                #
                # Not supposed to submit a schema for a view!
                #

                if use_query is None:
                    # Make a completely new copy of the source schema. Do we have to? Probably not. Pananoid.
                    targ_schema = []
                    for sf in tbl_obj.schema:
                        name = sf.name
                        field_type = sf.field_type
                        mode = sf.mode
                        desc = sf.description
                        fields = tuple(sf.fields)
                        # no "copy constructor"?
                        targ_schema.append(bigquery.SchemaField(name, field_type, mode, desc, fields))
                    targ_table = bigquery.Table(table_id, schema=targ_schema)
                else:
                    targ_table = bigquery.Table(table_id)

                targ_table.friendlyName = tbl_obj.friendly_name
                targ_table.description = tbl_obj.description

                #
                # "Number of rows" in a shadow empty table is provided through a private tag label:
                #

                if tbl_obj.labels is not None:
                    targ_table.labels = tbl_obj.labels.copy()
                else:
                    targ_table.labels = {}

                num_row_tag = "{}_{}".format(shadow_prefix, "num_rows")
                targ_table.labels[num_row_tag] = use_row_count

                #
                # The way a table turns into a view is by setting the view_query property:
                #

                if use_query is not None:
                    targ_table.view_query = use_query

                shadow_client.create_table(targ_table)

    return True


'''
----------------------------------------------------------------------------------------------
Delete all shadow tables (but keep views)
'''

def delete_all_tables(shadow_client, target_project):

    dataset_list = shadow_client.list_datasets()

    for dataset in dataset_list:
        table_list = list(shadow_client.list_tables(dataset.dataset_id))
        for tbl in table_list:
            tbl_obj = shadow_client.get_table(tbl)
            if tbl_obj.view_query is None:
                table_id = '{}.{}.{}'.format(target_project, dataset.dataset_id, tbl.table_id)
                print("Deleting {}".format(table_id))
                shadow_client.delete_table(table_id)

    return True

'''
----------------------------------------------------------------------------------------------
Delete empty BQ datasets after tables deleted
'''
def delete_empty_datasets(shadow_client, empty_datasets):

    dataset_list = shadow_client.list_datasets()
    for chk_dataset in dataset_list:
        if chk_dataset.dataset_id in empty_datasets:
            continue
        table_list = list(shadow_client.list_tables(chk_dataset.dataset_id))
        if len(table_list) == 0:
            shadow_client.delete_dataset(chk_dataset.dataset_id, delete_contents=True, not_found_ok=True)

    return True

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

    if confirm_google_vm():
        print('This job needs to be run with personal credentials on your desktop, not a GCP VM [EXITING]')
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

    source_project = params['SOURCE_PROJECT']
    shadow_project = params['SHADOW_PROJECT']
    do_batch = params['BQ_AS_BATCH']
    skip_tables = params['SKIP_TABLES']
    shadow_prefix = params['PRIVATE_METADATA_PREFIX']
    source_client = bigquery.Client(project=source_project)
    shadow_client = bigquery.Client(project=shadow_project)

    if 'clean_shadow' in steps:
        success = clean_shadow_project(shadow_client, shadow_project)
        if not success:
            print("clean_target failed")
            return

    empty_datasets = []
    if 'shadow_datasets' in steps:
        success = shadow_datasets(source_client, shadow_client, shadow_project, empty_datasets)
        if not success:
            print("shadow_datasets failed")
            return

        print('job completed')

    #
    # Typically, shadow projects will contain views of tables. We advertise the views, the tables are
    # not public. We just want to advertise the views, but the views need the tables to exist before
    # Google will accept the view SQL! And the view SQL is what makes a table a view! So, we need
    # to: 1) create all tables, 2) create all views, 3) delete the tables if the config says to do so,
    # 4) delete empty datasets now that there are no tables.
    #

    if 'create_all_shadow_tables' in steps:
        # Create just tables:
        success = create_all_shadow_tables(source_client, shadow_client, source_project,
                                           shadow_project, do_batch, shadow_prefix, True)
        if not success:
            print("create_all_shadow_tables failed")
            return

    if 'create_all_shadow_views' in steps:
        # Create just views:
        success = create_all_shadow_tables(source_client, shadow_client, source_project,
                                           shadow_project, do_batch, shadow_prefix, False)
        if not success:
            print("create_all_shadow_views failed")
            return

    if 'delete_all_shadow_tables' in steps:
        if skip_tables:
            success = delete_all_tables(shadow_client, shadow_project)
            if not success:
                print("delete_all_shadow_tables failed")
                return

    if 'delete_empty_datasets' in steps:
        if skip_tables:
            success = delete_empty_datasets(shadow_client, empty_datasets)
            if not success:
                print("delete_empty_datasets failed")
                return

    print('job completed')


if __name__ == "__main__":
    main(sys.argv)