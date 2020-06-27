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

from google.cloud import bigquery
from common_etl.support import create_clean_target, generate_dataset_desc_file, create_bq_dataset

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

def shadow_datasets(source_client, shadow_client, shadow_project):

    dataset_list = source_client.list_datasets()
    for src_dataset in dataset_list:
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

def create_all_shadow_tables(source_client, shadow_client, target_project):

    dataset_list = source_client.list_datasets()

    for dataset in dataset_list:
        table_list = list(source_client.list_tables(dataset.dataset_id))
        for tbl in table_list:
            print(str(tbl))
            tbl_obj = source_client.get_table(tbl)

            if tbl_obj.view_query is not None:
                print("WE HAVE A VIEW")
                print(tbl_obj.view_query)


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

            table_id = '{}.{}.{}'.format(target_project, dataset.dataset_id, tbl.table_id)
            print(table_id)

            targ_table = bigquery.Table(table_id, schema=targ_schema)
            targ_table.friendlyName = tbl_obj.friendly_name
            targ_table.description = tbl_obj.description
            if tbl_obj.labels is not None:
                targ_table.labels = tbl_obj.labels.copy()

            targ_table.labels['isb_cgc_shadow_metadata_row_num'] = tbl_obj.row_num

            #sql_template = 'SELECT name, post_abbr FROM `{}.{}.{}` WHERE name LIKE "W%"'
            #view.view_query = sql_template.format(project, source_dataset_id, source_table_id)

            table = shadow_client.create_table(targ_table)



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
    source_client = bigquery.Client(project=source_project)
    shadow_client = bigquery.Client(project=shadow_project)

    if 'clean_shadow' in steps:
        success = clean_shadow_project(shadow_client, shadow_project)
        if not success:
            print("clean_target failed")
            return

    if 'shadow_datasets' in steps:
        success = shadow_datasets(source_client, shadow_client, shadow_project)
        if not success:
            print("shadow_datasets failed")
            return

        print('job completed')

    if 'create_all_shadow_tables' in steps:
        success = create_all_shadow_tables(source_client, shadow_client, shadow_project)
        if not success:
            print("create_all_shadow_tables failed")
            return

        print('job completed')


if __name__ == "__main__":
    main(sys.argv)