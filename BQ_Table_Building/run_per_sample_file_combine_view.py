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
from google.cloud import bigquery
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
                               install_labels_and_desc, update_schema, generate_table_detail_files, publish_table, \
                               customize_labels_and_desc, update_status_tag, compare_two_tables

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

    return yaml_dict['files_and_buckets_and_tables'], yaml_dict['cloud_resources'], yaml_dict['steps']

def build_table_list(project):
    """
    Builds a list of tables to combine into a view


    """
    try:
        client = bigquery.Client() if project is None else bigquery.Client(project=project)
        table_list = []
        for dataset in list(client.list_datasets()):
            if '_versioned' not in dataset.dataset_id:
                dataset_id = dataset.dataset_id
                for table in list(client.list_tables(f"{project}.{dataset_id}")):
                    if 'per_sample_file_metadata' in table.table_id and '_current' in table.table_id:
                        table_list.append(f"{project}.{dataset_id}.{table.table_id}")
    except Exception as ex:
        print(ex)
        return False

    return table_list

def create_union_view(table_list, project=None):
    """
    Builds a BQ view from a union of all of one nodes tables

    :param project:
    :type project:
    :return:
    :rtype:
    """
    query = 'SELECT * FROM'
    with open(table_list, mode='r') as table_list:
        for count, table in enumerate(table_list, start=1):
            if count != len(table_list):
                if count != len(table_list):
                    query = f"{query} `{table}` UNION ALL SELECT * FROM"
                else:
                    query = f"{query} `{table}`"

    return query

def create_view(view_name, query):
    project, dataset, table = view_name.split("_")
    client = bigquery.Client(project)
    view = bigquery.Table(view_name)
    view.view_query(query)
    view = client.create_table(view)

    return view


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
        params, cloud_resources, steps = load_config(yaml_file.read())

# todo variable building
    home = expanduser("~")
    file_list = f"{home}/{params['TABLE_FILE_LIST']}"

# todo steps

    # todo 'build_table_list'
    if 'build_table_list' in steps:
        print('Build Table list')
        table_list = build_table_list(params['PUBLICATION_PROJECT'])

        with open(file_list, mode='w') as file_list:
            for table in table_list:
                file_list.write(f"{table}\n")

    # todo create a union view within node
    if 'create_combined_node_view' in steps:
        print('Create a combined view for each node')
        for cloud_resource in cloud_resources:
            view_query = create_union_view()






    # todo 'get_original_table_fields'
    if 'get_original_table_fields' in steps:
        print('Find original field names')

    # todo 'update_fields'
    if 'update_fields' in steps:
        print('Changing field names for WebApp and adding columns when necessary')

    # todo 'construct_view_query'
    if 'construct_view_query' in steps:
        print('Creating the query for the view')

    # todo 'create_view'
    if 'create_view' in steps:
        print('Creating View')

    print('job completed')


if __name__ == "__main__":
    main(sys.argv)

