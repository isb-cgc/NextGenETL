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
Read metadata from one project, copy into another.
'''
def build_bq_metadata(source_project, datasets, shadow_pid):
    bq_table_metadata_dict = {}

    ctrl_client = bigquery.Client(project=source_project)
    dataset_list = ctrl_client.list_datasets()
    for dataset in dataset_list:
            table_list = list(ctrl_client.list_tables(dataset.dataset_id))
            for tbl in table_list:
                tbl_metadata = ctrl_client.get_table(tbl).to_api_repr()
                print(str(tbl_metadata))

            dataset = ctrl_client.get_dataset(dataset.dataset_id)
            print(str(dataset))

            '''
            src_toks = source_table.split('.')
            src_proj = src_toks[0]
            src_dset = src_toks[1]
            src_tab = src_toks[2]

            trg_toks = target_table.split('.')
            trg_proj = trg_toks[0]
            trg_dset = trg_toks[1]
            trg_tab = trg_toks[2]

            src_client = bigquery.Client(src_proj)

            src_table_ref = src_client.dataset(src_dset).table(src_tab)
            s_table = src_client.get_table(src_table_ref)
            src_friendly = s_table.friendly_name

            trg_client = bigquery.Client(trg_proj)
            trg_table_ref = trg_client.dataset(trg_dset).table(trg_tab)
            t_table = src_client.get_table(trg_table_ref)
            t_table.friendly_name = src_friendly

            trg_client.update_table(t_table, ['friendlyName'])


            client = bigquery.Client()



            src_table_ref = client.dataset(source_dataset).table(source_table)
            trg_table_ref = client.dataset(target_dataset).table(dest_table)
            src_table = client.get_table(src_table_ref)
            trg_table = client.get_table(trg_table_ref)
            src_schema = src_table.schema
            trg_schema = []
            for src_sf in src_schema:
                trg_sf = bigquery.SchemaField(src_sf.name, src_sf.field_type, description=src_sf.description)
                trg_schema.append(trg_sf)
            trg_table.schema = trg_schema
            client.update_table(trg_table, ["schema"])
            return True
            '''

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

    if 'do_it' in steps:
        success = build_bq_metadata(source_project, None, None)
        if not success:
            print("do_it failed")
            return

        print('job completed')


if __name__ == "__main__":
    main(sys.argv)