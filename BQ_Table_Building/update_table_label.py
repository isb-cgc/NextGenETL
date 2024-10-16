"""

Copyright 2019-2024, Institute for Systems Biology

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

Extract GDC Metadata into Per-Project/Build File BQ Tables
This is still a work in progress (01/18/2020)

'''

import yaml
import sys
import io
from google.cloud import bigquery

from common_etl.support import confirm_google_vm

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

    return (yaml_dict['files_and_buckets_and_tables'], yaml_dict['steps'])

def update_table_label(target_dataset, dest_table, label_key, label_value, project=None):
    """
    Update the status tag of a big query table once a new version of the table has been created


    :param target_dataset: Dataset name
    :type target_dataset: basestring
    :param dest_table: Table name
    :type dest_table: basestring
    :param label_key: Label to be set
    :type label_key:basestring
    :param label_value: Value for the label to be set
    :type label_value: basestring
    :param project: Project name
    :type project: basestring
    :return: Whether the function works
    :rtype: bool
    """

    client = bigquery.Client() if project is None else bigquery.Client(project=project)
    table_ref = client.dataset(target_dataset).table(dest_table)
    table = client.get_table(table_ref)
    table.labels = {label_key: label_value}
    table = client.update_table(table, ["labels"])
    return True


'''
----------------------------------------------------------------------------------------------
Main Control Flow
Note that the actual steps run are configured in the YAML input! This allows you to e.g. skip previously run steps.
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

    if params is None:
        print("Bad YAML load")
        return

    if 'update_table_label' in steps:
        for label_name in params['UPDATE']:

           # Customize generic schema to this data program
           for table_id in params['UPDATE'][label_name]:

              label_value = str(params["UPDATE"][label_name][table_id])
              
              print(f'Updating table label for {table_id} with {label_name}:{label_value}')
             
              # Extract the project, dataset, and table name:
              target_project, target_dataset, target_table = table_id.split('.')
             
              # Write out the details
              success = update_table_label(target_dataset, target_table, label_name,
                                          label_value, target_project)
           if not success:
              print("update_table_label failed")
              return False

    print('job completed')

if __name__ == "__main__":
    main(sys.argv)

