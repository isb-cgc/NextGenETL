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
from os.path import expanduser
import yaml
import io
import requests
import string
from git import Repo
from json import loads as json_loads
from createSchemaP3 import build_schema

from common_etl.support import confirm_google_vm, create_clean_target, bucket_to_local

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
Build Pull List from TXT
'''

def build_pull_list_from_txt(local_file, local_pull_list):

    # open the file for reading
    links = open(local_file, 'r').read().strip().split('\n')
    # create a list of the files in the file list
    all_filenames = [x.split('/') for x in links]

    if not all_filenames:
        return False

    with open(local_pull_list, mode='w') as pull_list_file:
        for i in all_filenames:
            base_file, ext_sig = os.path.splitext(i[-1])
            last_ext = ext_sig.split('?')[0]
            if last_ext == ".gz":
                file, ext = os.path.splitext(base_file)
                # Check if tsv, add to files
                if ext == ".tsv" or ext == ".csv":
                    file = ''.join([i[6], "/", i[4], "/", i[7]])
                    link = '/'.join(i)
                    pull_list_file.write(file + "\t" + link + "\n")
            elif last_ext == ".tsv" or last_ext == ".csv":
                file = ''.join([i[6], "/", i[4], "/", base_file, last_ext])
                link = '/'.join(i)
                pull_list_file.write(file + "\t" + link + "\n")

    return True

'''
----------------------------------------------------------------------------------------------
Download files
'''

#def pull_from_aws():
#    for link in len(links):

#        response = requests.get(link)
#        if response.status_code == 200:

    # Don't forget to unzip

'''
Fix column names
'''

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
    local_files_dir = "{}/{}".format(home, params['LOCAL_FILES_DIR'])
    local_file = "{}/{}".format(home, params['DOWNLOAD_FILE'])
    local_pull_list = "{}/{}".format(home, params['LOCAL_PULL_LIST'])

    if 'clear_target_directory' in steps:
        print('clear_target_directory')
        create_clean_target(local_files_dir)

    if 'build_pull_list' in steps:
        bucket_to_local(params['WORKING_BUCKET'], params['COSMIC_FILE'], local_file)
        print('build_pull_list')

        success = build_pull_list_from_txt(local_file, local_pull_list)

        if not success:
           print("Build pull list failed")
           return

if __name__ == "__main__":
    main(sys.argv)