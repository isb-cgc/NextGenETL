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
import gzip
import shutil
import re
import string
from git import Repo
from json import loads as json_loads
from createSchemaP3 import build_schema

from common_etl.support import confirm_google_vm, create_clean_target, bucket_to_local, build_file_list,\
                                generate_table_detail_files

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
                    cleaned_file_name = clean_file_names(file)
                    new_file = ''.join([cleaned_file_name, "_", i[4], ext, last_ext])
                    link = '/'.join(i)
                    pull_list_file.write(new_file + "\t" + link + "\n")
            elif last_ext == ".tsv" or last_ext == ".csv":
                cleaned_file_name = clean_file_names(base_file)
                new_file = ''.join([cleaned_file_name, "_", i[4], last_ext])
                link = '/'.join(i)
                pull_list_file.write(new_file + "\t" + link + "\n")

    return True

'''
Clean file names
'''

def clean_file_names(file_name):
     split_name = file_name.split('_')
     if len(split_name) > 1:
         new_name = [x.capitalize() for x in split_name]
         if new_name[0] == "Ascat":
             new_name[0] = "ASCAT"
             new_name[1] = "Purity"
             final_name = '_'.join(new_name)
         else:
             final_name = '_'.join(new_name)
     else:
         new_name = re.findall('[A-Z][^A-Z]*', file_name)
         for i in new_name:
             if i == "Cosmic":
                 new_name.remove(i)
             if i == "Export":
                 new_name.remove(i)
         if ''.join(new_name).isupper():
             final_name = ''.join(new_name)
         elif new_name[0] == "Complete":
             abv = ''.join(new_name[1:len(new_name)])
             final_name = ''.join([new_name[0], "_", abv])
         else:
             final_name = '_'.join(new_name)
     return final_name

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
    file_traversal_list = "{}/{}".format(home, params['FILE_TRAVERSAL_LIST'])

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

    if 'download_from_cosmic' in steps:
        print("Download from Sanger")

        with open(local_pull_list, mode='r') as pull_list_file:
            pull_list = pull_list_file.read().splitlines()
        print("Preaparing to download {} files from AWS buckets\n".format(len(pull_list)))
        for line in pull_list:
            file_name, url = line.split('\t')
            file_location = ''.join([local_files_dir, "/", file_name])
            with open(file_location, mode='wb') as data_file:
                response = requests.get(url)
                if response.status_code == 200:
                    data_file.write(response.content)
                    # add an unzip step & dump zip file
                else:
                   print("Download failed. Problem downloading {}".format(file_name))
                   return
            file, ext = os.path.splitext(file_name.split('/')[-1])
            new_file_location = ''.join([local_files_dir, "/", file])
            if ext == ".gz":
                # Unzip the file and remove zip file
                print("Uncompressing {}".format(file))
                with gzip.open(file_location, "rb") as gzip_in:
                    with open(new_file_location, "wb") as uncomp_out:
                        shutil.copyfileobj(gzip_in, uncomp_out)
                os.remove(file_location)
            else:
                print("{} doesn't need to be uncompressed".format(file))


    if 'build_file_list' in steps:
        print('build_file_list')
        all_files = build_file_list(local_files_dir)
        with open(file_traversal_list, mode='w') as traversal_list:
            for line in all_files:
                traversal_list.write("{}\n".format(line))

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
        print('process_git_schema')

        with open(file_traversal_list, mode='r') as traversal_list_file:
            all_files = traversal_list_file.read().splitlines()

        for line in all_files:
            file, ext = os.path.splitext(line.split('/')[-1])
            # Where do we dump the schema git repository?
            schema_file_name = ''.join([file, ".json"])
            schema_file = "{}/{}/{}".format(params['SCHEMA_REPO_LOCAL'], params['RAW_SCHEMA_DIR'], schema_file_name)
            full_file_prefix = "{}/{}".format(params['PROX_DESC_PREFIX'], schema_file_name)
            print(schema_file + "\t" + full_file_prefix)

            # Write out the details
            success = generate_table_detail_files(schema_file, full_file_prefix)
            if not success:
                print("process_git_schemas failed")
                return

if __name__ == "__main__":
    main(sys.argv)