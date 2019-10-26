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
from json import loads as json_loads
from createSchemaP3 import build_schema

from common_etl.support import get_the_bq_manifest, confirm_google_vm, create_clean_target, \
                               generic_bq_harness, build_file_list, upload_to_bucket, csv_to_bq, \
                               build_pull_list_with_bq, BucketPuller, build_combined_schema, \
                               delete_table_bq_job


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

    return yaml_dict['files_and_buckets_and_tables'], yaml_dict['bq_filters'], yaml_dict['steps']


'''
----------------------------------------------------------------------------------------------
# ### Concatentate all Files
# Gather up all files and glue them into one big one. We also add columns for the
`source_file_name` and `source_file_id` (which is the name of the directory it is in).
WARNING! Currently hardwired to CNV file heading!
'''

def concat_all_files(all_files, one_big_tsv):
    print("building {}".format(one_big_tsv))
    first = True
    with open(one_big_tsv, 'w') as outfile:
        for filename in all_files:
            with open(filename, 'r') as readfile:
                norm_path = os.path.normpath(filename)
                path_pieces = norm_path.split(os.sep)
                file_name = path_pieces[-1]
                gdc_id = path_pieces[-2]
                for line in readfile:
                    if not line.startswith('GDC_Aliquot') or first:
                        outfile.write(line.rstrip('\n'))
                        outfile.write('\t')
                        outfile.write('source_file_name' if first else file_name)
                        outfile.write('\t')
                        outfile.write('source_file_id' if first else gdc_id)
                        outfile.write('\n')
                    first = False

'''
----------------------------------------------------------------------------------------------
Merge Skeleton With Aliquot Data
Creates the final BQ table by joining the skeleton with the aliquot ID info
'''

def join_with_aliquot_table(cnv_table, aliquot_table, target_dataset, dest_table, do_batch):

    sql = merge_bq_sql(cnv_table, aliquot_table)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)

'''
----------------------------------------------------------------------------------------------
# ### SQL Code For Final Table Generation
# Original author: Sheila Reynolds
'''
def merge_bq_sql(cnv_table, aliquot_table):

    return '''
        WITH
            a1 AS (SELECT DISTINCT GDC_Aliquot
                   FROM `{0}`),
            a2 AS (SELECT b.project_id AS project_short_name,
                          b.case_barcode,
                          b.sample_barcode,
                          b.aliquot_barcode,
                          b.case_gdc_id,
                          b.sample_gdc_id,
                          b.aliquot_gdc_id
                   FROM a1
                   JOIN `{1}` b ON a1.GDC_Aliquot = b.aliquot_gdc_id)
        SELECT
            project_short_name,
            case_barcode,
            sample_barcode,
            aliquot_barcode,
            chromosome,
            start AS start_pos,
            `end` AS end_pos,
            num_probes,
            segment_mean,
            case_gdc_id,
            sample_gdc_id,
            aliquot_gdc_id,
            source_file_id AS file_gdc_id
        FROM a2
        JOIN `{0}` b ON a2.aliquot_gdc_id = b.GDC_Aliquot
        '''.format(cnv_table, aliquot_table)

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
        params, bq_filters, steps = load_config(yaml_file.read())


    #
    # BQ does not like to be given paths that have "~". So make all local paths absolute:
    #

    home = expanduser("~")
    local_files_dir = "{}/{}".format(home, params['LOCAL_FILES_DIR'])
    one_big_tsv = "{}/{}".format(home, params['ONE_BIG_TSV'])
    manifest_file = "{}/{}".format(home, params['MANIFEST_FILE'])
    local_pull_list = "{}/{}".format(home, params['LOCAL_PULL_LIST'])
    file_traversal_list = "{}/{}".format(home, params['FILE_TRAVERSAL_LIST'])
    hold_schema_dict = "{}/{}".format(home, params['HOLD_SCHEMA_DICT'])
    hold_schema_list = "{}/{}".format(home, params['HOLD_SCHEMA_LIST'])

    # Schema that describes CNVR table:

    AUGMENTED_SCHEMA_FILE = "SchemaFiles/cnvr_augmented_schema_list.json"

    #
    # Use the filter set to get a manifest from GDC using their API. Note that if a pull list is
    # provided, these steps can be omitted:
    #

    if 'build_manifest_from_filters' in steps:
        max_files = params['MAX_FILES'] if 'MAX_FILES' in params else None

        manifest_success = get_the_bq_manifest(params['FILE_TABLE'], bq_filters, max_files,
                                               params['WORKING_PROJECT'], params['TARGET_DATASET'],
                                               params['BQ_MANIFEST_TABLE'], params['WORKING_BUCKET'],
                                               params['BUCKET_MANIFEST_TSV'], manifest_file,
                                               params['BQ_AS_BATCH'])
        if not manifest_success:
            print("Failure generating manifest")
            return


    if 'clear_target_directory' in steps:
        create_clean_target(local_files_dir)

    #
    # We need to create a "pull list" of gs:// URLs to pull from GDC buckets. If you have already
    # created a pull list, just plunk it in 'LOCAL_PULL_LIST' and skip this step. If creating a pull
    # list, uses BQ as long as you have built the manifest using BQ (that route uses the BQ Manifest
    # table that was created).
    #

    if 'build_pull_list' in steps:
        full_manifest = '{}.{}.{}'.format(params['WORKING_PROJECT'],
                                          params['TARGET_DATASET'],
                                          params['BQ_MANIFEST_TABLE'])
        build_pull_list_with_bq(full_manifest, params['INDEXD_BQ_TABLE'],
                                params['WORKING_PROJECT'], params['TARGET_DATASET'],
                                params['BQ_PULL_LIST_TABLE'],
                                params['WORKING_BUCKET'],
                                params['BUCKET_PULL_LIST'],
                                local_pull_list, params['BQ_AS_BATCH'])
    #
    # Now hitting GDC cloud buckets. Get the files in the pull list:
    #

    if 'download_from_gdc' in steps:
        with open(local_pull_list, mode='r') as pull_list_file:
            pull_list = pull_list_file.read().splitlines()
        print("Preparing to download %s files from buckets\n" % len(pull_list))
        bp = BucketPuller(10)
        bp.pull_from_buckets(pull_list, local_files_dir)

    if 'build_file_list' in steps:
        all_files = build_file_list(local_files_dir)
        with open(file_traversal_list, mode='w') as traversal_list:
            for line in all_files:
                traversal_list.write("{}\n".format(line))

    if 'concat_all_files' in steps:
        with open(file_traversal_list, mode='r') as traversal_list_file:
            all_files = traversal_list_file.read().splitlines()
        concat_all_files(all_files, one_big_tsv)

    if 'build_the_schema' in steps:
        typing_tups = build_schema(one_big_tsv, params['SCHEMA_SAMPLE_SKIPS'])
        build_combined_schema(None, AUGMENTED_SCHEMA_FILE,
                              typing_tups, hold_schema_list, hold_schema_dict)

    bucket_target_blob = '{}/{}'.format(params['WORKING_BUCKET_DIR'], params['BUCKET_TSV'])

    if 'upload_to_bucket' in steps:
        upload_to_bucket(params['WORKING_BUCKET'], bucket_target_blob, params['ONE_BIG_TSV'])

    if 'create_bq_from_tsv' in steps:
        bucket_src_url = 'gs://{}/{}'.format(params['WORKING_BUCKET'], bucket_target_blob)
        with open(hold_schema_list, mode='r') as schema_hold_dict:
            typed_schema = json_loads(schema_hold_dict.read())
        csv_to_bq(typed_schema, bucket_src_url, params['TARGET_DATASET'], params['TARGET_TABLE'], params['BQ_AS_BATCH'])

    if 'add_aliquot_fields' in steps:
        full_target_table = '{}.{}.{}'.format(params['WORKING_PROJECT'],
                                              params['TARGET_DATASET'],
                                              params['TARGET_TABLE'])
        success = join_with_aliquot_table(full_target_table, params['ALIQUOT_TABLE'],
                                          params['TARGET_DATASET'], params['FINAL_TARGET_TABLE'], params['BQ_AS_BATCH'])
        if not success:
            print("Join job failed")

    #
    # Clear out working temp tables:
    #

    if 'dump_working_tables' in steps:
        dump_table_tags = ['TARGET_TABLE']
        dump_tables = [params[x] for x in dump_table_tags]
        for table in dump_tables:
            delete_table_bq_job(params['TARGET_DATASET'], table)

    print('job completed')

if __name__ == "__main__":
    main(sys.argv)

