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

miRNA expression quantification (TCGA data in BigQuery: section 3.1.5.2)
'''

import sys
import os
import yaml
import io
from git import Repo
from json import loads as json_loads
from os.path import expanduser
from createSchemaP3 import build_schema
from common_etl.support import create_clean_target, build_file_list, generic_bq_harness, confirm_google_vm, \
                               upload_to_bucket, csv_to_bq, concat_all_files, delete_table_bq_job, \
                               build_pull_list_with_bq, update_schema, \
                               update_description, build_combined_schema, get_the_bq_manifest, BucketPuller, \
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
        return None, None, None, None

    return (yaml_dict['files_and_buckets_and_tables'], yaml_dict['bq_filters'],
            yaml_dict['steps'], yaml_dict['extra_fields'])

'''
----------------------------------------------------------------------------------------------
First BQ Processing: Add Aliquot IDs
The GDC file UUID for the isoform file was pulled from the bucket path.  We need aliquots,
samples, and associated barcodes. We get this by first attaching the aliquot IDs
using the file table that provides aliquot UUIDs for files.
'''
def attach_aliquot_ids(maf_table, file_table, target_dataset, dest_table, do_batch):

    sql = attach_aliquot_ids_sql(maf_table, file_table)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)

'''
----------------------------------------------------------------------------------------------
SQL for above
'''
def attach_aliquot_ids_sql(isoform_table, file_table):
    return '''
        WITH
        a1 AS (SELECT DISTINCT fileUUID FROM `{0}`),        
        a2 AS (SELECT b.associated_entities__entity_gdc_id AS aliquot_gdc_id,
                      a1.fileUUID
               FROM a1 JOIN `{1}` AS b ON a1.fileUUID= b.file_gdc_id
               WHERE b.associated_entities__entity_type = 'aliquot')
        SELECT 
               c.project_short_name,
               c.case_gdc_id,
               c.associated_entities__entity_gdc_id AS aliquot_gdc_id,
               a2.fileUUID
        FROM a2 JOIN `{1}` AS c ON a2.fileUUID = c.file_gdc_id
        WHERE c.associated_entities__entity_type = 'aliquot'       
        '''.format(isoform_table, file_table)


'''
----------------------------------------------------------------------------------------------
Second BQ Processing: Add Barcodes
With the aliquot UUIDs known, we can now use the aliquot table to glue in sample info
'''
def attach_barcodes(temp_table, aliquot_table, target_dataset, dest_table, do_batch):

    sql = attach_barcodes_sql(temp_table, aliquot_table)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)


'''
----------------------------------------------------------------------------------------------
SQL for above
'''
def attach_barcodes_sql(temp_table, aliquot_table):
    #
    # ATTENTION! For rel16, 32 aliquots appear twice, with conflicting entries for the 
    # sample_is_ffpe and sample_preservation_method fields. Previous release tables did not have these
    # fields. Added DISTINCT to avoid row duplication here:
    #
    #
    return '''
        SELECT DISTINCT
               a.project_short_name,
               c.case_barcode,
               c.sample_barcode,
               c.aliquot_barcode, 
               a.fileUUID,
               c.case_gdc_id,
               c.sample_gdc_id,
               a.aliquot_gdc_id
        FROM `{0}`as a JOIN `{1}` AS c ON a.aliquot_gdc_id = c.aliquot_gdc_id
        WHERE c.case_gdc_id = a.case_gdc_id
        '''.format(temp_table, aliquot_table)

'''
----------------------------------------------------------------------------------------------
Final BQ Step: Glue the New Info to the Original Table
All the new info we have pulled together goes in the first columns of the final table
'''
def final_merge(maf_table, barcode_table, target_dataset, dest_table, do_batch):

    sql = final_join_sql(maf_table, barcode_table)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)

'''
----------------------------------------------------------------------------------------------
SQL for above
'''
def final_join_sql(isoform_table, barcodes_table):    
    return '''
        SELECT a.project_short_name,
               a.case_barcode,
               a.sample_barcode,
               a.aliquot_barcode, 
               b.miRNA_ID as miRNA_id,
               b.read_count,
               b.reads_per_million_miRNA_mapped,
               b.cross_mapped,
               b.fileUUID as file_gdc_id,
               a.case_gdc_id,
               a.sample_gdc_id,
               a.aliquot_gdc_id
        FROM `{0}` as a JOIN `{1}` as b ON a.fileUUID = b.fileUUID
        '''.format(barcodes_table, isoform_table)

'''
----------------------------------------------------------------------------------------------
file_info() function
File name includes important information, extract that out. Important! The order and
semantics of this list matches that of the extraFields parameter!

'''
def file_info(aFile, program_prefix):

    norm_path = os.path.normpath(aFile)
    path_pieces = norm_path.split(os.sep)

    fileUUID = path_pieces[-2]

    return [ fileUUID ]

'''
----------------------------------------------------------------------------------------------
Main Control Flow
Note that the actual steps run are configured in the YAML input! This allows you to
e.g. skip previously run steps.
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
        params, bq_filters, steps, extra_cols = load_config(yaml_file.read())

    if params is None:
        print("Bad YAML load")
        return

    # Schema that describes table columns:

    AUGMENTED_SCHEMA_FILE = "SchemaFiles/mirna_augmented_schema_list.json"

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

    #
    # Best practice is to clear out the directory where the files are going. Don't want anything left over.
    # Also creates the destination directory
    #

    if 'clear_target_directory' in steps:
        create_clean_target(local_files_dir)

    #
    # Use the filter set to get a manifest. Note that is a pull list is
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

    #
    # If you have already created a pull list, just plunk it in 'LOCAL_PULL_LIST' and skip this step.
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
    # Now hitting GDC cloud buckets, not "downloading". Get the files in the pull list:
    #

    if 'download_from_gdc' in steps:       
        with open(local_pull_list, mode='r') as pull_list_file:
            pull_list = pull_list_file.read().splitlines()
        print("Preparing to download %s files from buckets\n" % len(pull_list))
        bp = BucketPuller(10)
        bp.pull_from_buckets(pull_list, local_files_dir)

    #
    # Traverse the tree of downloaded files and create a flat list of all files:
    #
    
    if 'build_traversal_list' in steps:
        all_files = build_file_list(local_files_dir)
        with open(file_traversal_list, mode='w') as traversal_list:
            for line in all_files:
                traversal_list.write("{}\n".format(line)) 
   
    #
    # Take all the files and make one BIG TSV file to upload:
    #
    
    if 'concat_all_files' in steps:       
        with open(file_traversal_list, mode='r') as traversal_list_file:
            all_files = traversal_list_file.read().splitlines()  
        concat_all_files(all_files, one_big_tsv,
                         params['PROGRAM_PREFIX'], extra_cols, file_info, None)

    #
    # Upload the giant TSV into a cloud bucket:
    #
    
    if 'upload_to_bucket' in steps:
        upload_to_bucket(params['WORKING_BUCKET'], params['BUCKET_SKEL_TSV'], one_big_tsv)

    #
    # Create the BQ table from the TSV:
    #
        
    if 'create_bq_from_tsv' in steps:
        bucket_src_url = 'gs://{}/{}'.format(params['WORKING_BUCKET'], params['BUCKET_SKEL_TSV'])
        with open(hold_schema_list, mode='r') as schema_hold_dict:
            typed_schema = json_loads(schema_hold_dict.read())
        csv_to_bq(typed_schema, bucket_src_url, params['TARGET_DATASET'], params['SKELETON_TABLE'], params['BQ_AS_BATCH'])

    #
    # Need to merge in aliquot and sample barcodes from other tables:
    #
           
    if 'collect_barcodes' in steps:
        skel_table = '{}.{}.{}'.format(params['WORKING_PROJECT'], 
                                       params['TARGET_DATASET'], 
                                       params['SKELETON_TABLE'])
        
        success = attach_aliquot_ids(skel_table, params['FILE_TABLE'], 
                                     params['TARGET_DATASET'], 
                                     params['BARCODE_STEP_1_TABLE'], params['BQ_AS_BATCH'])
        if not success:
            print("attach_aliquot_ids job failed")
            return

        step_1_table = '{}.{}.{}'.format(params['WORKING_PROJECT'], 
                                         params['TARGET_DATASET'], 
                                         params['BARCODE_STEP_1_TABLE'])
        success = attach_barcodes(step_1_table, params['ALIQUOT_TABLE'], 
                                  params['TARGET_DATASET'], params['BARCODE_STEP_2_TABLE'], params['BQ_AS_BATCH'])
        if not success:
            print("attach_barcodes job failed")
            return
   
    #
    # Merge the barcode info into the final table we are building:
    #

    if 'create_final_table' in steps:
        skel_table = '{}.{}.{}'.format(params['WORKING_PROJECT'], 
                                       params['TARGET_DATASET'], 
                                       params['SKELETON_TABLE'])
        barcodes_table = '{}.{}.{}'.format(params['WORKING_PROJECT'], 
                                           params['TARGET_DATASET'], 
                                           params['BARCODE_STEP_2_TABLE'])        
        success = final_merge(skel_table, barcodes_table, 
                              params['TARGET_DATASET'], params['FINAL_TARGET_TABLE'], params['BQ_AS_BATCH'])
        if not success:
            print("Join job failed")
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
        print('process_git_schema')
        # Where do we dump the schema git repository?
        schema_file = "{}/{}/{}".format(params['SCHEMA_REPO_LOCAL'], params['RAW_SCHEMA_DIR'], params['SCHEMA_FILE_NAME'])
        full_file_prefix = "{}/{}".format(params['PROX_DESC_PREFIX'], params['FINAL_TARGET_TABLE'])
        # Write out the details
        success = generate_table_detail_files(schema_file, full_file_prefix)
        if not success:
            print("process_git_schemas failed")
            return

    if 'analyze_the_schema' in steps:
        print('analyze_the_schema')
        typing_tups = build_schema(one_big_tsv, params['SCHEMA_SAMPLE_SKIPS'])
        full_file_prefix = "{}/{}".format(params['PROX_DESC_PREFIX'], params['FINAL_TARGET_TABLE'])
        schema_dict_loc = "{}_schema.json".format(full_file_prefix)
        build_combined_schema(None, schema_dict_loc,
                                typing_tups, hold_schema_list, hold_schema_dict)

    #
    # Update the per-field descriptions:
    #

    if 'update_field_descriptions' in steps:
        print('update_field_descriptions')
        full_file_prefix = "{}/{}".format(params['PROX_DESC_PREFIX'], params['FINAL_TARGET_TABLE'])
        schema_dict_loc = "{}_schema.json".format(full_file_prefix)
        schema_dict = {}
        with open(schema_dict_loc, mode='r') as schema_hold_dict:
            full_schema_list = json_loads(schema_hold_dict.read())
        for entry in full_schema_list:
            schema_dict[entry['name']] = {'description': entry['description']}

        success = update_schema_with_dict(params['TARGET_DATASET'], params['FINAL_TARGET_TABLE'], schema_dict)
        if not success:
            print("update_field_descriptions failed")
            return

    #
    # Add description and labels to the target table:
    #

    if 'update_table_description' in steps:
        print('update_table_description')
        full_file_prefix = "{}/{}".format(params['PROX_DESC_PREFIX'], params['FINAL_TARGET_TABLE'])
        success = install_labels_and_desc(params['TARGET_DATASET'], params['FINAL_TARGET_TABLE'], full_file_prefix)
        if not success:
            print("update_table_description failed")
            return

    #
    # The derived table we generate has no field descriptions. Add them from the scraped page:
    #

    # if 'update_final_schema' in steps:
    #     success = update_schema(params['TARGET_DATASET'], params['FINAL_TARGET_TABLE'], hold_schema_dict)
    #     if not success:
    #         print("Schema update failed")
    #         return
    
    #
    # Add the table description:
    #
    #
    # if 'add_table_description' in steps:
    #     update_description(params['TARGET_DATASET'], params['FINAL_TARGET_TABLE'], params['TABLE_DESCRIPTION'])
      
    #
    # Clear out working temp tables:
    #
    
    if 'dump_working_tables' in steps:   
        dump_table_tags = ['SKELETON_TABLE', 'BARCODE_STEP_1_TABLE', 'BARCODE_STEP_2_TABLE', 
                           'BQ_MANIFEST_TABLE', 'BQ_PULL_LIST_TABLE']
        dump_tables = [params[x] for x in dump_table_tags]
        for table in dump_tables:
            delete_table_bq_job(params['TARGET_DATASET'], table)    
    #
    # Done!
    #
    
    print('job completed')

if __name__ == "__main__":
    main(sys.argv)
