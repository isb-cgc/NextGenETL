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

VM NEEDS 26 GB MEMORY FOR THIS TO NOT DIE IN MERGE MODE, MORE IF DEBUG == True
In merge mode, the job needed 18 G peak to run, hitting the highest levels on UCEC
and UCS (which are processed near the very end). This required running on a n1-highmem-4
(4 vCPUs, 26 GB memory) machine, and took about 40 minutes. When the DEBUG Logging was
enabled, performance was degraded, and I moved to using a n1-highmem-8 (8 vCPUs, 52 GB memory)
machine. Note that only one processor gets used, so that is just increasing the memory.
That took 53 minutes. It appears that DEBUG logging created 37.5 million lines for
reading, 187 million lines for writing.
'''

import sys

import os
import yaml
import gzip
import shutil
import zipfile
# import requests
import io
from git import Repo
# import re
from json import loads as json_loads, dumps as json_dumps
from os.path import expanduser
# from bs4 import BeautifulSoup
from createSchemaP3 import build_schema

from common_etl.support import build_manifest_filter, get_the_manifest, create_clean_target, \
                               pull_from_buckets, build_file_list, generic_bq_harness, \
                               upload_to_bucket, csv_to_bq, delete_table_bq_job, \
                               build_pull_list_with_indexd, concat_all_merged_files, \
                               read_MAFs, write_MAFs, build_pull_list_with_bq, update_schema, \
                               update_description, build_combined_schema, get_the_bq_manifest, confirm_google_vm, \
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
        return None, None, None, None, None, None, None

    return (yaml_dict['files_and_buckets_and_tables'], yaml_dict['filters'], yaml_dict['bq_filters'], 
            yaml_dict['steps'], yaml_dict['extra_fields'], yaml_dict['key_fields'])


'''
----------------------------------------------------------------------------------------------
Scrape the Schema Description From GDC
The GDC has a page that describes the columns in the MAF file. Just scrape it off:
'''

# def scrape_schema(maf_url, first_col):
#     schema = []
#     resp = requests.request("GET", maf_url)
#
#     maf_page = None
#     if resp.status_code == 200:
#         maf_page = resp.content
#     else:
#         print()
#         print("Request URL: {} ".format(maf_url))
#         print("Problem downloading schema file. HTTP Status Code: {}".format(resp.status_code))
#         print("HTTP content: {}".format(resp.content))
#
#     soup = BeautifulSoup(maf_page, features="html.parser")
#     tbody = soup.find_all(text=re.compile('^.*{}.*$'.format(first_col)))[0].parent.parent.parent
#     for row in tbody.find_all('tr'):
#         elems = row.find_all('td')
#         desc = [x.string for x in elems[1]] # Have to deal with embedded link tags
#         row_dict = {
#             "name": elems[0].string.split(' - ', 1)[1],
#             "description": "".join(desc)
#         }
#         schema.append(row_dict)
#
#     return schema


'''
----------------------------------------------------------------------------------------------
Extract the TCGA Programs We Are Working With From File List
Extract from downloaded file names instead of using a specified list.
'''

def build_program_list(all_files):
    
    programs = set()
    for filename in all_files:
        info_list = file_info(filename, None)
        programs.add(info_list[0])
    
    return sorted(programs)
  
'''
----------------------------------------------------------------------------------------------
Extract the Callers We Are Working With From File List
Extract from downloaded file names, compare to expected list. Answer if they match.
'''

def check_caller_list(all_files, expected_callers):
    
    expected_set = set(expected_callers)
    callers = set()
    for filename in all_files:
        info_list = file_info(filename, None)
        callers.add(info_list[1])
    
    return callers == expected_set  



'''
----------------------------------------------------------------------------------------------
First BQ Processing: Add Aliquot IDs
The GDC files only provide tumor and normal BAM file IDS. We need aliquots, samples, and
associated barcodes. We get this by first attaching the aliquot IDs using the file table
that provides aliquot UUIDs for BAM files.
'''

def attach_aliquot_ids(maf_table, file_table, target_dataset, dest_table, do_batch):

    sql = attach_aliquot_ids_sql(maf_table, file_table)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)

'''
----------------------------------------------------------------------------------------------
SQL for above
'''

def attach_aliquot_ids_sql(maf_table, file_table):
    return '''
        WITH
        a1 AS (SELECT DISTINCT tumor_bam_uuid, normal_bam_uuid FROM `{0}`),        
        a2 AS (SELECT b.associated_entities__entity_gdc_id AS aliquot_gdc_id_tumor,
                      a1.tumor_bam_uuid,
                      a1.normal_bam_uuid
               FROM a1 JOIN `{1}` AS b ON a1.tumor_bam_uuid = b.file_gdc_id
               WHERE b.associated_entities__entity_type = 'aliquot')
        SELECT 
               c.project_short_name,
               c.case_gdc_id,
               c.associated_entities__entity_gdc_id AS aliquot_gdc_id_normal,
               a2.aliquot_gdc_id_tumor,
               a2.tumor_bam_uuid,
               a2.normal_bam_uuid
        FROM a2 JOIN `{1}` AS c ON a2.normal_bam_uuid = c.file_gdc_id
        WHERE c.associated_entities__entity_type = 'aliquot'
        '''.format(maf_table, file_table)


'''
----------------------------------------------------------------------------------------------
Second BQ Processing: Add Barcodes
With the aliquot UUIDs known, we can now use the aliquot table to glue in sample info
'''

def attach_barcodes(temp_table, aliquot_table, target_dataset, dest_table, do_batch, program):

    sql = attach_barcodes_sql(temp_table, aliquot_table, program)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)

'''
----------------------------------------------------------------------------------------------
SQL for above
'''

def attach_barcodes_sql(maf_table, aliquot_table, program):
    if program == 'TCGA':
        return '''
            WITH
            a1 AS (SELECT a.project_short_name,
                          a.case_gdc_id,
                          b.aliquot_barcode AS aliquot_barcode_tumor,
                          b.sample_barcode AS sample_barcode_tumor,
                          a.aliquot_gdc_id_tumor,
                          a.aliquot_gdc_id_normal,
                          a.tumor_bam_uuid,
                          a.normal_bam_uuid
                   FROM `{0}` AS a JOIN `{1}` AS b ON a.aliquot_gdc_id_tumor = b.aliquot_gdc_id)
            SELECT a1.project_short_name,
                   c.case_barcode,
                   a1.sample_barcode_tumor,
                   c.sample_barcode AS sample_barcode_normal,
                   a1.aliquot_barcode_tumor, 
                   c.aliquot_barcode AS aliquot_barcode_normal,
                   a1.tumor_bam_uuid,
                   a1.normal_bam_uuid
            FROM a1 JOIN `{1}` AS c ON a1.aliquot_gdc_id_normal = c.aliquot_gdc_id
            WHERE c.case_gdc_id = a1.case_gdc_id
            '''.format(maf_table, aliquot_table)
    else:
        return '''
            WITH
            a1 AS (SELECT b.project_id AS project_short_name,
                          a.case_id AS case_gdc_id,
                          b.aliquot_barcode AS aliquot_barcode_tumor,
                          b.sample_barcode AS sample_barcode_tumor,
                          a.Tumor_Sample_UUID AS aliquot_gdc_id_tumor,
                          a.Matched_Norm_Sample_UUID AS aliquot_gdc_id_normal,
                          a.tumor_bam_uuid AS tumor_file_submitter_uuid,
                          a.normal_bam_uuid AS normal_file_submitter_uuid
                FROM
                  `{0}` AS a JOIN `{1}` AS b ON a.Tumor_Sample_UUID = b.aliquot_gdc_id)
              SELECT a1.project_short_name,
                     c.case_barcode,
                     a1.sample_barcode_tumor,
                     c.sample_barcode AS sample_barcode_normal,
                     a1.aliquot_barcode_tumor,
                     c.aliquot_barcode AS aliquot_barcode_normal,
                     a1.tumor_file_submitter_uuid,
                     a1.normal_file_submitter_uuid
              FROM a1 JOIN `{1}` AS c ON a1.aliquot_gdc_id_normal = c.aliquot_gdc_id
              WHERE c.case_gdc_id = a1.case_gdc_id
        '''.format(maf_table, aliquot_table)
'''
----------------------------------------------------------------------------------------------
Final BQ Step: Glue the New Info to the Original Table
All the new info we have pulled together goes in the first columns of the final table
'''

def final_merge(maf_table, barcode_table, target_dataset, dest_table, do_batch, program):

    sql = final_join_sql(maf_table, barcode_table, program)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)

'''
----------------------------------------------------------------------------------------------
SQL for above
'''
def final_join_sql(maf_table, barcodes_table, program):
    if program == 'TCGA':
        return'''
             SELECT a.project_short_name,
                    a.case_barcode,
                    a.sample_barcode_tumor,
                    a.sample_barcode_normal,
                    a.aliquot_barcode_tumor, 
                    a.aliquot_barcode_normal,
                    b.*
             FROM `{0}` as a JOIN `{1}` as b ON a.tumor_bam_uuid = b.tumor_bam_uuid
        '''.format(barcodes_table, maf_table)
    else:
        return '''
             SELECT a.project_short_name,
                    a.case_barcode,
                    a.sample_barcode_tumor,
                    a.sample_barcode_normal,
                    a.aliquot_barcode_tumor, 
                    a.aliquot_barcode_normal,
                    b.*
             FROM `{0}` as a JOIN `{1}` as b ON a.tumor_file_submitter_uuid = b.tumor_bam_uuid
        '''.format(barcodes_table, maf_table)

'''
----------------------------------------------------------------------------------------------
file_info() function Author: Sheila Reynolds
File name includes important information, e.g. the program name and the caller. Extract that
out along with name and ID.
'''

def file_info(aFile, program):

    norm_path = os.path.normpath(aFile)
    path_pieces = norm_path.split(os.sep)

    if program == 'TCGA':
        file_name = path_pieces[-1]
        file_name_parts = file_name.split('.')
        callerName = file_name_parts[2]
        fileUUID = file_name_parts[3]
    else:
        fileUUID = path_pieces[-2]
        callerName = None

    return ( [ callerName, fileUUID ] )

'''
------------------------------------------------------------------------------
Concatenate all Files
'''

def concat_all_files(all_files, one_big_tsv, program):
    """
    Concatenate all Files
    Gather up all files and glue them into one big one. The file name and path often include features
    that we want to add into the table. The provided file_info_func returns a list of elements from
    the file path. Note if file is zipped,
    we unzip it, concat it, then toss the unzipped version.
    THIS VERSION OF THE FUNCTION USES THE FIRST LINE OF THE FIRST FILE TO BUILD THE HEADER LINE!
    """
    print("building {}".format(one_big_tsv))
    first = True
    header_id = None
    with open(one_big_tsv, 'w') as outfile:
        for filename in all_files:
            toss_zip = False
            if filename.endswith('.zip'):
                dir_name = os.path.dirname(filename)
                print("Unzipping {}".format(filename))
                with zipfile.ZipFile(filename, "r") as zip_ref:
                    zip_ref.extractall(dir_name)
                use_file_name = filename[:-4]
                toss_zip = True
            elif filename.endswith('.gz'):
                dir_name = os.path.dirname(filename)
                use_file_name = filename[:-3]
                print("Uncompressing {}".format(filename))
                with gzip.open(filename, "rb") as gzip_in:
                    with open(use_file_name, "wb") as uncomp_out:
                        shutil.copyfileobj(gzip_in, uncomp_out)
                toss_zip = True
            else:
                use_file_name = filename
            with open(use_file_name, 'r') as readfile:
                callerName, fileUUID = file_info(use_file_name, program)
                for line in readfile:
                    # Seeing comments in MAF files.
                    if not line.startswith('#'):
                        if first:
                            header_id = line.split('\t')[0]
                            outfile.write(line.rstrip('\n'))
                            outfile.write('\t')
                            outfile.write('file_gdc_id')
                            if program == "TCGA":
                                outfile.write('\t')
                                outfile.write('caller')
                            outfile.write('\n')
                            first = False
                        if not line.startswith(header_id):
                            outfile.write(line.rstrip('\n'))
                            outfile.write('\t')
                            outfile.write(fileUUID)
                            if program == "TCGA":
                                outfile.write('\t')
                                outfile.write(callerName)
                            outfile.write('\n')
                if toss_zip:
                    os.remove(use_file_name)

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
        params, filters, bq_filters, steps, extra_cols, key_fields = load_config(yaml_file.read())

    if params is None:
        print("Bad YAML load")
        return

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
    # hold_scraped_dict = "{}/{}".format(home, params['HOLD_SCRAPED_DICT'])

    AUGMENTED_SCHEMA_FILE =  "SchemaFiles/augmented_schema_list.json"

    #
    # Empirical evidence suggests this workflow is going to be very memory hungry if you are doing
    # merging, and requires at least 26 GB to be safe. Confirm that before starting!
    #

    # do_merging = params['DO_MERGED_OUTPUT']
    # if do_merging:
    #     meminfo = dict((i.split()[0].rstrip(':'),int(i.split()[1])) for i in open('/proc/meminfo').readlines())
    #     mem_kib = meminfo['MemTotal']
    #     print("Machine memory: {}".format(mem_kib))
    #     if int(mem_kib) < 26000000:
    #         print("Job requires at least 26 GB physical memory to complete")
    #         return

    #
    # Next, use the filter set to get a manifest from GDC using their API. Note that is a pull list is
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
    # Best practice is to clear out the directory where the files are going. Don't want anything left over:
    #
    
    if 'clear_target_directory' in steps:
        create_clean_target(local_files_dir)

    #
    # We need to create a "pull list" of gs:// URLs to pull from GDC buckets. If you have already
    # created a pull list, just plunk it in 'LOCAL_PULL_LIST' and skip this step. If creating a pull
    # list, you can do it using IndexD calls on a manifest file, OR using BQ as long as you have 
    # built the manifest using BQ (that route uses the BQ Manifest table that was created).
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
        pull_from_buckets(pull_list, local_files_dir)

    #
    # Traverse the tree of downloaded files and create a flat list of all files:
    #
    
    if 'build_traversal_list' in steps:
        all_files = build_file_list(local_files_dir)
        #program_list = build_program_list(all_files)
        #if not check_caller_list(all_files, callers):
        #    print("Unexpected caller mismatch! Expecting {}".format(callers))
        #    return
        with open(file_traversal_list, mode='w') as traversal_list:
            for line in all_files:
                traversal_list.write("{}\n".format(line))
   
    #
    # We can create either a table that merges identical mutations from the different callers into
    # one row, or keep them separate:
    #
    
    # if do_merging:
    #     do_debug = params['DO_DEBUG_LOGGING']
    #     target_count = int(params['EXPECTED_COLUMNS'])
    #     for program in program_list:
    #         print("Look at MAFS for {}".format(program))
    #         if 'run_maf_reader' in steps:
    #             with open(file_traversal_list, mode='r') as traversal_list_file:
    #                 all_files = traversal_list_file.read().splitlines()
    #             print("Start reading MAFS for {}".format(program))
    #             mut_calls, hdr_pick = read_MAFs(program, all_files,
    #                                             params['PROGRAM_PREFIX'], extra_cols,
    #                                             target_count, do_debug, key_fields,
    #                                             params['FIRST_MAF_COL'], file_info)
    #             print("Finish reading MAFS for {}".format(program))
    #
    #         if 'run_maf_writer' in steps:
    #             print("Start writing MAFS for {}".format(program))
    #             hist_count = write_MAFs(program, mut_calls, hdr_pick, callers, do_debug)
    #             for ii in range(len(hist_count)):
    #                 if hist_count[ii] > 0:
    #                     print(" %6d  %9d " % ( ii, hist_count[ii] ))
    #             print("Finish writing MAFS for {}".format(program))
    
    #
    # Take all the files and make one BIG TSV file to upload:
    #
    
    if 'concat_all_files' in steps:       
        # if do_merging:
        #     maf_list = ["mergeA." + tumor + ".maf" for tumor in program_list]
        #     concat_all_merged_files(maf_list, one_big_tsv)
        # else:
        with open(file_traversal_list, mode='r') as traversal_list_file:
            all_files = traversal_list_file.read().splitlines()
            concat_all_files(all_files, one_big_tsv, params['PROGRAM'])
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

    bucket_target_blob = '{}/{}'.format(params['WORKING_BUCKET_DIR'], params['BUCKET_TSV'])
    #
    # Scrape the column descriptions from the GDC web page
    #
    
    # if 'scrape_schema' in steps:
    #     scrape_list = scrape_schema(params['MAF_URL'], params['FIRST_MAF_COL'])
    #     with open(hold_scraped_dict, mode='w') as scraped_hold_list:
    #         scraped_hold_list.write(json_dumps(scrape_list))

    #
    # For the legacy table, the descriptions had lots of analysis tidbits. Very nice, but hard to maintain.
    # We just use hardwired schema descriptions now, most directly pulled from the GDC website:
    #
    
    # if 'build_the_schema' in steps:
    #     typing_tups = build_schema(one_big_tsv, params['SCHEMA_SAMPLE_SKIPS'])
    #     build_combined_schema(hold_scraped_dict, AUGMENTED_SCHEMA_FILE,
    #                           typing_tups, hold_schema_list, hold_schema_dict)
         
    #
    # Upload the giant TSV into a cloud bucket:
    #

    bucket_target_blob = '{}/{}'.format(params['WORKING_BUCKET_DIR'], params['BUCKET_TSV'])

    if 'upload_to_bucket' in steps:
        print('upload_to_bucket')
        upload_to_bucket(params['WORKING_BUCKET'], bucket_target_blob, one_big_tsv)

    #
    # Create the BQ table from the TSV:
    #
        
    if 'create_bq_from_tsv' in steps:
        print('create_bq_from_tsv')
        bucket_src_url = 'gs://{}/{}'.format(params['WORKING_BUCKET'], bucket_target_blob)
        with open(hold_schema_list, mode='r') as schema_hold_dict:
            typed_schema = json_loads(schema_hold_dict.read())
        csv_to_bq(typed_schema, bucket_src_url, params['TARGET_DATASET'], params['TARGET_TABLE'], params['BQ_AS_BATCH'])

    #
    # Need to merge in aliquot and sample barcodes from other tables:
    #
           
    if 'collect_barcodes' in steps:
        skel_table = '{}.{}.{}'.format(params['WORKING_PROJECT'], 
                                       params['TARGET_DATASET'], 
                                       params['TARGET_TABLE'])
        if params['PROGRAM'] == 'TCGA':
            success = attach_aliquot_ids(skel_table, params['FILE_TABLE'],
                                         params['TARGET_DATASET'],
                                         params['BARCODE_STEP_1_TABLE'], params['BQ_AS_BATCH'])
            if not success:
                print("attach_aliquot_ids job failed")
                return

            step_1_table = '{}.{}.{}'.format(params['WORKING_PROJECT'],
                                           params['TARGET_DATASET'],
                                             params['BARCODE_STEP_1_TABLE'])
        else:
            step_1_table = skel_table

        success = attach_barcodes(step_1_table, params['ALIQUOT_TABLE'],
                                  params['TARGET_DATASET'], params['BARCODE_STEP_2_TABLE'], params['BQ_AS_BATCH'],
                                  params['PROGRAM'])
        if not success:
            print("attach_barcodes job failed")
            return
   
    #
    # Merge the barcode info into the final table we are building:
    #

    if 'create_final_table' in steps:
        skel_table = '{}.{}.{}'.format(params['WORKING_PROJECT'], 
                                       params['TARGET_DATASET'], 
                                       params['TARGET_TABLE'])
        barcodes_table = '{}.{}.{}'.format(params['WORKING_PROJECT'], 
                                           params['TARGET_DATASET'], 
                                           params['BARCODE_STEP_2_TABLE'])        
        success = final_merge(skel_table, barcodes_table, 
                              params['TARGET_DATASET'], params['FINAL_TARGET_TABLE'], params['BQ_AS_BATCH'],
                              params['PROGRAM'])
        if not success:
            print("Join job failed")
            return
    
    #
    # The derived table we generate has no field descriptions. Add them from the scraped page:
    #
    
    if 'update_final_schema' in steps:    
        success = update_schema(params['TARGET_DATASET'], params['FINAL_TARGET_TABLE'], hold_schema_dict)
        if not success:
            print("Schema update failed")
            return       
    
    #
    # Add the table description:
    #
    
    if 'add_table_description' in steps:  
        desc = params['TABLE_DESCRIPTION'].format(params['MAF_URL'])
        update_description(params['TARGET_DATASET'], params['FINAL_TARGET_TABLE'], desc)    
      
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

