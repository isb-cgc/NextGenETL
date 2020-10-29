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

#
# This is just code extracted from the original prototype Jupyter notebook c. Spring 2019.
# A starting point for the DNA methylation pipeline
#

# ### Make sure the VM has BigQuery and Storage Read/Write permissions!
# ### These files are ~1.4TB total, so the machine needs this amount of disk space. 

# ## DNA Methylation (TCGA data in BigQuery: section 3.1.3.1)
# ### This is still a work in progress (5/17/19)
# 

import sys
import os
import yaml
import io
import requests
from json import loads as json_loads
from createSchemaP3 import build_schema
from common_etl.support import build_manifest_filter, get_the_manifest, create_clean_target, \
    pull_from_buckets, build_file_list, generic_bq_harness, confirm_google_vm,    \
    upload_to_bucket, csv_to_bq, concat_all_files, delete_table_bq_job,    \
    build_pull_list_with_indexd, build_pull_list_with_bq, update_schema,   \
    update_description, build_combined_schema, get_the_bq_manifest, BucketPuller


# ### The Configuration Reader
# Parses the YAML configuration into dictionaries


def load_config(yaml_config):
    yaml_dict = None
    config_stream = io.StringIO(yaml_config)
    try:
        yaml_dict = yaml.load(config_stream, Loader=yaml.FullLoader)
    except yaml.YAMLError as ex:
        print(ex)

    if yaml_dict is None:
        return None, None, None, None, None, None, None, None

    return (yaml_dict['files_and_buckets_and_tables'], yaml_dict['filters'], yaml_dict['bq_filters'], 
            yaml_dict['steps'], yaml_dict['retain_fields'], yaml_dict['extra_fields'], 
            yaml_dict['retain_platform_ref_fields'])


# ### First BQ Processing: Add Aliquot IDs
# The GDC file UUID for the isoform file was pulled from the bucket path.  We need aliquots, samples, and associated barcodes. We get this by first attaching the aliquot IDs using the file table that provides aliquot UUIDs for files.


def attach_aliquot_ids(maf_table, file_table, target_dataset, dest_table, do_batch):

    sql = attach_aliquot_ids_sql(maf_table, file_table)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)



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


# ### Second BQ Processing: Add Barcodes
# With the aliquot UUIDs known, we can now use the aliquot table to glue in sample info 

def attach_barcodes(temp_table, aliquot_table, target_dataset, dest_table, do_batch):

    sql = attach_barcodes_sql(temp_table, aliquot_table)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)


def attach_barcodes_sql(temp_table, aliquot_table):
    #
    # ATTENTION! For rel16, 32 aliquots appear twice, with conflicting entries for the 
    # sample_is_ffpe and sample_preservation_method fields. Previous release tables did not have these
    # fields. Added DISTINCT to avoid row duplication here:
    #
    return '''
        SELECT DISTINCT
               a.project_short_name,
               c.case_barcode,
               c.sample_barcode,
               c.aliquot_barcode, 
               a.fileUUID
        FROM `{0}`as a JOIN `{1}` AS c ON a.aliquot_gdc_id = c.aliquot_gdc_id
        WHERE c.case_gdc_id = a.case_gdc_id
        '''.format(temp_table, aliquot_table)


# ### Final BQ Step: Glue the New Info to the Original Table
# All the new info we have pulled together goes in the first columns of the final table
#

def final_merge(maf_table, barcode_table, target_dataset, dest_table, do_batch):

    sql = final_join_sql(maf_table, barcode_table)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)



def final_join_sql(isoform_table, barcodes_table):    
    return '''
        SELECT a.project_short_name,
               a.case_barcode,
               a.sample_barcode,
               a.aliquot_barcode, 
               b.*
        FROM `{0}` as a JOIN `{1}` as b ON a.fileUUID = b.fileUUID
        '''.format(barcodes_table, isoform_table)


# ### file_info() function
# File name includes important information, extract that out. Important! The order and semantics of this list matches that of the extraFields parameter!


def file_info(aFile, program_prefix):

    # This is a file name:
    # jhu-usc . edu_SARC . HumanMethylation450 . 12 . lvl-3 . TCGA-WK-A8XO-01A-11D-A37D-05 . gdc_hg38 . txt

    
    norm_path = os.path.normpath(aFile)
    path_pieces = norm_path.split(os.sep)

    fileUUID = path_pieces[-2]
    file_name = path_pieces[-1]
    file_name_parts = file_name.split('.')
    platform_name = file_name_parts[2]
    aliquot_barcode = file_name_parts[5]
    if not platform_name.startswith("Human"):
        raise Exception()
    
    return [ platform_name, aliquot_barcode, fileUUID ]


# ### Concatenate only selected columns of all the files

def concat_all_files_selected_cols(all_files, one_big_tsv, program_prefix, retain_cols, 
                                   extra_cols, file_info_func, split_more_func, check_col, drop_val):
    """
    Concatenate all Files
    Gather up all files and glue them into one big one. The file name and path often include features
    that we want to add into the table. The provided file_info_func returns a list of elements from
    the file path, and the extra_cols list maps these to extra column names. Note if file is zipped,
    we unzip it, concat it, then toss the unzipped version.
    THIS VERSION OF THE FUNCTION USES THE FIRST LINE OF THE FIRST FILE TO BUILD THE HEADER LINE!
    """
    print("building {}".format(one_big_tsv))
    first = True
    header_id = None
    hdr_line = []
    keep_cols = []
    use_line = []
    check_index = None
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
            if os.path.isfile(use_file_name):
                with open(use_file_name, 'r') as readfile:
                    file_info_list = file_info_func(use_file_name, program_prefix)
                    for line in readfile:
                        if line.startswith('#'):
                            continue
                        split_line = line.rstrip('\n').split("\t")
                        if first:
                            for col in extra_cols:
                                split_line.append(col)
                            header_id = split_line[0]
                            for i in range(0, len(split_line)):
                                if split_line[i] in retain_cols or split_line[i] in extra_cols:
                                    keep_cols.append(i)
                                    hdr_line.append(split_line[i])
                                if check_col is not None and split_line[i] == check_col:
                                    print("check_col {} split_line {} indx {}".format(check_col, split_line[i], i))
                                    check_index = i
                            print("Header starts with {}".format(header_id))
                            print("Keeping columns {}".format(str(keep_cols)))
                        else:
                            for i in range(len(extra_cols)):
                                split_line.append(file_info_list[i])
                        use_line.clear()
                        for i in keep_cols:
                            use_line.append(split_line[i]) 
                        if not line.startswith(header_id) or first:
                            if check_index is not None and use_line[check_index] == drop_val:
                                continue
                            if split_more_func is not None:
                                use_line = split_more_func(use_line, hdr_line, first)
                            outfile.write('\t'.join(use_line))
                            outfile.write('\n')
                        first = False
            else:
                print('{} was not found'.format(use_file_name))

            if toss_zip and os.path.isfile(use_file_name):
                os.remove(use_file_name)

    return


# ### Reduce file to distinct lines

def set_from_file(in_file, distinct_file):
    """
    Take a file and only keep distinct rows
    """
    print("building {}".format(distinct_file))
    seen_lines = set()
    with open(distinct_file, 'w') as outfile:
        with open(in_file, 'r') as readfile:
            for line in readfile:
                if line in seen_lines:
                    continue
                seen_lines.add(line)
                outfile.write(line)
    return


# ### Main Control Flow
# Note that the actual steps run are configured in the YAML input! This allows you to e.g. skip previously run steps.

def main():

    if not confirm_google_vm():
        print('This job needs to run on a Google Cloud Compute Engine to avoid storage egress charges [EXITING]')
        return
    
    print('job started')
       
    #
    # First thing is to load the configuration:
    #
    
    params, filters, bq_filters, steps, retain_cols, extra_cols, retain_platform_ref_fields = load_config(yaml_config)

    if params is None:
        print("Bad YAML load")
        return

    #
    # Use the filter set to get a manifest from GDC using their API. Note that if a pull list is
    # provided, these steps can be omitted:
    #
    
    if 'build_manifest_from_filters' in steps:
        max_files = params['MAX_FILES'] if 'MAX_FILES' in params else None
        if params['USE_GDC_API_FOR_MANIFEST']:  
            manifest_filter = build_manifest_filter(filters)
            manifest_success = get_the_manifest(manifest_filter, params['API_URL'], 
                                                params['MANIFEST_FILE'], max_files)
        else:    
            manifest_success = get_the_bq_manifest(params['FILE_TABLE'], bq_filters, max_files, 
                                                   params['WORKING_PROJECT'], params['TARGET_DATASET'], 
                                                   params['BQ_MANIFEST_TABLE'], params['WORKING_BUCKET'], 
                                                   params['BUCKET_MANIFEST_TSV'], params['MANIFEST_FILE'], 
                                                   params['BQ_AS_BATCH'])
        if not manifest_success:
            print("Failure generating manifest")
            return

    #
    # Best practice is to clear out the directory where the files are going. Don't want anything left over:
    #
    
    if 'clear_target_directory' in steps:
        create_clean_target(params['LOCAL_FILES_DIR'])

    #
    # We need to create a "pull list" of gs:// URLs to pull from GDC buckets. If you have already
    # created a pull list, just plunk it in 'LOCAL_PULL_LIST' and skip this step. If creating a pull
    # list, you can do it using IndexD calls on a manifest file, OR using BQ as long as you have 
    # built the manifest using BQ (that route uses the BQ Manifest table that was created).
    #
    
    if 'build_pull_list' in steps:
        
        if params['USE_INDEXD_FOR_PULL']: 
            build_pull_list_with_indexd(params['MANIFEST_FILE'], 
                                        params['INDEXD_IDS_PER_CALL'],  
                                        params['INDEXD_URL'], params['LOCAL_PULL_LIST'])
        else:
            full_manifest = '{}.{}.{}'.format(params['WORKING_PROJECT'], 
                                              params['TARGET_DATASET'], 
                                              params['BQ_MANIFEST_TABLE'])
    
            build_pull_list_with_bq(full_manifest, params['INDEXD_BQ_TABLE'], 
                                    params['WORKING_PROJECT'], params['TARGET_DATASET'],  
                                    params['BQ_PULL_LIST_TABLE'], 
                                    params['WORKING_BUCKET'], 
                                    params['BUCKET_PULL_LIST'],
                                    params['LOCAL_PULL_LIST'], params['BQ_AS_BATCH'])
 
    #
    # Now hitting GDC cloud buckets, not "downloading". Get the files in the pull list:
    #

    if 'download_from_gdc' in steps:       
        with open(params['LOCAL_PULL_LIST'], mode='r') as pull_list_file:
            pull_list = pull_list_file.read().splitlines()
        print("Preparing to download %s files from buckets\n" % len(pull_list))
        bp = BucketPuller(10)
        bp.pull_from_buckets(pull_list, params['LOCAL_FILES_DIR'])

    #
    # Traverse the tree of downloaded files and create a flat list of all files:
    #
    
    if 'build_traversal_list' in steps:
        all_files = build_file_list(params['LOCAL_FILES_DIR'])
        with open(params['FILE_TRAVERSAL_LIST'], mode='w') as traversal_list:
            for line in all_files:
                traversal_list.write("{}\n".format(line)) 
   
    #
    # Take all the files and make one BIG TSV file to upload:
    #
    
    print("fix me have to toss out NA rows!")
    if 'concat_all_files' in steps:       
        with open(params['FILE_TRAVERSAL_LIST'], mode='r') as traversal_list_file:
            all_files = traversal_list_file.read().splitlines()  
        concat_all_files_selected_cols(all_files, params['ONE_BIG_TSV'], 
                                       params['PROGRAM_PREFIX'], retain_cols, extra_cols, 
                                       file_info, None, "Beta_value", "NA")

    #
    # Build the platform reference table
    #
    
    if 'build_plat_ref' in steps:       
        with open(params['FILE_TRAVERSAL_LIST'], mode='r') as traversal_list_file:
            all_files = traversal_list_file.read().splitlines()  
        concat_all_files_selected_cols(all_files, params['ONE_BIG_REF_TSV'], 
                                       params['PROGRAM_PREFIX'], retain_platform_ref_fields, [], 
                                       file_info, None, None, None)  
        set_from_file(params['ONE_BIG_REF_TSV'], params['ONE_BIG_DISTINCT_REF_TSV'])    
            
    #
    # For the legacy table, the descriptions had lots of analysis tidbits. Very nice, but hard to maintain.
    # We just use hardwired schema descriptions now, most directly pulled from the GDC website:
    #
    
    if 'build_the_schema' in steps:
        typing_tups = build_schema(params['ONE_BIG_TSV'], params['SCHEMA_SAMPLE_SKIPS'])
        build_combined_schema(None, params['AUGMENTED_SCHEMA_FILE'], 
                              typing_tups, params['HOLD_SCHEMA_LIST'], params['HOLD_SCHEMA_DICT'])
         
    #
    # Upload the giant TSV into a cloud bucket:
    #
    
    if 'upload_to_bucket' in steps:
        upload_to_bucket(params['WORKING_BUCKET'], params['BUCKET_SKEL_TSV'], params['ONE_BIG_TSV'])

    #
    # Create the BQ table from the TSV:
    #
        
    if 'create_bq_from_tsv' in steps:
        bucket_src_url = 'gs://{}/{}'.format(params['WORKING_BUCKET'], params['BUCKET_SKEL_TSV'])
        with open(params['HOLD_SCHEMA_LIST'], mode='r') as schema_hold_dict:
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
    # The derived table we generate has no field descriptions. Add them from the scraped page:
    #
    
    if 'update_final_schema' in steps:    
        success = update_schema(params['TARGET_DATASET'], params['FINAL_TARGET_TABLE'], params['HOLD_SCHEMA_DICT']) 
        if not success:
            print("Schema update failed")
            return       
    
    #
    # Add the table description:
    #
    
    if 'add_table_description' in steps:  
        update_description(params['TARGET_DATASET'], params['FINAL_TARGET_TABLE'], params['TABLE_DESCRIPTION'])    
      
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


# In[14]:


if __name__ == "__main__":
    main(sys.argv)

