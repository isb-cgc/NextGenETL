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

Extract GDC Metadata into Per-Project/Build File BQ Tables
This is still a work in progress (10/16/19)

'''

import yaml
import sys
import io
import requests
from json import loads as json_loads

from common_etl.support import generic_bq_harness, confirm_google_vm, bq_harness_with_result

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

    return (yaml_dict['files_and_buckets_and_tables'], yaml_dict['steps'], 
            yaml_dict['builds'], yaml_dict['build_tags'])

'''
----------------------------------------------------------------------------------------------
Figure out the programs represented in the data
'''
def extract_program_names(release_table, do_batch):

    sql = extract_program_names_sql(release_table)
    results = bq_harness_with_result(sql, do_batch)
    retval = []
    for row in results:
        pn = row.program_name
        if pn is not None and pn != "None":
            retval.append(pn.replace(".", "_")) # handles BEATAML1.0 FIXME! Make it general
    return retval

'''
----------------------------------------------------------------------------------------------
SQL for above:
'''
def extract_program_names_sql(release_table):
    return '''
        SELECT DISTINCT program_name FROM `{0}` # program_name
        '''.format(release_table)

'''
----------------------------------------------------------------------------------------------
BAM and VCF extraction: BAMS, simple somatic, and annotated somatic VCFs in the target table
'''
def extract_aligned_file_data(release_table, program_name, target_dataset, dest_table, do_batch):

    sql = extract_file_data_sql(release_table, program_name)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)

'''
----------------------------------------------------------------------------------------------
SQL for above:
'''
def extract_file_data_sql(release_table, program_name):
    return '''
        SELECT 
            a.file_id as file_gdc_id,
            a.case_gdc_id,
            # When there are two aliquots (tumor/normal VCFs, it looks like the target table is using the second
            # no matter what is is...
            CASE WHEN (STRPOS(a.associated_entities__entity_gdc_id, ";") != 0)
                 THEN REGEXP_EXTRACT(a.associated_entities__entity_gdc_id,
                                     r"^[a-zA-Z0-9-]+;([a-zA-Z0-9-]+)$") 
              ELSE a.associated_entities__entity_gdc_id
            END as aliquot_id,
            a.project_short_name, # TCGA-OV
            REGEXP_EXTRACT(a.project_short_name, r"^[A-Z]+-([A-Z]+$)") as disease_code, # OV
            a.program_name, # TCGA
            #CASE WHEN (a.data_type = "Annotated Somatic Mutation") OR (a.data_type = "Raw Simple Somatic Mutation")
            #     THEN "WXS" 
            #  ELSE a.data_type
            a.experimental_strategy as data_type,
            a.data_category,
            a.experimental_strategy,
            a.file_type,
            a.file_size,
            a.data_format,
            a.platform,
            CAST(null AS STRING) as file_name_key,
            a.index_file_gdc_id as index_file_id,
            CAST(null AS STRING) as index_file_name_key,
            a.index_file_size,
            a.access,
            a.acl
        FROM `{0}` AS a
        WHERE ( a.program_name = '{1}' ) AND
              ( ( a.file_type = "simple_somatic_mutation" AND a.data_format = "VCF" ) OR
                ( a.file_type = "annotated_somatic_mutation" AND a.data_format = "VCF" ) OR
                ( a.file_type = "aligned_reads" AND a.data_format = "BAM" ) ) AND
              ( a.associated_entities__entity_type ="aliquot" )
        '''.format(release_table, program_name)


'''
----------------------------------------------------------------------------------------------
Slide extraction
'''
def extract_slide_file_data(release_table, program_name, target_dataset, dest_table, do_batch):

    sql = extract_file_data_sql_archived_slides(release_table, program_name)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)

'''
----------------------------------------------------------------------------------------------
SQL for above:
'''
def extract_file_data_sql_archived_slides(release_table, program_name):
    return '''
        SELECT 
            a.file_id as file_gdc_id,
            a.case_gdc_id,
            a.associated_entities__entity_gdc_id as slide_id,
            a.project_short_name, # TCGA-OV
            REGEXP_EXTRACT(a.project_short_name, r"^[A-Z]+-([A-Z]+$)") as disease_code, # OV
            a.program_name, # TCGA
            CASE WHEN (a.experimental_strategy = "Diagnostic Slide") 
                 THEN "Diagnostic image" 
                 WHEN (a.experimental_strategy = "Tissue Slide") 
                 THEN "Tissue slide image" 
            END as data_type,
            a.data_category,
            CAST(null AS STRING) as experimental_strategy,
            a.file_type,
            a.file_size,
            a.data_format,
            a.platform,
            CAST(null AS STRING) as file_name_key,
            CAST(null AS STRING) as index_file_id,
            CAST(null AS STRING) as index_file_name_key,
            CAST(null AS INT64) as index_file_size,
            a.access,
            a.acl
        FROM `{0}` AS a
        WHERE a.program_name = '{1}' AND ( a.file_type = "slide_image" AND a.data_format = "SVS" )
        '''.format(release_table, program_name)


'''
----------------------------------------------------------------------------------------------
Clinical extraction (CLIN and BIO files):
'''
def extract_clinbio_file_data(release_table, program_name, target_dataset, dest_table, do_batch):

    sql = extract_file_data_sql_archived_clinbio(release_table, program_name)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)

'''
----------------------------------------------------------------------------------------------
SQL for above:
'''
def extract_file_data_sql_archived_clinbio(release_table, program_name):
    return '''
        SELECT 
            a.file_id as file_gdc_id,
            a.case_gdc_id,
            a.associated_entities__entity_gdc_id as case_id,
            a.project_short_name, # TCGA-OV
            REGEXP_EXTRACT(a.project_short_name, r"^[A-Z]+-([A-Z]+$)") as disease_code, # OV
            a.program_name, # TCGA
            a.data_type,    
            a.data_category,
            a.experimental_strategy,
            a.file_type,
            a.file_size,
            a.data_format,
            a.platform,
            CAST(null AS STRING) as file_name_key,
            CAST(null AS STRING) as index_file_id,
            CAST(null AS STRING) as index_file_name_key,
            CAST(null AS INT64) as index_file_size,
            a.access,
            a.acl
        FROM `{0}` AS a
        WHERE ( a.program_name = '{1}' ) AND
              ( ( a.file_type = "clinical_supplement" AND a.data_format = "BCR XML" ) OR
                ( a.file_type = "biospecimen_supplement" AND a.data_format = "BCR XML" ) ) AND
              ( a.associated_entities__entity_type = "case" ) AND
              # Armor against multiple case entries:
              ( a.case_gdc_id NOT LIKE "%;%" )  AND
              # Armor against multiple case entries:
              ( a.case_gdc_id != "multi" )
        '''.format(release_table, program_name)


'''
----------------------------------------------------------------------------------------------
Get case barcodes associated with the clinical files:
'''
def extract_case_barcodes(release_table, aliquot_2_case_table, program_name, target_dataset, dest_table, do_batch):

    sql = case_barcodes_sql_archive(release_table, aliquot_2_case_table, program_name)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)

'''
----------------------------------------------------------------------------------------------
SQL for above:
'''
def case_barcodes_sql_archive(release_table, aliquot_2_case_table, program_name):
    return '''
        WITH
        a1 AS (SELECT DISTINCT case_gdc_id, case_barcode FROM `{1}` GROUP BY case_gdc_id, case_barcode)            
        SELECT
            a.file_gdc_id,
            a.case_gdc_id,
            a1.case_barcode,
            "NA" as sample_gdc_id,
            "NA" as sample_barcode,
            a.project_short_name,
            a.disease_code,
            a.program_name,
            # Archival table had null in this slot:
            CAST(null AS STRING) as data_type,
            a.data_category,
            a.experimental_strategy,
            a.file_type,
            a.file_size,
            a.data_format,
            a.platform,
            a.file_name_key,
            a.index_file_id,
            a.index_file_name_key,
            a.index_file_size,
            a.access,
            a.acl
        FROM `{0}` AS a JOIN a1 ON a.case_id = a1.case_gdc_id
        '''.format(release_table, aliquot_2_case_table, program_name)


'''
----------------------------------------------------------------------------------------------
Get sample and case barcodes associated with the sequence files:
'''
def extract_aliquot_barcodes(release_table, aliquot_2_case_table, program_name, target_dataset, dest_table, do_batch):

    sql = aliquot_barcodes_sql_archive(release_table, aliquot_2_case_table, program_name)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)

'''
----------------------------------------------------------------------------------------------
SQL for above:
'''
def aliquot_barcodes_sql_archive(release_table, aliquot_2_case_table, program_name):

    return '''
        SELECT
            a.file_gdc_id,
            a.case_gdc_id,
            c.case_barcode,
            c.sample_gdc_id,
            c.sample_barcode,
            a.project_short_name,
            a.disease_code,
            a.program_name,
            a.data_type,
            a.data_category,
            a.experimental_strategy,
            a.file_type,
            a.file_size,
            a.data_format,
            a.platform,
            a.file_name_key,
            a.index_file_id,
            a.index_file_name_key,
            a.index_file_size,
            a.access,
            a.acl
        FROM `{0}` AS a JOIN `{1}` AS c ON a.aliquot_id = c.aliquot_gdc_id
        '''.format(release_table, aliquot_2_case_table, program_name)


'''
----------------------------------------------------------------------------------------------
Get sample and case barcodes associated with the slide files:
'''
def extract_slide_barcodes(release_table, slide_2_case_table, program_name, target_dataset, dest_table, do_batch):

    sql = slide_barcodes_sql_archive(release_table, slide_2_case_table, program_name)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)

'''
----------------------------------------------------------------------------------------------
SQL for above:
'''
def slide_barcodes_sql_archive(release_table, slide_2_case_table, program_name):

    return '''
        # Some slides have two entries in the slide_2_case table if they depict two portions. Remove the dups:
        WITH a1 as (
        SELECT DISTINCT
            case_barcode,
            sample_gdc_id,
            sample_barcode,
            slide_gdc_id
        FROM `{1}` GROUP BY case_barcode, sample_gdc_id, sample_barcode, slide_gdc_id )
        SELECT
            a.file_gdc_id,
            a.case_gdc_id,
            a1.case_barcode,
            a1.sample_gdc_id,
            a1.sample_barcode,
            a.project_short_name,
            a.disease_code,
            a.program_name,
            a.data_type,
            a.data_category,
            a.experimental_strategy,
            a.file_type,
            a.file_size,
            a.data_format,
            a.platform,
            a.file_name_key,
            a.index_file_id,
            a.index_file_name_key,
            a.index_file_size,
            a.access,
            a.acl
        FROM `{0}` AS a JOIN a1 ON a.slide_id = a1.slide_gdc_id
        '''.format(release_table, slide_2_case_table, program_name)

'''
----------------------------------------------------------------------------------------------
Glue different tables together:
'''
def build_union(slide_table, align_table, clin_table, target_dataset, dest_table, do_batch):

    sql = union_sql(slide_table, align_table, clin_table)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)


'''
----------------------------------------------------------------------------------------------
SQL for above:
'''
def union_sql(slide_table, align_table, clin_table):
    return '''
        SELECT * FROM `{0}`
        UNION ALL
        SELECT * FROM `{1}`
        UNION ALL
        SELECT * FROM `{2}`
        '''.format(slide_table, align_table, clin_table)

'''
----------------------------------------------------------------------------------------------
Final Step:
Get the URIs in from the manifest file:
Two passes used, first for all files, second for index files:
'''
def install_uris(union_table, mapping_table, target_dataset, dest_table, do_batch):
    
    sql = install_uris_sql(union_table, mapping_table)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)

'''
----------------------------------------------------------------------------------------------
SQL for above:
'''
def install_uris_sql(union_table, mapping_table):    
    return '''
        WITH a1 as (
        SELECT
            a.file_gdc_id,
            a.case_gdc_id,
            a.case_barcode,
            a.sample_gdc_id,
            a.sample_barcode,
            a.project_short_name,
            a.disease_code,
            a.program_name,
            a.data_type,
            a.data_category,
            a.experimental_strategy,
            a.file_type,
            a.file_size,
            a.data_format,
            a.platform,
            c.file_gdc_url as file_name_key,
            a.index_file_id,
            a.index_file_name_key,
            a.index_file_size,
            a.access,
            a.acl
        FROM `{0}` AS a LEFT OUTER JOIN `{1}` AS c ON a.file_gdc_id = c.file_gdc_id )
        
        SELECT
            a1.file_gdc_id,
            a1.case_gdc_id,
            a1.case_barcode,
            a1.sample_gdc_id,
            a1.sample_barcode,          
            a1.project_short_name,
            a1.disease_code,
            a1.program_name,
            a1.data_type,
            a1.data_category,
            a1.experimental_strategy,
            a1.file_type,
            a1.file_size,
            a1.data_format,
            a1.platform,
            a1.file_name_key,
            a1.index_file_id,
            c.file_gdc_url as index_file_name_key,
            a1.index_file_size,
            a1.access,
            a1.acl
        FROM a1 LEFT OUTER JOIN `{1}` AS c ON a1.index_file_id = c.file_gdc_id        
        '''.format(union_table, mapping_table)


'''
----------------------------------------------------------------------------------------------
Do all the steps for a given dataset and build
'''
def do_dataset_and_build(steps, build, build_tag, dataset, params):

    file_table = "{}_{}".format(params['FILE_TABLE'], build_tag)
    
    table_collection = []
    
    #
    # Pull stuff from rel:
    #
     
    if 'pull_slides' in steps:        
        step_one_table = "{}_{}_{}".format(dataset, build, params['SLIDE_STEP_1_TABLE'])
        table_collection.append('{}.{}.{}'.format(params['WORKING_PROJECT'], params['TARGET_DATASET'], step_one_table))
        success = extract_slide_file_data(file_table, dataset, params['TARGET_DATASET'], 
                                          step_one_table, params['BQ_AS_BATCH'])

        if not success:
            print("{} {} pull_slides job failed".format(dataset, build))
            return False  
        
    if 'pull_align' in steps:
        step_one_table = "{}_{}_{}".format(dataset, build, params['ALIGN_STEP_1_TABLE'])
        table_collection.append('{}.{}.{}'.format(params['WORKING_PROJECT'], params['TARGET_DATASET'], step_one_table))
        success = extract_aligned_file_data(file_table, dataset, params['TARGET_DATASET'], 
                                            step_one_table, params['BQ_AS_BATCH'])        
        if not success:
            print("{} {} pull_align job failed".format(dataset, build))
            return False    

        
    if 'pull_clinbio' in steps:
        step_one_table = "{}_{}_{}".format(dataset, build, params['CLINBIO_STEP_1_TABLE'])
        table_collection.append('{}.{}.{}'.format(params['WORKING_PROJECT'], params['TARGET_DATASET'], step_one_table))
        success = extract_clinbio_file_data(file_table, dataset, params['TARGET_DATASET'], 
                                            step_one_table, params['BQ_AS_BATCH']) 
        if not success:
            print("{} {} pull_clinbio job failed".format(dataset, build))
            return False      
        
    if 'slide_barcodes' in steps:
        in_table = '{}.{}.{}'.format(params['WORKING_PROJECT'], 
                                     params['TARGET_DATASET'], 
                                     "{}_{}_{}".format(dataset, build, params['SLIDE_STEP_1_TABLE']))
        step_two_table = "{}_{}_{}".format(dataset, build, params['SLIDE_STEP_2_TABLE'])
        table_collection.append('{}.{}.{}'.format(params['WORKING_PROJECT'], params['TARGET_DATASET'], step_two_table))
        success = extract_slide_barcodes(in_table, params['SLIDE_TABLE'], dataset, params['TARGET_DATASET'], 
                                         step_two_table, params['BQ_AS_BATCH'])

        if not success:
            print("{} {} slide_barcodes job failed".format(dataset, build))
            return False
        
    if 'align_barcodes' in steps:                                         
        in_table = '{}.{}.{}'.format(params['WORKING_PROJECT'], 
                                     params['TARGET_DATASET'], 
                                     "{}_{}_{}".format(dataset, build, params['ALIGN_STEP_1_TABLE']))

        step_two_table = "{}_{}_{}".format(dataset, build, params['ALIGN_STEP_2_TABLE'])
        table_collection.append('{}.{}.{}'.format(params['WORKING_PROJECT'], params['TARGET_DATASET'], step_two_table))
        success = extract_aliquot_barcodes(in_table, params['ALIQUOT_TABLE'], dataset, params['TARGET_DATASET'], 
                                           step_two_table, params['BQ_AS_BATCH'])

        if not success:
            print("{} {} align_barcodes job failed".format(dataset, build))
            return False
        
    if 'clinbio_barcodes' in steps:
        in_table = '{}.{}.{}'.format(params['WORKING_PROJECT'], 
                                     params['TARGET_DATASET'], 
                                     "{}_{}_{}".format(dataset, build, params['CLINBIO_STEP_1_TABLE']))

        step_two_table = "{}_{}_{}".format(dataset, build, params['CLINBIO_STEP_2_TABLE'])
        table_collection.append('{}.{}.{}'.format(params['WORKING_PROJECT'], params['TARGET_DATASET'], step_two_table))
        success = extract_case_barcodes(in_table, params['ALIQUOT_TABLE'], dataset, params['TARGET_DATASET'], 
                                        step_two_table, params['BQ_AS_BATCH'])

        if not success:
            print("{} {} clin_barcodes job failed".format(dataset, build))
            return False                 
        
    if 'union_tables' in steps:
        slide_table = '{}.{}.{}'.format(params['WORKING_PROJECT'], 
                                        params['TARGET_DATASET'], 
                                        "{}_{}_{}".format(dataset, build, params['SLIDE_STEP_2_TABLE']))
        align_table = '{}.{}.{}'.format(params['WORKING_PROJECT'], 
                                        params['TARGET_DATASET'], 
                                        "{}_{}_{}".format(dataset, build, params['ALIGN_STEP_2_TABLE']))
        clinbio_table = '{}.{}.{}'.format(params['WORKING_PROJECT'], 
                                          params['TARGET_DATASET'], 
                                          "{}_{}_{}".format(dataset, build, params['CLINBIO_STEP_2_TABLE']))        

        union_table = "{}_{}_{}".format(dataset, build, params['UNION_TABLE'])
        table_collection,append('{}.{}.{}'.format(params['WORKING_PROJECT'], params['TARGET_DATASET'], union_table))
        success = build_union(slide_table, align_table, clinbio_table,
                              params['TARGET_DATASET'], union_table, params['BQ_AS_BATCH'])
        if not success:
            print("{} {} union_tables job failed".format(dataset, build))
            return False
   
    #
    # Merge the barcode info into the final table we are building:
    #

    if 'create_final_table' in steps:
        union_table = '{}.{}.{}'.format(params['WORKING_PROJECT'], 
                                        params['TARGET_DATASET'], 
                                        "{}_{}_{}".format(dataset, build, params['UNION_TABLE']))        
        success = install_uris(union_table, params['UUID_2_URL_TABLE'], 
                               params['TARGET_DATASET'], 
                               "{}_{}_{}".format(dataset, build, params['FINAL_TABLE']), params['BQ_AS_BATCH'])
        if not success:
            print("{} {} create_final_table job failed".format(dataset, build))
            return False

    #
    # Done!
    #
    
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
        params, steps, builds, build_tags = load_config(yaml_file.read())

    if params is None:
        print("Bad YAML load")
        return

    count = 0
    for build in builds:
        build_tag = build_tags[count]
        count += 1
        file_table = "{}_{}".format(params['FILE_TABLE'], build_tag)
        datasets = extract_program_names(file_table, params['BQ_AS_BATCH'])
        for dataset in datasets:
            print ("Processing build {} ({}) for program {}".format(build, build_tag, dataset))  
            ok = do_dataset_and_build(steps, build, build_tag, dataset, params)
            if not ok:
                return
            
    print('job completed')

if __name__ == "__main__":
    main(sys.argv)

