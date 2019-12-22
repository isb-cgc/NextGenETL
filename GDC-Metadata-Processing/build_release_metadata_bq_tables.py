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

NOTE AS CURRENTLY SET UP IT DOES NOT PULL IN GENE EXPRESSION TEXT FILES
Inventory as of Release 18 for files that are not multi-case ("multi" or ";" delimited)
program_name	file_type	data_format	count
BEATAML1.0	simple_somatic_mutation	VCF	103
BEATAML1.0	aligned_reads	BAM	119
CGCI	biospecimen_supplement	BCR PPS XML	120
CGCI	biospecimen_supplement	BCR XML	120
CGCI	biospecimen_supplement	BCR SSF XML	93
CGCI	clinical_supplement	BCR XML	117
CGCI	aligned_reads	BAM	500
CGCI	gene_expression	TSV	226
CGCI	gene_expression	TXT	339
CPTAC	aligned_reads	BAM	3227
CPTAC	annotated_somatic_mutation	VCF	1296
CPTAC	gene_expression	TXT	1551
CPTAC	gene_expression	TSV	1034
CPTAC	simple_somatic_mutation	VCF	1296
CTSP	aligned_reads	BAM	212
CTSP	gene_expression	TSV	82
CTSP	gene_expression	TXT	123
FM	simple_somatic_mutation	VCF	18004
FM	annotated_somatic_mutation	VCF	18004
HCMI	biospecimen_supplement	BCR XML	7
HCMI	annotated_somatic_mutation	VCF	56
HCMI	aligned_reads	BAM	84
HCMI	gene_expression	TXT	42
HCMI	gene_expression	TSV	28
HCMI	simple_somatic_mutation	VCF	56
MMRF	annotated_somatic_mutation	VCF	4368
MMRF	aligned_reads	BAM	6577
MMRF	gene_expression	TXT	2577
MMRF	gene_expression	TSV	1718
MMRF	simple_somatic_mutation	VCF	4368
NCICCR	aligned_reads	BAM	2400
NCICCR	gene_expression	TXT	1443
NCICCR	gene_expression	TSV	962
ORGANOID	aligned_reads	BAM	298
ORGANOID	annotated_somatic_mutation	VCF	65
ORGANOID	gene_expression	TXT	165
ORGANOID	gene_expression	TSV	110
ORGANOID	simple_somatic_mutation	VCF	65
TARGET	aligned_reads	BAM	3121
TARGET	gene_expression	TXT	2130
TARGET	annotated_somatic_mutation	VCF	1615
TARGET	mirna_expression	TXT	1170
TARGET	simple_somatic_mutation	VCF	2050
TARGET	gene_expression	TSV	472
TCGA	aligned_reads	BAM	44873
TCGA	biospecimen_supplement	BCR XML	11314
TCGA	gene_expression	TXT	33279
TCGA	methylation_beta_value	TXT	12359
TCGA	slide_image	SVS	30072
TCGA	mirna_expression	TXT	22164
TCGA	copy_number_segment	TXT	45258
TCGA	biospecimen_supplement	BCR SSF XML	10557
TCGA	clinical_supplement	BCR XML	11167
TCGA	clinical_supplement	BCR OMF XML	1051
TCGA	biospecimen_supplement	BCR Auxiliary XML	2884
TCGA	simple_somatic_mutation	VCF	44461
TCGA	annotated_somatic_mutation	VCF	44461
VAREPOP	annotated_somatic_mutation	VCF	7
VAREPOP	aligned_reads	BAM	7
VAREPOP	simple_somatic_mutation	VCF	7

Distinct file types:

extract_alignment_file_data_sql:
  simple_somatic_mutation
  annotated_somatic_mutation
  aligned_reads

extract_file_data_sql_slides:
  slide_image

extract_file_data_sql_clinbio:
  biospecimen_supplement
  clinical_supplement

extract_other_file_data_sql:
  copy_number_segment
  gene_expression
  methylation_beta_value
  mirna_expression

Notes:
CGCI (only) has null aliquot IDS for "gene_expression" type (Rel 18)
FM clinbio entries = 0 because they are all multi-case (Rel 17)

'''

import yaml
import sys
import io

from common_etl.support import generic_bq_harness, confirm_google_vm, \
                               bq_harness_with_result, delete_table_bq_job

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

    return (yaml_dict['files_and_buckets_and_tables'], yaml_dict['steps'], 
            yaml_dict['builds'], yaml_dict['build_tags'], yaml_dict['path_tags'],
            yaml_dict['programs'], yaml_dict['filter_sets'])

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
def extract_aligned_file_data(release_table, program_name, filter_list, target_dataset, dest_table, do_batch):

    sql = extract_alignment_file_data_sql(release_table, program_name, filter_list)
    print(sql)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)

'''
----------------------------------------------------------------------------------------------
SQL for above:
'''
def extract_alignment_file_data_sql(release_table, program_name, filter_list):

    terms = []
    for pair in filter_list:
        print(pair)
        print(type(pair))

        for key_vals in pair.items():
            terms.append('a.{} = "{}"'.format(key_vals[0], key_vals[1]))

    filter_term = " OR ".join(terms)

    # (a.file_type = "copy_number_segment"
    # OR
    # a.file_type = "gene_expression"
    # OR
    # a.file_type = "methylation_beta_value"
    # OR
    # # CGCI has null aliquot IDS for gene_expression type:
    # a.file_type = "mirna_expression" )  # AND
    # # ( a.associated_entities__entity_type ="aliquot" )



    return '''
        SELECT 
            a.file_id as file_gdc_id,
            a.case_gdc_id,
            # When there are two aliquots (tumor/normal VCFs, it looks like the target table is using the second
            # no matter what it is...
            CASE WHEN (STRPOS(a.associated_entities__entity_gdc_id, ";") != 0)
                 THEN REGEXP_EXTRACT(a.associated_entities__entity_gdc_id,
                                     r"^[a-zA-Z0-9-]+;([a-zA-Z0-9-]+)$") 
              ELSE a.associated_entities__entity_gdc_id
            END as aliquot_id,
            a.project_short_name, # TCGA-OV
            # Some names have two hyphens, not just one:
            CASE WHEN (a.project_short_name LIKE '%-%-%') THEN
                   REGEXP_EXTRACT(a.project_short_name, r"^[A-Z]+-([A-Z]+)-[A-Z0-9]+$")
                 ELSE
                   REGEXP_EXTRACT(a.project_short_name, r"^[A-Z]+-([A-Z]+$)")
            END as disease_code, # OV
            a.program_name, # TCGA
            # TARGET LEGACY needs this ditched:
            # a.experimental_strategy as data_type,
            a.data_type,
            a.data_category,
            a.experimental_strategy,
            a.file_type as [type],
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
        WHERE ( a.program_name = '{1}' ) AND ( {2} )
        '''.format(release_table, program_name, filter_term)

'''
----------------------------------------------------------------------------------------------
Slide extraction
'''
def extract_slide_file_data(release_table, program_name, target_dataset, dest_table, do_batch):

    sql = extract_file_data_sql_slides(release_table, program_name)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)

'''
----------------------------------------------------------------------------------------------
SQL for above:
'''
def extract_file_data_sql_slides(release_table, program_name):
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
            a.file_type as [type],
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
        # Do not restrict type
        # WHERE a.program_name = '{1}' AND ( a.type = "slide_image" AND a.data_format = "SVS" )
        WHERE a.program_name = '{1}' AND a.type = "slide_image"
        '''.format(release_table, program_name)

'''
----------------------------------------------------------------------------------------------
Clinical extraction (CLIN and BIO files):
'''
def extract_clinbio_file_data(release_table, program_name, target_dataset, dest_table, do_batch):

    sql = extract_file_data_sql_clinbio(release_table, program_name)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)

'''
----------------------------------------------------------------------------------------------
SQL for above:
'''
def extract_file_data_sql_clinbio(release_table, program_name):
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
            a.file_type as [type],
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
              # Do not restrict the data format:
              #( ( a.type = "clinical_supplement" AND a.data_format = "BCR XML" ) OR
              #  ( a.type = "biospecimen_supplement" AND a.data_format = "BCR XML" ) ) AND
              ( a.type = "clinical_supplement" OR a.type = "biospecimen_supplement" ) AND
              ( a.associated_entities__entity_type = "case" ) AND
              # This dropping of multi-case entries makes FM table empty:
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

    sql = case_barcodes_sql(release_table, aliquot_2_case_table, program_name)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)

'''
----------------------------------------------------------------------------------------------
SQL for above:
'''
def case_barcodes_sql(release_table, aliquot_2_case_table, program_name):
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
            # FIXME DO WE WANT TO DROP THIS ARCHIVAL FIX???
            # Archival table had null in this slot:
            CAST(null AS STRING) as data_type,
            a.data_category,
            a.experimental_strategy,
            a.type,
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

    sql = aliquot_barcodes_sql(release_table, aliquot_2_case_table, program_name)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)

'''
----------------------------------------------------------------------------------------------
SQL for above:
'''
def aliquot_barcodes_sql(release_table, aliquot_2_case_table, program_name):

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
            a.type,
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

    sql = slide_barcodes_sql(release_table, slide_2_case_table, program_name)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)

'''
----------------------------------------------------------------------------------------------
SQL for above:
'''
def slide_barcodes_sql(release_table, slide_2_case_table, program_name):

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
            a.type,
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
def build_union(table_list, target_dataset, dest_table, do_batch):

    sql = union_sql(table_list)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)


'''
----------------------------------------------------------------------------------------------
SQL for above:
'''
def union_sql(table_list):
    terms = []
    for table in table_list:
        terms.append("SELECT * FROM `{0}`".format(table))
    filter_term = " UNION ALL ".join(terms)
    return filter_term

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
            a.type,
            a.file_size,
            a.data_format,
            a.platform,
            c.gcs_path as file_name_key,
            a.index_file_id,
            a.index_file_name_key,
            a.index_file_size,
            a.access,
            a.acl
        FROM `{0}` AS a LEFT OUTER JOIN `{1}` AS c ON a.file_gdc_id = c.file_uuid )
        
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
            a1.type,
            a1.file_size,
            a1.data_format,
            a1.platform,
            a1.file_name_key,
            a1.index_file_id,
            c.gcs_path as index_file_name_key,
            a1.index_file_size,
            a1.access,
            a1.acl
        FROM a1 LEFT OUTER JOIN `{1}` AS c ON a1.index_file_id = c.file_uuid
        '''.format(union_table, mapping_table)


'''
----------------------------------------------------------------------------------------------
Do all the steps for a given dataset and build
'''


def do_dataset_and_build(steps, build, build_tag, path_tag, filter_list, dataset, params):

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
        success = extract_aligned_file_data(file_table, dataset, filter_list['sequence'], params['TARGET_DATASET'],
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
        table_list = [slide_table, align_table, clinbio_table]

        union_table = "{}_{}_{}".format(dataset, build, params['UNION_TABLE'])
        table_collection.append('{}.{}.{}'.format(params['WORKING_PROJECT'], params['TARGET_DATASET'], union_table))
        success = build_union(table_list,
                              params['TARGET_DATASET'], union_table, params['BQ_AS_BATCH'])
        if not success:
            print("{} {} union_tables job failed".format(dataset, build))
            return False


    # Merge the barcode info into the final table we are building:


    if 'create_final_table' in steps:
        union_table = '{}.{}.{}'.format(params['WORKING_PROJECT'], 
                                        params['TARGET_DATASET'], 
                                        "{}_{}_{}".format(dataset, build, params['UNION_TABLE']))        
        success = install_uris(union_table, "{}{}".format(params['UUID_2_URL_TABLE'], path_tag),
                               params['TARGET_DATASET'], 
                               "{}_{}_{}".format(dataset, build, params['FINAL_TABLE']), params['BQ_AS_BATCH'])
        if not success:
            print("{} {} create_final_table job failed".format(dataset, build))
            return False

    #
    # Clear out working temp tables:
    #

    if 'dump_working_tables' in steps:
        dump_table_tags = ['SLIDE_STEP_1_TABLE', 'SLIDE_STEP_2_TABLE', 'ALIGN_STEP_1_TABLE',
                           'ALIGN_STEP_2_TABLE', 'CLINBIO_STEP_1_TABLE', 'CLINBIO_STEP_2_TABLE']
        dump_tables = ["{}_{}_{}".format(dataset, build, params[x]) for x in dump_table_tags]
        for table in dump_tables:
            delete_table_bq_job(params['TARGET_DATASET'], table)

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
        params, steps, builds, build_tags, path_tags, programs, filter_sets = load_config(yaml_file.read())

    if params is None:
        print("Bad YAML load")
        return

    for build, build_tag, path_tag in zip(builds, build_tags, path_tags):
        file_table = "{}_{}".format(params['FILE_TABLE'], build_tag)
        datasets = programs
        if len(datasets) == 0:
            datasets = extract_program_names(file_table, params['BQ_AS_BATCH'])
        for dataset in datasets:
            filter_list = filter_sets[dataset][build_tag]
            print(filter_list)
            print ("Processing build {} ({}) for program {}".format(build, build_tag, dataset))
            ok = do_dataset_and_build(steps, build, build_tag, path_tag, filter_list, dataset, params)
            if not ok:
                return
            
    print('job completed')

if __name__ == "__main__":
    main(sys.argv)

