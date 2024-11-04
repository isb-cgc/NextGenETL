"""

Copyright 2022, Institute for Systems Biology

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

import sys, shutil, os, io, re
import zipfile, gzip
import yaml, json
from git import Repo
from os.path import expanduser
from createSchemaP3 import build_schema
from datetime import date
from types import SimpleNamespace
from google.cloud import bigquery
from common_etl.support import create_clean_target, generic_bq_harness, upload_to_bucket, \
                               csv_to_bq_write_depo, delete_table_bq_job, confirm_google_vm, \
                               build_file_list, get_the_bq_manifest, BucketPuller, build_pull_list_with_bq, \
                               concat_all_files, generic_bq_harness_write_depo,retrieve_table_schema, \
                               update_schema_with_dict, install_labels_and_desc, \
                               compare_two_tables, publish_table, bq_harness_with_result
                               

# The configuration reader. Parses the YAML configuration into dictionaries
def load_config(yaml_config):
    yaml_dict = None
    config_stream = io.StringIO(yaml_config)
    try:  yaml_dict = yaml.load(config_stream, Loader=yaml.FullLoader)
    except yaml.YAMLError as ex:  print(ex)

    schema_repo_local = yaml_dict['files_folders_tables']['SCHEMA_REPO_LOCAL']
    schema_repo_branch = yaml_dict['files_folders_tables']['SCHEMA_REPO_BRANCH']
    schema_repo_url = yaml_dict['files_folders_tables']['SCHEMA_REPO_URL']
    pull_table_info_from_git( schema_repo_local, schema_repo_url, schema_repo_branch )
    
    mapping_file = yaml_dict['files_folders_tables']['METADATA_MAPPING']
    with open( f'{schema_repo_local}/{mapping_file}' ) as inf:
        yaml_dict['files_folders_tables']['MAPPING'] = json.loads( inf.read() )
    
    if yaml_dict is None:
        return None, None, None, None, None
    else:
        return yaml_dict['files_folders_tables'],\
               yaml_dict['filters'],\
               yaml_dict['table_description'],\
               yaml_dict['steps']

# Delete All Intermediate Tables and (Optionally) the Final Result:
def table_cleaner(dump_tables, delete_result):
    if delete_result:
        delete_table_bq_job(params.SCRATCH_DATASET, f'{draft_table}_{release}')
    for table in dump_tables:
        delete_table_bq_job(params.SCRATCH_DATASET, table)

# Associate Aliquot And Case IDs to File IDs
# BQ ETL step 2: find the case and aliquot gdc_ids that go with each gexp file
def build_aliquot_and_case(upload_table, file_table, target_dataset, output_table, write_depo, do_batch, program, case_aliquot_fix):
   if program == "TARGET":
      sql = attach_aliquot_and_case_ids_sql_with_fix(upload_table, file_table, case_aliquot_fix)
   else:
      sql = attach_aliquot_and_case_ids_sql( upload_table, file_table )
   return generic_bq_harness_write_depo( sql, target_dataset, output_table, do_batch, write_depo )

# The files we get from GDC just have gene and expression columns. What aliquot is this for?
# Use the file table to associate case and aliquot GDC ids for each file we are using.
def attach_aliquot_and_case_ids_sql(upload_table, file_table):
    return f'''
        WITH a1 AS (SELECT DISTINCT source_file_id
                FROM `{upload_table}`)
        SELECT b.project_short_name,
               b.case_gdc_id,
               b.analysis_input_file_gdc_ids,
               b.associated_entities__entity_gdc_id AS aliquot_gdc_id,
               b.file_name,
               b.file_gdc_id,
               b.platform
        FROM a1 JOIN `{file_table}` AS b ON a1.source_file_id = b.file_gdc_id
        WHERE b.associated_entities__entity_type = 'aliquot' '''


# The files we get from GDC just have gene and expression columns. What aliquot is this for?
# Use the file table to associate case and aliquot GDC ids for each file we are using and update 
# some case and aliquots for TARGET files.

def attach_aliquot_and_case_ids_sql_with_fix(upload_table, file_table, case_aliquot_fix):
   when_clauses = {}
   for field_name, values in case_aliquot_fix.items():
      when_clause = ""
      
      for correct, incorrect in values.items():
         when_clause = f'{when_clause} WHEN {field_name} = "{correct};{incorrect}" THEN "{correct}" WHEN {field_name} = "{incorrect};{correct}" THEN "{correct}"'
      
      when_clauses[field_name] = when_clause
   
   return f"""WITH
     a1 AS (
     SELECT
       DISTINCT source_file_id
     FROM
       `{upload_table}`),
     a2 AS (
     SELECT
       b.project_short_name,
       b.case_gdc_id,
       b.analysis_input_file_gdc_ids,
       b.associated_entities__entity_gdc_id AS aliquot_gdc_id,
       b.file_name,
       b.file_gdc_id,
       b.platform
     FROM
       a1
     JOIN
       `{file_table}` AS b
     ON
       a1.source_file_id = b.file_gdc_id
     WHERE
       b.associated_entities__entity_type = 'aliquot')
   SELECT
     project_short_name,
     CASE
      {when_clauses["case_gdc_id"]}
     ELSE
     case_gdc_id
   END
     AS case_gdc_id,
     analysis_input_file_gdc_ids,
     CASE
      {when_clauses["aliquot_gdc_id"]}
     ELSE
     aliquot_gdc_id
   END
     AS aliquot_gdc_id,
     file_name,
     file_gdc_id,
     platform
   FROM
     a2
   """
         



# Associate Barcodes for Aliquot And Case IDs
# BQ ETL step 3: attach aliquot and case barcodes for IDS
def attach_barcodes(step2_table, aliquot_table, case_table, target_dataset, output_table, do_replace, do_batch):
    sql = attach_barcodes_sql( step2_table, aliquot_table, case_table )
    return generic_bq_harness( sql, target_dataset, output_table, do_batch, do_replace )

# Get the barcodes for the aliquot and case IDs
# Except statement removes one duplicated column 
# GDC creates a randomized analyte + portion id for TARGET
def attach_barcodes_sql(step2_table, aliquot_table, case_table):
    return f'''
        WITH a1 AS (
          SELECT a.project_short_name,
                 b.case_barcode,
                 b.sample_barcode,
                 b.aliquot_barcode,
                 a.case_gdc_id,
                 b.sample_gdc_id,
                 a.aliquot_gdc_id,
                 b.sample_type_name,
                 a.file_gdc_id,
                 a.platform,
                 a.file_name
            FROM `{step2_table}` AS a 
            JOIN (SELECT DISTINCT * 
                  FROM (SELECT * EXCEPT (analyte_gdc_id, portion_gdc_id) FROM `{aliquot_table}`) ) AS b 
                    ON a.aliquot_gdc_id = b.aliquot_gdc_id)
        SELECT a1.project_short_name,
               a1.case_barcode,
               c.primary_site,
               a1.sample_type_name,
               a1.case_gdc_id,
               a1.sample_barcode,
               a1.aliquot_barcode,
               a1.sample_gdc_id,
               a1.aliquot_gdc_id,
               a1.file_gdc_id,
               a1.platform,
               a1.file_name
        FROM a1 JOIN `{case_table}` AS c ON a1.case_barcode = c.case_barcode and a1.project_short_name = c.project_id
        '''


# Merge Counts and Metadata
def glue_metadata(step3_table, count_table, target_dataset, output_table, do_replace, do_batch):
    sql = glue_metadata_sql( step3_table, count_table )
    return generic_bq_harness( sql, target_dataset, output_table, do_batch, do_replace )

# SQL code for above
def glue_metadata_sql(step3_table, count_table):
    return f"""
        SELECT a.project_short_name,
               a.case_barcode,
               a.primary_site,
               a.sample_type_name,
               a.sample_barcode,
               a.aliquot_barcode,
               REGEXP_EXTRACT(b.gene_id, r"^[^.]+") as Ensembl_gene_id,
               b.gene_id as Ensembl_gene_id_v,
               b.gene_name,
               b.gene_type,
               b.unstranded,
               b.stranded_first,
               b.stranded_second,
               b.tpm_unstranded,
               b.fpkm_unstranded,
               b.fpkm_uq_unstranded,
               a.case_gdc_id,
               a.sample_gdc_id,
               a.aliquot_gdc_id,
               a.file_gdc_id,
               a.platform,
               a.file_name
        FROM `{step3_table}` AS a JOIN `{count_table}` AS b ON a.file_gdc_id = b.source_file_id 
        WHERE gene_id <> '__no_feature'
            AND gene_id <> '__ambiguous' 
            AND gene_id <> '__too_low_aQual' 
            AND gene_id <> '__not_aligned' 
            AND gene_id <> '__alignment_not_unique' """



# Pull in Platform Info From Master File: Turns out the analysis files with counts do not have
# an associated platform. That info is (usually) attached to the original file the counts
# were derived from. Pull that data out so we can use it.
def extract_platform_for_files(step2_table, file_table, target_dataset, output_table, do_replace, do_batch):
    sql = extract_platform_for_files_sql( step2_table, file_table )
    return generic_bq_harness( sql, target_dataset, output_table, do_batch, do_replace )

def extract_platform_for_files_sql(step2_table, file_table):
    return f'''
        WITH
            a1 AS (SELECT DISTINCT analysis_input_file_gdc_ids
                   FROM `{step2_table}`),
            a2 AS (SELECT a1.analysis_input_file_gdc_ids,
                          b.platform
                   FROM a1 JOIN `{file_table}` as b ON a1.analysis_input_file_gdc_ids = b.file_gdc_id)
        SELECT
               b.project_short_name,
               b.analysis_input_file_gdc_ids,
               b.case_gdc_id,
               b.aliquot_gdc_id,
               b.file_name,
               b.file_gdc_id,
               a2.platform
        FROM `{step2_table}` AS b 
        JOIN a2 ON a2.analysis_input_file_gdc_ids = b.analysis_input_file_gdc_ids '''


def merge_samples_by_aliquot(input_table, output_table, target_dataset, do_batch):
    sql = merge_samples_by_aliquot_sql(input_table)
    return generic_bq_harness(sql, target_dataset, output_table, do_batch, 'TRUE')

def merge_samples_by_aliquot_sql(input_table):
    return f"""
    SELECT 
        project_short_name,
        primary_site,
        case_barcode, 
        string_agg(sample_barcode, ';') as sample_barcode,
        aliquot_barcode,
        gene_name,
        gene_type,
        Ensembl_gene_id,
        Ensembl_gene_id_v,
        unstranded,
        stranded_first,
        stranded_second,
        tpm_unstranded,
        fpkm_unstranded,
        fpkm_uq_unstranded,
        sample_type_name,
        case_gdc_id,
        string_agg(sample_gdc_id, ';') as sample_gdc_id,
        aliquot_gdc_id,
        file_gdc_id,
        platform
    FROM 
        `{input_table}`
    group by 
        project_short_name,
        primary_site,
        case_barcode, 
        aliquot_barcode,
        gene_name,
        gene_type,
        Ensembl_gene_id,
        Ensembl_gene_id_v,
        unstranded,
        stranded_first,
        stranded_second,
        tpm_unstranded,
        fpkm_unstranded,
        fpkm_uq_unstranded,
        sample_type_name,
        case_gdc_id,
        aliquot_gdc_id,
        file_gdc_id,
        platform
    """



#####  MAIN WOKFLOW FUNCTIONS
def build_manifest_from_filters( release, filters, manifest_file, manifest_table ):
    full_filters = filters + [{'program_name': params.PROGRAM}]
    max_files = params.MAX_FILES if 'MAX_FILES' in params_dict else None
    manifest_success = get_the_bq_manifest( params.FILEDATA_TABLE.format(release), full_filters, 
                            max_files, params.WORKING_PROJECT, params.SCRATCH_DATASET, manifest_table,
                            params.WORKING_BUCKET, params.BUCKET_MANIFEST_TSV, manifest_file, params.BQ_AS_BATCH  )
    if not manifest_success:
        sys.exit( "Failure generating manifest" )

def download_from_gdc( local_pull_list, local_files_dir ):
    pull_list = []
    with open(local_pull_list, mode='r') as pull_list_file:
        pull_list_raw = pull_list_file.read().splitlines()

    for x in pull_list_raw:
        link = x.replace("[", "").replace("]", "").replace("'", "").split(", ")
        print(link[2])
        pull_list.append(link[2])
       
    print( f"Preparing to download {len(pull_list)} files from buckets\n" )
    if os.path.exists( local_files_dir ): shutil.rmtree( local_files_dir )
    os.makedirs( local_files_dir)
    bp = BucketPuller(10)
    bp.pull_from_buckets(pull_list, local_files_dir)
                
def build_list_and_concat( local_files_dir, one_big_tsv ):
    all_files = build_file_list( local_files_dir )
    header_addon = ['source_file_id']
    def file_info_func(use_file_name, program_prefix):
        path_bits = use_file_name.split(os.sep)
        return( [path_bits[-2]] )
    concat_all_files(all_files, one_big_tsv, params.PROGRAM, header_addon, file_info_func, None)         

def pull_table_info_from_git( schema_repo_local, schema_repo_url, schema_repo_branch ): #### REPLACE FROM common/support.py
    print( 'pull_table_info_from_git' )
    try:
        create_clean_target( schema_repo_local )
        repo = Repo.clone_from( schema_repo_url, schema_repo_local )
        repo.git.checkout( schema_repo_branch )
    except Exception as ex:
        sys.exit( f"pull_table_info_from_git failed: {str(ex)}" )

def create_abridged_schema( one_big_tsv, field_lookup_json ):
    with open(one_big_tsv, 'r') as inf: header = inf.readline().rstrip('\n').split('\t')
    with open(field_lookup_json, 'r') as inf: field_dict = json.loads( inf.read() )
    typed_schema = [ { 'description': field_dict[field]['description'], 
                       'name': field, 
                       'type': field_dict[field]['type']} for field in header ]
    return typed_schema

def bq_metadata_steps( release, upload_table, files_to_case_table, barcodes_table, ftc_plat_table, counts_metadata_table, draft_table, program, case_aliquot_fix ):
    scratchp    = f'{params.WORKING_PROJECT}.{params.SCRATCH_DATASET}.'
    aliq_table  = params.ALIQUOT_TABLE.format(release)
    case_table  = params.CASE_TABLE.format(release)
    file_table  = params.FILEDATA_TABLE.format(release)
    
    if not build_aliquot_and_case( scratchp+upload_table, file_table, params.SCRATCH_DATASET, files_to_case_table, "WRITE_TRUNCATE", params.BQ_AS_BATCH, program, case_aliquot_fix ):
        sys.exit( "Attaching case and aliquot ids to files failed" )
    if not extract_platform_for_files( scratchp+files_to_case_table, file_table, params.SCRATCH_DATASET, ftc_plat_table, True, params.BQ_AS_BATCH ):
        sys.exit( "Extraction of platform information failed" )
    if not attach_barcodes( scratchp+ftc_plat_table, aliq_table, case_table, params.SCRATCH_DATASET, barcodes_table, True, params.BQ_AS_BATCH ):
        sys.exit( "Attaching barcodes to ids failed" )
    if not glue_metadata( scratchp+barcodes_table, scratchp+upload_table, params.SCRATCH_DATASET, counts_metadata_table, True, params.BQ_AS_BATCH ):
        sys.exit( "Merging counts and metadata failed" )
    merge_samples_by_aliquot( scratchp+counts_metadata_table, draft_table, params.SCRATCH_DATASET, params.BQ_AS_BATCH )

def schema_tags( ):
    tags = {
        'access': 'open',
        'data_type': 'gene_expression',
        'reference_genome_0': params.BUILD,
        'source': 'gdc',
        'category': 'processed_-omics_data',
        'experimental_strategy': 'rnaseq',
        'status': 'current'}
    for key,value in params.MAPPING[params.PROGRAM].items():
        if 'program' in key: tags[key.replace('_label', '')] = value
    return tags

def install_table_metadata( table_id, metadata ):
    client = bigquery.Client()
    table = client.get_table(table_id)
    table.labels = metadata['labels']
    table.friendly_name = metadata['friendlyName']
    table.description = metadata['description']
    client.update_table(table, ["labels", "friendly_name", "description"])
    assert table.labels == metadata['labels']
    assert table.friendly_name == metadata['friendlyName']
    assert table.description == metadata['description']

def cluster_table( input_table, output_table, in_dataset, out_dataset, cluster_fields ):
    cluster_string = ', '.join(cluster_fields)
    sql = f'''
    CREATE TABLE `{out_dataset}.{output_table}` 
    CLUSTER BY {cluster_string} 
    AS SELECT * FROM `{in_dataset}.{input_table}`'''
    return( sql )

def update_final_schema( draft_table, field_lookup_json, table_description ):
    draft_table_path = f'{params.WORKING_PROJECT}.{params.SCRATCH_DATASET}.{draft_table}'
    with open(field_lookup_json, 'r') as inf: field_dict = json.loads( inf.read() )
    update_schema_with_dict( params.SCRATCH_DATASET, draft_table, field_dict )
    metadata = { 'description': table_description.format( params.PROGRAM, params.RELEASE, params.PRETTY_DATE ),
                 'labels': schema_tags( ),
                 'friendlyName': f'{params.PROGRAM} {params.BUILD.upper()} RNASEQ GENE EXPRESSION REL {params.RELEASE} VERSIONED' }
    install_table_metadata( draft_table_path, metadata )

def dump_working_tables( local_files_dir, manifest_file, one_big_tsv, dump_tables ):
    with open(manifest_file, 'r') as inf:  
        for line in inf.readlines()[1:]:
            name = line.split("\t")[0]
            shutil.rmtree( f'{local_files_dir}/{name}' )
    os.remove(one_big_tsv)
    table_cleaner(dump_tables, False)

def publish( draft_table, publication_table, bq_program ):
    source_table = f'{params.WORKING_PROJECT}.{params.SCRATCH_DATASET}.{draft_table}_cluster'
    publication_dest = f'{params.PUBLICATION_PROJECT}.{bq_program}_versioned.{publication_table}_r{params.RELEASE}'
    print( f'publishing current and versioned {params.PROGRAM} tables' )
    if not publish_table( source_table, publication_dest, overwrite = False ):
        sys.exit( 'versioned publication failed' )
    publication_dest = f'{params.PUBLICATION_PROJECT}.{bq_program}.{publication_table}_current'
    success = delete_table_bq_job( bq_program, f'{publication_table}_current', params.PUBLICATION_PROJECT )
    if not success: sys.exit('deletion failed')
    if not publish_table( source_table, publication_dest, overwrite = True ):
        sys.exit( 'current publication failed' )    

def run_archive( manifest_table, pull_list_table ):
    print('archive files from VM')
    archive_file_prefix = f"{date.today()}_{params.PUBLICATION_DATASET}"
    if params_dict['ARCHIVE_YAML']:
        yaml_file = re.search(r"\/(\w*.yaml)$", args[1])
        archive_yaml = f"{params.ARCHIVE_BUCKET_DIR}/{params.ARCHIVE_CONFIG}/{archive_file_prefix}_{yaml_file.group(1)}"
        upload_to_bucket( params.ARCHIVE_BUCKET, archive_yaml, args[1] )
    archive_pull_file = f"{params.ARCHIVE_BUCKET_DIR}/{archive_file_prefix}_{params.LOCAL_PULL_LIST}"
    upload_to_bucket( params.ARCHIVE_BUCKET, archive_pull_file, pull_list_table )
    archive_manifest_file = f"{params.ARCHIVE_BUCKET_DIR}/{archive_file_prefix}_{params.MANIFEST_FILE}"
    upload_to_bucket( params.ARCHIVE_BUCKET, archive_manifest_file, manifest_table )

def run_update_status_tag():
    print('Update previous table')
    success = update_status_tag( f'{params.PUBLICATION_DATASET}_versioned', f'{publication_table}_r{params.PREVIOUS_RELEASE}',
                  'archived', params.PUBLICATION_PROJECT )
    if not success:
        sys.exit( "update status tag table failed" )





## Main Control Flow
## Note that the actual steps run are configured in the YAML input! This allows you
## to e.g. skip previously run steps.
def main(args):
    if not confirm_google_vm():
        sys.exit('This job needs to run on a Google Cloud Compute Engine to avoid storage egress charges [EXITING]')
    if len(args) != 2:
        sys.exit("\nUsage : {} <configuration_yaml>".format(args[0]))
    
    print('job started') 
    with open(args[1], mode='r') as yaml_file:
        global params_dict, params
        params_dict, filters, table_description, steps = load_config( yaml_file.read() )
        params = SimpleNamespace(**params_dict)
    
    print(filters)
    home = expanduser("~")
    
    
    for program in params.PROGRAMS:
        # BQ does not like to be given paths that have "~". So make all local paths absolute:
        params.PROGRAM      = program
        bq_program          = params.MAPPING[program]['bq_dataset']
        local_files_dir     = f'{home}/gexp/{bq_program}_gexpFilesHoldMini'
        one_big_tsv         = f'{home}/gexp/{bq_program}_GEXP-joinedData.tsv'
        manifest_file       = f'{home}/gexp/{bq_program}_GEXP-manifest.tsv'
        local_pull_list     = f'{home}/gexp/{bq_program}_gexp_pull_list.tsv'
        file_traversal_list = f'{home}/gexp/{bq_program}_gexp_traversal_list.tsv'
        field_lookup_json   = f'{params.SCHEMA_REPO_LOCAL}/{params.SCHEMA_FIELD_LOOKUP}'
        # Create table names
        release             = f'r{params.RELEASE}'
        bucket_blob         = f'{params.WORKING_BUCKET_DIR}/{params.DATE}-{bq_program}-RNAseq.tsv'
        full_bucket_path    = f'gs://{params.WORKING_BUCKET}/{bucket_blob}'
        upload_table        = f'{bq_program}_{params.DATE}_RNAseq_raw'
        manifest_table      = f'{bq_program}_{params.DATE}_RNAseq_manifest'
        pull_list_table     = f'{bq_program}_{params.DATE}_RNAseq_pull_list'
        files_to_case_table = f'{bq_program}_{params.DATE}_RNAseq_files_to_case'
        ftc_plat_table      = f'{bq_program}_{params.DATE}_RNAseq_files_to_case_with_plat'
        barcodes_table      = f'{bq_program}_{params.DATE}_RNAseq_barcodes'
        counts_metadata_table = f'{bq_program}_{params.DATE}_counts_and_meta'
        draft_table         = f'{bq_program}_RNAseq_{params.BUILD}_gdc'
        publication_table   = f'RNAseq_{params.BUILD}_gdc'

        #if 'clear_target_directory' in steps:        create_clean_target( local_files_dir )
        if 'build_manifest_from_filters' in steps:   build_manifest_from_filters( release, filters, manifest_file, manifest_table )
        if 'build_pull_list' in steps:
            build_pull_list_with_bq( f'{params.WORKING_PROJECT}.{params.SCRATCH_DATASET}.{manifest_table}',
                        params.INDEXD_BQ_TABLE.format(release), params.WORKING_PROJECT, params.SCRATCH_DATASET,
                        pull_list_table, params.WORKING_BUCKET, params.BUCKET_PULL_LIST, local_pull_list,
                        params.BQ_AS_BATCH )
        if 'download_from_gdc' in steps:             download_from_gdc( local_pull_list, local_files_dir )
        if 'build_list_and_concat' in steps:         build_list_and_concat( local_files_dir, one_big_tsv )
        if 'concat_all_files' in steps:              concat_all_files( file_set, file_traversal_list )
        if 'upload_to_bucket' in steps:              upload_to_bucket( params.WORKING_BUCKET, bucket_blob, one_big_tsv )
        if 'delete_all_bq' in steps:                 table_cleaner( dump_tables, True )
        if 'create_bq_from_tsv' in steps:
            typed_schema = create_abridged_schema( one_big_tsv, field_lookup_json )
            csv_to_bq_write_depo( typed_schema, full_bucket_path, params.SCRATCH_DATASET, upload_table, params.BQ_AS_BATCH, "WRITE_TRUNCATE" )
        if 'bq_metadata_steps' in steps:
            bq_metadata_steps( release, upload_table, files_to_case_table, barcodes_table, ftc_plat_table, counts_metadata_table, draft_table, program, params.CASE_ALIQUOT_FIX )
        if 'cluster_table' in steps:
            sql = cluster_table( draft_table, draft_table+'_cluster', params.SCRATCH_DATASET, params.SCRATCH_DATASET, ['project_short_name', 'case_barcode', 'sample_barcode', 'aliquot_barcode'] )
            success = bq_harness_with_result(sql, False, verbose=True)
        if 'update_final_schema' in steps:           update_final_schema( draft_table+'_cluster', field_lookup_json, table_description )
        if 'dump_working_tables' in steps:           
            dump_tables = [upload_table, manifest_table, pull_list_table, files_to_case_table, 
                ftc_plat_table, barcodes_table, counts_metadata_table]
            dump_working_tables( local_files_dir, manifest_file, one_big_tsv, dump_tables )
        # if 'compare_remove_old_current' in steps: compare_to_last_publish () #### REPLACE need to grab support function
        if 'publish' in steps:                       publish( draft_table, publication_table, bq_program )
        #if 'update_current_schema' in steps:         update_final_schema( , field_lookup, table_description )
        if 'update_status_tag' in steps:             run_update_status_tag(  )  # Update previous versioned table with archived tag
        if 'archive' in steps:                       run_archive( manifest_table, pull_list_table )

    print('job completed')

if __name__ == "__main__":
    main(sys.argv)
