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
import shutil
import os
import yaml
import io
from git import Repo
import zipfile
import gzip
from os.path import expanduser
from json import loads as json_loads
from createSchemaP3 import build_schema
from datetime import date
import re
from common_etl.support import create_clean_target, generic_bq_harness, upload_to_bucket, \
                               csv_to_bq_write_depo, delete_table_bq_job, confirm_google_vm, \
                               build_file_list, get_the_bq_manifest, BucketPuller, build_pull_list_with_bq, \
                               build_combined_schema, generic_bq_harness_write_depo, \
                               install_labels_and_desc, update_schema, generate_table_detail_files, publish_table, \
                               customize_labels_and_desc

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

    return yaml_dict['files_and_buckets_and_tables'], yaml_dict['file_sets'], yaml_dict['schema_tags'], \
           yaml_dict['steps']

'''
----------------------------------------------------------------------------------------------
Concatenate all Files
Gather up all files and glue them into one big one. We also add columns for
the `source_file_name` and `source_file_id` (which is the name of the directory
it is in). Note if file is zipped, we unzip it, concat it, then toss the unzipped version.
'''

def concat_all_files(all_files, one_big_tsv, header):
    print("building {}".format(one_big_tsv))
    first = True
    header_pieces = None
    header_id = None 
    if header is not None:
        header_pieces = header.split(',')
        header_pieces = [item.strip() for item in header_pieces]
        header_id = header_pieces[0]
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
                norm_path = os.path.normpath(filename)
                path_pieces = norm_path.split(os.sep)
                file_name = path_pieces[-1]
                gdc_id = path_pieces[-2]
                for line in readfile:
                    # Seeing comments in MAF files. Kinda specific; make configurable
                    if line.startswith('#'):
                        continue
                    if first:
                        if header_id is None:
                            header_id = line.split('\t')[0]
                            print("Header starts with {}".format(header_id))
                            outfile.write(line.rstrip('\n'))
                        else:
                            outfile.write('\t'.join(header_pieces))
                        outfile.write('\t')
                        outfile.write('source_file_name')
                        outfile.write('\t')
                        outfile.write('source_file_id')
                        outfile.write('\n')          
                        first = False                       
                    if header is not None or not line.startswith(header_id):
                        outfile.write(line.rstrip('\n'))
                        outfile.write('\t')
                        outfile.write(file_name)
                        outfile.write('\t')
                        outfile.write(gdc_id)
                        outfile.write('\n')             
                if toss_zip:
                    os.remove(use_file_name)

'''
----------------------------------------------------------------------------------------------
Delete All Intermediate Tables and (Optionally) the Final Result:
'''
def table_cleaner(dump_tables, delete_result):
    if delete_result:
        delete_table_bq_job(params['SCRATCH_DATASET'], draft_table.format(release))

    for table in dump_tables:
        delete_table_bq_job(params['SCRATCH_DATASET'], table)

'''
----------------------------------------------------------------------------------------------
Associate Aliquot And Case IDs to File IDs
BQ ETL step 2: find the case and aliquot gdc_ids that go with each gexp file
'''
def build_aliquot_and_case(gexp_table, file_table, target_dataset, output_table, write_depo, sql_dict, do_batch):

    sql = attach_aliquot_and_case_ids_sql(gexp_table, file_table, sql_dict)
    return generic_bq_harness_write_depo(sql, target_dataset, output_table, do_batch, write_depo)

'''
----------------------------------------------------------------------------------------------
SQL code for above
The files we get from GDC just have gene and expression columns. What aliquot is this for?
Use the file table to associate case and aliquot GDC ids for each file we are using.
'''
def attach_aliquot_and_case_ids_sql(gexp_table, file_table, sql_dict):
    return '''
        WITH a1 AS (SELECT DISTINCT source_file_id
                FROM `{0}`)
        SELECT b.project_short_name,
               b.case_gdc_id,
               b.analysis_input_file_gdc_ids,
               b.associated_entities__entity_gdc_id AS aliquot_gdc_id,
               b.file_name,
               b.file_gdc_id,
               b.platform
        FROM a1 JOIN `{1}` AS b ON a1.source_file_id = b.file_gdc_id
        WHERE b.associated_entities__entity_type = 'aliquot'
        '''.format(gexp_table, file_table)

'''
----------------------------------------------------------------------------------------------
Associate Barcodes for Aliquot And Case IDs
BQ ETL step 3: attach aliquot and case barcodes for IDS
'''
def attach_barcodes(step2_table, aliquot_table, case_table, target_dataset, output_table, do_replace, do_batch):

    sql = attach_barcodes_sql(step2_table, aliquot_table, case_table)
    return generic_bq_harness(sql, target_dataset, output_table, do_batch, do_replace)

'''
----------------------------------------------------------------------------------------------
SQL code for above
Get the barcodes for the aliquot and case IDs
Except statement removes one duplicated column 
GDC creates a randomized analyte + portion id for TARGET
'''
def attach_barcodes_sql(step2_table, aliquot_table, case_table):
    return '''
        WITH a1 AS (SELECT a.project_short_name,
                           b.case_barcode,
                           b.sample_barcode,
                           b.aliquot_barcode,
                           a.case_gdc_id,
                           b.sample_gdc_id,
                           a.aliquot_gdc_id,
                           a.file_gdc_id,
                           a.platform,
                           a.file_name
                    FROM `{0}` AS a 
                    JOIN (SELECT DISTINCT * FROM 
                    (SELECT * EXCEPT (analyte_gdc_id, portion_gdc_id) FROM `{1}`)
            ) AS b ON a.aliquot_gdc_id = b.aliquot_gdc_id)
        SELECT a1.project_short_name,
               a1.case_barcode,
               c.primary_site,
               a1.case_gdc_id,
               a1.sample_barcode,
               a1.aliquot_barcode,
               a1.sample_gdc_id,
               a1.aliquot_gdc_id,
               a1.file_gdc_id,
               a1.platform,
               a1.file_name
        FROM a1 JOIN `{2}` AS c ON a1.case_barcode = c.case_barcode
        '''.format(step2_table, aliquot_table, case_table)
'''
----------------------------------------------------------------------------------------------
Merge Counts and Metadata
'''
def merge_counts_and_metadata(step3_table, counts_table, target_dataset, output_table, do_replace,
                              sql_dict, do_batch):

    sql = glue_counts_and_metadata_sql(step3_table, counts_table, sql_dict)
    return generic_bq_harness(sql, target_dataset, output_table, do_batch, do_replace)

'''
----------------------------------------------------------------------------------------------
SQL code for above
'''
def glue_counts_and_metadata_sql(step3_table, count_table, sql_dict):
    count_col_name = sql_dict['count_column']
    print(count_col_name)
    file_col_name = sql_dict['file_column']
    return '''
        SELECT a.project_short_name,
               a.case_barcode,
               a.primary_site,
               a.sample_barcode,
               a.aliquot_barcode,
               REGEXP_EXTRACT(b.Ensembl_gene_id_v, r"^[^.]+") as Ensembl_gene_id,
               b.Ensembl_gene_id_v,
               b.{0},
               a.case_gdc_id,
               a.sample_gdc_id,
               a.aliquot_gdc_id,
               a.file_gdc_id as {1},
               a.platform,
               a.file_name
        FROM `{2}` AS a JOIN `{3}` AS b ON a.file_gdc_id = b.source_file_id 
        WHERE Ensembl_gene_id_v <> "__no_feature" 
            AND Ensembl_gene_id_v <> "__ambiguous" 
            AND Ensembl_gene_id_v <> "__too_low_aQual" 
            AND Ensembl_gene_id_v <> "__not_aligned" 
            AND Ensembl_gene_id_v <> "__alignment_not_unique"
        '''.format(count_col_name, file_col_name, step3_table, count_table)

'''
----------------------------------------------------------------------------------------------
Three-Way Merge of Separate Count Tables to Single Table
'''

def all_counts_to_one_table(target_dataset, output_table, do_replace, sql_dict, do_batch):

    sql = join_three_sql(None, None, sql_dict)
    return generic_bq_harness(sql, target_dataset, output_table, do_batch, do_replace)

'''
----------------------------------------------------------------------------------------------
SQL code for above
'''
def join_three_sql(table1, table2, sql_dict):
    table_1_vals = sql_dict['table_0']
    table_2_vals = sql_dict['table_1']
    table_3_vals = sql_dict['table_2']
    return '''
        WITH
            ab AS (SELECT
                      a.project_short_name,
                      a.case_barcode,
                      a.primary_site,
                      a.sample_barcode,
                      a.aliquot_barcode,
                      a.Ensembl_gene_id,
                      a.Ensembl_gene_id_v,
                      a.{0},
                      b.{1},
                      a.case_gdc_id,
                      a.sample_gdc_id,
                      a.aliquot_gdc_id,
                      a.{3},
                      b.{4},
                      a.platform
        FROM `{6}` AS a JOIN `{7}` AS b
        ON (a.aliquot_gdc_id = b.aliquot_gdc_id) AND (a.Ensembl_gene_id_v = b.Ensembl_gene_id_v))
        SELECT
          c.project_short_name,
          c.case_barcode,
          c.primary_site,
          c.sample_barcode,
          c.aliquot_barcode,
          c.Ensembl_gene_id,
          c.Ensembl_gene_id_v,
          ab.{0},
          ab.{1},
          c.{2},
          c.case_gdc_id,
          c.sample_gdc_id,
          c.aliquot_gdc_id,
          ab.{3},
          ab.{4},
          c.{5},
          c.platform
        FROM ab JOIN `{8}` AS c
        ON (ab.aliquot_gdc_id = c.aliquot_gdc_id) AND (ab.Ensembl_gene_id_v = c.Ensembl_gene_id_v)
        '''.format(table_1_vals['count_column'], table_2_vals['count_column'], table_3_vals['count_column'],
                   table_1_vals['file_column'], table_2_vals['file_column'], table_3_vals['file_column'],
                   table_1_vals['table'], table_2_vals['table'], table_3_vals['table'])

'''
----------------------------------------------------------------------------------------------
Pull in Platform Info From Master File: Turns out the analysis files with counts do not have
an associated platform. That info is (usually) attached to the original file the counts
were derived from. Pull that data out so we can use it.
'''


def extract_platform_for_files(step2_table, file_table, target_dataset, output_table,
                               do_replace, sql_dict, do_batch):

    sql = extract_platform_for_files_sql(step2_table, file_table, sql_dict)
    return generic_bq_harness(sql, target_dataset, output_table, do_batch, do_replace)

'''
----------------------------------------------------------------------------------------------
SQL code for above
'''

def extract_platform_for_files_sql(step2_table, file_table, sql_dict):
    return '''
        WITH
            a1 AS (SELECT DISTINCT analysis_input_file_gdc_ids
                   FROM `{0}`),
            a2 AS (SELECT a1.analysis_input_file_gdc_ids,
                          b.platform
                   FROM a1 JOIN `{1}` as b ON a1.analysis_input_file_gdc_ids = b.file_gdc_id)
        SELECT
               b.project_short_name,
               b.analysis_input_file_gdc_ids,
               b.case_gdc_id,
               b.aliquot_gdc_id,
               b.file_name,
               b.file_gdc_id,
               a2.platform
        FROM `{0}` AS b JOIN a2 ON a2.analysis_input_file_gdc_ids = b.analysis_input_file_gdc_ids
        '''.format(step2_table, file_table)

'''
----------------------------------------------------------------------------------------------
Merge Gene Names Into Final Table
'''

def glue_in_gene_names(three_counts_table, gene_table, target_dataset, output_table, do_replace, sql_dict, do_batch):

    sql = glue_in_gene_names_sql(three_counts_table, gene_table, sql_dict)
    print(sql)
    return generic_bq_harness(sql, target_dataset, output_table, do_batch, do_replace)

'''
----------------------------------------------------------------------------------------------
SQL code for above
'''

def glue_in_gene_names_sql(three_counts_table, gene_table, sql_dict):
    table_1_vals = sql_dict['table_0']
    table_2_vals = sql_dict['table_1']
    table_3_vals = sql_dict['table_2']
    
    return '''
        WITH gene_reference AS (
        SELECT
            DISTINCT gene_name,
                    gene_id,
                    gene_id_v,
                    gene_type
        FROM
            `{7}`)              
        SELECT 
          a.project_short_name,
          a.case_barcode,
          a.primary_site,
          a.sample_barcode,
          a.aliquot_barcode,
          b.gene_name,
          b.gene_type,
          a.Ensembl_gene_id,
          a.Ensembl_gene_id_v,
          a.{0},
          a.{1},
          a.{2},
          a.case_gdc_id,
          a.sample_gdc_id,
          a.aliquot_gdc_id,
          a.{3},
          a.{4},
          a.{5},
          a.platform
        FROM `{6}` AS a JOIN gene_reference AS b ON a.Ensembl_gene_id = b.gene_id
        '''.format(table_1_vals['count_column'], table_2_vals['count_column'], table_3_vals['count_column'],
                   table_1_vals['file_column'], table_2_vals['file_column'], table_3_vals['file_column'],
                   three_counts_table, gene_table)

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
        params, file_sets, schema_tags, steps = load_config(yaml_file.read())

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

    # Which table are we building?
    release = "".join(["r", str(params['RELEASE'])])
    use_schema = params['VER_SCHEMA_FILE_NAME']
    if 'current' in steps:
        print('This workflow will update the schema for the "current" table')
        release = 'current'
        use_schema = params['SCHEMA_FILE_NAME']

    # Create table names
    upload_table = '_'.join([params['PROGRAM'], params['DATA_TYPE'], '{}'])
    manifest_table = '_'.join([params['PROGRAM'],params['DATA_TYPE'], 'manifest', '{}'])
    pull_list_table = '_'.join([params['PROGRAM'],params['DATA_TYPE'], 'pull', 'list', '{}'])
    files_to_case_table = '_'.join([params['PROGRAM'],params['DATA_TYPE'], 'files_to_case'])
    files_to_case_w_plat_table = '_'.join([params['PROGRAM'],params['DATA_TYPE'], 'files_to_case_with_plat'])
    barcodes_table = '_'.join([params['PROGRAM'],params['DATA_TYPE'], 'barcodes'])
    counts_w_metadata_table = '_'.join([params['PROGRAM'], params['DATA_TYPE'], 'counts_and_meta', '{}'])
    merged_counts_table = '_'.join([params['PROGRAM'], params['DATA_TYPE'], 'merged_counts'])
    draft_table = '_'.join([params['PROGRAM'], params['DATA_TYPE'], params['BUILD'], 'gdc', '{}'])
    publication_table = '_'.join([params['DATA_TYPE'], params['BUILD'], 'gdc', '{}'])

    if params['RELEASE'] < 21 and 'METADATA_REL' not in params:
        print("The input release is before new metadata process, "
              "please specify which release of the metadata to use.")

    metadata_rel = "".join(["r", str(params['METADATA_REL'])]) if 'METADATA_REL' in params else params['RELEASE']


    if 'clear_target_directory' in steps:
        for file_set in file_sets:
            count_name, _ = next(iter(file_set.items()))
            create_clean_target(local_files_dir.format(count_name))

    if 'build_manifest_from_filters' in steps:
        for file_set in file_sets:
            count_name, count_dict = next(iter(file_set.items()))
            mani_for_count = manifest_file.format(count_name)
            table_for_count = manifest_table.format(count_name)
            tsv_for_count = params['BUCKET_MANIFEST_TSV'].format(count_name)
            max_files = params['MAX_FILES'] if 'MAX_FILES' in params else None
            manifest_success = get_the_bq_manifest(params['FILE_TABLE'].format(metadata_rel), count_dict['filters'], max_files,
                                                   params['WORKING_PROJECT'], params['SCRATCH_DATASET'],
                                                   table_for_count, params['WORKING_BUCKET'],
                                                   tsv_for_count, mani_for_count,
                                                   params['BQ_AS_BATCH'])
            if not manifest_success:
                print("Failure generating manifest")
                return

    #
    # We need to create a "pull list" of gs:// URLs to pull from GDC buckets. If you have already
    # created a pull list, just plunk it in 'LOCAL_PULL_LIST' and skip this step. If creating a pull
    # list, uses BQ as long as you have built the manifest using BQ (that route uses the BQ Manifest
    # table that was created).
    #

    if 'build_pull_list' in steps:
        for file_set in file_sets:
            count_name, count_dict = next(iter(file_set.items()))
            table_for_count = manifest_table.format(count_name)
            local_pull_for_count = local_pull_list.format(count_name)
            pull_table_for_count = pull_list_table.format(count_name)
            bucket_pull_list_for_count = params['BUCKET_PULL_LIST'].format(count_name)
            full_manifest = '{}.{}.{}'.format(params['WORKING_PROJECT'],
                                              params['SCRATCH_DATASET'],
                                              table_for_count)
            build_pull_list_with_bq(full_manifest, params['INDEXD_BQ_TABLE'].format(metadata_rel),
                                    params['WORKING_PROJECT'], params['SCRATCH_DATASET'],
                                    pull_table_for_count,
                                    params['WORKING_BUCKET'],
                                    bucket_pull_list_for_count,
                                    local_pull_for_count, params['BQ_AS_BATCH'])
    #
    # Now hitting GDC cloud buckets. Get the files in the pull list:
    #

    if 'download_from_gdc' in steps:
        for file_set in file_sets:
            count_name, _ = next(iter(file_set.items()))
            pull_for_count = local_pull_list.format(count_name)
            with open(pull_for_count, mode='r') as pull_list_file:
                pull_list = pull_list_file.read().splitlines()
            print("Preparing to download %s files from buckets\n" % len(pull_list))
            bp = BucketPuller(10)
            local_files_dir_for_count = local_files_dir.format(count_name)
            bp.pull_from_buckets(pull_list, local_files_dir_for_count)

    if 'build_file_list' in steps:
        for file_set in file_sets:
            count_name, _ = next(iter(file_set.items()))
            local_files_dir_for_count = local_files_dir.format(count_name)
            all_files = build_file_list(local_files_dir_for_count)
            file_traversal_list_for_count = file_traversal_list.format(count_name)
            with open(file_traversal_list_for_count, mode='w') as traversal_list:
                for line in all_files:
                    traversal_list.write("{}\n".format(line))

    if 'concat_all_files' in steps:
        for file_set in file_sets:
            count_name, count_dict = next(iter(file_set.items()))
            header = count_dict['header'] if 'header' in count_dict else None
            file_traversal_list_for_count = file_traversal_list.format(count_name)
            with open(file_traversal_list_for_count, mode='r') as traversal_list_file:
                all_files = traversal_list_file.read().splitlines()
                concat_all_files(all_files, one_big_tsv.format(count_name), header)

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
        schema_file = "{}/{}/{}".format(params['SCHEMA_REPO_LOCAL'], params['RAW_SCHEMA_DIR'], use_schema)
        full_file_prefix = "{}/{}".format(params['PROX_DESC_PREFIX'], draft_table.format(release))
        # Write out the details
        success = generate_table_detail_files(schema_file, full_file_prefix)
        if not success:
            print("process_git_schemas failed")
            return

    # Customize generic schema to this data program:

    if 'replace_schema_tags' in steps:
        print('replace_schema_tags')
        pn = params['PROGRAM']
        dataset_tuple = (pn, pn.replace(".", "_"))
        tag_map_list = []
        for tag_pair in schema_tags:
            for tag in tag_pair:
                val = tag_pair[tag]
                use_pair = {}
                tag_map_list.append(use_pair)
                if val.find('~-') == 0 or val.find('~lc-') == 0 or val.find('~lcbqs-') == 0:
                    chunks = val.split('-', 1)
                    if chunks[1] == 'programs':
                        if val.find('~lcbqs-') == 0:
                            rep_val = dataset_tuple[1].lower()  # can't have "." in a tag...
                        else:
                            rep_val = dataset_tuple[0]
                    elif chunks[1] == 'builds':
                        rep_val = params['BUILD']
                    else:
                        raise Exception()
                    if val.find('~lc-') == 0:
                        rep_val = rep_val.lower()
                    use_pair[tag] = rep_val
                else:
                    use_pair[tag] = val
        full_file_prefix = "{}/{}".format(params['PROX_DESC_PREFIX'], draft_table.format(release))

        # Write out the details
        success = customize_labels_and_desc(full_file_prefix, tag_map_list)

        if not success:
            print("replace_schema_tags failed")
            return False

    if 'analyze_the_schema' in steps:
        print('analyze_the_schema')
        for file_set in file_sets:
            count_name, _ = next(iter(file_set.items()))
            typing_tups = build_schema(one_big_tsv.format(count_name), params['SCHEMA_SAMPLE_SKIPS'])
            full_file_prefix = "{}/{}".format(params['PROX_DESC_PREFIX'], draft_table.format(release))
            schema_dict_loc = "{}_schema.json".format(full_file_prefix)
            build_combined_schema(None, schema_dict_loc,
                                  typing_tups, hold_schema_list.format(count_name), hold_schema_dict.format(count_name))

    bucket_target_blob_sets = {}
    for file_set in file_sets:
        count_name, _ = next(iter(file_set.items()))
        bucket_target_blob_sets[count_name] = '{}/{}-{}-{}-{}.tsv'.format(params['WORKING_BUCKET_DIR'], params['DATE'],
                                                                          params['PROGRAM'], params['DATA_TYPE'],
                                                                          count_name)

    if 'upload_to_bucket' in steps:
        for file_set in file_sets:
            count_name, _ = next(iter(file_set.items()))
            upload_to_bucket(params['WORKING_BUCKET'], 
                             bucket_target_blob_sets[count_name],
                             one_big_tsv.format(count_name))
            
    if 'delete_all_bq' in steps:
        table_cleaner(params, file_sets, True)

    if 'create_bq_from_tsv' in steps:
        for file_set in file_sets:
            count_name, _ = next(iter(file_set.items()))
            bucket_src_url = 'gs://{}/{}'.format(params['WORKING_BUCKET'], bucket_target_blob_sets[count_name])
            hold_schema_list_for_count = hold_schema_list.format(count_name)
            with open(hold_schema_list_for_count, mode='r') as schema_hold_dict:
                typed_schema = json_loads(schema_hold_dict.read())
            csv_to_bq_write_depo(typed_schema, bucket_src_url,
                                 params['SCRATCH_DATASET'],
                                 upload_table.format(count_name), params['BQ_AS_BATCH'], None)

    if 'attach_ids_to_files' in steps:
        count = 0
        for file_set in file_sets:
            count_name, _ = next(iter(file_set.items()))
            write_depo = "WRITE_TRUNCATE" if (count == 0) else "WRITE_APPEND"
            gexp_table = '{}.{}.{}'.format(params['WORKING_PROJECT'], 
                                           params['SCRATCH_DATASET'],
                                           upload_table.format(count_name))
            success = build_aliquot_and_case(gexp_table, params['FILEDATA_TABLE'], 
                                             params['SCRATCH_DATASET'],
                                             files_to_case_table, write_depo, {}, params['BQ_AS_BATCH'])
            count += 1

        if not success:
            print("attach_ids_to_files failed")
            return
              
    if 'extract_platform' in steps:
        step2_table = '{}.{}.{}'.format(params['WORKING_PROJECT'], 
                                        params['SCRATCH_DATASET'],
                                        files_to_case_table)
        success = extract_platform_for_files(step2_table, params['FILEDATA_TABLE'],
                                             params['SCRATCH_DATASET'],
                                             files_to_case_w_plat_table, True, {}, params['BQ_AS_BATCH'])

        if not success:
            print("extract_platform failed")
            return
            
    if 'attach_barcodes_to_ids' in steps:
        step2_table = '{}.{}.{}'.format(params['WORKING_PROJECT'], 
                                        params['SCRATCH_DATASET'],
                                        files_to_case_w_plat_table)

        if params['RELEASE'] < 25:
            case_table = params['CASE_TABLE'].format('25')
        else:
            case_table = params['CASE_TABLE'].format(params['RELEASE'])

        success = attach_barcodes(step2_table, params['ALIQUOT_TABLE'].format(metadata_rel), case_table,
                                  params['SCRATCH_DATASET'],
                                  barcodes_table, True, params['BQ_AS_BATCH'])

        if not success:
            print("attach_barcodes_to_ids failed")
            return
            
    if 'merge_counts_and_metadata' in steps:
        for file_set in file_sets:
            count_name, count_dict = next(iter(file_set.items()))
            if 'header' not in count_dict:
                print("must have defined headers to work")
                break
            header = count_dict['header']
            print(header)
            sql_dict = {}
            sql_dict['count_column'] = header.split(',')[1].strip()
            sql_dict['file_column'] = 'file_gdc_id_{}'.format(count_name)
            
            step3_table = '{}.{}.{}'.format(params['WORKING_PROJECT'], 
                                            params['SCRATCH_DATASET'],
                                            barcodes_table)
            counts_table = '{}.{}.{}'.format(params['WORKING_PROJECT'], 
                                             params['SCRATCH_DATASET'],
                                             upload_table.format(count_name))
            
            success = merge_counts_and_metadata(step3_table, counts_table, 
                                                params['SCRATCH_DATASET'],
                                                counts_w_metadata_table.format(count_name),
                                                True, sql_dict, params['BQ_AS_BATCH'])

            if not success:
                print("merge_counts_and_metadata failed")
                return
                
    if 'merge_all' in steps:
        sql_dict = {}
        count = 0
        for file_set in file_sets:
            count_name, count_dict = next(iter(file_set.items()))
            dict_for_set = {}
            sql_dict['table_{}'.format(count)] = dict_for_set
            count += 1
            if 'header' not in count_dict:
                print("must have defined headers to work")
                return
            header = count_dict['header']
            dict_for_set['count_column'] = header.split(',')[1].strip()
            dict_for_set['file_column'] = 'file_gdc_id_{}'.format(count_name)
            dict_for_set['table'] = '{}.{}.{}'.format(params['WORKING_PROJECT'], 
                                                      params['SCRATCH_DATASET'],
                                                      counts_w_metadata_table.format(count_name))

        success = all_counts_to_one_table(params['SCRATCH_DATASET'],
                                          merged_counts_table,
                                          True, sql_dict, params['BQ_AS_BATCH'])

        if not success:
            print("merge_counts_and_metadata failed")
            return
            
            
    if 'glue_gene_names' in steps:
        sql_dict = {}
        count = 0
        for file_set in file_sets:
            count_name, count_dict = next(iter(file_set.items()))
            dict_for_set = {}
            sql_dict['table_{}'.format(count)] = dict_for_set
            count += 1
            if 'header' not in count_dict:
                print("must have defined headers to work")
                return
            header = count_dict['header']
            dict_for_set['count_column'] = header.split(',')[1].strip()
            dict_for_set['file_column'] = 'file_gdc_id_{}'.format(count_name) 

        three_counts_table = '{}.{}.{}'.format(params['WORKING_PROJECT'], 
                                               params['SCRATCH_DATASET'],
                                               merged_counts_table)

        success = glue_in_gene_names(three_counts_table, params['GENE_NAMES_TABLE'], 
                                     params['SCRATCH_DATASET'],
                                     draft_table.format(release),
                                     True, sql_dict, params['BQ_AS_BATCH'])

        if not success:
            print("glue_gene_names failed")
            return

        #
        # Create second table
        #

    if 'create_current_table' in steps:
        source_table = '{}.{}.{}'.format(params['WORKING_PROJECT'], params['SCRATCH_DATASET'],
                                         draft_table.format(release))
        current_dest = '{}.{}.{}'.format(params['WORKING_PROJECT'], params['SCRATCH_DATASET'],
                                         draft_table.format('current'))

        success = publish_table(source_table, current_dest)

        if not success:
            print("create current table failed")
            print("remember to rerun schema steps for current table")
            return

    #
    # The derived table we generate has no field descriptions. Add them from the github json files:
    #

    if 'update_final_schema' in steps:
        success = update_schema(params['SCRATCH_DATASET'], draft_table.format(release), hold_schema_dict)
        if not success:
            print("Schema update failed")
            return

    #
    # Add description and labels to the target table:
    #

    if 'add_table_description' in steps:
        print('update_table_description')
        full_file_prefix = "{}/{}".format(params['PROX_DESC_PREFIX'], draft_table.format(release))
        success = install_labels_and_desc(params['SCRATCH_DATASET'], draft_table.format(release), full_file_prefix)
        if not success:
            print("update_table_description failed")
            return

    #
    # publish table:
    #

    if 'publish' in steps:

        source_table = '{}.{}.{}'.format(params['WORKING_PROJECT'], params['SCRATCH_DATASET'],
                                         draft_table.format(release))
        publication_dest = '{}.{}.{}'.format(params['PUBLICATION_PROJECT'], params['PUBLICATION_DATASET'],
                                             publication_table.format(release))

        success = publish_table(source_table, publication_dest)

        if not success:
            print("publish table failed")
            return

    if 'dump_working_tables' in steps:
        dump_tables = [files_to_case_table, files_to_case_w_plat_table, barcodes_table, counts_w_metadata_table,
                       merge_counts_and_metadata, merged_counts_table, draft_table]
        for file_set in file_sets:
            count_name, _ = next(iter(file_set.items()))
            dump_tables.append(upload_table.format(count_name))
            dump_tables.append(counts_w_metadata_table.format(count_name))
            dump_tables.append(manifest_table.format(count_name))
            dump_tables.append(pull_list_table.format(count_name))

        table_cleaner(dump_tables, False)

    #
    # archive files on VM:
    #

    bucket_archive_blob_sets = {}
    for file_set in file_sets:
        count_name, _ = next(iter(file_set.items()))
        bucket_target_blob_sets[count_name] = '{}/{}-{}-{}-{}'.format(params['ARCHIVE_BUCKET_DIR'], params['DATE'],
                                                                      params['PROGRAM'], params['DATA_TYPE'],
                                                                      params['RELEASE'], count_name)

    if 'archive' in steps:

        print('archive files from VM')
        archive_file_prefix = "{}_{}".format(date.today(), params['PUBLICATION_DATASET'])
        yaml_file = re.search(r"\/(\w*.yaml)$", args[1])
        archive_yaml = "{}/{}/{}_{}".format(params['ARCHIVE_BUCKET_DIR'],
                                            params['ARCHIVE_CONFIG'],
                                            archive_file_prefix,
                                            yaml_file.group(1))
        upload_to_bucket(params['ARCHIVE_BUCKET'],
                         archive_yaml,
                         args[1])
        for file_set in file_sets:
            count_name, count_dict = next(iter(file_set.items()))
            pull_file_name = params['LOCAL_PULL_LIST']
            archive_pull_file = "{}/{}_{}".format(params['ARCHIVE_BUCKET_DIR'],
                                                  archive_file_prefix,
                                                  pull_file_name.format(count_name))
            upload_to_bucket(params['ARCHIVE_BUCKET'],
                             archive_pull_file,
                             local_pull_list.format(count_name))
            manifest_file_name = params['MANIFEST_FILE']
            archive_manifest_file = "{}/{}_{}".format(params['ARCHIVE_BUCKET_DIR'],
                                                  archive_file_prefix,
                                                  manifest_file_name.format(count_name))
            upload_to_bucket(params['ARCHIVE_BUCKET'],
                            archive_manifest_file,
                             manifest_file.format(count_name))

    print('job completed')

if __name__ == "__main__":
    main(sys.argv)

