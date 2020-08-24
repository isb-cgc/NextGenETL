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
import os
import shutil

import requests

'''
Make sure the VM has BigQuery and Storage Read/Write permissions!
'''

import sys
from os.path import expanduser
import yaml
import io
from git import Repo
from json import loads as json_loads
from createSchemaP3 import build_schema

from common_etl.support import get_the_bq_manifest, confirm_google_vm, create_clean_target, \
                               generic_bq_harness, build_file_list, upload_to_bucket, csv_to_bq, \
                               build_pull_list_with_bq, BucketPuller, build_combined_schema, \
                               delete_table_bq_job, install_labels_and_desc, update_schema_with_dict, \
                               generate_table_detail_files, publish_table


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

    return yaml_dict['files_and_buckets_and_tables'], yaml_dict['steps']

'''
-----------------------------------------------------------------------------------------------------------------------
Join PDC_Quant _Matrix_Biospeciman_Joined_Table_One_Study and PDC_Genes_To_Protein_Mapping_File(generate fron Ron's csv
spread sheet
'''

def join_quant_natrix_and_pdc_genes_to_protein_table(quant_matrix_table, pdc_genes_to_protein_table, target_dataset, joined_table, do_batch):
    sql = build_join_quant_matrix_and_pdc_genes_to_protein_sql(quant_matrix_table, pdc_genes_to_protein_table)
    return generic_bq_harness(sql, target_dataset, joined_table, do_batch, True)

def build_join_quant_matrix_and_pdc_genes_to_protein_sql(quant_matrix_table, pdc_genes_to_protein_table):
    return '''
      WITH a1 as (
          SELECT 
            A.study_id, 
            A.aliquot_submitter_id,
            A.gene,
            A.log2_ratio,
            A.study_name,
            A.aliquot_id,
            A.sample_id 
            B.ncbi_gene_id,
            B.authority,
            B.description,
            B.organism,
            B.chromosome,
            B.locus,
            B.proteins,
            B.assays,
            B.access,
            B.cud_label,
            B.updated,
            B.gene_uuid
          FROM `{0}` as A 
          JOIN `{1}` as B 
          ON ((B.gene_name = A.gene))
    SELECT * FROM a1
        '''.format(quant_matrix_table, pdc_genes_to_protein_table)

'''
----------------------------------------------------------------------------------------------
Join quant matrix table and biospeciman table
'''

def join_quant_matrix_and_biospeciman_table(quant_matrix_table, biospeciman_table, target_dataset, joined_table, do_batch):
    sql = build_join_quant_matrix_and_biospeciman_table_sql(quant_matrix_table, biospeciman_table)
    return generic_bq_harness(sql, target_dataset, joined_table, do_batch, True)

def build_join_quant_matrix_and_biospeciman_table_sql(quant_matrix_table, biospeciman_table):
    return '''
      WITH a1 as (
          SELECT 
            A.study_id, 
            A.aliquot_submitter_id,
            A.gene,
            A.log2_ratio,
            B.study_name,
            B.aliquot_id,
            B.sample_id 
          FROM `{0}` as A 
          JOIN `{1}` as B 
          ON ((B.aliquot_submitter_id = A.aliquot_submitter_id) AND (B.study_id = A.study_id)))
    SELECT * FROM a1
        '''.format(quant_matrix_table, biospeciman_table)

'''
----------------------------------------------------------------------------------------------
Table comparison
'''

def compare_mi_ron_table(mi_table, ron_table, target_dataset, compare_result_table, do_batch):
    sql = build_compare_mi_ron_table_sql(mi_table, ron_table)
    return generic_bq_harness(sql, target_dataset, compare_result_table, do_batch, True)

'''
----------------------------------------------------------------------------------------------
# SQL Code For Table comparison
'''
def build_compare_mi_ron_table_sql(mi_table, ron_table):

    return '''
      WITH a1 as (
          SELECT 
            A.gene, 
            A.aliquot_submitter_id, 
            CAST(A.log2_ratio as FLOAT64) as log2_ratio_f,
            CAST(B.protein_abundance_log2ratio as FLOAT64) as protein_abundance_log2ratio_f, 
            CAST(A.log2_ratio as FLOAT64) - CAST(B.protein_abundance_log2ratio as FLOAT64) as diff 
          FROM `{0}` as A 
          JOIN `{1}` as B 
          ON ((B.aliquot_submitter_id = A.aliquot_submitter_id) AND (B.gene_symbol = A.gene)))
    SELECT * FROM a1 WHERE 
      diff != 0.0 
      AND (IS_NAN(log2_ratio_f) AND NOT IS_NAN(protein_abundance_log2ratio_f))
      AND (NOT IS_NAN(log2_ratio_f) AND IS_NAN(protein_abundance_log2ratio_f))  
      AND NOT IS_NAN(diff)
        '''.format(mi_table, ron_table)



def get_quant_matrix_table_one_study(pdc_api_end_point, study_id, study_submitter_id):
    quant_log2_ratio_query = ('{ quantDataMatrix(study_submitter_id: \"'
                              + study_submitter_id + '\" data_type: \"log2_ratio\") }')

    quant_res = requests.post(pdc_api_end_point, json={'query': quant_log2_ratio_query})

    if not quant_res.ok:
        print('Error: PDC API request did not return OK')
        return None

    json_res = quant_res.json()

    if 'errors' in json_res:
        print('No quant matrix for study_submitter_id = ' + study_submitter_id)
        return None

    print('Got quant matrix for study_submitter_id = ' + study_submitter_id)
    quant_matrix = json_res[u'data'][u'quantDataMatrix']

    first_row_data = quant_matrix[0]
    for i in range(1, len(first_row_data)):
        if ":" in first_row_data[i]:
            aliquot_submitter_id = first_row_data[i].split(":")[1]
        else:
            print('no : in here ' + first_row_data[i])
            aliquot_submitter_id = first_row_data[i]
        quant_matrix[0][i] = aliquot_submitter_id

    print('Converted first row to aliquot_submitter_id')

    num_rows = len(quant_matrix)
    num_cols = len(quant_matrix[0])
    quant_matrix_table = []
    quant_matrix_table.append(['study_id', 'aliquot_submitter_id', 'gene', 'log2_ratio'])
    for i in range(1, num_rows):
        for j in range(1, num_cols):
            log2_value = quant_matrix[i][j]
            gene = quant_matrix[i][0]
            aliquot_submitter_id = quant_matrix[0][j]
            quant_matrix_table.append([study_id, aliquot_submitter_id, gene, log2_value])

    print('Converted quant matrix into rows of log2ratio values')
    return quant_matrix_table


def write_quant_matrix_table_to_tsv(quant_matrix_table, tsv_file):
    with open(tsv_file, "w") as tsv_out:
        num_rows = len(quant_matrix_table)
        for i in range(0, num_rows):
            tsv_out.write("\t".join([quant_matrix_table[i][0],
                                    quant_matrix_table[i][1],
                                    quant_matrix_table[i][2],
                                     quant_matrix_table[i][3]]) + "\n")
    return True


def get_biospeciman_table_one_study(pdc_api_end_point, study_id):
    print('Getting biospeciman table for study ' + study_id)
    biospeciman_query = '{ biospecimenPerStudy(study_id: "' + \
            study_id + '"' + \
            ') { aliquot_id sample_id case_id aliquot_submitter_id sample_submitter_id case_submitter_id aliquot_status' \
            ' case_status sample_status project_name sample_type disease_type primary_site pool taxon} }'

    biospeciman_res = requests.post(pdc_api_end_point, json={'query': biospeciman_query})

    if not biospeciman_res.ok:
        print('Error: PDC API request did not return OK')
        return None

    json_res = biospeciman_res.json()

    if 'errors' in json_res:
        print('No biospeciman table for study_id = ' + study_id)
        return None

    print('Got biospeciman table for study_id = ' + study_id)
    biospeciman = json_res[u'data'][u'biospecimenPerStudy']

    print('Getting study info for study ' + study_id)
    study_info_query = '{ study(study_id: "' + \
                study_id + '"' + \
                ') { study_id pdc_study_id study_submitter_id study_name} }'

    study_info_res = requests.post(pdc_api_end_point, json={'query': study_info_query})

    if not study_info_res.ok:
        print('Error: PDC API request did not return OK')
        return None

    json_res = study_info_res.json()
    study_info = json_res[u'data'][u'study']
    study_name = study_info[0][u'study_name']

    print('Generating biospeciman table')
    num_rows = len(biospeciman)
    table = []
    table.append(['study_id', 'aliquot_submitter_id', 'study_name', 'aliquot_id', 'sample_id'])
    for i in range(0, num_rows):
        aliquot_id = biospeciman[i][u'aliquot_id']
        sample_id = biospeciman[i][u'sample_id']
        aliquot_submitter_id = biospeciman[i][u'aliquot_submitter_id']
        table.append([study_id, aliquot_submitter_id, study_name, aliquot_id, sample_id])

    return table


def write_biospeciman_table_to_tsv(biospeciman_table, tsv_file):
    with open(tsv_file, "w") as tsv_out:
        num_rows = len(biospeciman_table)
        for i in range(0, num_rows):
            tsv_out.write("\t".join([biospeciman_table[i][0],
                                    biospeciman_table[i][1],
                                    biospeciman_table[i][2],
                                    biospeciman_table[i][3],
                                    biospeciman_table[i][4], ]) + "\n")
    return True


def create_clean_target(local_files_dir):
    """
    GDC download client builds a tree of files in directories. This routine clears the tree out if it exists.
    """

    if os.path.exists(local_files_dir):
        print("deleting {}".format(local_files_dir))
        try:
            shutil.rmtree(local_files_dir)
        except OSError as e:
            print("Error: %s - %s." % (e.filename, e.strerror))

        print("done {}".format(local_files_dir))

    if not os.path.exists(local_files_dir):
        os.makedirs(local_files_dir)


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
    # Schemas and table descriptions are maintained in the github repo:
    #
    home = expanduser("~")
    local_files_dir = "{}/{}".format(home, params['LOCAL_FILES_DIR'])
    quant_matrix_tsv = "{}/{}".format(home, params['QUANT_MATRIX_TSV'])
    biospeciman_tsv = "{}/{}".format(home, params['BIOSPECIMAN_TSV'])

    hold_schema_dict_quant_matrix = "{}/{}".format(home, params['HOLD_SCHEMA_DICT_QUANT_MATRIX'])
    hold_schema_list_quant_matrix = "{}/{}".format(home, params['HOLD_SCHEMA_LIST_QUANT_MATRIX'])
    hold_schema_dict_biospeciman = "{}/{}".format(home, params['HOLD_SCHEMA_DICT_BIOSPECIMAN'])
    hold_schema_list_biospeciman = "{}/{}".format(home, params['HOLD_SCHEMA_LIST_BIOSPECIMAN'])

    if 'clear_target_directory' in steps:
        print('clear_target_directory')
        create_clean_target(local_files_dir)

    # Quant matrix table...
    if 'get_quant_matrix_table_one_study' in steps:
        print('get_quant_matrix_table_one_study')
        try:
            quant_matrix_table = get_quant_matrix_table_one_study(params['PDC_API_END_POINT'],
                                                                  params['ONE_STUDY_ID'],
                                                                  params['ONE_STUDY_SUBMITTER_ID'])
        except Exception as ex:
            print("get_quant_matrix_table_one_study failed: {}".format(str(ex)))
            return

    if 'write_quant_matrix_table_to_tsv' in steps:
        print('write_quant_matrix_table_to_tsv')
        success = write_quant_matrix_table_to_tsv(quant_matrix_table, quant_matrix_tsv)
        if not success:
            print("Failure writing quant matrix table to tsv")
            return

    bucket_quant_matrix = '{}/{}'.format(params['WORKING_BUCKET_DIR'], params['BUCKET_TSV_QUANT_MATRIX'])

    if 'upload_quant_matrix_tsv_to_bucket' in steps:
        print('upload_quant_matrix_tsv_to_bucket')
        upload_to_bucket(params['WORKING_BUCKET'], bucket_quant_matrix, quant_matrix_tsv)

    if 'create_quant_matrix_bq_from_tsv' in steps:
        print('create_quant_matrix_bq_from_tsv')
        typing_tups = build_schema(quant_matrix_tsv, params['SCHEMA_SAMPLE_SKIPS'])
        build_combined_schema(None, None,
                              typing_tups, hold_schema_list_quant_matrix, hold_schema_dict_quant_matrix)
        bucket_src_url = 'gs://{}/{}'.format(params['WORKING_BUCKET'], bucket_quant_matrix)
        with open(hold_schema_list_quant_matrix, mode='r') as schema_hold_dict:
            typed_schema = json_loads(schema_hold_dict.read())
        csv_to_bq(typed_schema, bucket_src_url, params['TARGET_DATASET'], params['TARGET_TABLE_QUANT_MATRIX'], params['BQ_AS_BATCH'])


    # Biospeciman table...
    if 'get_biospeciman_table_one_study' in steps:
        print('get_biospeciman_table_one_study')
        try:
            biospeciman_table = get_biospeciman_table_one_study(params['PDC_API_END_POINT'],
                                                                params['ONE_STUDY_ID'])
        except Exception as ex:
            print("get_biospeciman_table_one_study failed: {}".format(str(ex)))
            return

    if 'write_biospeciman_table_to_tsv' in steps:
        print('write_biospeciman_table_to_tsv')
        success = write_biospeciman_table_to_tsv(biospeciman_table, biospeciman_tsv)
        if not success:
            print("Failure writing biospeciman table to tsv")
            return

    if 'upload_biospeciman_tsv_to_bucket' in steps:
        print('upload_biospeciman_tsv_to_bucket')
        upload_to_bucket(params['WORKING_BUCKET'], bucket_quant_matrix, biospeciman_tsv)

    if 'create_biospeciman_bq_from_tsv' in steps:
        print('create_biospeciman_bq_from_tsv')
        typing_tups = build_schema(biospeciman_tsv, params['SCHEMA_SAMPLE_SKIPS'])
        build_combined_schema(None, None,
                              typing_tups, hold_schema_list_biospeciman, hold_schema_dict_biospeciman)
        bucket_src_url = 'gs://{}/{}'.format(params['WORKING_BUCKET'], bucket_quant_matrix)
        with open(hold_schema_list_biospeciman, mode='r') as schema_hold_dict:
            typed_schema = json_loads(schema_hold_dict.read())
        csv_to_bq(typed_schema, bucket_src_url, params['TARGET_DATASET'], params['TARGET_TABLE_BIOSPECIMAN'], params['BQ_AS_BATCH'])


    # Join quant matrix table and biospeciman table...
    if 'join_quant_matrix_and_biospeciman_table' in steps:
        print('join_quant_matrix_and_biospeciman_table')
        quant_matrix_bq_table = "{}.{}.{}".format(params['WORKING_PROJECT'],
                                                  params['TARGET_DATASET'],
                                                  params['TARGET_TABLE_QUANT_MATRIX'])
        biospeciman_bq_table = "{}.{}.{}".format(params['WORKING_PROJECT'],
                                                  params['TARGET_DATASET'],
                                                  params['TARGET_TABLE_BIOSPECIMAN'])
        join_quant_matrix_and_biospeciman_table(quant_matrix_bq_table,
                                                biospeciman_bq_table,
                                                params['TARGET_DATASET'],
                                                params['JOINED_QUANT_MATRIX_BIOSPECIMAN_TABLE'],
                                                params['BQ_AS_BATCH'])


    # Join PDC_Quant _Matrix_Biospeciman_Joined_Table_One_Study and PDC_Genes_To_Protein_Mapping_File
    if 'join_quant_matrix_and_pdc_genes_to_protein_table' in steps:
        print('join_quant_matrix_and_pdc_genes_to_protein_table')
        quant_matrix_biospeciman_bq_table = "{}.{}.{}".format(params['WORKING_PROJECT'],
                                                  params['TARGET_DATASET'],
                                                  params['TARGET_TABLE_QUANT_MATRIX_BIOSPECIMAN'])
        pdc_genes_protein_table = "{}.{}.{}".format(params['WORKING_PROJECT'],
                                                  params['TARGET_DATASET'],
                                                  params['TARGET_TABLE_PDC_GENES_PROTEIN'])
        join_quant_natrix_and_pdc_genes_to_protein_table(quant_matrix_biospeciman_bq_table,
                                                pdc_genes_protein_table,
                                                params['TARGET_DATASET'],
                                                params['JOINED_QUANT_MATRIX_BIOSPECIMAN_PDC_GENE_PROTEIN_TABLE'],
                                                params['BQ_AS_BATCH'])

    # Compare result to Ron's table...
    if 'compare_mi_ron_table' in steps:
        print('compare_mi_ron_table')
        compare_mi_ron_table(params['MI_TABLE'],
                             params['RON_TABLE'],
                             params['TARGET_DATASET'],
                             params['COMPARE_RESULT'],
                             params['BQ_AS_BATCH'])

    print('job completed')


if __name__ == "__main__":
    main(sys.argv)
