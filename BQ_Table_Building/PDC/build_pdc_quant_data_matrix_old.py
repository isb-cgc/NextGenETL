"""
Copyright 2020-2021, Institute for Systems Biology

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""
import re
import csv
import ftplib
import gzip
import time
import os
import sys

from functools import cmp_to_key
from google.cloud import storage

from common_etl.utils import (get_query_results, format_seconds, get_scratch_fp, upload_to_bucket,
                              get_graphql_api_response, has_fatal_error, load_bq_schema_from_json,
                              create_and_load_table_from_tsv, create_tsv_row, load_table_from_query, exists_bq_table,
                              load_config, publish_table, construct_table_name, construct_table_name_from_list)

from BQ_Table_Building.PDC.pdc_utils import (get_pdc_studies_list, build_table_from_tsv, get_filename, get_dev_table_id,
                                             update_column_metadata, update_pdc_table_metadata, get_prefix)

API_PARAMS = dict()
BQ_PARAMS = dict()
YAML_HEADERS = ('api_params', 'bq_params', 'steps')


# ***** QUANT DATA MATRIX FUNCTIONS

def make_quant_data_matrix_query(study_submitter_id, data_type):
    """
    Creates a graphQL string for querying the PDC API's allCases endpoint.
    :param study_submitter_id: Study submitter id for query arguments
    :param data_type: Data type for query arguments (e.g. log2_ratio)
    :return: GraphQL query string
    """

    return '''{{ 
            quantDataMatrix(study_submitter_id: \"{}\" data_type: \"{}\" acceptDUA: true) 
        }}'''.format(study_submitter_id, data_type)


def make_proteome_quant_table_query(study):
    """
    Create sql query to create proteome quant data matrix table for a given study.
    :param study: PDC study name
    :return: sql query string
    """
    raw_quant_table_name = create_raw_quant_table_name(study_id_dict=study)
    raw_quant_table_id = get_dev_table_id(BQ_PARAMS,
                                      dataset=BQ_PARAMS['QUANT_DATASET'],
                                      table_name=raw_quant_table_name)

    case_aliquot_table_name = construct_table_name(API_PARAMS,
                                                   prefix=get_prefix(API_PARAMS, API_PARAMS['ALIQUOT_ENDPOINT']))
    case_aliquot_table_id = get_dev_table_id(BQ_PARAMS,
                                             dataset=BQ_PARAMS['META_DATASET'],
                                             table_name=case_aliquot_table_name)

    # todo should this be changed?
    gene_table_name = '{}_{}'.format(BQ_PARAMS['GENE_TABLE'], API_PARAMS['RELEASE'])
    gene_table_id = get_dev_table_id(BQ_PARAMS,
                                     dataset=BQ_PARAMS['META_DATASET'],
                                     table_name=gene_table_name)

    return """
    WITH csa_mapping AS (SELECT case_id, s.sample_id, a.aliquot_id, arm.aliquot_run_metadata_id
    FROM `{}` 
    CROSS JOIN UNNEST(samples) as s
    CROSS JOIN UNNEST(s.aliquots) as a
    CROSS JOIN UNNEST(a.aliquot_run_metadata) as arm)

    SELECT c.case_id, c.sample_id, c.aliquot_id, 
    q.aliquot_submitter_id, q.aliquot_run_metadata_id, q.study_name, q.protein_abundance_log2ratio,
    g.*
    FROM `{}` as q
    INNER JOIN csa_mapping AS c 
    ON c.aliquot_run_metadata_id = q.aliquot_run_metadata_id
    INNER JOIN `{}` as g 
    ON g.gene_symbol = q.gene_symbol
    """.format(case_aliquot_table_id, quant_table_id, gene_table_id)


'''
def build_quant_tsv(study_id_dict, data_type, tsv_fp):
    """
    Output quant data rows in tsv format, for future BQ ingestion.
    :param study_id_dict: dictionary of study ids
    :param data_type: data type of API request, e.g. log2_ratio
    :param tsv_fp: output filepath for tsv file
    :return:
    """
    study_submitter_id = study_id_dict['study_submitter_id']
    study_name = study_id_dict['study_name']
    lines_written = 0

    quant_query = make_quant_data_matrix_query(study_submitter_id, data_type)
    res_json = get_graphql_api_response(API_PARAMS, quant_query, fail_on_error=False)

    if not res_json or not res_json['data']['quantDataMatrix']:
        return lines_written

    aliquot_metadata = list()

    id_row = res_json['data']['quantDataMatrix'].pop(0)
    id_row.pop(0)  # remove gene column header string

    # process first row, which gives us the aliquot ids and idx positions
    for el in id_row:
        aliquot_run_metadata_id = ""
        aliquot_submitter_id = ""

        split_el = el.split(':')

        if len(split_el) != 2:
            print("Quant API returns non-standard aliquot_run_metadata_id entry: {}".format(el, ))
        else:
            if split_el[0]:
                aliquot_run_metadata_id = split_el[0]
            if split_el[1]:
                aliquot_submitter_id = split_el[1]

        aliquot_metadata.append({
            "aliquot_run_metadata_id": aliquot_run_metadata_id,
            "aliquot_submitter_id": aliquot_submitter_id})

    # iterate over each gene row and add to the correct aliquot_run obj
    with open(tsv_fp, 'w') as fh:
        fh.write(create_tsv_row(['aliquot_run_metadata_id',
                                 'aliquot_submitter_id',
                                 'study_name',
                                 'gene_symbol',
                                 'protein_abundance_log2ratio']))

        for row in res_json['data']['quantDataMatrix']:
            gene_symbol = row.pop(0)

            for i, log2_ratio in enumerate(row):
                fh.write(create_tsv_row([aliquot_metadata[i]['aliquot_run_metadata_id'],
                                         aliquot_metadata[i]['aliquot_submitter_id'],
                                         study_name,
                                         gene_symbol,
                                         log2_ratio]))
            lines_written += 1

        return lines_written
'''

def get_quant_files():
    """
    Get set of quant data matrix files from Google Cloud Bucket.
    :return: file name set
    """
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(BQ_PARAMS['WORKING_BUCKET'], prefix=BQ_PARAMS['WORKING_BUCKET_DIR'])
    files = set()

    for blob in blobs:
        filename = blob.name.split('/')[-1]
        files.add(filename)

    return files


def has_quant_table(study_submitter_id):
    """
    Determines whether a given study has quant data matrix data in a BQ table.
    :param study_submitter_id: PDC study submitter id
    :return: True if table exists, false otherwise
    """
    quant_prefix = get_prefix(API_PARAMS, API_PARAMS['QUANT_ENDPOINT'])
    table_name = construct_table_name(API_PARAMS,
                                      prefix=quant_prefix,
                                      suffix=study_submitter_id)
    table_id = get_dev_table_id(BQ_PARAMS,
                                dataset=BQ_PARAMS['QUANT_DATASET'],
                                table_name=table_name)

    return exists_bq_table(table_id)


def get_proteome_studies(studies_list):
    """
    Get list of proteome studies.
    :param studies_list: list of all non-embargoed studies
    :return: list of proteome studies
    """
    proteome_studies_list = list()

    for study in studies_list:
        if study['analytical_fraction'] == "Proteome":
            proteome_studies_list.append(study)

    return proteome_studies_list


def get_quant_table_name(study, is_final=True):
    """
    Get quant table name for given study
    :param study: study metadata dict
    :param is_final: if True, query is requesting published table name; otherwise dev table name
    :return: if True, return published table name, otherwise return dev table name
    """
    quant_prefix = get_prefix(API_PARAMS, API_PARAMS['QUANT_ENDPOINT'])
    analytical_fraction = study['analytical_fraction']

    if not is_final:
        return construct_table_name(API_PARAMS,
                                    prefix=quant_prefix,
                                    suffix=study['pdc_study_id'])
    else:
        study_name = study['study_name']
        study_name = study_name.replace(analytical_fraction, "")

        return "_".join([quant_prefix,
                         analytical_fraction.lower(),
                         change_study_name_to_table_name_format(study_name),
                         API_PARAMS['DATA_SOURCE'],
                         API_PARAMS['RELEASE']])


def change_study_name_to_table_name_format(study_name):
    """
    Converts study name to table name format.
    :param study_name: PDC study associated with table data
    :return: table name
    """
    study_name = re.sub('[^0-9a-zA-Z_]+', '_', study_name)

    study_name_list = study_name.split(" ")
    new_study_name_list = list()

    for name in study_name_list:
        if not name.isupper():
            name = name.lower()
        if name:
            new_study_name_list.append(name)

    return "_".join(new_study_name_list)


def main(args):
    start_time = time.time()
    print("PDC script started at {}".format(time.strftime("%x %X", time.localtime())))

    steps = None

    try:
        global API_PARAMS, BQ_PARAMS
        API_PARAMS, BQ_PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        has_fatal_error(err, ValueError)

    studies_list = get_pdc_studies_list(API_PARAMS, BQ_PARAMS, include_embargoed=False)
    '''
    if 'build_quant_tsvs' in steps:
        quant_prefix = get_prefix(API_PARAMS, API_PARAMS['QUANT_ENDPOINT'])

        for study_id_dict in studies_list:
            quant_tsv_file = get_filename(API_PARAMS,
                                          file_extension='tsv',
                                          prefix=quant_prefix,
                                          suffix=study_id_dict['pdc_study_id'])

            quant_tsv_path = get_scratch_fp(BQ_PARAMS, quant_tsv_file)

            lines_written = build_quant_tsv(study_id_dict, 'log2_ratio', quant_tsv_path)
            print("\n{0} lines written for {1}".format(lines_written, study_id_dict['study_name']))

            if lines_written > 0:
                upload_to_bucket(BQ_PARAMS, quant_tsv_path, delete_local=True)
                print("{0} uploaded to Google Cloud bucket!".format(quant_tsv_file))
                os.remove(quant_tsv_path)


    if 'build_quant_tables' in steps:
        quant_prefix = get_prefix(API_PARAMS, API_PARAMS['QUANT_ENDPOINT'])

        print("Building quant tables...")
        blob_files = get_quant_files()

        for study_id_dict in studies_list:
            quant_tsv_file = get_filename(API_PARAMS,
                                          file_extension='tsv',
                                          prefix=quant_prefix,
                                          suffix=study_id_dict['pdc_study_id'])

            if quant_tsv_file not in blob_files:
                print('Skipping table build for {} (jsonl not found in bucket)'.format(study_id_dict['study_name']))
            else:
                print("Building table for {} ({})".format(study_id_dict['pdc_study_id'],
                                                          study_id_dict['analytical_fraction']))
                build_table_from_tsv(API_PARAMS, BQ_PARAMS,
                                     table_prefix=quant_prefix,
                                     table_suffix=study_id_dict['pdc_study_id'])

    if 'build_uniprot_tsv' in steps:
        gz_file_name = API_PARAMS['UNIPROT_MAPPING_FP'].split('/')[-1]
        split_file = gz_file_name.split('.')
        mapping_file = split_file[0] + '_' + API_PARAMS['UNIPROT_RELEASE'] + API_PARAMS['UNIPROT_FILE_EXT']

        download_from_uniprot_ftp(mapping_file, API_PARAMS['UNIPROT_MAPPING_FP'], 'UniProt mapping')
        upload_to_bucket(BQ_PARAMS, 
                         scratch_fp=get_scratch_fp(BQ_PARAMS, mapping_file), 
                         delete_local=True)

    if 'build_uniprot_table' in steps:
        gz_file_name = API_PARAMS['UNIPROT_MAPPING_FP'].split('/')[-1]
        split_file = gz_file_name.split('.')

        mapping_table = construct_table_name(API_PARAMS,
                                             prefix=BQ_PARAMS['UNIPROT_MAPPING_TABLE'],
                                             release=API_PARAMS['UNIPROT_RELEASE'])

        fps_table_id = get_dev_table_id(BQ_PARAMS,
                                        dataset=BQ_PARAMS['META_DATASET'],
                                        table_name=mapping_table)

        print("\nBuilding {0}... ".format(fps_table_id))

        # todo change how schema is generated?
        fps_schema_file = construct_table_name_from_list(fps_table_id.split("."))
        schema_filename = fps_schema_file + '.json'
        schema = load_bq_schema_from_json(BQ_PARAMS, schema_filename)

        data_file = split_file[0] + '_' + API_PARAMS['UNIPROT_RELEASE'] + API_PARAMS['UNIPROT_FILE_EXT']

        create_and_load_table_from_tsv(bq_params=BQ_PARAMS, tsv_file=data_file, table_id=fps_table_id,
                                       num_header_rows=0, schema=schema)
        print("Uniprot table built!")

    if 'build_swissprot_table' in steps:
        table_name = construct_table_name(API_PARAMS,
                                          prefix=BQ_PARAMS['SWISSPROT_TABLE'],
                                          release=API_PARAMS['UNIPROT_RELEASE'])

        fps_table_id = get_dev_table_id(BQ_PARAMS,
                                        dataset=BQ_PARAMS['META_DATASET'],
                                        table_name=table_name)

        print("Building {0}... ".format(fps_table_id))
        fps_schema_file = construct_table_name_from_list(fps_table_id.split("."))
        schema_filename = fps_schema_file + '.json'
        schema = load_bq_schema_from_json(BQ_PARAMS, schema_filename)

        data_file = table_name + API_PARAMS['UNIPROT_FILE_EXT']

        create_and_load_table_from_tsv(bq_params=BQ_PARAMS, tsv_file=data_file, table_id=fps_table_id,
                                       num_header_rows=0, schema=schema)

        print("Swiss-prot table built!")
    '''

    if 'build_gene_tsv' in steps:
        gene_symbol_list = build_gene_symbol_list(studies_list)
        gene_tsv_file = get_filename(API_PARAMS,
                                     file_extension='tsv',
                                     prefix=BQ_PARAMS['GENE_TABLE'])

        gene_tsv_path = get_scratch_fp(BQ_PARAMS, gene_tsv_file)

        build_gene_tsv(gene_symbol_list=gene_symbol_list,
                       gene_tsv=gene_tsv_path,
                       append=API_PARAMS['RESUME_GENE_TSV'])
        upload_to_bucket(BQ_PARAMS, scratch_fp=gene_tsv_path, delete_local=True)

    if 'build_gene_table' in steps:
        gene_tsv_file = get_filename(API_PARAMS,
                                     file_extension='tsv',
                                     prefix=BQ_PARAMS['GENE_TABLE'])

        gene_tsv_path = get_scratch_fp(BQ_PARAMS, gene_tsv_file)

        with open(gene_tsv_path, 'r') as tsv_file:
            gene_reader = csv.reader(tsv_file, delimiter='\t')

            passed_first_row = False
            num_columns = None

            for row in gene_reader:
                if not passed_first_row:
                    num_columns = len(row)
                    passed_first_row = True
                    continue

                if len(row) != num_columns:
                    print(row)

        build_table_from_tsv(API_PARAMS, BQ_PARAMS, table_prefix=BQ_PARAMS['GENE_TABLE'])

    if 'build_proteome_quant_tables' in steps:
        quant_prefix = get_prefix(API_PARAMS, API_PARAMS['QUANT_ENDPOINT'])

        for study in studies_list:

            # only run the build script for analytes we're currently publishing
            if study['analytical_fraction'] not in BQ_PARAMS["BUILD_ANALYTES"]:
                continue

            pdc_study_id = study['pdc_study_id']
            raw_table_name = get_quant_table_name(study, is_final=False)
            raw_table_id = get_dev_table_id(BQ_PARAMS, dataset=BQ_PARAMS['QUANT_DATASET'], table_name=raw_table_name)

            if exists_bq_table(raw_table_id):
                final_table_name = get_quant_table_name(study)
                final_table_id = get_dev_table_id(BQ_PARAMS, final_table_name)

                load_table_from_query(BQ_PARAMS,
                                      table_id=final_table_id,
                                      query=make_proteome_quant_table_query(pdc_study_id))

                update_column_metadata(API_PARAMS, BQ_PARAMS, final_table_id)

        update_pdc_table_metadata(API_PARAMS, BQ_PARAMS, table_type=quant_prefix)

    if "publish_proteome_tables" in steps:
        for study in get_proteome_studies(studies_list):
            table_name = get_quant_table_name(study)
            project_submitter_id = study['project_submitter_id']

            if project_submitter_id not in BQ_PARAMS['PROJECT_MAP']:
                has_fatal_error("{} metadata missing from PROJECT_MAP".format(project_submitter_id))

            publish_table(API_PARAMS, BQ_PARAMS,
                          public_dataset=BQ_PARAMS['PROJECT_MAP'][project_submitter_id]['DATASET'],
                          source_table_id=get_dev_table_id(BQ_PARAMS, table_name),
                          overwrite=True)

    end = time.time() - start_time
    print("Finished program execution in {}!\n".format(format_seconds(end)))


if __name__ == '__main__':
    main(sys.argv)
