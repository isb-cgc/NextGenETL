import re
import csv
import ftplib
import gzip
import time
import os
import sys
import requests

from functools import cmp_to_key
from google.cloud import storage

from common_etl.utils import (get_query_results, format_seconds, get_scratch_fp, upload_to_bucket,
                              get_graphql_api_response, has_fatal_error, load_bq_schema_from_json,
                              create_and_load_table_from_tsv, create_tsv_row, load_table_from_query, exists_bq_table,
                              load_config, publish_table, construct_table_name, construct_table_name_from_list,
                              create_and_upload_schema_for_tsv, return_schema_object_for_bq, get_rel_prefix)

from BQ_Table_Building.PDC.pdc_utils import (get_pdc_studies_list, get_filename, get_dev_table_id,
                                             update_column_metadata, update_pdc_table_metadata, get_prefix)

API_PARAMS = dict()
BQ_PARAMS = dict()
YAML_HEADERS = ('api_params', 'bq_params', 'steps')


def retrieve_uniprot_kb_genes():
    """

    Retrieve Swiss-Prot ids and gene names from UniProtKB REST API.
    :return: REST API response text (tsv)
    """
    query = 'organism:9606+AND+reviewed:yes'
    data_format = 'tab'
    columns = 'id,genes(PREFERRED),database(GeneID),database(HGNC)'

    request_url = 'https://www.uniprot.org/uniprot/?query={}&format={}&columns={}'.format(query, data_format, columns)

    response = requests.get(request_url)
    return response.text


def make_quant_data_matrix_query(pdc_study_id, data_type):
    """

    Create graphQL string for querying the PDC API's quantDataMatrix endpoint.
    :param pdc_study_id: PDC study id for query argument
    :param data_type: Data type for query argument (e.g. log2_ratio)
    :return: GraphQL query string
    """

    return '''{{ 
            quantDataMatrix(pdc_study_id: \"{}\" data_type: \"{}\" acceptDUA: true) 
        }}'''.format(pdc_study_id, data_type)


def build_quant_tsv(study_id_dict, data_type, tsv_fp, header):
    """

    Output quant data rows in tsv format, for future BQ ingestion.
    :param study_id_dict: dictionary of study ids
    :param data_type: data type of API request, e.g. log2_ratio
    :param tsv_fp: output filepath for tsv file
    :param header: header for quant tsv file
    :return: count of lines written to tsv
    """
    study_name = study_id_dict['study_name']
    lines_written = 0
    quant_endpoint = API_PARAMS['QUANT_ENDPOINT']

    quant_query = make_quant_data_matrix_query(study_id_dict['pdc_study_id'], data_type)
    res_json = get_graphql_api_response(API_PARAMS, quant_query, fail_on_error=False)

    if not res_json or not res_json['data'][quant_endpoint]:
        return lines_written

    aliquot_metadata = list()

    id_row = res_json['data'][quant_endpoint].pop(0)
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

            if not aliquot_submitter_id or not aliquot_run_metadata_id:
                has_fatal_error("Unexpected value for aliquot_run_metadata_id:aliquot_submitter_id ({})".format(el))

        aliquot_metadata.append({
            "aliquot_run_metadata_id": aliquot_run_metadata_id,
            "aliquot_submitter_id": aliquot_submitter_id})

    # iterate over each gene row and add to the correct aliquot_run obj
    with open(tsv_fp, 'w') as fh:
        fh.write(create_tsv_row(header))

        for row in res_json['data'][quant_endpoint]:
            gene_symbol = row.pop(0)

            for i, log2_ratio in enumerate(row):
                fh.write(create_tsv_row([aliquot_metadata[i]['aliquot_run_metadata_id'],
                                         aliquot_metadata[i]['aliquot_submitter_id'],
                                         study_name,
                                         gene_symbol,
                                         log2_ratio]))
            lines_written += 1

        return lines_written


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
        version = get_rel_prefix(API_PARAMS)
        if "quant" in filename and "schema" not in filename and version in filename:
            files.add(filename)

    return files


def create_raw_quant_table_name(study_id_dict, include_release=True):
    """

    todo
    :param study_id_dict:
    :param include_release:
    :return:
    """
    prefix = get_prefix(API_PARAMS, API_PARAMS['QUANT_ENDPOINT'])
    suffix = study_id_dict['analytical_fraction'] + '_' + study_id_dict['pdc_study_id']
    return construct_table_name(API_PARAMS,
                                prefix=prefix,
                                suffix=suffix,
                                include_release=include_release)


# todo check
def make_gene_symbols_per_study_query(pdc_study_id):
    """
    Returns list of gene symbols for a given PDC study id.
    :param pdc_study_id: PDC study id for which to retrieve the symbols
    :return: sql query string
    """
    endpoint = 'quantDataMatrix'
    prefix = get_prefix(API_PARAMS, endpoint)

    table_name = construct_table_name(API_PARAMS,
                                      prefix=prefix,
                                      suffix=pdc_study_id,
                                      release=API_PARAMS['RELEASE'])

    table_id = get_dev_table_id(BQ_PARAMS, table_name)

    return """
        SELECT DISTINCT(gene_symbol)
        FROM `{}`
    """.format(table_id)


# todo check
'''
def add_gene_symbols_per_study(study_obj, gene_symbol_set):
    """

    Create master gene symbol set by querying the gene symbols for each study.
    :param study_obj: todo
    :param gene_symbol_set: set of gene symbols
    """
    quant_prefix = get_prefix(API_PARAMS, API_PARAMS['QUANT_ENDPOINT'])
    raw_quant_table_name = create_raw_quant_table_name(study_id_dict=study_obj)

    raw_quant_table_id = get_dev_table_id(BQ_PARAMS,
                                          dataset=BQ_PARAMS['QUANT_DATASET'],
                                          table_name=raw_quant_table_name)

    if exists_bq_table(raw_quant_table_id):
        results = get_query_results(make_gene_symbols_per_study_query(pdc_study_id))

        for row in results:
            gene_symbol_set.add(row['gene_symbol'])


# todo check
def build_gene_symbol_list(studies_list):
    """
    Creates sorted list of all gene symbols used for any current PDC study
    :param studies_list: list of non-embargoed PDC studies
    :return: alphabetical gene symbol list
    """
    quant_prefix = get_prefix(API_PARAMS, API_PARAMS['QUANT_ENDPOINT'])

    print("Building gene symbol tsv!")
    gene_symbol_set = set()

    for study in studies_list:
        table_name = construct_table_name(API_PARAMS,
                                          prefix=quant_prefix,
                                          suffix=study['pdc_study_id'],
                                          release=API_PARAMS['RELEASE'])

        table_id = get_dev_table_id(BQ_PARAMS,
                                    dataset=BQ_PARAMS['DEV_DATASET'],
                                    table_name=table_name)

        if exists_bq_table(table_id):
            add_gene_symbols_per_study(study, gene_symbol_set)
            print("- Added {}, current count: {}".format(study['pdc_study_id'], len(gene_symbol_set)))
        else:
            print("- No table for {}, skipping.".format(study['pdc_study_id']))

    gene_symbol_list = list(sorted(gene_symbol_set))
    return gene_symbol_list
'''


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

    swissprot_file_name = get_filename(API_PARAMS,
                                       file_extension='tsv',
                                       prefix=BQ_PARAMS['SWISSPROT_TABLE'],
                                       release=API_PARAMS['SWISSPROT_RELEASE'])
    swissprot_table_name = construct_table_name(API_PARAMS,
                                                prefix=BQ_PARAMS['SWISSPROT_TABLE'],
                                                release=API_PARAMS['SWISSPROT_RELEASE'])
    swissprot_table_id = get_dev_table_id(BQ_PARAMS,
                                          dataset=BQ_PARAMS['META_DATASET'],
                                          table_name=swissprot_table_name)

    if 'build_swissprot_tsv' in steps:
        swissprot_fp = get_scratch_fp(BQ_PARAMS, swissprot_file_name)

        swissprot_data = retrieve_uniprot_kb_genes()

        with open(swissprot_fp, 'w') as swissprot_file:
            swissprot_file.write(swissprot_data)

        create_and_upload_schema_for_tsv(API_PARAMS, BQ_PARAMS, table_name=BQ_PARAMS['SWISSPROT_TABLE'],
                                         tsv_fp=swissprot_fp, header_row=0, skip_rows=1,
                                         release=API_PARAMS['SWISSPROT_RELEASE'])

        upload_to_bucket(BQ_PARAMS, swissprot_fp, delete_local=True)

    if 'build_swissprot_table' in steps:
        swissprot_schema = return_schema_object_for_bq(API_PARAMS, BQ_PARAMS,
                                                       table_name=BQ_PARAMS['SWISSPROT_TABLE'],
                                                       release=API_PARAMS['SWISSPROT_RELEASE'])

        create_and_load_table_from_tsv(BQ_PARAMS,
                                       tsv_file=swissprot_file_name,
                                       table_id=swissprot_table_id,
                                       num_header_rows=0,
                                       schema=swissprot_schema)
        print("SwissProt table built!")

    if 'build_quant_tsvs' in steps:
        for study_id_dict in studies_list:
            quant_table_name_no_version = create_raw_quant_table_name(study_id_dict, include_release=False)
            raw_quant_tsv_file = create_raw_quant_table_name(study_id_dict) + '.tsv'
            quant_tsv_path = get_scratch_fp(BQ_PARAMS, raw_quant_tsv_file)

            # todo change gene_symbol to gene name?
            raw_quant_header = ['aliquot_run_metadata_id',
                                'aliquot_submitter_id',
                                'study_name',
                                'gene_symbol',
                                'protein_abundance_log2ratio']

            data_types_dict = {
                'aliquot_run_metadata_id': 'STRING',
                'aliquot_submitter_id': 'STRING',
                'study_name': 'STRING',
                'gene_symbol': 'STRING',
                'protein_abundance_log2ratio': 'FLOAT64'
            }

            lines_written = build_quant_tsv(study_id_dict, 'log2_ratio', quant_tsv_path, raw_quant_header)

            if lines_written > 0:
                create_and_upload_schema_for_tsv(API_PARAMS, BQ_PARAMS,
                                                 table_name=quant_table_name_no_version,
                                                 tsv_fp=quant_tsv_path,
                                                 types_dict=data_types_dict,
                                                 header_list=raw_quant_header,
                                                 skip_rows=1)

                upload_to_bucket(BQ_PARAMS, quant_tsv_path, delete_local=True)
                print("\n{0} lines written for {1}".format(lines_written, study_id_dict['study_name']))
                print("{0} uploaded to Google Cloud bucket!".format(raw_quant_tsv_file))
            else:
                print("\n{0} lines written for {1}; not uploaded.".format(lines_written, study_id_dict['study_name']))

    if 'build_quant_tables' in steps:
        print("Building quant tables...")
        blob_files = get_quant_files()

        for file_name in blob_files:
            print(file_name)
        exit()

        quant_table_name_no_version = create_raw_quant_table_name(study_id_dict, include_release=False)

        raw_quant_schema = return_schema_object_for_bq(API_PARAMS, BQ_PARAMS, table_name=quant_table_name_no_version)

        for study_id_dict in studies_list:
            raw_quant_tsv_file = create_raw_quant_table_name(study_id_dict) + '.tsv'

            if raw_quant_tsv_file not in blob_files:
                print('Skipping table build for {} (jsonl not found in bucket)'.format(raw_quant_tsv_file))
            else:
                print("Building table for {}".format(raw_quant_tsv_file))

                raw_quant_table_name = create_raw_quant_table_name(study_id_dict)
                raw_quant_table_id = get_dev_table_id(BQ_PARAMS, BQ_PARAMS['QUANT_DATASET'], raw_quant_table_name)
                create_and_load_table_from_tsv(BQ_PARAMS,
                                               tsv_file=raw_quant_tsv_file,
                                               table_id=raw_quant_table_id,
                                               num_header_rows=1,
                                               schema=raw_quant_schema)

    end = time.time() - start_time
    print("Finished program execution in {}!\n".format(format_seconds(end)))


if __name__ == '__main__':
    main(sys.argv)
