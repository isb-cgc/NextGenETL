"""
Copyright 2020, Institute for Systems Biology

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
import json
import sys

from functools import cmp_to_key
from datetime import date
from google.cloud import storage, bigquery

from common_etl.utils import (get_filepath, get_query_results, format_seconds, write_list_to_jsonl, get_scratch_fp,
                              upload_to_bucket, get_graphql_api_response, has_fatal_error, load_bq_schema_from_json,
                              create_and_load_table_from_tsv, create_and_load_table, create_tsv_row,
                              load_table_from_query, delete_bq_table, copy_bq_table, exists_bq_table,
                              update_schema, update_table_metadata, delete_bq_dataset, load_config,
                              update_table_labels, list_bq_tables, publish_table, build_table_id,
                              construct_table_name, get_rel_prefix, build_table_name_from_list)

API_PARAMS = dict()
BQ_PARAMS = dict()
YAML_HEADERS = ('api_params', 'bq_params', 'steps')


# ***** FUNCTIONS USED BY MULTIPLE PROCESSES


def get_filename(file_extension, prefix, suffix=None, include_release=True, release=None):
    """
    todo
    :param file_extension:
    :param prefix:
    :param suffix:
    :param include_release:
    :param release:
    :return:
    """
    filename = construct_table_name(BQ_PARAMS, prefix, suffix, include_release, release=release)
    return "{}.{}".format(filename, file_extension)


def get_dev_table_id(table_name, dataset=None):
    """
    todo
    :param table_name:
    :param dataset:
    :return:
    """
    project = BQ_PARAMS['DEV_PROJECT']
    if not dataset:
        dataset = BQ_PARAMS['DEV_DATASET']

    return "{}.{}.{}".format(project, dataset, table_name)


def get_records(endpoint, select_statement, dataset):
    """
    todo
    :param endpoint:
    :param select_statement:
    :param dataset:
    :return:
    """
    table_name = construct_table_name(BQ_PARAMS, API_PARAMS['ENDPOINT_SETTINGS'][endpoint]['output_name'])
    table_id = get_dev_table_id(table_name, dataset=dataset)

    query = select_statement
    query += " FROM `{}`".format(table_id)

    records = list()

    for row in get_query_results(query):
        records.append(dict(row.items()))

    return records


def infer_schema_file_location_by_table_id(table_id):
    """
    todo
    :param table_id:
    :return:
    """
    split_table_id = table_id.split('.')
    filepath = ".".join(split_table_id) + ".json"
    return filepath


def delete_from_steps(step, steps):
    """
    todo
    :param step:
    :param steps:
    :return:
    """
    delete_idx = steps.index(step)
    steps.pop(delete_idx)


def build_jsonl_from_pdc_api(endpoint, request_function, request_params=tuple(), alter_json_function=None,
                             ids=None, insert_id=False):
    """
    todo
    :param endpoint:
    :param request_function:
    :param request_params:
    :param alter_json_function:
    :param ids:
    :param insert_id:
    :return:
    """
    print("Sending {} API request: ".format(endpoint))
    if ids:
        joined_record_list = list()
        for idx, id_entry in enumerate(ids):
            combined_request_parameters = request_params + (id_entry,)
            record_list = request_data_from_pdc_api(endpoint, request_function, combined_request_parameters)

            if alter_json_function and insert_id:
                alter_json_function(record_list, id_entry)
            elif alter_json_function:
                alter_json_function(record_list)

            joined_record_list += record_list

            if len(ids) < 100:
                print(" - {:6d} current records (added {})".format(len(joined_record_list), id_entry))
            elif len(joined_record_list) % 1000 == 0 and len(joined_record_list) != 0:
                print(" - {} records appended.".format(len(joined_record_list)))
    else:
        joined_record_list = request_data_from_pdc_api(endpoint, request_function, request_params)
        print(" - collected {} records".format(len(joined_record_list)))

        if alter_json_function:
            alter_json_function(joined_record_list)

    jsonl_filename = get_filename('jsonl', API_PARAMS['ENDPOINT_SETTINGS'][endpoint]['output_name'])
    local_filepath = get_scratch_fp(BQ_PARAMS, jsonl_filename)

    write_list_to_jsonl(local_filepath, joined_record_list)
    upload_to_bucket(BQ_PARAMS, local_filepath, delete_local=True)


def request_data_from_pdc_api(endpoint, request_body_function, request_parameters=None):
    is_paginated = API_PARAMS['ENDPOINT_SETTINGS'][endpoint]['is_paginated']
    payload_key = API_PARAMS['ENDPOINT_SETTINGS'][endpoint]['payload_key']

    def append_api_response_data():
        """
        todo
        :return:
        """
        api_response = get_graphql_api_response(API_PARAMS, graphql_request_body)

        response_body = api_response['data'] if not is_paginated else api_response['data'][endpoint]

        for record in response_body[payload_key]:
            record_list.append(record)

        return response_body['pagination']['pages'] if 'pagination' in response_body else None

    record_list = list()

    if not is_paginated:
        # * operator unpacks tuple for use as positional function args
        graphql_request_body = request_body_function(*request_parameters)
        total_pages = append_api_response_data()

        # should be None, if value is returned then endpoint is actually paginated
        if total_pages:
            has_fatal_error("Paginated API response ({} pages), but is_paginated set to False.".format(total_pages))
    else:
        limit = API_PARAMS['PAGINATED_LIMIT']
        offset = 0
        page = 1

        paginated_request_params = request_parameters + (offset, limit)

        # * operator unpacks tuple for use as positional function args
        graphql_request_body = request_body_function(*paginated_request_params)
        total_pages = append_api_response_data()

        # Useful for endpoints which don't access per-study data, otherwise too verbose
        if 'Study' not in endpoint:
            print(" - Appended page {} of {}".format(page, total_pages))

        if not total_pages:
            has_fatal_error("API did not return a value for total pages, but is_paginated set to True.")

        while page < total_pages:
            offset += limit
            page += 1

            paginated_request_params = request_parameters + (offset, limit)
            graphql_request_body = request_body_function(*paginated_request_params)
            new_total_pages = append_api_response_data()
            if 'Study' not in endpoint:
                print(" - Appended page {} of {}".format(page, total_pages))

            if new_total_pages != total_pages:
                has_fatal_error("Page count change mid-ingestion (from {} to {})".format(total_pages, new_total_pages))

    return record_list


def build_clinical_table_from_jsonl(table_prefix, filename, infer_schema=False, schema=None):
    """
    todo
    :param table_prefix:
    :param filename:
    :param infer_schema:
    :param schema:
    :return:
    """
    table_name = construct_table_name(BQ_PARAMS, table_prefix)
    table_id = get_dev_table_id(table_name, dataset=BQ_PARAMS['CLINICAL_DATASET'])

    print("Creating {}:".format(table_id))
    if infer_schema:
        create_and_load_table(BQ_PARAMS, filename, table_id, schema)

    if not infer_schema and not schema:
        schema_filename = infer_schema_file_location_by_table_id(table_id)
        schema = load_bq_schema_from_json(BQ_PARAMS, schema_filename)

        if not schema:
            has_fatal_error("No schema, exiting")

        create_and_load_table(BQ_PARAMS, filename, table_id, schema)

    return table_id


def build_table_from_jsonl(endpoint, infer_schema=False):
    table_name = construct_table_name(BQ_PARAMS, API_PARAMS['ENDPOINT_SETTINGS'][endpoint]['output_name'])
    filename = get_filename('jsonl', API_PARAMS['ENDPOINT_SETTINGS'][endpoint]['output_name'])
    table_id = get_dev_table_id(table_name, dataset=BQ_PARAMS['CLINICAL_DATASET'])
    print("Creating {}:".format(table_id))

    if infer_schema:
        schema = None
    else:
        schema_filename = infer_schema_file_location_by_table_id(table_id)
        schema = load_bq_schema_from_json(BQ_PARAMS, schema_filename)

        if not schema:
            has_fatal_error("No schema found and infer_schema set to False, exiting")

    create_and_load_table(BQ_PARAMS, filename, table_id, schema)


def build_table_from_tsv(project, dataset, table_prefix, table_suffix=None, backup_table_suffix=None):
    """
    todo
    :param project:
    :param dataset:
    :param table_prefix:
    :param table_suffix:
    :param backup_table_suffix:
    :return:
    """
    build_start = time.time()

    table_name = construct_table_name(BQ_PARAMS, table_prefix, table_suffix)
    table_id = build_table_id(project, dataset, table_name)

    schema_filename = '{}/{}/{}.json'.format(project, dataset, table_name)
    schema = load_bq_schema_from_json(BQ_PARAMS, schema_filename)

    if not schema and backup_table_suffix:
        print("No schema file found for {}, trying backup ({})".format(table_suffix, backup_table_suffix))
        table_name = construct_table_name(BQ_PARAMS, table_prefix, backup_table_suffix)
        table_id = build_table_id(project, dataset, table_name)
        schema_filename = '{}/{}/{}.json'.format(project, dataset, table_name)
        schema = load_bq_schema_from_json(BQ_PARAMS, schema_filename)

    # still no schema? return
    if not schema:
        print("No schema file found for {}, skipping table.".format(table_id))
        return

    print("\nBuilding {0}... ".format(table_id))
    tsv_name = get_filename('tsv', table_prefix, table_suffix)
    create_and_load_table_from_tsv(BQ_PARAMS, tsv_name, schema, table_id, BQ_PARAMS['NULL_MARKER'])

    build_end = time.time() - build_start
    print("Table built in {0}!\n".format(format_seconds(build_end)))


# ***** GENE TABLE FUNCTIONS

def make_gene_symbols_per_study_query(pdc_study_id):
    """
    todo
    :param pdc_study_id:
    :return:
    """
    # todo make function to build these names
    table_name = construct_table_name(BQ_PARAMS, BQ_PARAMS['QUANT_DATA_TABLE'],
                                      pdc_study_id, release=BQ_PARAMS['RELEASE'])
    table_id = get_dev_table_id(table_name)

    return """
        SELECT DISTINCT(gene_symbol)
        FROM `{}`
    """.format(table_id)


def make_gene_query(gene_name):
    """
    todo
    :param gene_name:
    :return:
    """
    return '''
    {{ 
        geneSpectralCount(gene_name: \"{}\" acceptDUA: true) {{
            gene_id
            gene_name
            NCBI_gene_id 
            authority 
            description 
            organism 
            chromosome 
            locus 
            proteins 
            assays
        }}
    }}
    '''.format(gene_name)


def make_swissprot_query():
    """
    todo
    :return:
    """
    table_name = construct_table_name(BQ_PARAMS, BQ_PARAMS['SWISSPROT_TABLE'], release=BQ_PARAMS['UNIPROT_RELEASE'])
    swissprot_table_id = get_dev_table_id(table_name, dataset=BQ_PARAMS['META_DATASET'])
    return """
    SELECT swissprot_id 
    FROM `{}`
    """.format(swissprot_table_id)


def add_gene_symbols_per_study(pdc_study_id, gene_symbol_set):
    """
    todo
    :param pdc_study_id:
    :param gene_symbol_set:
    :return:
    """
    table_name = construct_table_name(BQ_PARAMS, BQ_PARAMS['QUANT_DATA_TABLE'], pdc_study_id,
                                      release=BQ_PARAMS['RELEASE'])
    table_id = get_dev_table_id(table_name, dataset=BQ_PARAMS['DEV_DATASET'])

    if exists_bq_table(table_id):
        results = get_query_results(make_gene_symbols_per_study_query(pdc_study_id))

        for row in results:
            gene_symbol_set.add(row['gene_symbol'])


def build_gene_symbol_list(studies_list):
    """
    todo
    :param studies_list:
    :return:
    """
    print("Building gene symbol tsv!")
    gene_symbol_set = set()

    for study in studies_list:
        table_name = construct_table_name(BQ_PARAMS, BQ_PARAMS['QUANT_DATA_TABLE'], study['pdc_study_id'],
                                          release=BQ_PARAMS['RELEASE'])
        table_id = get_dev_table_id(table_name, dataset=BQ_PARAMS['DEV_DATASET'])

        if exists_bq_table(table_id):
            add_gene_symbols_per_study(study['pdc_study_id'], gene_symbol_set)
            print("- Added {}, current count: {}".format(study['pdc_study_id'], len(gene_symbol_set)))
        else:
            print("- No table for {}, skipping.".format(study['pdc_study_id']))

    gene_symbol_list = list(sorted(gene_symbol_set))
    return gene_symbol_list


def build_gene_tsv(gene_symbol_list, gene_tsv, append=False):
    """
    todo
    :param gene_symbol_list:
    :param gene_tsv:
    :param append:
    :return:
    """
    compare_uniprot_ids = cmp_to_key(sort_uniprot_by_age)
    swissprot_set = {row[0] for row in get_query_results(make_swissprot_query())}

    gene_symbol_set = set(gene_symbol_list)
    gene_tsv_exists = os.path.exists(gene_tsv)

    if append:
        print("Resuming geneSpectralCount API calls... ", end='')

        if gene_tsv_exists:
            with open(gene_tsv, 'r') as tsv_file:
                gene_reader = csv.reader(tsv_file, delimiter='\t')
                next(gene_reader)
                saved_genes = {row[1] for row in gene_reader}

            gene_symbol_set = gene_symbol_set - saved_genes

        remaining_genes = len(gene_symbol_set)
        call_count_str = "{} gene API calls remaining".format(remaining_genes)
        call_count_str += "--skipping step." if not remaining_genes else "."
        print(call_count_str)

        if not remaining_genes:
            return

    file_mode = 'a' if append else 'w'

    with open(gene_tsv, file_mode) as gene_fh:
        if not append or not gene_tsv_exists:
            gene_fh.write(create_tsv_row(['gene_id',
                                          'gene_symbol',
                                          'NCBI_gene_id',
                                          'authority',
                                          'authority_gene_id',
                                          'description',
                                          'organism',
                                          'chromosome',
                                          'locus',
                                          'uniprotkb_id',
                                          'uniprotkb_ids',
                                          'proteins',
                                          'assays'],
                                         null_marker=BQ_PARAMS['NULL_MARKER']))

        count = 0

        for gene_symbol in gene_symbol_set:
            json_res = get_graphql_api_response(API_PARAMS, make_gene_query(gene_symbol))
            time.sleep(0.1)  # need a delay to avoid making too many api requests and getting 500 server error
            gene_data = json_res['data']['geneSpectralCount'][0]

            if not gene_data or not gene_data['gene_name']:
                print("No geneSpectralCount data found for {}".format(gene_symbol))
                continue

            for key in gene_data.keys():
                gene_data[key] = str(gene_data[key]).strip()

                if not gene_data[key] or gene_data[key] == '':
                    gene_data[key] = 'None'

            split_authority = gene_data['authority'].split(':')
            if len(split_authority) > 2:
                has_fatal_error(
                    "Authority should split into <= two elements. Actual: {}".format(gene_data['authority']))

            authority_gene_id = split_authority[1] if (len(split_authority) > 1 and split_authority[1]) else ""
            authority = split_authority[0] if split_authority[0] else ""

            uniprot_accession_str = filter_uniprot_accession_nums(gene_data['proteins'])
            swissprot_str, swissprot_count = filter_swissprot_accession_nums(gene_data['proteins'], swissprot_set)
            uniprotkb_id = ""

            if swissprot_count == 1:
                uniprotkb_id = swissprot_str
            elif swissprot_count > 1:
                swissprot_list = sorted(swissprot_str.split(';'), key=compare_uniprot_ids)
                uniprotkb_id = swissprot_list.pop(0)
            elif uniprot_accession_str and len(uniprot_accession_str) > 1:
                uniprot_list = sorted(uniprot_accession_str.split(';'), key=compare_uniprot_ids)
                uniprotkb_id = uniprot_list[0]

            uniprotkb_ids = uniprot_accession_str

            if swissprot_count == 0:
                print("No swissprots counted, returns {}; {}".format(uniprotkb_id, uniprotkb_ids))
                print("(for string: {})".format(swissprot_str))

            if swissprot_count > 1:
                print("More than one swissprot counted, returns {}; {}".format(uniprotkb_id, uniprotkb_ids))
                print("(for string: {})".format(swissprot_str))

            gene_fh.write(create_tsv_row([gene_data['gene_id'],
                                          gene_data['gene_name'],
                                          gene_data['NCBI_gene_id'],
                                          authority,
                                          authority_gene_id,
                                          gene_data['description'],
                                          gene_data['organism'],
                                          gene_data['chromosome'],
                                          gene_data['locus'],
                                          uniprotkb_id,
                                          uniprotkb_ids,
                                          gene_data['proteins'],
                                          gene_data['assays']],
                                         null_marker=BQ_PARAMS['NULL_MARKER']))
            count += 1

            if count % 50 == 0:
                print("Added {} genes".format(count))


def download_from_uniprot_ftp(local_file, server_fp, type_str):
    """
    todo
    :param local_file:
    :param server_fp:
    :param type_str:
    :return:
    """
    print("Creating {} tsv... ".format(type_str), end="")

    gz_destination_file = server_fp.split('/')[-1]
    versioned_fp = get_scratch_fp(BQ_PARAMS, local_file)

    with ftplib.FTP(API_PARAMS['UNIPROT_FTP_DOMAIN']) as ftp:
        try:
            ftp.login()

            # write remote gz to local file via ftp connection
            with open(get_scratch_fp(BQ_PARAMS, gz_destination_file), 'wb') as fp:
                ftp.retrbinary('RETR ' + server_fp, fp.write)

            gz_destination_fp = get_scratch_fp(BQ_PARAMS, gz_destination_file)

            with gzip.open(gz_destination_fp, 'rt') as zipped_file:
                with open(versioned_fp, 'w') as dest_tsv_file:
                    for row in zipped_file:
                        dest_tsv_file.write(row)

        except ftplib.all_errors as e:
            has_fatal_error("Error getting UniProt file via FTP:\n {}".format(e), ftplib.error_perm)

    print(" done!")


def is_uniprot_accession_number(id_str):
    """
    todo
    :param id_str:
    :return:
    """

    # based on format specified at https://web.expasy.org/docs/userman.html#AC_line
    def is_alphanumeric(char):
        if char.isdigit() or char.isalpha():
            return True
        return False

    def is_opq_char(char):
        if 'O' in char or 'P' in char or 'Q' in char:
            return True

    """
    is length 6 or 10?
    is idx 1, 5 a digit?
    is idx 3, 4 alphanumeric?

    if 10 char:
    is idx 0 A-N, R-Z?
    is idx 6 alpha?
    is idx 7, 8 alphanumeric?
    is idx 9 a digit?

    if 6 char:
    is idx 0 O, P, Q?
        is idx 2 alphanumeric?
    else alpha?
        is idx 2 alpha?
    """

    id_length = len(id_str)
    id_str = str.upper(id_str)
    id_str = id_str.strip()

    if id_length != 6 and id_length != 10:
        return False
    if not id_str[1].isdigit() or not id_str[5].isdigit():
        return False
    if not is_alphanumeric(id_str[3]) or not is_alphanumeric(id_str[4]):
        return False

    if id_length == 10:
        if is_opq_char(id_str[0]) or not id_str[0].isalpha():
            return False
        if not id_str[2].isalpha() or not id_str[6].isalpha():
            return False
        if not is_alphanumeric(id_str[7]) or not is_alphanumeric(id_str[8]):
            return False
        if not id_str[9].isdigit():
            return False
    else:
        if is_opq_char(id_str[0]):
            if not is_alphanumeric(id_str[2]):
                return False
        elif not id_str[0].isalpha():
            return False
        else:
            # don't get cute and try to remove this, needed
            if not id_str[2].isalpha():
                return False

    return True


def sort_uniprot_by_age(a, b):
    """
    To use:
    compare_uniprot_ids = cmp_to_key(sort_uniprot_by_age)
    uniprot_list.sort(key=compare_uniprot_ids)
    """

    sort_order = ['P', 'Q', 'O', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'R',
                  'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z']

    if a == b:
        return 0

    index_of_a = sort_order.index(a[0])
    index_of_b = sort_order.index(b[0])
    len_of_a = len(a)
    len_of_b = len(b)

    # 10 digit uniprot ids are more recent than 6 digit ids
    if len_of_a > len_of_b:
        return 1
    if len_of_b > len_of_a:
        return -1

    # if IDs start with same letter, sort by digit at index 1
    if index_of_a == index_of_b:
        return int(a[1]) - int(b[1])

    return index_of_a - index_of_b


def filter_uniprot_accession_nums(proteins_str):
    """
    todo
    :param proteins_str:
    :return:
    """
    uniprot_list = []

    split_protein_list = proteins_str.split(';')

    for protein in split_protein_list:
        # doesn't include isoforms
        if is_uniprot_accession_number(protein):
            uniprot_list.append(protein)

    uniprot_id_str = ";".join(uniprot_list)

    if not uniprot_id_str:
        uniprot_id_str = None

    return uniprot_id_str


def filter_swissprot_accession_nums(proteins, swissprot_set):
    """
    todo
    :param proteins:
    :param swissprot_set:
    :return:
    """
    protein_list = proteins.split(";")

    swissprot_count = 0
    swissprot_str = ''

    for protein in protein_list:
        if protein in swissprot_set:
            if swissprot_count >= 1:
                swissprot_str += ';'
            swissprot_str += protein
            swissprot_count += 1

    return swissprot_str, swissprot_count


# ***** QUANT DATA MATRIX FUNCTIONS

def make_quant_data_matrix_query(study_submitter_id, data_type):
    """
    todo
    :param study_submitter_id:
    :param data_type:
    :return:
    """
    return '{{ quantDataMatrix(study_submitter_id: \"{}\" data_type: \"{}\" acceptDUA: true) }}'.format(
        study_submitter_id, data_type)


def make_proteome_quant_table_query(study):
    """
    todo
    :param study:
    :return:
    """
    quant_table_name = "{}_{}_{}".format(BQ_PARAMS['QUANT_DATA_TABLE'], study, BQ_PARAMS['RELEASE'])
    quant_table_id = get_dev_table_id(quant_table_name)

    case_aliquot_table_name = '{}_{}'.format(BQ_PARAMS['CASE_ALIQUOT_TABLE'], BQ_PARAMS['RELEASE'])
    case_aliquot_table_id = get_dev_table_id(case_aliquot_table_name, dataset=BQ_PARAMS['META_DATASET'])

    gene_table_name = '{}_{}'.format(BQ_PARAMS['GENE_TABLE'], BQ_PARAMS['RELEASE'])
    gene_table_id = get_dev_table_id(gene_table_name, dataset=BQ_PARAMS['META_DATASET'])

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


def build_quant_tsv(study_id_dict, data_type, tsv_fp):
    """
    todo
    :param study_id_dict:
    :param data_type:
    :param tsv_fp:
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
                                 'protein_abundance_log2ratio'],
                                null_marker=BQ_PARAMS['NULL_MARKER']))

        for row in res_json['data']['quantDataMatrix']:
            gene_symbol = row.pop(0)

            for i, log2_ratio in enumerate(row):
                fh.write(create_tsv_row([aliquot_metadata[i]['aliquot_run_metadata_id'],
                                         aliquot_metadata[i]['aliquot_submitter_id'],
                                         study_name,
                                         gene_symbol,
                                         log2_ratio],
                                        null_marker=BQ_PARAMS['NULL_MARKER']))
            lines_written += 1

        return lines_written


def get_quant_files():
    """
    todo
    :return:
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
    todo
    :param study_submitter_id:
    :return:
    """
    table_name = construct_table_name(BQ_PARAMS, BQ_PARAMS['QUANT_DATA_TABLE'], study_submitter_id)
    table_id = get_dev_table_id(table_name, dataset=BQ_PARAMS['DEV_DATASET'])
    return exists_bq_table(table_id)


def get_proteome_studies(studies_list):
    """
    todo
    :param studies_list:
    :return:
    """
    proteome_studies_list = list()

    for study in studies_list:
        if study['analytical_fraction'] == "Proteome":
            proteome_studies_list.append(study)

    return proteome_studies_list


def get_quant_table_name(study, is_final=True):
    """
    todo
    :param study:
    :param is_final:
    :return:
    """
    analytical_fraction = study['analytical_fraction']

    if not is_final:
        return construct_table_name(BQ_PARAMS, BQ_PARAMS['QUANT_DATA_TABLE'], study['pdc_study_id'],
                                    release=BQ_PARAMS['RELEASE'])
    else:
        study_name = study['study_name']
        study_name = study_name.replace(analytical_fraction, "")

        return "_".join([BQ_PARAMS['QUANT_DATA_TABLE'],
                         analytical_fraction.lower(),
                         change_study_name_to_table_name_format(study_name),
                         BQ_PARAMS['DATA_SOURCE'],
                         BQ_PARAMS['RELEASE']])


def change_study_name_to_table_name_format(study_name):
    """
    todo
    :param study_name:
    :return:
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


# ***** STUDY TABLE CREATION FUNCTIONS

def make_all_programs_query():
    """
    todo
    :return:
    """
    return """{
        allPrograms (acceptDUA: true) {
            program_id
            program_submitter_id
            name
            start_date
            end_date
            program_manager
            projects {
                project_id
                project_submitter_id
                name
                studies {
                    pdc_study_id
                    study_id
                    study_submitter_id
                    submitter_id_name
                    analytical_fraction
                    experiment_type
                    acquisition_type
                } 
            }
        }
    }"""


def make_study_query(pdc_study_id):
    """
    todo
    :param pdc_study_id:
    :return:
    """
    return """{{ study 
    (pdc_study_id: \"{}\" acceptDUA: true) {{ 
        study_name
        disease_type
        primary_site
        embargo_date
    }} }}
    """.format(pdc_study_id)


def alter_all_programs_json(all_programs_json_obj):
    """
    todo
    :param all_programs_json_obj:
    :return:
    """
    temp_programs_json_obj_list = list()

    for program in all_programs_json_obj:
        program['program_name'] = program.pop("name", None)
        print("Processing {}".format(program['program_name']))
        projects = program.pop("projects", None)
        for project in projects:
            project['project_name'] = project.pop("name", None)
            studies = project.pop("studies", None)
            for study in studies:
                # grab a few add't fields from study endpoint
                json_res = get_graphql_api_response(API_PARAMS, make_study_query(study['pdc_study_id']))
                study_metadata = json_res['data']['study'][0]

                # ** unpacks each dictionary's items without altering program and project
                study_obj = {**program, **project, **study, **study_metadata}

                # normalize empty strings (turn into null)
                for k, v in study_obj.items():
                    if not v:
                        study_obj[k] = None

                temp_programs_json_obj_list.append(study_obj)

    all_programs_json_obj.clear()
    all_programs_json_obj.extend(temp_programs_json_obj_list)


def retrieve_all_studies_query(output_name):
    """
    todo
    :param output_name:
    :return:
    """
    table_name = construct_table_name(BQ_PARAMS, output_name)
    table_id = get_dev_table_id(table_name, dataset=BQ_PARAMS['META_DATASET'])

    return """
    SELECT pdc_study_id, study_name, embargo_date, project_submitter_id, analytical_fraction
    FROM  `{}`
    """.format(table_id)


def print_embargoed_studies(excluded_studies_list):
    """
    todo
    :param excluded_studies_list:
    :return:
    """
    print("\nStudies excluded due to data embargo:")

    for study in sorted(excluded_studies_list, key=lambda item: item['study_name']):
        print(" - {} ({}, expires {})".format(study['study_name'], study['pdc_study_id'], study['embargo_date']))

    print()


def is_under_embargo(embargo_date):
    """
    todo
    :param embargo_date:
    :return:
    """
    if not embargo_date or embargo_date < date.today():
        return False
    return True


# ***** FILE METADATA FUNCTIONS

def make_files_per_study_query(study_id):
    """
    todo
    :param study_id:
    :return:
    """
    return """
    {{ filesPerStudy (pdc_study_id: \"{}\" acceptDUA: true) {{
            pdc_study_id 
            study_submitter_id
            study_name 
            file_id 
            file_name 
            file_submitter_id 
            file_type 
            md5sum 
            file_location 
            file_size 
            data_category 
            file_format
            signedUrl {{
                url
            }}
        }} 
    }}""".format(study_id)


def make_file_id_query(table_id):
    """
    todo
    :param table_id:
    :return:
    """
    return """
    SELECT file_id
    FROM `{}`
    ORDER BY file_id
    """.format(table_id)


def make_file_metadata_query(file_id):
    """
    todo
    :param file_id:
    :return:
    """
    return """
    {{ fileMetadata(file_id: \"{}\" acceptDUA: true) {{
        file_id 
        file_name
        fraction_number 
        experiment_type 
        plex_or_dataset_name 
        analyte 
        instrument 
        study_run_metadata_submitter_id 
        study_run_metadata_id 
        aliquots {{
            aliquot_id
            aliquot_submitter_id
            sample_id
            sample_submitter_id
            case_id
            case_submitter_id
            }}
        }} 
    }}    
    """.format(file_id)


def make_associated_entities_query():
    """
    todo
    :return:
    """
    table_name = construct_table_name(BQ_PARAMS, API_PARAMS['ENDPOINT_SETTINGS']['fileMetadata']['output_name'])
    table_id = get_dev_table_id(table_name, dataset=BQ_PARAMS['META_DATASET'])

    return """SELECT file_id, 
    aliq.case_id as case_id, 
    aliq.aliquot_id as entity_id, 
    aliq.aliquot_submitter_id as entity_submitter_id, 
    "aliquot" as entity_type
    FROM `{}`
    CROSS JOIN UNNEST(aliquots) as aliq
    GROUP BY file_id, case_id, entity_id, entity_submitter_id, entity_type
    """.format(table_id)


def make_combined_file_metadata_query():
    """
    todo
    :return:
    """
    file_metadata_output_name = API_PARAMS['ENDPOINT_SETTINGS']['fileMetadata']['output_name']
    file_metadata_table_name = construct_table_name(BQ_PARAMS, file_metadata_output_name)
    file_metadata_table_id = get_dev_table_id(file_metadata_table_name, dataset=BQ_PARAMS['META_DATASET'])

    file_per_study_output_name = API_PARAMS['ENDPOINT_SETTINGS']['filesPerStudy']['output_name']
    file_per_study_table_name = construct_table_name(BQ_PARAMS, file_per_study_output_name)
    file_per_study_table_id = get_dev_table_id(file_per_study_table_name, dataset=BQ_PARAMS['META_DATASET'])

    return """
    SELECT distinct fps.file_id, fps.file_name, fps.embargo_date, fps.pdc_study_ids,
        fm.study_run_metadata_id, fm.study_run_metadata_submitter_id,
        fps.file_format, fps.file_type, fps.data_category, fps.file_size, 
        fm.fraction_number, fm.experiment_type, fm.plex_or_dataset_name, fm.analyte, fm.instrument, 
        fps.md5sum, fps.url, "open" AS `access`
    FROM `{}` AS fps
    INNER JOIN `{}` AS fm
        ON fm.file_id = fps.file_id
    """.format(file_per_study_table_id, file_metadata_table_id)


def modify_api_file_metadata_table_query(fm_table_id):
    """
    todo
    :param fm_table_id:
    :return:
    """
    temp_table_id = fm_table_id + "_temp"

    return """
        WITH grouped_instruments AS (
            SELECT file_id, 
                ARRAY_TO_STRING(ARRAY_AGG(instrument), ';') as instruments
            FROM `{0}`
        GROUP BY file_id
        )

        SELECT g.file_id, f.analyte, f.experiment_type, g.instruments as instrument, 
            f.study_run_metadata_submitter_id, f.study_run_metadata_id, f.plex_or_dataset_name,
            f.fraction_number, f.aliquots
        FROM grouped_instruments g
        LEFT JOIN `{0}` f
            ON g.file_id = f.file_id
        """.format(temp_table_id)


def modify_per_study_file_table_query(fps_table_id):
    """
    todo
    :param fps_table_id:
    :return:
    """
    temp_table_id = fps_table_id + "_temp"

    study_table_name = construct_table_name(BQ_PARAMS, API_PARAMS["ENDPOINT_SETTINGS"]["allPrograms"]["output_name"])
    study_table_id = get_dev_table_id(study_table_name, dataset="PDC_metadata")

    return """
        WITH grouped_study_ids AS (
            SELECT fps.file_id, stud.embargo_date, 
                ARRAY_TO_STRING(ARRAY_AGG(stud.pdc_study_id), ';') as pdc_study_ids
            FROM `{0}` fps
            JOIN `{1}` stud
                ON fps.pdc_study_id = stud.pdc_study_id
        GROUP BY fps.file_id, stud.embargo_date
        )

        SELECT distinct g.file_id, f.file_name, g.embargo_date, g.pdc_study_ids,
            f.data_category, f.file_format, f.file_type, f.file_size, f.md5sum, 
            SPLIT(f.url, '?')[OFFSET(0)] as url
        FROM grouped_study_ids g
        INNER JOIN `{0}` f
            ON g.file_id = f.file_id
        """.format(temp_table_id, study_table_id)


def alter_files_per_study_json(files_per_study_obj_list):
    """
    todo
    :param files_per_study_obj_list:
    :return:
    """
    for files_per_study_obj in files_per_study_obj_list:
        signed_url = files_per_study_obj.pop('signedUrl', None)
        url = signed_url.pop('url', None)

        if not url:
            print("url not found in filesPerStudy response:\n{}\n".format(files_per_study_obj))

        files_per_study_obj['url'] = url


def get_file_ids(endpoint):
    """
    todo
    :param endpoint:
    :return:
    """
    table_name = construct_table_name(BQ_PARAMS, API_PARAMS['ENDPOINT_SETTINGS'][endpoint]['output_name'])
    table_id = get_dev_table_id(table_name, dataset=BQ_PARAMS['META_DATASET'])
    return get_query_results(make_file_id_query(table_id))  # todo fix back


def build_file_pdc_metadata_jsonl(file_ids):
    """
    todo
    :param file_ids:
    :return:
    """
    jsonl_start = time.time()
    file_metadata_list = []

    for count, row in enumerate(file_ids):
        file_id = row['file_id']
        file_metadata_res = get_graphql_api_response(API_PARAMS, make_file_metadata_query(file_id))

        if 'data' not in file_metadata_res:
            print("No data returned by file metadata query for {}".format(file_id))
            continue

        for metadata_row in file_metadata_res['data']['fileMetadata']:
            if 'fraction_number' in metadata_row and metadata_row['fraction_number']:
                fraction_number = metadata_row['fraction_number'].strip()

                if fraction_number == 'N/A' or fraction_number == 'NOFRACTION' or not fraction_number.isalnum():
                    fraction_number = None
                elif fraction_number == 'Pool' or fraction_number == 'pool':
                    fraction_number = 'POOL'

                metadata_row['fraction_number'] = fraction_number

            file_metadata_list.append(metadata_row)
            count += 1

            if count % 50 == 0:
                print("{} of {} files retrieved".format(count, file_ids.total_rows))

    file_metadata_jsonl_file = get_filename('jsonl',
                                            API_PARAMS['ENDPOINT_SETTINGS']['fileMetadata']['output_name'])
    file_metadata_jsonl_path = get_scratch_fp(BQ_PARAMS, file_metadata_jsonl_file)

    write_list_to_jsonl(file_metadata_jsonl_path, file_metadata_list)
    upload_to_bucket(BQ_PARAMS, file_metadata_jsonl_path)

    jsonl_end = time.time() - jsonl_start
    print("File PDC metadata jsonl file created in {0}!\n".format(format_seconds(jsonl_end)))


# ***** CASE CLINICAL FUNCTIONS

def make_cases_query():
    """
    todo
    :return:
    """
    return """{
        allCases (acceptDUA: true) {
            case_id 
            case_submitter_id 
            project_submitter_id 
            primary_site 
            disease_type
            externalReferences { 
                external_reference_id 
                reference_resource_shortname 
                reference_resource_name 
                reference_entity_location 
            }
        }
    }"""


def get_cases():
    endpoint = 'allCases'
    dataset = BQ_PARAMS['CLINICAL_DATASET']

    select_statement = "SELECT case_id, case_submitter_id, project_submitter_id, primary_site, disease_type"

    return get_records(endpoint, select_statement, dataset)


def get_case_demographics():
    endpoint = 'paginatedCaseDemographicsPerStudy'
    dataset = BQ_PARAMS['CLINICAL_DATASET']

    select_statement = """
        SELECT demographic_id, demographic_submitter_id, case_id, case_submitter_id, gender, ethnicity, race, 
        days_to_birth, days_to_death, year_of_birth, year_of_death, vital_status, cause_of_death
        """

    return get_records(endpoint, select_statement, dataset)


def get_case_diagnoses():
    endpoint = 'paginatedCaseDiagnosesPerStudy'
    dataset = BQ_PARAMS['CLINICAL_DATASET']
    select_statement = "SELECT case_id, case_submitter_id, diagnoses"

    return get_records(endpoint, select_statement, dataset)


def make_cases_aliquots_query(offset, limit):
    """
    todo
    :param offset:
    :param limit:
    :return:
    """
    return '''{{ 
        paginatedCasesSamplesAliquots(offset:{0} limit:{1} acceptDUA: true) {{ 
            total casesSamplesAliquots {{
                case_id 
                case_submitter_id
                samples {{
                    sample_id 
                    aliquots {{ 
                        aliquot_id 
                        aliquot_submitter_id
                        aliquot_run_metadata {{ 
                            aliquot_run_metadata_id
                        }}
                    }}
                }}
            }}
            pagination {{ 
                count 
                from 
                page 
                total 
                pages 
                size 
            }}
        }}
    }}'''.format(offset, limit)


def make_cases_diagnoses_query(pdc_study_id, offset, limit):
    """
    todo
    :param pdc_study_id:
    :param offset:
    :param limit:
    :return:
    """
    return ''' {{ 
        paginatedCaseDiagnosesPerStudy(pdc_study_id: "{0}" offset: {1} limit: {2} acceptDUA: true) {{
            total caseDiagnosesPerStudy {{
                case_id
                case_submitter_id
                diagnoses {{
                    diagnosis_id
                    tissue_or_organ_of_origin
                    age_at_diagnosis
                    primary_diagnosis
                    tumor_grade
                    tumor_stage
                    diagnosis_submitter_id
                    classification_of_tumor
                    days_to_last_follow_up
                    days_to_last_known_disease_status
                    days_to_recurrence
                    last_known_disease_status
                    morphology
                    progression_or_recurrence
                    site_of_resection_or_biopsy
                    prior_malignancy
                    ajcc_clinical_m
                    ajcc_clinical_n
                    ajcc_clinical_stage
                    ajcc_clinical_t
                    ajcc_pathologic_m
                    ajcc_pathologic_n
                    ajcc_pathologic_stage
                    ajcc_pathologic_t
                    ann_arbor_b_symptoms
                    ann_arbor_clinical_stage
                    ann_arbor_extranodal_involvement
                    ann_arbor_pathologic_stage
                    best_overall_response
                    burkitt_lymphoma_clinical_variant
                    circumferential_resection_margin
                    colon_polyps_history
                    days_to_best_overall_response
                    days_to_diagnosis
                    days_to_hiv_diagnosis
                    days_to_new_event
                    figo_stage
                    hiv_positive
                    hpv_positive_type
                    hpv_status
                    iss_stage
                    laterality
                    ldh_level_at_diagnosis
                    ldh_normal_range_upper
                    lymph_nodes_positive
                    lymphatic_invasion_present
                    method_of_diagnosis
                    new_event_anatomic_site
                    new_event_type
                    overall_survival
                    perineural_invasion_present
                    prior_treatment
                    progression_free_survival
                    progression_free_survival_event
                    residual_disease
                    vascular_invasion_present
                    year_of_diagnosis
                }}
            }}
            pagination {{
                count
                from 
                page 
                total
                pages
                size
            }}
        }}
    }}'''.format(pdc_study_id, offset, limit)


def make_cases_demographics_query(pdc_study_id, offset, limit):
    """
    todo
    :param pdc_study_id:
    :param offset:
    :param limit:
    :return:
    """
    return """{{ 
        paginatedCaseDemographicsPerStudy (pdc_study_id: "{0}" offset: {1} limit: {2} acceptDUA: true) {{ 
            total caseDemographicsPerStudy {{ 
                case_id 
                case_submitter_id
                demographics {{ 
                    demographic_id
                    ethnicity
                    gender
                    demographic_submitter_id
                    race
                    cause_of_death
                    days_to_birth
                    days_to_death
                    vital_status
                    year_of_birth
                    year_of_death 
                }} 
            }} 
            pagination {{ 
                count 
                from 
                page 
                total 
                pages 
                size 
            }} 
        }} 
    }}""".format(pdc_study_id, offset, limit)


def alter_case_demographics_json(json_obj_list, pdc_study_id):
    """
    todo
    :param json_obj_list:
    :param pdc_study_id:
    :return:
    """
    for case in json_obj_list:

        demographics = case.pop("demographics")

        if len(demographics) > 1:
            ref_dict = None
            has_fatal_error("Cannot unnest case demographics because multiple records exist.")
        elif len(demographics) == 1:
            ref_dict = demographics[0]
        else:
            demographics_key_list = ["demographic_id", "ethnicity", "gender", "demographic_submitter_id",
                                     "race", "cause_of_death", "days_to_birth", "days_to_death",
                                     "vital_status", "year_of_birth", "year_of_death"]

            ref_dict = dict.fromkeys(demographics_key_list, None)

        case['pdc_study_id'] = pdc_study_id
        case.update(ref_dict)


def alter_case_diagnoses_json(json_obj_list, pdc_study_id):
    """
    todo
    :param json_obj_list:
    :param pdc_study_id:
    :return:
    """
    for case in json_obj_list:
        case['pdc_study_id'] = pdc_study_id


def get_cases_by_project_submitter(studies_list):
    """
    todo
    :param studies_list:
    :return:
    """
    # get unique project_submitter_ids from studies_list
    cases_by_project_submitter = dict()

    # todo remove when fixed by PDC
    cases_by_project_submitter['LUAD-100'] = {'cases': list(), 'max_diagnosis_count': 0}

    for study in studies_list:
        cases_by_project_submitter[study['project_submitter_id']] = {
            'cases': list(),
            'max_diagnosis_count': 0
        }

    return cases_by_project_submitter


def remove_null_values(json_obj_list):
    """
    todo
    :param json_obj_list:
    :return:
    """
    for obj in json_obj_list:
        obj_keys = list(obj.keys())

        for key in obj_keys:
            if not obj[key]:
                obj.pop(key)
            elif isinstance(obj[key], list):
                remove_null_values(obj[key])


def remove_nulls_and_create_temp_table(records, project_name, is_diagnoses=False, infer_schema=False):
    """
    todo
    :param records:
    :param project_name:
    :param is_diagnoses:
    :param infer_schema:
    :return:
    """
    clinical_type = "clinical" if not is_diagnoses else "clinical_diagnoses"
    print('a')

    remove_null_values(records)
    print('b')

    clinical_jsonl_filename = get_filename('jsonl', project_name, clinical_type)
    local_clinical_filepath = get_scratch_fp(BQ_PARAMS, clinical_jsonl_filename)
    write_list_to_jsonl(local_clinical_filepath, records)
    print('c')
    upload_to_bucket(BQ_PARAMS, local_clinical_filepath, delete_local=True)
    print('d')

    clinical_table_prefix = "temp_" + project_name + "_" + clinical_type
    return build_clinical_table_from_jsonl(clinical_table_prefix, clinical_jsonl_filename, infer_schema)


def create_ordered_clinical_table(temp_table_id, project_name, clinical_type):
    """
    todo
    :param temp_table_id:
    :param project_name:
    :param clinical_type:
    :return:
    """
    client = bigquery.Client()
    temp_table = client.get_table(temp_table_id)
    table_schema = temp_table.schema

    shortened_project_name = BQ_PARAMS["PROJECT_ABBREVIATION_MAP"][project_name]
    table_prefix = "_".join([clinical_type, shortened_project_name, BQ_PARAMS['DATA_SOURCE']])
    table_name = construct_table_name(BQ_PARAMS, table_prefix)
    table_id = get_dev_table_id(table_name, dataset=BQ_PARAMS['CLINICAL_DATASET'])

    fields = {
        "parent_level": list()
    }

    for schema_field in table_schema:
        if schema_field.field_type == "RECORD":
            fields[schema_field.name] = list()
            for child_schema_field in schema_field.fields:
                column_position = BQ_PARAMS['COLUMN_ORDER'].index(child_schema_field.name)
                fields[schema_field.name].append((child_schema_field.name, column_position))
        else:
            column_position = BQ_PARAMS['COLUMN_ORDER'].index(schema_field.name)
            fields["parent_level"].append((schema_field.name, column_position))

    # sort list by index, output list of column names
    parent_select_list = [tup[0] for tup in sorted(fields['parent_level'], key=lambda t: t[1])]
    parent_select_str = ", ".join(parent_select_list)

    fields.pop("parent_level")

    subqueries = ""

    if len(fields) > 0:
        nested_field_list = fields.keys()

        for field in nested_field_list:
            select_list = [tup[0] for tup in sorted(fields[field], key=lambda t: t[1])]
            select_str = ", ".join(select_list)

            subquery = """
                , ARRAY(
                    SELECT AS STRUCT
                        {0}
                    FROM clinical.{1}
                ) AS {1}
            """.format(select_str, field)

            subqueries += subquery

    query = """
    SELECT {}
    {}
    FROM {} clinical
    """.format(parent_select_str, subqueries, temp_table_id)

    load_table_from_query(BQ_PARAMS, table_id, query)
    update_column_metadata(BQ_PARAMS['CLINICAL_TABLE'], table_id)
    delete_bq_table(temp_table_id)


# ***** BIOSPECIMEN TABLE FUNCTIONS

def make_biospecimen_per_study_query(pdc_study_id):
    """
    todo
    :param pdc_study_id:
    :return:
    """
    return '''
        {{ biospecimenPerStudy( pdc_study_id: \"{}\" acceptDUA: true) {{
            aliquot_id 
            sample_id 
            case_id 
            aliquot_submitter_id 
            sample_submitter_id 
            case_submitter_id 
            aliquot_status 
            case_status 
            sample_status 
            project_name 
            sample_type 
            disease_type 
            primary_site 
            pool 
            taxon
        }}
    }}'''.format(pdc_study_id)


def alter_biospecimen_per_study_obj(json_obj_list, pdc_study_id):
    """
    todo
    :param json_obj_list:
    :param pdc_study_id:
    :return:
    """
    for case in json_obj_list:
        case['pdc_study_id'] = pdc_study_id


# ***** MISC FUNCTIONS


def create_modified_temp_table(table_id, query):
    """
    todo
    :param table_id:
    :param query:
    :return:
    """
    temp_table_id = table_id + '_temp'
    delete_bq_table(temp_table_id)
    copy_bq_table(BQ_PARAMS, table_id, temp_table_id)
    load_table_from_query(BQ_PARAMS, table_id, query)


def get_schema_filename(data_type, suffix=None):
    """
    todo
    :param data_type:
    :param suffix:
    :return:
    """
    source = BQ_PARAMS["DATA_SOURCE"]
    file_list = [source, data_type]

    if suffix:
        file_list.append(suffix)
    file_list.append(get_rel_prefix(BQ_PARAMS))

    return build_table_name_from_list(file_list)


def update_column_metadata(table_type, table_id):
    """
    todo
    :param table_type:
    :param table_id:
    :return:
    """
    file_path = "/".join([BQ_PARAMS['BQ_REPO'], BQ_PARAMS['FIELD_DESC_DIR']])
    field_desc_file_name = get_schema_filename(table_type, BQ_PARAMS['FIELD_DESC_FILE_SUFFIX']) + '.json'
    field_desc_fp = get_filepath(file_path, field_desc_file_name)

    if not os.path.exists(field_desc_fp):
        has_fatal_error("BQEcosystem schema path not found", FileNotFoundError)
    with open(field_desc_fp) as field_output:
        descriptions = json.load(field_output)
        print("Updating metadata for {}\n".format(table_id))
        update_schema(table_id, descriptions)


def update_pdc_table_metadata(dataset, table_type=None):
    """
    todo
    :param dataset:
    :param table_type:
    :return:
    """
    fp_list = [BQ_PARAMS['BQ_REPO'], BQ_PARAMS['TABLE_METADATA_DIR'], BQ_PARAMS["RELEASE"]]
    metadata_fp = get_filepath(fp_list)
    metadata_files = [f for f in os.listdir(metadata_fp) if os.path.isfile(os.path.join(metadata_fp, f))]

    filtered_metadata_files = list()

    if not table_type:
        filtered_metadata_files = metadata_files
    else:
        for metadata_file in metadata_files:
            if table_type in str(metadata_file):
                filtered_metadata_files.append(metadata_file)

    print("Updating table metadata:")
    for table_metadata_json_file in filtered_metadata_files:
        table_id = get_dev_table_id(table_metadata_json_file.split('.')[-2], dataset=dataset)

        if not exists_bq_table(table_id):
            print("skipping {} (no bq table found)".format(table_id))
            continue

        print("- {}".format(table_id))
        json_fp = get_filepath(metadata_fp, table_metadata_json_file)

        with open(json_fp) as json_file_output:
            metadata = json.load(json_file_output)
            update_table_metadata(table_id, metadata)


def main(args):
    start_time = time.time()
    print("PDC script started at {}".format(time.strftime("%x %X", time.localtime())))

    steps = None

    try:
        global API_PARAMS, BQ_PARAMS
        API_PARAMS, BQ_PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        has_fatal_error(err, ValueError)

    # ** UTILITY STEPS **

    if 'delete_tables' in steps:
        for fps_table_id in BQ_PARAMS['DELETE_TABLES']:
            delete_bq_table(fps_table_id)
            print("Deleted table: {}".format(fps_table_id))

        delete_from_steps('delete_tables', steps)  # allows for exit without building study lists if not used

    if 'delete_datasets' in steps:
        for dataset in BQ_PARAMS['DELETE_DATASETS']:
            delete_bq_dataset(dataset)
            print("Deleted dataset: {}".format(dataset))

        delete_from_steps('delete_datasets', steps)  # allows for exit without building study lists if not used

    if "update_table_labels" in steps:
        table_labels = BQ_PARAMS['TABLE_LABEL_UPDATES']
        for table_id in table_labels.keys():
            if "remove" not in table_labels[table_id]:
                labels_to_remove = None
            else:
                labels_to_remove = table_labels[table_id]["remove"]

            if "add" not in table_labels[table_id]:
                labels_to_add = None
            else:
                labels_to_add = table_labels[table_id]["add"]

            update_table_labels(table_id, labels_to_remove, labels_to_add)

        delete_from_steps('update_table_labels', steps)  # allows for exit without building study lists if not used

    # ** STUDY METADATA STEPS **

    if 'build_studies_jsonl' in steps:
        build_jsonl_from_pdc_api(endpoint='allPrograms',
                                 request_function=make_all_programs_query,
                                 alter_json_function=alter_all_programs_json)

        delete_from_steps('build_studies_jsonl', steps)  # allows for exit without building study lists if not used

    if 'build_studies_table' in steps:
        build_table_from_jsonl(endpoint='allPrograms', infer_schema=True)

        delete_from_steps('build_studies_table', steps)  # allows for exit without building study lists if not used

    # build embargoed and open studies lists (only if subsequent steps exist)
    if len(steps) > 0:
        studies_list = list()
        embargoed_studies_list = list()
        pdc_study_ids = list()
        embargoed_pdc_study_ids = list()

        studies_output_name = API_PARAMS['ENDPOINT_SETTINGS']['allPrograms']['output_name']

        for study in get_query_results(retrieve_all_studies_query(studies_output_name)):
            if is_under_embargo(study['embargo_date']):
                embargoed_studies_list.append(dict(study.items()))
            else:
                studies_list.append(dict(study.items()))

        print_embargoed_studies(embargoed_studies_list)

        for study in sorted(studies_list, key=lambda item: item['pdc_study_id']):
            pdc_study_ids.append(study['pdc_study_id'])

        for study in embargoed_studies_list:
            embargoed_pdc_study_ids.append(study['pdc_study_id'])

        all_pdc_study_ids = embargoed_pdc_study_ids + pdc_study_ids
    else:
        # quit nagging me, pycharm
        all_pdc_study_ids = None
        pdc_study_ids = None
        studies_list = None

    # ** FILE METADATA STEPS **

    if 'build_per_study_file_jsonl' in steps:
        build_jsonl_from_pdc_api(endpoint="filesPerStudy",
                                 request_function=make_files_per_study_query,
                                 alter_json_function=alter_files_per_study_json,
                                 ids=all_pdc_study_ids)

    if 'build_per_study_file_table' in steps:
        build_table_from_jsonl(endpoint="filesPerStudy", infer_schema=True)

    if 'alter_per_study_file_table' in steps:
        fps_table_name = construct_table_name(BQ_PARAMS,
                                              API_PARAMS["ENDPOINT_SETTINGS"]["filesPerStudy"]["output_name"])
        fps_table_id = get_dev_table_id(fps_table_name, dataset=BQ_PARAMS['META_DATASET'])

        create_modified_temp_table(fps_table_id, modify_per_study_file_table_query(fps_table_id))

    if 'build_api_file_metadata_jsonl' in steps:
        file_ids = get_file_ids("filesPerStudy")
        build_file_pdc_metadata_jsonl(file_ids)

    if 'build_api_file_metadata_table' in steps:
        build_table_from_jsonl("fileMetadata", infer_schema=True)

    if 'alter_api_file_metadata_table' in steps:
        fm_table_name = construct_table_name(BQ_PARAMS, API_PARAMS["ENDPOINT_SETTINGS"]["fileMetadata"]["output_name"])
        fm_table_id = get_dev_table_id(fm_table_name, dataset=BQ_PARAMS['META_DATASET'])

        create_modified_temp_table(fm_table_id, modify_api_file_metadata_table_query(fm_table_id))

    if 'build_file_associated_entries_table' in steps:
        # Note, this assumes aliquot id will exist, because that's true. This will either be null,
        # or it'll have an aliquot id. If this ever changes, we'll need to adjust, but not expected that it will.
        table_name = construct_table_name(BQ_PARAMS, BQ_PARAMS['FILE_ASSOC_MAPPING_TABLE'])
        full_table_id = get_dev_table_id(table_name, dataset=BQ_PARAMS['META_DATASET'])
        load_table_from_query(BQ_PARAMS, full_table_id, make_associated_entities_query())
        update_column_metadata(BQ_PARAMS['FILE_METADATA'], full_table_id)

    if 'build_file_metadata_table' in steps:
        table_name = construct_table_name(BQ_PARAMS, BQ_PARAMS['FILE_METADATA'])
        full_table_id = get_dev_table_id(table_name, dataset=BQ_PARAMS['META_DATASET'])
        load_table_from_query(BQ_PARAMS, full_table_id, make_combined_file_metadata_query())
        update_column_metadata(BQ_PARAMS['FILE_METADATA'], full_table_id)

    if 'update_file_metadata_tables_metadata' in steps:
        update_pdc_table_metadata(BQ_PARAMS['META_DATASET'], BQ_PARAMS['FILE_METADATA'])
        update_pdc_table_metadata(BQ_PARAMS['META_DATASET'], BQ_PARAMS['FILE_ASSOC_MAPPING_TABLE'])

    # ** CASE CLINICAL STEPS **

    if 'build_cases_jsonl' in steps:
        build_jsonl_from_pdc_api(endpoint="allCases",
                                 request_function=make_cases_query)

    if 'build_cases_table' in steps:
        build_table_from_jsonl("allCases", infer_schema=True)

    if 'build_cases_aliquots_jsonl' in steps:
        build_jsonl_from_pdc_api(endpoint="paginatedCasesSamplesAliquots",
                                 request_function=make_cases_aliquots_query)

    if 'build_cases_aliquots_table' in steps:
        build_table_from_jsonl("paginatedCasesSamplesAliquots", infer_schema=True)

    if 'build_case_diagnoses_jsonl' in steps:
        build_jsonl_from_pdc_api(endpoint="paginatedCaseDiagnosesPerStudy",
                                 request_function=make_cases_diagnoses_query,
                                 alter_json_function=alter_case_diagnoses_json,
                                 ids=pdc_study_ids,
                                 insert_id=True)

    if 'build_case_diagnoses_table' in steps:
        build_table_from_jsonl(endpoint='paginatedCaseDiagnosesPerStudy', infer_schema=True)

    if 'build_case_demographics_jsonl' in steps:
        build_jsonl_from_pdc_api(endpoint="paginatedCaseDemographicsPerStudy",
                                 request_function=make_cases_demographics_query,
                                 alter_json_function=alter_case_demographics_json,
                                 ids=pdc_study_ids,
                                 insert_id=True)

    if 'build_case_demographics_table' in steps:
        build_table_from_jsonl(endpoint='paginatedCaseDemographicsPerStudy', infer_schema=True)

    if 'build_case_clinical_jsonl_and_tables_per_project' in steps:
        # get unique project_submitter_ids from studies_list
        cases_by_project_submitter = get_cases_by_project_submitter(studies_list)

        # get all case records, append to list for its project submitter id
        for case in get_cases():
            if not case:
                continue
            if 'project_submitter_id' not in case:
                continue

            project_submitter_id = case['project_submitter_id']
            cases_by_project_submitter[project_submitter_id]['cases'].append(case)

        # get all demographic records
        demographic_records = get_case_demographics()
        demographic_records_by_case_id = dict()

        # create dict where key = (case_id, case_submitter_id) and value = dict of remaining query results
        for record in demographic_records:
            case_id_key_tuple = (record.pop("case_id"), record.pop("case_submitter_id"))
            demographic_records_by_case_id[case_id_key_tuple] = record

        # get all diagnoses records, create dict where
        # key = (case_id, case_submitter_id) and value = dict of remaining query results
        diagnosis_records = get_case_diagnoses()
        diagnosis_records_by_case_id = dict()

        for record in diagnosis_records:
            case_id_key_tuple = (record.pop("case_id"), record.pop("case_submitter_id"))
            diagnosis_records_by_case_id[case_id_key_tuple] = record

        # iterate over project_submitter_id dict. (for project in project_dict, for case in project)
        # retrieve case demographic and diagnoses for case, pop, add to case record
        # get length of each diagnosis record and compare to max_diagnoses_record_length, update if larger

        cases_with_no_clinical_data = list()

        for project_name, project_dict in cases_by_project_submitter.items():

            for case in project_dict['cases']:
                case_id_key_tuple = (case['case_id'], case['case_submitter_id'])

                if case_id_key_tuple not in diagnosis_records_by_case_id:
                    if case_id_key_tuple not in demographic_records_by_case_id:
                        cases_with_no_clinical_data.append(case_id_key_tuple)
                        continue

                if case_id_key_tuple in diagnosis_records_by_case_id:
                    diagnosis_record = diagnosis_records_by_case_id[case_id_key_tuple]

                    if len(diagnosis_record['diagnoses']) > project_dict['max_diagnosis_count']:
                        project_dict['max_diagnosis_count'] = len(diagnosis_record['diagnoses'])

                    case.update(diagnosis_record)

                if case_id_key_tuple in demographic_records_by_case_id:
                    demographic_record = demographic_records_by_case_id[case_id_key_tuple]

                    case.update(demographic_record)

        # todo remove when fixed by PDC
        cases_by_project_submitter['Academia Sinica LUAD-100'] = cases_by_project_submitter.pop('LUAD-100')

        print("{} cases with no clinical data".format(len(cases_with_no_clinical_data)))

        for project_name, project_dict in cases_by_project_submitter.items():
            record_count = len(project_dict['cases'])
            max_diagnosis_count = project_dict['max_diagnosis_count']

            print("{}: {} records, {} max diagnoses".format(project_name, record_count, max_diagnosis_count))

            clinical_records = []
            clinical_diagnoses_records = []

            # iterate over now-populated project dicts
            # - if max diagnosis record length is 1, create single PROJECT_clinical_pdc_current table
            # - else create a PROJECT_clinical_pdc_current table and a PROJECT_clinical_diagnoses_pdc_current table
            cases = project_dict['cases']
            for case in cases:
                if 'case_id' not in case:
                    continue
                clinical_case_record = case
                clinical_diagnoses_record = dict()
                diagnoses = case.pop('diagnoses') if 'diagnoses' in case else None

                if not clinical_case_record or max_diagnosis_count == 0:
                    continue
                if max_diagnosis_count == 1 and diagnoses:
                    clinical_case_record.update(diagnoses[0])
                elif max_diagnosis_count > 1 and diagnoses:
                    for diagnosis in diagnoses:
                        clinical_diagnoses_record['case_id'] = clinical_case_record['case_id']
                        clinical_diagnoses_record['case_submitter_id'] = clinical_case_record['case_submitter_id']
                        clinical_diagnoses_record['project_submitter_id'] = clinical_case_record['project_submitter_id']
                        clinical_diagnoses_record.update(diagnosis)
                        clinical_diagnoses_records.append(clinical_diagnoses_record)

                clinical_records.append(clinical_case_record)

            if clinical_records:
                temp_clinical_table_id = remove_nulls_and_create_temp_table(clinical_records,
                                                                            project_name,
                                                                            infer_schema=True)
                create_ordered_clinical_table(temp_clinical_table_id,
                                              project_name,
                                              BQ_PARAMS['CLINICAL_TABLE'])
            if clinical_diagnoses_records:
                temp_diagnoses_table_id = remove_nulls_and_create_temp_table(clinical_diagnoses_records,
                                                                             project_name,
                                                                             is_diagnoses=True,
                                                                             infer_schema=True)

                create_ordered_clinical_table(temp_diagnoses_table_id,
                                              project_name,
                                              BQ_PARAMS['CLINICAL_DIAGNOSES_TABLE'])

    if 'update_clinical_tables_metadata' in steps:
        update_pdc_table_metadata(BQ_PARAMS['CLINICAL_DATASET'], table_type=BQ_PARAMS['CLINICAL_TABLE'])

    # ** CASE METADATA STEPS **

    if 'build_biospecimen_jsonl' in steps:
        build_jsonl_from_pdc_api(endpoint="biospecimenPerStudy",
                                 request_function=make_biospecimen_per_study_query,
                                 alter_json_function=alter_biospecimen_per_study_obj,
                                 ids=all_pdc_study_ids,
                                 insert_id=True)

    if 'build_biospecimen_table' in steps:
        build_table_from_jsonl(endpoint="biospecimenPerStudy", infer_schema=True)

    # todo merge PDC study ids for given case_id into single row, probably?

    # ** QUANT DATA MATRIX STEPS **

    if 'build_quant_tsvs' in steps:
        for study_id_dict in studies_list:
            quant_tsv_file = get_filename('tsv', BQ_PARAMS['QUANT_DATA_TABLE'], study_id_dict['pdc_study_id'])
            quant_tsv_path = get_scratch_fp(BQ_PARAMS, quant_tsv_file)

            lines_written = build_quant_tsv(study_id_dict, 'log2_ratio', quant_tsv_path)
            print("\n{0} lines written for {1}".format(lines_written, study_id_dict['study_name']))

            if lines_written > 0:
                upload_to_bucket(BQ_PARAMS, quant_tsv_path)
                print("{0} uploaded to Google Cloud bucket!".format(quant_tsv_file))
                os.remove(quant_tsv_path)

    if 'build_quant_tables' in steps:
        print("Building quant tables...")
        blob_files = get_quant_files()

        for study_id_dict in studies_list:
            quant_tsv_file = get_filename('tsv', BQ_PARAMS['QUANT_DATA_TABLE'], study_id_dict['pdc_study_id'])

            if quant_tsv_file not in blob_files:
                print('Skipping table build for {} (jsonl not found in bucket)'.format(study_id_dict['study_name']))
            else:
                build_table_from_tsv(BQ_PARAMS['DEV_PROJECT'],
                                     BQ_PARAMS['DEV_DATASET'],
                                     BQ_PARAMS['QUANT_DATA_TABLE'],
                                     study_id_dict['pdc_study_id'])

    if 'build_uniprot_tsv' in steps:
        gz_file_name = API_PARAMS['UNIPROT_MAPPING_FP'].split('/')[-1]
        split_file = gz_file_name.split('.')
        mapping_file = split_file[0] + '_' + BQ_PARAMS['UNIPROT_RELEASE'] + API_PARAMS['UNIPROT_FILE_EXT']

        download_from_uniprot_ftp(mapping_file, API_PARAMS['UNIPROT_MAPPING_FP'], 'UniProt mapping')
        upload_to_bucket(BQ_PARAMS, get_scratch_fp(BQ_PARAMS, mapping_file))

    if 'build_uniprot_table' in steps:
        gz_file_name = API_PARAMS['UNIPROT_MAPPING_FP'].split('/')[-1]
        split_file = gz_file_name.split('.')

        mapping_table = construct_table_name(BQ_PARAMS, BQ_PARAMS['UNIPROT_MAPPING_TABLE'],
                                             release=BQ_PARAMS['UNIPROT_RELEASE'])
        fps_table_id = get_dev_table_id(mapping_table, dataset=BQ_PARAMS['META_DATASET'])

        print("\nBuilding {0}... ".format(fps_table_id))

        fps_schema_file = build_table_name_from_list(fps_table_id.split("."))
        schema_filename = fps_schema_file + '.json'
        schema = load_bq_schema_from_json(BQ_PARAMS, schema_filename)

        data_file = split_file[0] + '_' + BQ_PARAMS['UNIPROT_RELEASE'] + API_PARAMS['UNIPROT_FILE_EXT']

        create_and_load_table_from_tsv(BQ_PARAMS, data_file, schema, fps_table_id,
                                       BQ_PARAMS['NULL_MARKER'], num_header_rows=0)
        print("Uniprot table built!")

    if 'build_swissprot_table' in steps:
        table_name = construct_table_name(BQ_PARAMS, BQ_PARAMS['SWISSPROT_TABLE'], release=BQ_PARAMS['UNIPROT_RELEASE'])
        fps_table_id = get_dev_table_id(table_name, dataset=BQ_PARAMS['META_DATASET'])

        print("Building {0}... ".format(fps_table_id))
        fps_schema_file = build_table_name_from_list(fps_table_id.split("."))
        schema_filename = fps_schema_file + '.json'
        schema = load_bq_schema_from_json(BQ_PARAMS, schema_filename)

        data_file = table_name + API_PARAMS['UNIPROT_FILE_EXT']
        create_and_load_table_from_tsv(BQ_PARAMS, data_file, schema, fps_table_id,
                                       BQ_PARAMS['NULL_MARKER'], num_header_rows=0)
        print("Swiss-prot table built!")

    if 'build_gene_tsv' in steps:
        gene_symbol_list = build_gene_symbol_list(studies_list)
        gene_tsv_file = get_filename('tsv', BQ_PARAMS['GENE_TABLE'])
        gene_tsv_path = get_scratch_fp(BQ_PARAMS, gene_tsv_file)

        build_gene_tsv(gene_symbol_list, gene_tsv_path, append=API_PARAMS['RESUME_GENE_TSV'])
        upload_to_bucket(BQ_PARAMS, gene_tsv_path)

    if 'build_gene_table' in steps:
        gene_tsv_file = get_filename('tsv', BQ_PARAMS['GENE_TABLE'])
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

        build_table_from_tsv(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['META_DATASET'], BQ_PARAMS['GENE_TABLE'])

    if 'build_proteome_quant_tables' in steps:
        for study in studies_list:

            # only run the build script for analytes we're currently publishing
            if study['analytical_fraction'] not in BQ_PARAMS["BUILD_ANALYTES"]:
                continue

            pdc_study_id = study['pdc_study_id']
            raw_table_name = get_quant_table_name(study, is_final=False)

            if exists_bq_table(get_dev_table_id(raw_table_name, dataset=BQ_PARAMS['DEV_DATASET'])):
                final_table_name = get_quant_table_name(study)
                final_table_id = get_dev_table_id(final_table_name)

                load_table_from_query(BQ_PARAMS, final_table_id, make_proteome_quant_table_query(pdc_study_id))

                update_column_metadata(BQ_PARAMS['QUANT_DATA_TABLE'], final_table_id)

        update_pdc_table_metadata(BQ_PARAMS['DEV_DATASET'], table_type=BQ_PARAMS['QUANT_DATA_TABLE'])

    # ** PUBLISH STEPS **

    if "publish_proteome_tables" in steps:
        for study in get_proteome_studies(studies_list):
            table_name = get_quant_table_name(study)
            project_submitter_id = study['project_submitter_id']

            if project_submitter_id not in BQ_PARAMS['PROD_DATASET_MAP']:
                has_fatal_error("{} metadata missing from PROD_DATASET_MAP".format(project_submitter_id))

            public_dataset = BQ_PARAMS['PROD_DATASET_MAP'][project_submitter_id]
            source_table_id = get_dev_table_id(table_name)

            publish_table(BQ_PARAMS, public_dataset, source_table_id, overwrite=True)

    if "publish_clinical_tables" in steps:
        # create dict of project shortnames and the dataset they belong to
        dataset_map = dict()

        for project_submitter_id in BQ_PARAMS['PROJECT_ABBREVIATION_MAP']:
            key = BQ_PARAMS['PROJECT_ABBREVIATION_MAP'][project_submitter_id]
            val = BQ_PARAMS['PROD_DATASET_MAP'][project_submitter_id]
            dataset_map[key] = val

        # iterate over existing dev project clinical tables for current API version
        current_clinical_table_list = list_bq_tables(BQ_PARAMS['CLINICAL_DATASET'], BQ_PARAMS['RELEASE'])
        removal_list = ['clinical_diagnoses_', 'clinical_', "_pdc_" + BQ_PARAMS['RELEASE']]

        for table_name in current_clinical_table_list:
            project_shortname = table_name

            # strip table name down to project shortname
            for rem_str in removal_list:
                if rem_str in project_shortname:
                    project_shortname = project_shortname.replace(rem_str, '')

            public_dataset = dataset_map[project_shortname]
            source_table_id = get_dev_table_id(table_name, dataset=BQ_PARAMS['CLINICAL_DATASET'])

            publish_table(BQ_PARAMS, public_dataset, source_table_id, overwrite=True)

    if "publish_file_metadata_tables" in steps:
        file_metadata_table_name = construct_table_name(BQ_PARAMS, BQ_PARAMS['FILE_METADATA'])
        file_metadata_table_id = get_dev_table_id(file_metadata_table_name, dataset=BQ_PARAMS['META_DATASET'])
        publish_table(BQ_PARAMS, BQ_PARAMS['FILE_METADATA'], file_metadata_table_id, overwrite=True)

        mapping_table_name = construct_table_name(BQ_PARAMS, BQ_PARAMS['FILE_ASSOC_MAPPING_TABLE'])
        mapping_table_id = get_dev_table_id(mapping_table_name, dataset=BQ_PARAMS['META_DATASET'])
        publish_table(BQ_PARAMS, BQ_PARAMS['FILE_ASSOC_MAPPING_TABLE'], mapping_table_id, overwrite=True)

    end = time.time() - start_time
    print("Finished program execution in {}!\n".format(format_seconds(end)))


if __name__ == '__main__':
    main(sys.argv)
