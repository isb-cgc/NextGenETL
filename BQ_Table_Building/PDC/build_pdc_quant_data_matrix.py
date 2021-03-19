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
                                             update_column_metadata, update_pdc_table_metadata)

API_PARAMS = dict()
BQ_PARAMS = dict()
YAML_HEADERS = ('api_params', 'bq_params', 'steps')


# ***** GENE TABLE FUNCTIONS

def make_gene_symbols_per_study_query(pdc_study_id):
    """
    Returns list of gene symbols for a given PDC study id.
    :param pdc_study_id: PDC study id for which to retrieve the symbols
    :return: sql query string
    """
    table_name = construct_table_name(API_PARAMS,
                                      prefix=BQ_PARAMS['QUANT_DATA_TABLE'],
                                      suffix=pdc_study_id,
                                      release=API_PARAMS['RELEASE'])

    table_id = get_dev_table_id(BQ_PARAMS, table_name)

    return """
        SELECT DISTINCT(gene_symbol)
        FROM `{}`
    """.format(table_id)


def make_gene_query(gene_name):
    """
    Creates a graphQL string for querying the PDC API's geneSpectralCount endpoint.
    :param gene_name: the gene name for query argument
    :return: GraphQL query string
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
    Make query to select Swiss-Prot id from UniProt table.
    :return: sql query string
    """
    table_name = construct_table_name(API_PARAMS,
                                      prefix=BQ_PARAMS['SWISSPROT_TABLE'],
                                      release=API_PARAMS['UNIPROT_RELEASE'])

    swissprot_table_id = get_dev_table_id(BQ_PARAMS,
                                          dataset=BQ_PARAMS['META_DATASET'],
                                          table_name=table_name)
    return """
    SELECT swissprot_id 
    FROM `{}`
    """.format(swissprot_table_id)


def add_gene_symbols_per_study(pdc_study_id, gene_symbol_set):
    """
    Creates master gene symbol set by querying the gene symbols for each study.
    :param pdc_study_id: PDC study id to use in query
    :param gene_symbol_set: set of gene symbols
    """
    table_name = construct_table_name(API_PARAMS,
                                      prefix=BQ_PARAMS['QUANT_DATA_TABLE'],
                                      suffix=pdc_study_id,
                                      release=API_PARAMS['RELEASE'])

    table_id = get_dev_table_id(BQ_PARAMS,
                                dataset=BQ_PARAMS['DEV_DATASET'],
                                table_name=table_name)

    if exists_bq_table(table_id):
        results = get_query_results(make_gene_symbols_per_study_query(pdc_study_id))

        for row in results:
            gene_symbol_set.add(row['gene_symbol'])


def build_gene_symbol_list(studies_list):
    """
    Creates sorted list of all gene symbols used for any current PDC study
    :param studies_list: list of non-embargoed PDC studies
    :return: alphabetical gene symbol list
    """
    print("Building gene symbol tsv!")
    gene_symbol_set = set()

    for study in studies_list:
        table_name = construct_table_name(API_PARAMS,
                                          prefix=BQ_PARAMS['QUANT_DATA_TABLE'],
                                          suffix=study['pdc_study_id'],
                                          release=API_PARAMS['RELEASE'])

        table_id = get_dev_table_id(BQ_PARAMS,
                                    dataset=BQ_PARAMS['DEV_DATASET'],
                                    table_name=table_name)

        if exists_bq_table(table_id):
            add_gene_symbols_per_study(study['pdc_study_id'], gene_symbol_set)
            print("- Added {}, current count: {}".format(study['pdc_study_id'], len(gene_symbol_set)))
        else:
            print("- No table for {}, skipping.".format(study['pdc_study_id']))

    gene_symbol_list = list(sorted(gene_symbol_set))
    return gene_symbol_list


def build_gene_tsv(gene_symbol_list, gene_tsv, append=False):
    """
    Create tsv from geneSpectralCount API responses
    :param gene_symbol_list: list of gene symbols
    :param gene_tsv: file location of geneSpectralCount output tsv
    :param append: if True, appends to existing file. Defaults to overwrite.
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
                                          'assays']))

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
                                          gene_data['assays']]))
            count += 1

            if count % 50 == 0:
                print("Added {} genes".format(count))


def download_from_uniprot_ftp(local_file, server_fp, type_str):
    """
    Download file from UniProt's FTP server.
    :param local_file: file to which to write download
    :param server_fp: FTP server location
    :param type_str: Type of file downloaded from UniProt (e.g. Gene mappings)
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
    Is id_str a valid UniProt accession number based on their canonical formats?
    :param id_str: string to evaluate
    :return: True if string is in UniProt accession number format, false otherwise
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
    Qualifying as a UniProt accession number, all must be true:
    
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
    Filter proteins list, searching for ids in UniProt accession number format.
    :param proteins_str: semi-colon delimited string of protein ids
    :return: semi-colon delimited string of UniProt accession number ids
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
    Filter proteins list string by Swiss-Prot id.
    :param proteins: semi-colon delimited string of protein ids
    :param swissprot_set: semi-colon delimited string of Swiss-Prot ids
    :return: Swiss-Prot ids list string, Swiss-Prot id count
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
    quant_table_name = "{}_{}_{}".format(BQ_PARAMS['QUANT_DATA_TABLE'], study, API_PARAMS['RELEASE'])
    quant_table_id = get_dev_table_id(BQ_PARAMS, quant_table_name)

    case_aliquot_table_name = '{}_{}'.format(BQ_PARAMS['CASE_ALIQUOT_TABLE'], API_PARAMS['RELEASE'])
    case_aliquot_table_id = get_dev_table_id(BQ_PARAMS, dataset=BQ_PARAMS['META_DATASET'],
                                             table_name=case_aliquot_table_name)

    gene_table_name = '{}_{}'.format(BQ_PARAMS['GENE_TABLE'], API_PARAMS['RELEASE'])
    gene_table_id = get_dev_table_id(BQ_PARAMS, dataset=BQ_PARAMS['META_DATASET'], table_name=gene_table_name)

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
    table_name = construct_table_name(API_PARAMS,
                                      prefix=BQ_PARAMS['QUANT_DATA_TABLE'],
                                      suffix=study_submitter_id)

    table_id = get_dev_table_id(BQ_PARAMS,
                                dataset=BQ_PARAMS['DEV_DATASET'],
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
    analytical_fraction = study['analytical_fraction']

    if not is_final:
        return construct_table_name(API_PARAMS,
                                    prefix=BQ_PARAMS['QUANT_DATA_TABLE'],
                                    suffix=study['pdc_study_id'],
                                    release=API_PARAMS['RELEASE'])
    else:
        study_name = study['study_name']
        study_name = study_name.replace(analytical_fraction, "")

        return "_".join([BQ_PARAMS['QUANT_DATA_TABLE'],
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

    if 'build_quant_tsvs' in steps:
        for study_id_dict in studies_list:
            quant_tsv_file = get_filename(API_PARAMS,
                                          file_extension='tsv',
                                          prefix=BQ_PARAMS['QUANT_DATA_TABLE'],
                                          suffix=study_id_dict['pdc_study_id'])

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
            quant_tsv_file = get_filename(API_PARAMS,
                                          file_extension='tsv',
                                          prefix=BQ_PARAMS['QUANT_DATA_TABLE'],
                                          suffix=study_id_dict['pdc_study_id'])

            if quant_tsv_file not in blob_files:
                print('Skipping table build for {} (jsonl not found in bucket)'.format(study_id_dict['study_name']))
            else:
                build_table_from_tsv(API_PARAMS, BQ_PARAMS,
                                     table_prefix=BQ_PARAMS['QUANT_DATA_TABLE'],
                                     table_suffix=study_id_dict['pdc_study_id'])

    if 'build_uniprot_tsv' in steps:
        gz_file_name = API_PARAMS['UNIPROT_MAPPING_FP'].split('/')[-1]
        split_file = gz_file_name.split('.')
        mapping_file = split_file[0] + '_' + API_PARAMS['UNIPROT_RELEASE'] + API_PARAMS['UNIPROT_FILE_EXT']

        download_from_uniprot_ftp(mapping_file, API_PARAMS['UNIPROT_MAPPING_FP'], 'UniProt mapping')
        upload_to_bucket(BQ_PARAMS, get_scratch_fp(BQ_PARAMS, mapping_file))

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

        fps_schema_file = construct_table_name_from_list(fps_table_id.split("."))
        schema_filename = fps_schema_file + '.json'
        schema = load_bq_schema_from_json(BQ_PARAMS, schema_filename)

        data_file = split_file[0] + '_' + API_PARAMS['UNIPROT_RELEASE'] + API_PARAMS['UNIPROT_FILE_EXT']

        create_and_load_table_from_tsv(bq_params=BQ_PARAMS,
                                       tsv_file=data_file,
                                       schema=schema,
                                       table_id=fps_table_id,
                                       num_header_rows=0)
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

        create_and_load_table_from_tsv(bq_params=BQ_PARAMS,
                                       tsv_file=data_file,
                                       schema=schema,
                                       table_id=fps_table_id,
                                       num_header_rows=0)

        print("Swiss-prot table built!")

    if 'build_gene_tsv' in steps:
        gene_symbol_list = build_gene_symbol_list(studies_list)
        gene_tsv_file = get_filename(API_PARAMS,
                                     file_extension='tsv',
                                     prefix=BQ_PARAMS['GENE_TABLE'])

        gene_tsv_path = get_scratch_fp(BQ_PARAMS, gene_tsv_file)

        build_gene_tsv(gene_symbol_list, gene_tsv_path, append=API_PARAMS['RESUME_GENE_TSV'])
        upload_to_bucket(BQ_PARAMS, gene_tsv_path)

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
        for study in studies_list:

            # only run the build script for analytes we're currently publishing
            if study['analytical_fraction'] not in BQ_PARAMS["BUILD_ANALYTES"]:
                continue

            pdc_study_id = study['pdc_study_id']
            raw_table_name = get_quant_table_name(study, is_final=False)

            if exists_bq_table(
                    get_dev_table_id(BQ_PARAMS, dataset=BQ_PARAMS['DEV_DATASET'], table_name=raw_table_name)):
                final_table_name = get_quant_table_name(study)
                final_table_id = get_dev_table_id(BQ_PARAMS, final_table_name)

                load_table_from_query(BQ_PARAMS,
                                      table_id=final_table_id,
                                      query=make_proteome_quant_table_query(pdc_study_id))

                update_column_metadata(API_PARAMS, BQ_PARAMS, final_table_id)

        update_pdc_table_metadata(API_PARAMS, BQ_PARAMS, table_type=BQ_PARAMS['QUANT_DATA_TABLE'])

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
