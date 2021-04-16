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
                                             update_column_metadata, update_pdc_table_metadata, get_prefix,
                                             build_obj_from_pdc_api, request_data_from_pdc_api)

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
        # kind of a hacky fix, but we'll move to CDA before it matters (before there are 9000+ studies)
        if "quant" in filename and "schema" not in filename and version in filename and "PDC0" in filename:
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


def make_genes_count_query():
    return ''' {
        getPaginatedGenes(offset: 0 limit: 1 acceptDUA: true) {
            total
        }
    }'''


def get_gene_record_count():
    gene_record_query = make_genes_count_query()

    count_res = get_graphql_api_response(API_PARAMS, gene_record_query)

    print(count_res)
    exit()

    for row in count_res:
        return row[0]


def make_gene_symbols_per_study_query(pdc_study_id):
    """

    Return list of gene symbols for a given PDC study id.
    :param pdc_study_id: PDC study id for which to retrieve the symbols
    :return: sql query string
    """
    quant_prefix = get_prefix(API_PARAMS, API_PARAMS['QUANT_ENDPOINT'])
    quant_table_name = construct_table_name(API_PARAMS,
                                            prefix=quant_prefix,
                                            suffix=pdc_study_id)
    quant_table_id = get_dev_table_id(BQ_PARAMS, quant_table_name)

    return """
        SELECT DISTINCT(gene_symbol)
        FROM `{}`
    """.format(quant_table_id)


def alter_paginated_gene_list():
    # todo
    pass


def add_gene_symbols_per_study(study_obj, gene_symbol_set):
    """

    Create master gene symbol set by querying the gene symbols for each study.
    :param study_obj: object containing single study's metadata
    :param gene_symbol_set: set of gene symbols
    """
    raw_quant_table_name = create_raw_quant_table_name(study_id_dict=study_obj)

    raw_quant_table_id = get_dev_table_id(BQ_PARAMS,
                                          dataset=BQ_PARAMS['QUANT_DATASET'],
                                          table_name=raw_quant_table_name)

    if exists_bq_table(raw_quant_table_id):
        results = get_query_results(make_gene_symbols_per_study_query(study_obj['pdc_study_id']))

        for row in results:
            gene_symbol_set.add(row['gene_symbol'])


def build_gene_symbol_list(studies_list):
    """

    Create sorted list of all gene symbols used for any current PDC study.
    :param studies_list: list of non-embargoed PDC studies
    :return: alphabetically-sorted gene symbol list
    """
    print("Building gene symbol tsv!")
    gene_symbol_set = set()

    for study in studies_list:
        raw_quant_table_name = create_raw_quant_table_name(study_id_dict=study)

        raw_quant_table_id = get_dev_table_id(BQ_PARAMS,
                                              dataset=BQ_PARAMS['QUANT_DATASET'],
                                              table_name=raw_quant_table_name)

        if exists_bq_table(raw_quant_table_id):
            add_gene_symbols_per_study(study, gene_symbol_set)
            print("- Added {}, current count: {}".format(study['pdc_study_id'], len(gene_symbol_set)))
        else:
            print("- No table for {}, skipping.".format(study['pdc_study_id']))

    gene_symbol_list = list(sorted(gene_symbol_set))
    return gene_symbol_list


def make_gene_query(gene_name):
    """

    Create a graphQL string for querying the PDC API's geneSpectralCount endpoint.
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


def make_paginated_gene_query(offset, limit):
    return """
        {{
          getPaginatedGenes(offset:{0} limit: {1} acceptDUA:true) {{ 
            total genesProper{{
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
        }}
    """.format(offset, limit)


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


def sort_swissprot_by_age(a, b):
    """

    Comparator for Swiss-Prot ids. To use:
        compare_uniprot_ids = cmp_to_key(sort_swissprot_by_age)
        uniprot_list.sort(key=compare_uniprot_ids)
    :param a: first param to compare
    :param b: second param to compare
    :return int value representing order (<0 means a comes first, >0 means b is first, 0 means equal)
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


def is_uniprot_accession_number(id_str):
    """

    Determine whether id_str is a valid UniProt accession number based on canonical format.
        To qualify as a UniProt accession number, all must be true:
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


# todo note added gene_tsv_headers=None
def build_gene_tsv(gene_symbol_list, gene_tsv, append=False, gene_tsv_headers=None):
    """
    Create tsv from geneSpectralCount API responses
    :param gene_symbol_list: list of gene symbols
    :param gene_tsv: file location of geneSpectralCount output tsv
    :param append: if True, appends to existing file. Defaults to overwrite.
    """

    # uses comparator to order swissprot ids
    compare_uniprot_ids = cmp_to_key(sort_swissprot_by_age)
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
            gene_fh.write(create_tsv_row(gene_tsv_headers))

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
            unversioned_quant_table_name = create_raw_quant_table_name(study_id_dict, include_release=False)
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
                                                 table_name=unversioned_quant_table_name,
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
        # used to verify the presence of quant file (meaning there was data for that study in quantDataMatrix)
        quant_blob_files = get_quant_files()

        for study_id_dict in studies_list:
            raw_quant_tsv_file = create_raw_quant_table_name(study_id_dict) + '.tsv'

            if raw_quant_tsv_file not in quant_blob_files:
                print('Skipping table build for {} (jsonl not found in bucket)'.format(raw_quant_tsv_file))
            else:
                print("Building table for {}".format(raw_quant_tsv_file))

                raw_quant_table_name = create_raw_quant_table_name(study_id_dict)
                raw_quant_table_id = get_dev_table_id(BQ_PARAMS, BQ_PARAMS['QUANT_DATASET'], raw_quant_table_name)

                unversioned_quant_table_name = create_raw_quant_table_name(study_id_dict, include_release=False)
                raw_quant_schema = return_schema_object_for_bq(API_PARAMS, BQ_PARAMS,
                                                               table_name=unversioned_quant_table_name)

                create_and_load_table_from_tsv(BQ_PARAMS,
                                               tsv_file=raw_quant_tsv_file,
                                               table_id=raw_quant_table_id,
                                               num_header_rows=1,
                                               schema=raw_quant_schema)

    if 'build_gene_jsonl' in steps:
        record_count = get_gene_record_count()

        print(record_count)
        exit()

        api_gene_list = build_obj_from_pdc_api(API_PARAMS,
                                               endpoint=API_PARAMS['GENE_ENDPOINT'],
                                               request_function=make_paginated_gene_query,
                                               alter_json_function=alter_paginated_gene_list,
                                               )

        '''
        gene_tsv_headers = ['gene_id',
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
                            'assays']

        gene_symbol_list = build_gene_symbol_list(studies_list)
        gene_tsv_file = get_filename(API_PARAMS,
                                     file_extension='tsv',
                                     prefix=BQ_PARAMS['GENE_TABLE'])

        gene_tsv_path = get_scratch_fp(BQ_PARAMS, gene_tsv_file)

        build_gene_tsv(gene_symbol_list=gene_symbol_list,
                       gene_tsv=gene_tsv_path,
                       append=API_PARAMS['RESUME_GENE_TSV'],
                       gene_tsv_headers=gene_tsv_headers)
        upload_to_bucket(BQ_PARAMS, scratch_fp=gene_tsv_path, delete_local=True)
        '''
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

        # todo
        # build_table_from_tsv(API_PARAMS, BQ_PARAMS, table_prefix=BQ_PARAMS['GENE_TABLE'])


    end = time.time() - start_time
    print("Finished program execution in {}!\n".format(format_seconds(end)))


if __name__ == '__main__':
    main(sys.argv)
