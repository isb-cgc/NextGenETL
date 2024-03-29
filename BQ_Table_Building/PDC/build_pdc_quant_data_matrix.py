"""
Copyright 2021, Institute for Systems Biology

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

import time
import sys
import requests

from functools import cmp_to_key
from google.cloud import storage

from common_etl.utils import (get_query_results, format_seconds, get_scratch_fp, upload_to_bucket, write_list_to_tsv,
                              get_graphql_api_response, has_fatal_error, create_and_load_table_from_tsv, create_tsv_row,
                              load_table_from_query, exists_bq_table, load_config, construct_table_name,
                              create_and_upload_schema_for_tsv, retrieve_bq_schema_object, delete_bq_table,
                              create_and_upload_schema_for_json, write_list_to_jsonl_and_upload, publish_table,
                              make_string_bq_friendly, test_table_for_version_changes)

from BQ_Table_Building.PDC.pdc_utils import (get_pdc_studies_list, get_filename, update_table_schema_from_generic_pdc,
                                             get_prefix, build_obj_from_pdc_api, build_table_from_jsonl,
                                             get_publish_table_ids, find_most_recent_published_table_id,
                                             find_most_recent_published_table_id_uniprot)

API_PARAMS = dict()
BQ_PARAMS = dict()
YAML_HEADERS = ('api_params', 'bq_params', 'steps')


def retrieve_uniprot_kb_genes():
    """
    Retrieve Swiss-Prot ids and gene names from UniProtKB REST API.
    :return: REST API response text (tsv)
    """
    query = 'organism_id:9606'
    data_format = 'tsv'
    columns = 'id,gene_primary,xref_refseq,reviewed'

    request_url = f'https://rest.uniprot.org/uniprotkb/search?query={query}&format={data_format}&fields={columns}&size=500'

    response = requests.get(request_url)
    return response.text


def make_uniprot_query():
    """
    Make query to select Swiss-Prot id from UniProt table.
    :return: sql query string
    """
    uniprot_table_name = construct_table_name(API_PARAMS,
                                              prefix=BQ_PARAMS['UNIPROT_TABLE'],
                                              release=API_PARAMS['UNIPROT_RELEASE'])
    uniprot_table_id = f"{BQ_PARAMS['DEV_PROJECT']}.{BQ_PARAMS['META_DATASET']}.{uniprot_table_name}"

    return f"""
        SELECT Entry_Name AS uniprot_id
        FROM `{uniprot_table_id}`
    """


def make_refseq_filtered_status_mapping_query(refseq_table_id):
    """
    Create query to filter refseq - uniprot mapping data; where both uniprot reviewed and unreviewed records exist
    for a given RefSeq id, keep only the reviewed record.
    :param refseq_table_id: reference to intermediate refseq->uniprot mapping table
    :return: filter query, used for final mapping table creation
    """
    return f"""
    WITH reviewed AS (
        SELECT *
        FROM `{refseq_table_id}`
        WHERE uniprot_review_status = 'reviewed'
    ), unreviewed AS (
        SELECT *
        FROM `{refseq_table_id}`
        WHERE uniprot_review_status = 'unreviewed'   
            AND refseq_id NOT IN (
                SELECT refseq_id 
                FROM reviewed
            )
    )
    
    SELECT * FROM reviewed
    UNION ALL 
    SELECT * FROM unreviewed
    """


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


def make_paginated_gene_query(offset, limit):
    """
    Make GraphQL query to retrieve gene data from PDC API.
    :param offset: Pagination offset (first record index to return)
    :param limit: Pagination limit (max records to return)
    :return: Paginated gene query string
    """
    return f"""
        {{
          getPaginatedGenes(offset:{offset} limit: {limit} acceptDUA:true) {{ 
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
            pagination {{
                count
                from 
                page 
                total
                pages
                size
            }}
          }}
        }}
    """


def alter_paginated_gene_list(json_obj_list):
    """

    :param json_obj_list:
    :return:
    """
    compare_uniprot_ids = cmp_to_key(sort_swissprot_by_age)
    swissprot_set = {row[0] for row in get_query_results(make_uniprot_query())}
    for gene in json_obj_list:
        authority_records_dict = dict()

        gene_authority = gene.pop('authority')

        if gene_authority and len(gene_authority) > 0:
            authority_records = gene_authority.split('; ')

            for authority_record in authority_records:
                split_authority = authority_record.split(':')

                if len(split_authority) > 2:
                    has_fatal_error(f"Authority should split into <= two elements. Actual: {gene_authority}")
                elif len(split_authority) == 2:
                    gene_id = split_authority[1]
                    auth = split_authority[0]
                    authority_records_dict[auth] = gene_id

            # this is a mouse gene database, exclude
            if "MGI" in authority_records_dict:
                authority_records_dict.pop("MGI")

            if len(authority_records_dict) > 1:
                has_fatal_error(f"Unable to select authority record to include: {authority_records_dict}")
            elif len(authority_records_dict) == 1:
                for auth, gene_id in authority_records_dict.items():
                    gene['authority'] = auth
                    gene['authority_gene_id'] = gene_id
                    break
            else:
                gene['authority'] = None
                gene['authority_gene_id'] = None

        gene['gene_symbol'] = gene.pop('gene_name')
        gene['gene_description'] = gene.pop('description')

        uniprot_accession_str = filter_uniprot_accession_nums(gene['proteins'])
        swissprot_str, swissprot_count = filter_swissprot_accession_nums(gene['proteins'], swissprot_set)
        uniprotkb_id = ""

        # returns oldest swiss-prot id; if none, returns oldest uniprot id
        if swissprot_count == 1:
            uniprotkb_id = swissprot_str
        elif swissprot_count > 1:
            swissprot_list = sorted(swissprot_str.split(';'), key=compare_uniprot_ids)
            uniprotkb_id = swissprot_list.pop(0)
        elif uniprot_accession_str and len(uniprot_accession_str) > 1:
            uniprot_list = sorted(uniprot_accession_str.split(';'), key=compare_uniprot_ids)
            uniprotkb_id = uniprot_list[0]

        uniprotkb_ids = uniprot_accession_str

        gene['uniprotkb_id'] = uniprotkb_id
        gene['uniprotkb_ids'] = uniprotkb_ids


def make_quant_data_matrix_query(pdc_study_id, data_type):
    """
    Create graphQL string for querying the PDC API's quantDataMatrix endpoint.
    :param pdc_study_id: PDC study id for query argument
    :param data_type: Data type for query argument (e.g. log2_ratio)
    :return: GraphQL query string
    """
    return f'{{ quantDataMatrix(pdc_study_id: \"{pdc_study_id}\" data_type: \"{data_type}\" acceptDUA: true) }}'


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
            print(f"Quant API returns non-standard aliquot_run_metadata_id entry: {el}")
        else:
            if split_el[0]:
                aliquot_run_metadata_id = split_el[0]
            if split_el[1]:
                aliquot_submitter_id = split_el[1]

            if not aliquot_submitter_id or not aliquot_run_metadata_id:
                has_fatal_error(f"Unexpected value for aliquot_run_metadata_id:aliquot_submitter_id ({el})")

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
        # kind of a hacky fix, but we'll move to CDA_old before it matters (before there are 9000+ studies)
        if "quant" in filename and "schema" not in filename and API_PARAMS['RELEASE'] in filename \
                and "PDC0" in filename:
            files.add(filename)

    return files


def get_quant_table_name(study, is_final, include_release=True):
    """
    Get quant table name for given study.
    :param study: study metadata dict
    :param is_final: if True, query is requesting published table name; otherwise dev table name
    :param include_release: Include release in table name (boolean)
    :return: if True, return published table name, otherwise return dev table name
    """

    def change_study_name_to_table_name_format(_study_name):
        """
        Convert study name to table name format.
        :param _study_name: PDC study associated with table data
        :return: table name
        """
        _study_name = _study_name.replace(study['analytical_fraction'], "")
        study_name_list = _study_name.split(" ")
        hyphen_split_study_name_list = list()

        for study_name_part in study_name_list:
            if '-' in study_name_part:
                hyphen_study_name_part_list = study_name_part.split('-')

                for name_part in hyphen_study_name_part_list:
                    hyphen_split_study_name_list.append(name_part)
            else:
                hyphen_split_study_name_list.append(study_name_part)

        new_study_name_list = list()

        for name in hyphen_split_study_name_list:

            if not name:
                continue
            if not name.isupper():
                name = name.lower()
            if name:
                new_study_name_list.append(name)

        _study_name = " ".join(new_study_name_list)
        _study_name = make_string_bq_friendly(_study_name)
        return _study_name

    quant_prefix = get_prefix(API_PARAMS, API_PARAMS['QUANT_ENDPOINT'])

    if not is_final:
        quant_suffix = study['analytical_fraction'] + '_' + study['pdc_study_id']
        return construct_table_name(API_PARAMS,
                                    prefix=quant_prefix,
                                    suffix=quant_suffix,
                                    include_release=include_release)
    else:
        study_name = study['study_name']
        study_name = change_study_name_to_table_name_format(study_name)
        analytical_fraction = study['analytical_fraction'].lower()

        # return table name in following format: quant_<analyte>_<study_name>_pdc_<version>
        return "_".join([quant_prefix, analytical_fraction, study_name, API_PARAMS['DATA_SOURCE'],
                         API_PARAMS['RELEASE']])


def make_quant_table_query(raw_table_id, study):
    """
    Create sql query to create proteome quant data matrix table for a given study.
    :param raw_table_id: table id for raw quantDataMatrix output
    :param study: PDC study name
    :return: sql query string
    """
    analytical_fraction = study['analytical_fraction']

    aliquot_run_table_name = construct_table_name(API_PARAMS, prefix=BQ_PARAMS['ALIQUOT_RUN_METADATA_TABLE'])
    aliquot_run_table_id = f"{BQ_PARAMS['DEV_PROJECT']}.{BQ_PARAMS['META_DATASET']}.{aliquot_run_table_name}"
    gene_table_name = construct_table_name(API_PARAMS, prefix=get_prefix(API_PARAMS, API_PARAMS['GENE_ENDPOINT']))
    gene_table_id = f"{BQ_PARAMS['DEV_PROJECT']}.{BQ_PARAMS['META_DATASET']}.{gene_table_name}"

    if analytical_fraction == 'Proteome':
        return f"""
            SELECT aliq.case_id, aliq.sample_id, aliq.aliquot_id, 
                quant.aliquot_submitter_id, quant.aliquot_run_metadata_id, quant.study_name, 
                quant.protein_abundance_log2ratio, gene.gene_id, gene.gene_symbol, gene.NCBI_gene_id, gene.authority, 
                gene.authority_gene_id, gene.gene_description, gene.organism, gene.chromosome, gene.locus, 
                gene.uniprotkb_id, gene.uniprotkb_ids, gene.proteins, gene.assays
            FROM `{raw_table_id}` AS quant
            INNER JOIN `{aliquot_run_table_id}` AS aliq 
                ON quant.aliquot_run_metadata_id = aliq.aliquot_run_metadata_id
            INNER JOIN `{gene_table_id}` AS gene
                ON gene.gene_symbol = quant.gene_symbol
        """
    else:
        site_column_name = API_PARAMS['QUANT_REPLACEMENT_MAP'][analytical_fraction]['site_column_name']
        id_column_name = API_PARAMS['QUANT_REPLACEMENT_MAP'][analytical_fraction]['id_column_name']

        return f"""
            SELECT aliq.case_id, aliq.sample_id, aliq.aliquot_id, 
                q.aliquot_submitter_id, q.aliquot_run_metadata_id, q.study_name, q.protein_abundance_log2ratio, 
                SPLIT(q.gene_symbol, ':')[OFFSET(0)] AS `{id_column_name}`, 
                SPLIT(q.gene_symbol, ':')[OFFSET(1)] AS `{site_column_name}`
            FROM `{raw_table_id}` AS q
            INNER JOIN `{aliquot_run_table_id}` AS aliq 
                ON q.aliquot_run_metadata_id = aliq.aliquot_run_metadata_id
        """


def get_publish_table_ids_refseq(api_params, bq_params, source_table_id, public_dataset):
    """
    Create current and versioned table ids.
    :param api_params: api_params supplied in yaml config
    :param bq_params: bq_params supplied in yaml config
    :param source_table_id: id of source table (located in dev project)
    :param public_dataset: base name of dataset in public project where table should be published
    :return: public current table id, public versioned table id
    """
    split_table_id = source_table_id.split('.')

    current_table_name = split_table_id[-1].replace(api_params['UNIPROT_RELEASE'], 'current')
    curr_table_id = f"{bq_params['PROD_PROJECT']}.{public_dataset}.{current_table_name}"
    vers_table_id = f"{bq_params['PROD_PROJECT']}.{public_dataset}_versioned.{split_table_id[-1]}"

    return curr_table_id, vers_table_id


def main(args):
    start_time = time.time()
    print(f"PDC script started at {time.strftime('%x %X', time.localtime())}")

    steps = None

    try:
        global API_PARAMS, BQ_PARAMS
        API_PARAMS, BQ_PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        has_fatal_error(err, ValueError)

    studies_list = get_pdc_studies_list(API_PARAMS, BQ_PARAMS, include_embargoed=False)

    uniprot_file_name = get_filename(API_PARAMS,
                                     file_extension='tsv',
                                     prefix=BQ_PARAMS['UNIPROT_TABLE'],
                                     release=API_PARAMS['UNIPROT_RELEASE'])

    if 'build_uniprot_tsv' in steps:
        print("Retrieving data from UniProtKB")
        uniprot_fp = get_scratch_fp(BQ_PARAMS, uniprot_file_name)

        uniprot_data = retrieve_uniprot_kb_genes()

        with open(uniprot_fp, 'w') as uniprot_file:
            uniprot_file.write(uniprot_data)

        print("Creating schema for UniProt mapping table")
        create_and_upload_schema_for_tsv(API_PARAMS, BQ_PARAMS, tsv_fp=uniprot_fp,
                                         table_name=BQ_PARAMS['UNIPROT_TABLE'], header_row=0, skip_rows=1,
                                         release=API_PARAMS['UNIPROT_RELEASE'])

        upload_to_bucket(BQ_PARAMS, uniprot_fp, delete_local=True)

    if 'build_uniprot_table' in steps:
        uniprot_table_name = construct_table_name(API_PARAMS,
                                                  prefix=BQ_PARAMS['UNIPROT_TABLE'],
                                                  release=API_PARAMS['UNIPROT_RELEASE'])
        uniprot_table_id = f"{BQ_PARAMS['DEV_PROJECT']}.{BQ_PARAMS['META_DATASET']}.{uniprot_table_name}"

        uniprot_schema = retrieve_bq_schema_object(API_PARAMS, BQ_PARAMS,
                                                   table_name=BQ_PARAMS['UNIPROT_TABLE'],
                                                   release=API_PARAMS['UNIPROT_RELEASE'])
        create_and_load_table_from_tsv(BQ_PARAMS,
                                       tsv_file=uniprot_file_name,
                                       table_id=uniprot_table_id,
                                       num_header_rows=1,
                                       schema=uniprot_schema)

        print("UniProt table built!")

    if 'create_refseq_table' in steps:
        # todo it'd be nice to clean this up some--it works, but we tacked stuff on; could be simplified
        print("Building RefSeq mapping table!")
        refseq_id_list = list()

        uniprot_table_name = construct_table_name(API_PARAMS,
                                                  prefix=BQ_PARAMS['UNIPROT_TABLE'],
                                                  release=API_PARAMS['UNIPROT_RELEASE'])
        uniprot_table_id = f"{BQ_PARAMS['DEV_PROJECT']}.{BQ_PARAMS['META_DATASET']}.{uniprot_table_name}"

        res = get_query_results(f"SELECT * FROM {uniprot_table_id}")

        for row in res:
            uniprot_id = row['Entry_Name']
            status = row['Reviewed']
            gene_symbol = row['Gene_Names_primary']
            # remove additional uniprot accession from mapping string

            ref_seq_str = row['RefSeq']

            if not ref_seq_str:
                continue

            ref_seq_list = ref_seq_str.strip(';').split(';')

            if not ref_seq_list or len(ref_seq_list) == 0:
                continue

            for refseq_id_paired in ref_seq_list:
                if not refseq_id_paired:
                    continue

                # refseq paired with an isoform
                if " [" in refseq_id_paired:
                    ref_seq_paired_split = refseq_id_paired.strip("]").split(" [")

                    if len(ref_seq_paired_split) != 2:
                        has_fatal_error(f"Couldn't parse Swiss-Prot/RefSeq mapping for {refseq_id_paired}")

                    uniprot_id = ref_seq_paired_split[1]
                    refseq_id = ref_seq_paired_split[0]
                else:
                    refseq_id = refseq_id_paired

                if refseq_id:
                    refseq_id_list.append([uniprot_id, status, gene_symbol, refseq_id])

        refseq_file_name = get_filename(API_PARAMS,
                                        file_extension='tsv',
                                        prefix=BQ_PARAMS['REFSEQ_UNIPROT_TABLE'],
                                        release=API_PARAMS['UNIPROT_RELEASE'])

        refseq_fp = get_scratch_fp(BQ_PARAMS, refseq_file_name)
        write_list_to_tsv(refseq_fp, refseq_id_list)
        upload_to_bucket(BQ_PARAMS, scratch_fp=refseq_fp)

        refseq_uniprot_headers = ['uniprot_id', 'uniprot_review_status', 'gene_symbol', 'refseq_id']

        create_and_upload_schema_for_tsv(API_PARAMS, BQ_PARAMS, tsv_fp=refseq_fp,
                                         table_name=BQ_PARAMS['REFSEQ_UNIPROT_TABLE'],
                                         header_list=refseq_uniprot_headers, skip_rows=0,
                                         release=API_PARAMS['UNIPROT_RELEASE'])

        refseq_schema = retrieve_bq_schema_object(API_PARAMS, BQ_PARAMS,
                                                  table_name=BQ_PARAMS['REFSEQ_UNIPROT_TABLE'],
                                                  release=API_PARAMS['UNIPROT_RELEASE'])

        refseq_table_name = construct_table_name(API_PARAMS,
                                                 prefix=BQ_PARAMS['REFSEQ_UNIPROT_TABLE'],
                                                 release=API_PARAMS['UNIPROT_RELEASE'])
        refseq_table_id = f"{BQ_PARAMS['DEV_PROJECT']}.{BQ_PARAMS['META_DATASET']}.{refseq_table_name}"

        create_and_load_table_from_tsv(BQ_PARAMS,
                                       tsv_file=refseq_file_name,
                                       table_id=refseq_table_id,
                                       num_header_rows=0,
                                       schema=refseq_schema)

        final_refseq_table_name = construct_table_name(API_PARAMS,
                                                       prefix=BQ_PARAMS['REFSEQ_UNIPROT_FINAL_TABLE'],
                                                       release=API_PARAMS['UNIPROT_RELEASE'])
        final_refseq_table_id = f"{BQ_PARAMS['DEV_PROJECT']}.{BQ_PARAMS['META_DATASET']}.{final_refseq_table_name}"

        load_table_from_query(BQ_PARAMS,
                              table_id=final_refseq_table_id,
                              query=make_refseq_filtered_status_mapping_query(refseq_table_id))

        schema_tags = {
            "uniprot-version": API_PARAMS['UNIPROT_RELEASE']
        }

        update_table_schema_from_generic_pdc(API_PARAMS, BQ_PARAMS,
                                             table_id=final_refseq_table_id,
                                             schema_tags=schema_tags,
                                             metadata_file=BQ_PARAMS['GENERIC_REFSEQ_TABLE_METADATA_FILE'])

        if exists_bq_table(final_refseq_table_id):
            delete_bq_table(refseq_table_id)

    if 'build_gene_jsonl' in steps:
        gene_record_list = build_obj_from_pdc_api(API_PARAMS,
                                                  endpoint=API_PARAMS['GENE_ENDPOINT'],
                                                  request_function=make_paginated_gene_query,
                                                  alter_json_function=alter_paginated_gene_list)

        create_and_upload_schema_for_json(API_PARAMS, BQ_PARAMS,
                                          record_list=gene_record_list,
                                          table_name=get_prefix(API_PARAMS, API_PARAMS['GENE_ENDPOINT']),
                                          include_release=True)

        write_list_to_jsonl_and_upload(API_PARAMS, BQ_PARAMS,
                                       prefix=get_prefix(API_PARAMS, API_PARAMS['GENE_ENDPOINT']),
                                       record_list=gene_record_list)

    if 'build_gene_table' in steps:
        gene_prefix = get_prefix(API_PARAMS, API_PARAMS['GENE_ENDPOINT'])

        gene_schema = retrieve_bq_schema_object(API_PARAMS, BQ_PARAMS, table_name=gene_prefix)

        gene_table_id = build_table_from_jsonl(API_PARAMS, BQ_PARAMS,
                                               endpoint=API_PARAMS['GENE_ENDPOINT'],
                                               schema=gene_schema)

        update_table_schema_from_generic_pdc(API_PARAMS, BQ_PARAMS,
                                             table_id=gene_table_id,
                                             metadata_file=BQ_PARAMS['GENERIC_GENE_TABLE_METADATA_FILE'])

    if 'build_quant_tsvs' in steps:
        for study_id_dict in studies_list:
            unversioned_quant_table_name = get_quant_table_name(study_id_dict, is_final=False, include_release=False)
            raw_quant_tsv_file = get_quant_table_name(study_id_dict, is_final=False) + '.tsv'
            quant_tsv_path = get_scratch_fp(BQ_PARAMS, raw_quant_tsv_file)

            raw_quant_header = ['aliquot_run_metadata_id',
                                'aliquot_submitter_id',
                                'study_name',
                                'gene_symbol',
                                'protein_abundance_log2ratio']

            lines_written = build_quant_tsv(study_id_dict, 'log2_ratio', quant_tsv_path, raw_quant_header)

            if lines_written > 0:
                create_and_upload_schema_for_tsv(API_PARAMS, BQ_PARAMS, tsv_fp=quant_tsv_path,
                                                 table_name=unversioned_quant_table_name, header_list=raw_quant_header,
                                                 skip_rows=1, row_check_interval=100)

                upload_to_bucket(BQ_PARAMS, quant_tsv_path, delete_local=True)
                print(f"\n{lines_written} lines written for {study_id_dict['study_name']}")
                print(f"{raw_quant_tsv_file} uploaded to Google Cloud bucket!\n")
            else:
                print(f"\n{lines_written} lines written for {study_id_dict['study_name']}; not uploaded.\n")

    if 'build_quant_tables' in steps:
        print("Building quant tables...")
        # used to verify the presence of quant file (meaning there was data for that study in quantDataMatrix)
        quant_blob_files = get_quant_files()
        built_table_counts = {
            "Proteome": 0,
            "Phosphoproteome": 0,
            "Acetylome": 0,
            "Glycoproteome": 0,
            "Ubiquitylome": 0
        }

        for study_id_dict in studies_list:
            quant_table_name = get_quant_table_name(study=study_id_dict, is_final=False)
            raw_quant_tsv_file = f"{quant_table_name}.tsv"

            if raw_quant_tsv_file not in quant_blob_files:
                print(f'Skipping table build for {raw_quant_tsv_file} (file not found in bucket)')
            else:
                print(f"Building table for {raw_quant_tsv_file}")

                raw_quant_table = get_quant_table_name(study=study_id_dict, is_final=False)
                raw_quant_table_id = f"{BQ_PARAMS['DEV_PROJECT']}.{BQ_PARAMS['QUANT_RAW_DATASET']}.{raw_quant_table}"

                unversioned_quant_table_name = get_quant_table_name(study_id_dict,
                                                                    is_final=False,
                                                                    include_release=False)
                raw_quant_schema = retrieve_bq_schema_object(API_PARAMS, BQ_PARAMS,
                                                             table_name=unversioned_quant_table_name)

                create_and_load_table_from_tsv(BQ_PARAMS,
                                               tsv_file=raw_quant_tsv_file,
                                               table_id=raw_quant_table_id,
                                               num_header_rows=1,
                                               schema=raw_quant_schema)
                built_table_counts[study_id_dict['analytical_fraction']] += 1

        print("quantDataMatrix table counts per analytical fraction:")

        for analytical_fraction in built_table_counts.keys():
            print(f" - {analytical_fraction}: {built_table_counts[analytical_fraction]}")

    if 'build_final_quant_tables' in steps:
        print("Building final quant tables!")

        for study in studies_list:
            raw_table_name = get_quant_table_name(study, is_final=False)
            raw_table_id = f"{BQ_PARAMS['DEV_PROJECT']}.{BQ_PARAMS['QUANT_RAW_DATASET']}.{raw_table_name}"

            if exists_bq_table(raw_table_id):
                final_table_name = get_quant_table_name(study, is_final=True)
                final_dev_table_id = f"{BQ_PARAMS['DEV_PROJECT']}.{BQ_PARAMS['QUANT_FINAL_DATASET']}.{final_table_name}"

                final_quant_table_query = make_quant_table_query(raw_table_id, study)

                load_table_from_query(BQ_PARAMS,
                                      table_id=final_dev_table_id,
                                      query=final_quant_table_query)

                program_labels_list = study['program_labels'].split("; ")

                schema_tags = {
                    "project-name": study['program_short_name'],
                    "study-name": study["study_name"],
                    "pdc-study-id": study["pdc_study_id"],
                    "study-name-upper": study['study_friendly_name'].upper(),
                }

                if len(program_labels_list) > 2:
                    has_fatal_error("PDC quant isn't set up to handle >2 program labels yet; needs to be added.")
                elif len(program_labels_list) == 0:
                    has_fatal_error(f"No program label included for {study['project_name']}, add to PDCStudy.yaml")
                elif len(program_labels_list) == 1:
                    schema_tags['program-name-lower'] = study['program_labels'].lower()

                    update_table_schema_from_generic_pdc(API_PARAMS, BQ_PARAMS,
                                                         table_id=final_dev_table_id,
                                                         schema_tags=schema_tags)
                else:
                    schema_tags['program-name-0-lower'] = program_labels_list[0].lower()
                    schema_tags['program-name-1-lower'] = program_labels_list[1].lower()
                    generic_metadata_file = BQ_PARAMS['GENERIC_TABLE_METADATA_FILE_2_PROGRAM']

                    update_table_schema_from_generic_pdc(API_PARAMS, BQ_PARAMS,
                                                         table_id=final_dev_table_id,
                                                         schema_tags=schema_tags,
                                                         metadata_file=generic_metadata_file)

    if 'test_new_version_refseq_mapping_table' in steps:
        refseq_table_name = construct_table_name(API_PARAMS,
                                                 prefix=BQ_PARAMS['REFSEQ_UNIPROT_FINAL_TABLE'],
                                                 release=API_PARAMS['UNIPROT_RELEASE'])
        refseq_table_id = f"{BQ_PARAMS['DEV_PROJECT']}.{BQ_PARAMS['META_DATASET']}.{refseq_table_name}"

        test_table_for_version_changes(API_PARAMS, BQ_PARAMS,
                                       public_dataset=BQ_PARAMS['PUBLIC_META_DATASET'],
                                       source_table_id=refseq_table_id,
                                       get_publish_table_ids=get_publish_table_ids_refseq,
                                       find_most_recent_published_table_id=find_most_recent_published_table_id_uniprot,
                                       id_keys="refseq_id")

    if 'test_new_version_gene_and_quant_tables' in steps:

        gene_table_name = construct_table_name(API_PARAMS, prefix=get_prefix(API_PARAMS, API_PARAMS['GENE_ENDPOINT']))
        gene_table_id = f"{BQ_PARAMS['DEV_PROJECT']}.{BQ_PARAMS['META_DATASET']}.{gene_table_name}"

        test_table_for_version_changes(API_PARAMS, BQ_PARAMS,
                                       public_dataset=BQ_PARAMS['PUBLIC_META_DATASET'],
                                       source_table_id=gene_table_id,
                                       get_publish_table_ids=get_publish_table_ids,
                                       find_most_recent_published_table_id=find_most_recent_published_table_id,
                                       id_keys="gene_id")

        # check for quant table (for each study) and publish if one exists
        for study in studies_list:
            quant_table_name = get_quant_table_name(study, is_final=True)
            quant_table_id = f"{BQ_PARAMS['DEV_PROJECT']}.{BQ_PARAMS['QUANT_FINAL_DATASET']}.{quant_table_name}"

            if exists_bq_table(quant_table_id):
                test_table_for_version_changes(API_PARAMS, BQ_PARAMS,
                                               public_dataset=study['program_short_name'],
                                               source_table_id=quant_table_id,
                                               get_publish_table_ids=get_publish_table_ids,
                                               find_most_recent_published_table_id=find_most_recent_published_table_id,
                                               id_keys="aliquot_id")

    if 'publish_refseq_mapping_table' in steps:
        print("Publishing RefSeq table!")

        refseq_table_name = construct_table_name(API_PARAMS,
                                                 prefix=BQ_PARAMS['REFSEQ_UNIPROT_FINAL_TABLE'],
                                                 release=API_PARAMS['UNIPROT_RELEASE'])
        refseq_table_id = f"{BQ_PARAMS['DEV_PROJECT']}.{BQ_PARAMS['META_DATASET']}.{refseq_table_name}"

        publish_table(API_PARAMS, BQ_PARAMS,
                      public_dataset=BQ_PARAMS['PUBLIC_META_DATASET'],
                      source_table_id=refseq_table_id,
                      get_publish_table_ids=get_publish_table_ids_refseq,
                      find_most_recent_published_table_id=find_most_recent_published_table_id_uniprot,
                      overwrite=True)

    if 'publish_gene_and_quant_tables' in steps:
        # publish gene mapping table
        print("Publishing gene and quant tables!")

        gene_table_name = construct_table_name(API_PARAMS, prefix=get_prefix(API_PARAMS, API_PARAMS['GENE_ENDPOINT']))
        gene_table_id = f"{BQ_PARAMS['DEV_PROJECT']}.{BQ_PARAMS['META_DATASET']}.{gene_table_name}"

        publish_table(API_PARAMS, BQ_PARAMS,
                      public_dataset=BQ_PARAMS['PUBLIC_META_DATASET'],
                      source_table_id=gene_table_id,
                      get_publish_table_ids=get_publish_table_ids,
                      find_most_recent_published_table_id=find_most_recent_published_table_id,
                      overwrite=True)

        # check for quant table (for each study) and publish if one exists
        for study in studies_list:
            quant_table_name = get_quant_table_name(study, is_final=True)
            quant_table_id = f"{BQ_PARAMS['DEV_PROJECT']}.{BQ_PARAMS['QUANT_FINAL_DATASET']}.{quant_table_name}"

            if exists_bq_table(quant_table_id):
                publish_table(API_PARAMS, BQ_PARAMS,
                              public_dataset=study['program_short_name'],
                              source_table_id=quant_table_id,
                              get_publish_table_ids=get_publish_table_ids,
                              find_most_recent_published_table_id=find_most_recent_published_table_id,
                              overwrite=True)

    end = time.time() - start_time
    print(f"Finished program execution in {format_seconds(end)}!\n")


if __name__ == '__main__':
    main(sys.argv)
