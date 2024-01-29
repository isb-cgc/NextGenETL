"""
Copyright 2024, Institute for Systems Biology

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
import logging
import os
import sys
import time
import re
import requests

from typing import Any, Union
from functools import cmp_to_key

from requests.adapters import HTTPAdapter, Retry

from cda_bq_etl.gcs_helpers import upload_to_bucket, download_from_bucket
from cda_bq_etl.pdc_helpers import build_obj_from_pdc_api, get_graphql_api_response
from cda_bq_etl.utils import (load_config, format_seconds, create_dev_table_id, create_metadata_table_id,
                              create_quant_table_id, make_string_bq_friendly, get_scratch_fp, construct_table_name)
from cda_bq_etl.bq_helpers import (create_table_from_query, update_table_schema_from_generic,
                                   create_and_upload_schema_for_json, retrieve_bq_schema_object,
                                   create_and_load_table_from_jsonl, exists_bq_table, delete_bq_table,
                                   get_uniprot_schema_tags, query_and_retrieve_result, get_gene_info_schema_tags,
                                   create_and_upload_schema_for_tsv, create_and_load_table_from_tsv)
from cda_bq_etl.data_helpers import initialize_logging, write_list_to_jsonl_and_upload, create_tsv_row

PARAMS = dict()
YAML_HEADERS = ('params', 'steps')


def query_uniprot_kb_and_create_jsonl_list():
    """
    Retrieve UniProt id, review status, primary gene name and RefSeq ID from UniProt REST API.
    Modified from example found at https://www.uniprot.org/help/api_queries. Hat tip :)
    :return: List of records returned by UniProt REST API
    """

    def get_next_link(headers):
        if "Link" in headers:
            match = re_next_link.match(headers["Link"])
            if match:
                return match.group(1)

    def get_batch(batch_url):
        while batch_url:
            response = session.get(batch_url)
            response.raise_for_status()
            total = response.headers["x-total-results"]
            yield response, total
            batch_url = get_next_link(response.headers)

    logger = logging.getLogger("base_script")

    re_next_link = re.compile(r'<(.+)>; rel="next"')
    retries = Retry(total=5, backoff_factor=0.25, status_forcelist=[500, 502, 503, 504])
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retries))

    query = 'organism_id:9606'
    return_format = 'tsv'
    size = '500'
    field_list = ['id', 'reviewed', 'gene_primary', 'xref_refseq']
    fields = "%2C".join(field_list)

    url = f"{PARAMS['UNIPROT_REST_API_URL']}/search?fields={fields}&format={return_format}&query={query}&size={size}"

    refseq_id_jsonl_list = list()

    record_count = 0

    for records, total_records in get_batch(url):
        for uniprot_row in records.text.splitlines()[1:]:
            record_count += 1

            uniprot_record = uniprot_row.split('\t')

            uniprot_id = uniprot_record[0]
            status = uniprot_record[1]
            gene_symbol = uniprot_record[2]
            refseq_str = uniprot_record[3]

            refseq_list = refseq_str.strip(';').split(';')

            if not refseq_list:
                # No refseq info from UniProt for uniprot_id, skipping
                continue

            for refseq_item in refseq_list:
                if not refseq_item:
                    # No refseq info from UniProt for uniprot_id, skipping
                    continue
                elif '[' in refseq_item:
                    # sometimes these items are pairs in the following format: "refseq_id [uniprot_id]"
                    # in this case, we replace the original uniprot_id with the one provided in brackets
                    paired_refseq_id_list = refseq_item.strip("]").split(" [")
                    refseq_id = paired_refseq_id_list[0]
                    uniprot_id = paired_refseq_id_list[1]
                else:
                    refseq_id = refseq_item

                refseq_row_dict = {
                    "uniprot_id": uniprot_id,
                    "uniprot_review_status": status,
                    "gene_symbol": gene_symbol,
                    "refseq_id": refseq_id
                }

                refseq_id_jsonl_list.append(refseq_row_dict)

        if record_count % 5000 == 0:
            logger.info(f'{record_count} / {total_records}')

    return refseq_id_jsonl_list


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


def get_study_list() -> list[dict[str, str]]:
    """
    Retrieve a list of all PDC studies and their metadata.
    :return: list of study dicts
    """

    def make_pdc_study_query() -> str:
        return f"""
            SELECT DISTINCT pdc_study_id, 
                submitter_id_name AS study_name, 
                project_submitter_id, 
                analytical_fraction, 
                program_short_name, 
                project_short_name, 
                project_friendly_name, 
                study_friendly_name, 
                program_labels
            FROM  `{create_metadata_table_id(PARAMS, 'studies')}`
            ORDER BY pdc_study_id
        """

    result = query_and_retrieve_result(make_pdc_study_query())

    studies_list = list()

    for study in result:
        studies_list.append(dict(study))

    return studies_list


def get_gene_record_list() -> list[dict[str, Union[None, str, float, int, bool]]]:
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

    def make_uniprot_query():
        return f"""
             SELECT uniprot_id
             FROM `{create_metadata_table_id(PARAMS, 'refseq_mapping')}`
         """

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

        if (id_length != 6 and id_length != 10) \
                or (not id_str[1].isdigit() or not id_str[5].isdigit()) \
                or (not is_alphanumeric(id_str[3]) or not is_alphanumeric(id_str[4])):
            return False

        if id_length == 10:
            if (is_opq_char(id_str[0]) or not id_str[0].isalpha()) \
                    or (not id_str[2].isalpha() or not id_str[6].isalpha()) \
                    or (not is_alphanumeric(id_str[7]) or not is_alphanumeric(id_str[8])) \
                    or (not id_str[9].isdigit()):
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

    def sort_swissprot_by_age(a: str, b: str) -> int:
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

    def filter_uniprot_accession_nums(proteins_str):
        """
        Filter proteins list, searching for ids in UniProt accession number format.
        :param proteins_str: semicolon delimited string of protein ids
        :return: semicolon delimited string of UniProt accession number ids
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
        :param proteins: semicolon delimited string of protein ids
        :param swissprot_set: semicolon delimited string of Swiss-Prot ids
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

    def alter_paginated_gene_list(json_obj_list):
        logger = logging.getLogger('base_script')
        compare_uniprot_ids = cmp_to_key(sort_swissprot_by_age)
        swissprot_set = {row[0] for row in query_and_retrieve_result(make_uniprot_query())}

        for gene in json_obj_list:
            authority_records_dict = dict()

            gene_authority = gene.pop('authority')

            if gene_authority and len(gene_authority) > 0:
                authority_records = gene_authority.split('; ')

                for authority_record in authority_records:
                    split_authority = authority_record.split(':')

                    if len(split_authority) > 2:
                        logger.critical(f"Authority should split into <= two elements. Actual: {gene_authority}")
                        exit(-1)
                    elif len(split_authority) == 2:
                        gene_id = split_authority[1]
                        auth = split_authority[0]
                        authority_records_dict[auth] = gene_id

                # this is a mouse gene database, exclude
                if "MGI" in authority_records_dict:
                    authority_records_dict.pop("MGI")

                if len(authority_records_dict) > 1:
                    logger.critical(f"Unable to select authority record to include: {authority_records_dict}")
                    exit(-1)
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

    return build_obj_from_pdc_api(params=PARAMS,
                                  endpoint=PARAMS['GENE_ENDPOINT'],
                                  request_function=make_paginated_gene_query,
                                  alter_json_function=alter_paginated_gene_list)


def get_quant_table_name(study: dict[str, str], is_final: bool):
    """
    Get quant table name for given study.
    :param study: study metadata dict
    :param is_final: if True, query is requesting published table name; otherwise dev table name
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

    quant_prefix = PARAMS['ENDPOINT_SETTINGS']['quantDataMatrix']['output_name']

    study_name = study['study_name']
    study_name = change_study_name_to_table_name_format(study_name)
    analytical_fraction = study['analytical_fraction'].lower()

    table_name = "_".join([quant_prefix, analytical_fraction, study_name, 'pdc', PARAMS['RELEASE']])

    if not is_final:
        table_name = table_name + '_raw'

    # return table name in following format: quant_<analyte>_<study_name>_pdc_<version>
    # if not final, append '_raw'
    return table_name


def build_quant_tsv(study_id_dict: dict[str, str], data_type: str, tsv_fp: str, header: list[str]) -> int:
    """
    Output quant data rows in tsv format, for future BQ ingestion.
    :param study_id_dict: dictionary of study ids
    :param data_type: data type of API request, e.g. log2_ratio
    :param tsv_fp: output filepath for tsv file
    :param header: header for quant tsv file
    :return: count of lines written to tsv
    """

    def make_quant_data_matrix_query():
        # Create graphQL string for querying the PDC REST API's quantDataMatrix endpoint.
        pdc_study_id = study_id_dict['pdc_study_id']

        return f'{{ quantDataMatrix(pdc_study_id: \"{pdc_study_id}\" data_type: \"{data_type}\" acceptDUA: true) }}'

    logger = logging.getLogger("base_script")

    lines_written = 0

    res_json = get_graphql_api_response(params=PARAMS,
                                        query=make_quant_data_matrix_query(),
                                        fail_on_error=False)

    quant_endpoint = PARAMS['QUANT_ENDPOINT']

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
                logger.critical(f"Unexpected value for aliquot_run_metadata_id:aliquot_submitter_id ({el})")
                exit(-1)

        aliquot_metadata.append({
            "aliquot_run_metadata_id": aliquot_run_metadata_id,
            "aliquot_submitter_id": aliquot_submitter_id
        })

    study_name = study_id_dict['study_name']

    # iterate over each gene row and add to the correct aliquot_run obj
    with open(tsv_fp, 'w') as fh:
        fh.write(create_tsv_row(header))

        for row in res_json['data'][quant_endpoint]:
            gene_symbol = row.pop(0)

            for i, log2_ratio in enumerate(row):
                tsv_row_list = [aliquot_metadata[i]['aliquot_run_metadata_id'],
                                aliquot_metadata[i]['aliquot_submitter_id'],
                                study_name,
                                gene_symbol,
                                log2_ratio]

                fh.write(create_tsv_row(tsv_row_list))
                lines_written += 1

    return lines_written


def write_file_list(file_list: list[str]) -> str:
    quant_file_list_name = f"{PARAMS['QUANT_FILE_LIST_BASE_NAME']}_{PARAMS['RELEASE']}.txt"
    quant_file_list_path = get_scratch_fp(PARAMS, quant_file_list_name)

    with open(quant_file_list_path, "w") as fh:
        for file_name in file_list:
            fh.write(f"{file_name}\n")

    return quant_file_list_path


def get_quant_file_list(quant_file_list_path: str):
    with open(quant_file_list_path, 'r') as fh:
        file_contents = fh.read()

    return file_contents.strip("\n").split("\n")


def make_quant_table_query(raw_table_id: str, study_id_dict: dict[str, str]) -> str:
    """
    Create sql query to create proteome quant data matrix table for a given study.
    :param raw_table_id: table id for raw quantDataMatrix output
    :param study_id_dict: PDC study dict
    :return: sql query string
    """
    analytical_fraction = study_id_dict['analytical_fraction']

    if analytical_fraction == 'Proteome':
        return f"""
            SELECT sample_case.case_id, 
                sample_case.sample_id, 
                sample_aliq.aliquot_id, 
                quant.aliquot_submitter_id, 
                quant.aliquot_run_metadata_id, 
                quant.study_name, 
                quant.protein_abundance_log2ratio, 
                gene.gene_id, 
                gene.gene_symbol, 
                gene.NCBI_gene_id, 
                gene.authority, 
                gene.authority_gene_id, 
                gene.gene_description, 
                gene.organism, 
                gene.chromosome, 
                gene.locus, 
                gene.uniprotkb_id, 
                gene.uniprotkb_ids, 
                gene.proteins, 
                gene.assays
            FROM `{raw_table_id}` AS quant
            JOIN `{create_dev_table_id(PARAMS, 'aliquot_aliquot_run_metadata_id')}` AS aliq_run
                ON quant.aliquot_run_metadata_id = aliq_run.aliquot_run_metadata_id
            JOIN `{create_dev_table_id(PARAMS, 'sample_aliquot_id')}` sample_aliq
                ON aliq_run.aliquot_id = sample_aliq.aliquot_id
            JOIN `{create_dev_table_id(PARAMS, 'sample_case_id')}` sample_case
                ON sample_aliq.sample_id = sample_case.sample_id
            JOIN `{create_metadata_table_id(PARAMS, 'gene_info')}` gene
                ON quant.gene_symbol = gene.gene_symbol
        """
    else:
        site_column_name = PARAMS['QUANT_REPLACEMENT_MAP'][analytical_fraction]['site_column_name']
        id_column_name = PARAMS['QUANT_REPLACEMENT_MAP'][analytical_fraction]['id_column_name']

        return f"""
            SELECT sample_case.case_id, 
                sample_case.sample_id, 
                sample_aliq.aliquot_id,
                quant.aliquot_submitter_id, 
                quant.aliquot_run_metadata_id, 
                quant.study_name, 
                quant.protein_abundance_log2ratio, 
                SPLIT(quant.gene_symbol, ':')[OFFSET(0)] AS `{id_column_name}`, 
                SPLIT(quant.gene_symbol, ':')[OFFSET(1)] AS `{site_column_name}`
            FROM `{raw_table_id}` AS quant
            JOIN `{create_dev_table_id(PARAMS, 'aliquot_aliquot_run_metadata_id')}` AS aliq_run
                ON quant.aliquot_run_metadata_id = aliq_run.aliquot_run_metadata_id
            JOIN `{create_dev_table_id(PARAMS, 'sample_aliquot_id')}` sample_aliq
                ON aliq_run.aliquot_id = sample_aliq.aliquot_id
            JOIN `{create_dev_table_id(PARAMS, 'sample_case_id')}` sample_case
                ON sample_aliq.sample_id = sample_case.sample_id
        """


def main(args):
    try:
        start_time = time.time()

        global PARAMS
        PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        sys.exit(err)

    log_file_time = time.strftime('%Y.%m.%d-%H.%M.%S', time.localtime())
    log_filepath = f"{PARAMS['LOGFILE_PATH']}.{log_file_time}"
    logger = initialize_logging(log_filepath)

    studies_list = get_study_list()

    if 'build_and_upload_refseq_uniprot_jsonl' in steps:
        logger.info("Retrieving RefSeq records from UniProtKB")
        refseq_jsonl_list = query_uniprot_kb_and_create_jsonl_list()

        write_list_to_jsonl_and_upload(params=PARAMS,
                                       prefix=PARAMS['UNFILTERED_REFSEQ_TABLE_NAME'],
                                       record_list=refseq_jsonl_list,
                                       release=PARAMS['UNIPROT_RELEASE'])

        create_and_upload_schema_for_json(params=PARAMS,
                                          record_list=refseq_jsonl_list,
                                          table_name=PARAMS['UNFILTERED_REFSEQ_TABLE_NAME'],
                                          release=PARAMS['UNIPROT_RELEASE'])

    if 'create_refseq_uniprot_table' in steps:
        logger.info("Building RefSeq -> UniProt mapping table")

        unfiltered_refseq_table_id = create_metadata_table_id(PARAMS, PARAMS['UNFILTERED_REFSEQ_TABLE_NAME'])
        refseq_jsonl_filename = f"{PARAMS['UNFILTERED_REFSEQ_TABLE_NAME']}_{PARAMS['UNIPROT_RELEASE']}.jsonl"

        refseq_table_schema = retrieve_bq_schema_object(params=PARAMS,
                                                        table_name=PARAMS['UNFILTERED_REFSEQ_TABLE_NAME'],
                                                        release=PARAMS['UNIPROT_RELEASE'])
        create_and_load_table_from_jsonl(params=PARAMS,
                                         jsonl_file=refseq_jsonl_filename,
                                         table_id=unfiltered_refseq_table_id,
                                         schema=refseq_table_schema)

        # where both reviewed and unreviewed records exist for a RefSeq id, drop the unreviewed record
        logger.info("Creating filtered RefSeq -> UniProt mapping table")

        filtered_refseq_table_id = create_metadata_table_id(PARAMS, table_name=PARAMS['FILTERED_REFSEQ_TABLE_NAME'])

        create_table_from_query(params=PARAMS,
                                table_id=filtered_refseq_table_id,
                                query=make_refseq_filtered_status_mapping_query(unfiltered_refseq_table_id))

        schema_tags = get_uniprot_schema_tags(PARAMS)

        update_table_schema_from_generic(params=PARAMS,
                                         table_id=filtered_refseq_table_id,
                                         schema_tags=schema_tags,
                                         metadata_file=PARAMS['GENERIC_REFSEQ_TABLE_METADATA_FILE'])

        if exists_bq_table(filtered_refseq_table_id):
            # delete the unfiltered intermediate table
            logger.info("Deleting unfiltered RefSeq -> UniProt mapping table")
            delete_bq_table(unfiltered_refseq_table_id)

    if 'build_gene_jsonl' in steps:
        gene_table_base_name = PARAMS['ENDPOINT_SETTINGS']['getPaginatedGenes']['output_name']

        gene_record_list = get_gene_record_list()

        write_list_to_jsonl_and_upload(params=PARAMS,
                                       prefix=gene_table_base_name,
                                       record_list=gene_record_list)

        create_and_upload_schema_for_json(params=PARAMS,
                                          record_list=gene_record_list,
                                          table_name=gene_table_base_name,
                                          include_release=True)

    if 'build_gene_table' in steps:
        logger.info("Building gene info table!")
        gene_table_base_name = PARAMS['ENDPOINT_SETTINGS']['getPaginatedGenes']['output_name']
        gene_jsonl_filename = f"{gene_table_base_name}_{PARAMS['RELEASE']}.jsonl"

        gene_table_schema = retrieve_bq_schema_object(PARAMS, table_name=gene_table_base_name, include_release=True)

        create_and_load_table_from_jsonl(params=PARAMS,
                                         jsonl_file=gene_jsonl_filename,
                                         table_id=create_metadata_table_id(PARAMS, gene_table_base_name),
                                         schema=gene_table_schema)

        schema_tags = get_gene_info_schema_tags(PARAMS)

        update_table_schema_from_generic(params=PARAMS,
                                         table_id=create_metadata_table_id(PARAMS, gene_table_base_name),
                                         schema_tags=schema_tags,
                                         metadata_file=PARAMS['GENERIC_GENE_TABLE_METADATA_FILE'])

    if 'build_and_upload_quant_tsvs' in steps:
        logger.info("Building and uploading quant tsvs and schemas!")
        quant_file_list = list()

        for study_id_dict in studies_list:
            raw_quant_table_name = get_quant_table_name(study_id_dict, is_final=False)
            raw_quant_tsv_file = f'{raw_quant_table_name}.tsv'
            schema_file = f'schema_{raw_quant_table_name}.json'
            schema_fp = get_scratch_fp(PARAMS, schema_file)

            quant_tsv_path = get_scratch_fp(PARAMS, raw_quant_tsv_file)

            raw_quant_header = ['aliquot_run_metadata_id',
                                'aliquot_submitter_id',
                                'study_name',
                                'gene_symbol',
                                'protein_abundance_log2ratio']

            num_tsv_rows = build_quant_tsv(study_id_dict, 'log2_ratio', quant_tsv_path, raw_quant_header)

            if num_tsv_rows > 0:
                upload_to_bucket(PARAMS, quant_tsv_path, delete_local=False, verbose=False)
                logger.info(f"{num_tsv_rows} lines written for {study_id_dict['study_name']}")
                logger.info(f"{raw_quant_tsv_file} uploaded to Google Cloud bucket!")
                quant_file_list.append(raw_quant_tsv_file)

                create_and_upload_schema_for_tsv(params=PARAMS,
                                                 tsv_fp=quant_tsv_path,
                                                 header_row=0,
                                                 skip_rows=1,
                                                 schema_fp=schema_fp,
                                                 sample_interval=1000)
                os.remove(quant_tsv_path)
            else:
                logger.info(f"{num_tsv_rows} lines written for {study_id_dict['study_name']}; not uploaded.")

        quant_file_list_path = write_file_list(quant_file_list)
        upload_to_bucket(PARAMS, quant_file_list_path, delete_local=True, verbose=False)

    if 'build_raw_quant_tables' in steps:
        logger.info("Building raw quant tables!")

        quant_file_list_name = f"{PARAMS['QUANT_FILE_LIST_BASE_NAME']}_{PARAMS['RELEASE']}.txt"
        download_from_bucket(params=PARAMS, filename=quant_file_list_name)
        quant_file_list_path = get_scratch_fp(PARAMS, quant_file_list_name)
        quant_file_list = get_quant_file_list(quant_file_list_path)

        built_table_counts = {
            "Proteome": 0,
            "Phosphoproteome": 0,
            "Acetylome": 0,
            "Glycoproteome": 0,
            "Ubiquitylome": 0
        }

        for study_id_dict in studies_list:
            raw_quant_table_name = get_quant_table_name(study_id_dict, is_final=False)
            raw_quant_tsv_file = f'{raw_quant_table_name}.tsv'

            if raw_quant_tsv_file not in quant_file_list:
                logger.info(f'Skipping table build for {raw_quant_tsv_file} (empty file, not written to bucket)')
                continue

            logger.info(f"Building table for {raw_quant_tsv_file}")

            raw_quant_table_name = get_quant_table_name(study=study_id_dict, is_final=False)
            raw_quant_table_id = create_quant_table_id(PARAMS, raw_quant_table_name, is_final=False)

            schema_file = f'schema_{raw_quant_table_name}.json'

            raw_quant_schema = retrieve_bq_schema_object(params=PARAMS,
                                                         schema_filename=schema_file)

            create_and_load_table_from_tsv(params=PARAMS,
                                           tsv_file=raw_quant_tsv_file,
                                           table_id=raw_quant_table_id,
                                           num_header_rows=1,
                                           schema=raw_quant_schema)

            built_table_counts[study_id_dict['analytical_fraction']] += 1

        logger.info("quantDataMatrix table counts per analytical fraction:")

        for analytical_fraction in built_table_counts.keys():
            logger.info(f" - {analytical_fraction}: {built_table_counts[analytical_fraction]}")

    if 'build_final_quant_tables' in steps:
        logger.info("Building final quant tables!")

        for study_id_dict in studies_list:
            raw_quant_table_name = get_quant_table_name(study_id_dict, is_final=False)
            raw_table_id = create_quant_table_id(PARAMS, raw_quant_table_name, is_final=False)

            if not exists_bq_table(raw_table_id):
                continue

            final_quant_table_name = get_quant_table_name(study_id_dict, is_final=True)
            final_quant_table_id = create_quant_table_id(PARAMS, final_quant_table_name, is_final=True)

            create_table_from_query(params=PARAMS,
                                    table_id=final_quant_table_id,
                                    query=make_quant_table_query(raw_table_id, study_id_dict))

            program_labels_list = study_id_dict['program_labels'].split("; ")

            schema_tags = {
                "project-name": study_id_dict['program_short_name'],
                "study-name": study_id_dict["study_name"],
                "pdc-study-id": study_id_dict["pdc_study_id"],
                "study-name-upper": study_id_dict['study_friendly_name'].upper()
            }

            if len(program_labels_list) > 2:
                logger.critical("PDC quant isn't set up to handle >2 program labels yet; needs to be added.")
                exit(-1)
            elif len(program_labels_list) == 0:
                logger.critical(f"No program label for {study_id_dict['project_name']}, add to PDCStudy.yaml")
                exit(-1)
            elif len(program_labels_list) == 1:
                schema_tags['program-name-lower'] = study_id_dict['program_labels'].lower()

                update_table_schema_from_generic(params=PARAMS,
                                                 table_id=final_quant_table_id,
                                                 schema_tags=schema_tags,
                                                 metadata_file=PARAMS['GENERIC_TABLE_METADATA_FILE'])
            else:
                schema_tags['program-name-0-lower'] = program_labels_list[0].lower()
                schema_tags['program-name-1-lower'] = program_labels_list[1].lower()

                update_table_schema_from_generic(params=PARAMS,
                                                 table_id=final_quant_table_id,
                                                 schema_tags=schema_tags,
                                                 metadata_file=PARAMS['GENERIC_TABLE_METADATA_FILE_2_PROGRAM'])

    end_time = time.time()

    logger.info(f"Script completed in: {format_seconds(end_time - start_time)}")


if __name__ == "__main__":
    main(sys.argv)
