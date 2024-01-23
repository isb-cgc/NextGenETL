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
import sys
import time
import re
from typing import Any

import requests

from requests.adapters import HTTPAdapter, Retry

from cda_bq_etl.utils import load_config, format_seconds, create_dev_table_id, create_metadata_table_id
from cda_bq_etl.bq_helpers import (create_table_from_query, update_table_schema_from_generic,
                                   create_and_upload_schema_for_json, retrieve_bq_schema_object,
                                   create_and_load_table_from_jsonl, exists_bq_table, delete_bq_table)
from cda_bq_etl.data_helpers import initialize_logging, write_list_to_jsonl_and_upload

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

    url = f'https://rest.uniprot.org/uniprotkb/search?fields={fields}&format={return_format}&query={query}&size={size}'

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
                logger.info(f"No refseq info from UniProt for {uniprot_id}, skipping")
                continue

            for refseq_item in refseq_list:
                if not refseq_item:
                    logger.info(f"No refseq info from UniProt for {uniprot_id}, skipping")
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


def get_study_list() -> list[str, Any]:
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
        """

    # todo finish this
    pass


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

        refseq_table_schema = retrieve_bq_schema_object(PARAMS,
                                                        table_name=PARAMS['UNFILTERED_REFSEQ_TABLE_NAME'],
                                                        release=PARAMS['UNIPROT_RELEASE'])
        create_and_load_table_from_jsonl(PARAMS,
                                         jsonl_file=refseq_jsonl_filename,
                                         table_id=unfiltered_refseq_table_id,
                                         schema=refseq_table_schema)

        # where both reviewed and unreviewed records exist for a RefSeq id, drop the unreviewed record
        logger.info("Creating filtered RefSeq -> UniProt mapping table")

        filtered_refseq_table_id = create_metadata_table_id(PARAMS, table_name=PARAMS['FILTERED_REFSEQ_TABLE_NAME'])

        create_table_from_query(PARAMS,
                                table_id=filtered_refseq_table_id,
                                query=make_refseq_filtered_status_mapping_query(filtered_refseq_table_id))

        schema_tags = {"uniprot-version": PARAMS['UNIPROT_RELEASE']}

        update_table_schema_from_generic(PARAMS,
                                         table_id=filtered_refseq_table_id,
                                         schema_tags=schema_tags)

        if exists_bq_table(filtered_refseq_table_id):
            # delete the unfiltered intermediate table
            '''
            logger.info("Deleting unfiltered RefSeq -> UniProt mapping table")
            # delete_bq_table(unfiltered_refseq_table_id)
            '''
            # todo delete unfiltered intermediate table
            pass

    '''
    if 'build_gene_jsonl' in steps:
        gene_record_list = build_obj_from_pdc_api(PARAMS,
                                                  endpoint=PARAMS['GENE_ENDPOINT'],
                                                  request_function=make_paginated_gene_query,
                                                  alter_json_function=alter_paginated_gene_list)

        create_and_upload_schema_for_json(PARAMS,
                                          record_list=gene_record_list,
                                          table_name=get_prefix(PARAMS, PARAMS['GENE_ENDPOINT']),
                                          include_release=True)

        write_list_to_jsonl_and_upload(PARAMS,
                                       prefix=get_prefix(PARAMS, PARAMS['GENE_ENDPOINT']),
                                       record_list=gene_record_list)
    
    
    '''

    end_time = time.time()

    logger.info(f"Script completed in: {format_seconds(end_time - start_time)}")


if __name__ == "__main__":
    main(sys.argv)
