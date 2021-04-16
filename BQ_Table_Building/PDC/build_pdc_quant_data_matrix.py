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
                              create_and_upload_schema_from_tsv, return_schema_object_for_bq)

from BQ_Table_Building.PDC.pdc_utils import (get_pdc_studies_list, build_table_from_tsv, get_filename, get_dev_table_id,
                                             update_column_metadata, update_pdc_table_metadata, get_prefix)

API_PARAMS = dict()
BQ_PARAMS = dict()
YAML_HEADERS = ('api_params', 'bq_params', 'steps')


def retrieve_uniprot_kb_genes():
    query = 'organism:9606+AND+reviewed:yes'
    data_format = 'tab'
    columns = 'id,genes(PREFERRED),database(GeneID),database(HGNC)'

    request_url = 'https://www.uniprot.org/uniprot/?query={}&format={}&columns={}'.format(query, data_format, columns)

    response = requests.get(request_url)
    return response.text


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

        create_and_upload_schema_from_tsv(API_PARAMS, BQ_PARAMS,
                                          table_name=swissprot_table_name,
                                          tsv_fp=swissprot_fp,
                                          header_row=0,
                                          skip_rows=1,
                                          release=API_PARAMS['SWISSPROT_RELEASE'])

        upload_to_bucket(BQ_PARAMS, swissprot_fp)

    if 'build_swissprot_table' in steps:
        print("\nBuilding {0}... ".format(swissprot_table_id))

        swissprot_schema = return_schema_object_for_bq(API_PARAMS, BQ_PARAMS,
                                                       table_type=BQ_PARAMS['SWISSPROT_TABLE'],
                                                       release=API_PARAMS['SWISSPROT_RELEASE'])

        build_table_from_tsv(API_PARAMS, BQ_PARAMS,
                             tsv_file=swissprot_file_name,
                             table_id=swissprot_table_id,
                             num_header_rows=0,
                             schema=swissprot_schema)
        print("SwissProt table built!")


if __name__ == '__main__':
    main(sys.argv)
