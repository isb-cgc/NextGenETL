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
import sys
import time
import requests

from cda_bq_etl.utils import load_config, format_seconds, create_dev_table_id, create_metadata_table_id
from cda_bq_etl.bq_helpers import create_table_from_query, update_table_schema_from_generic
from cda_bq_etl.data_helpers import initialize_logging

PARAMS = dict()
YAML_HEADERS = ('params', 'steps')


def retrieve_uniprot_kb_genes():
    """
    Retrieve Swiss-Prot ids and gene names from UniProtKB REST API.
    :return: REST API response text (tsv)
    """
    query = 'organism_id:9606'
    data_format = 'tsv'
    columns = 'id,reviewed,gene_primary,xref_refseq'

    request_url = f'https://rest.uniprot.org/uniprotkb/search?query={query}&format={data_format}&fields={columns}'

    response = requests.get(request_url)
    return response.text


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

    '''
    # get PDC study list -- occurs outside of steps because used in multiple places
    # studies_list
    return f"""
    SELECT distinct pdc_study_id, submitter_id_name AS study_name, embargo_date, project_submitter_id, 
    analytical_fraction, program_short_name, project_short_name, project_friendly_name, study_friendly_name,
    program_labels
    FROM  `{table_id}`
    """
    '''
    # create uniprot file name based on settings in yaml config
    # uniprot_file_name
    
    ### steps

    if 'build_uniprot_tsv' in steps:
        print("Retrieving data from UniProtKB")
        uniprot_data = retrieve_uniprot_kb_genes()

        uniprot_row_list = uniprot_data.split("\n")
        uniprot_headers = uniprot_row_list.pop(0)

        for uniprot_row in uniprot_row_list:
            uniprot_record = uniprot_row.strip(';').split('\t')
            uniprot_id = uniprot_record[0]
            status = uniprot_record[1]
            gene_names = uniprot_record[2]
            refseq_str = uniprot_record[3]

            print(f"""0: {uniprot_id}\n1: {status}\n2: {gene_names}\n3: {refseq_str}\n""")

        # uniprot_fp = get_scratch_fp(PARAMS, uniprot_file_name)
                
        # with open(uniprot_fp, 'w') as uniprot_file:
        #     uniprot_file.write(uniprot_data)

        # print("Creating schema for UniProt mapping table")
        # create_and_upload_schema_for_tsv(...)

        # upload_to_bucket(PARAMS, uniprot_fp, delete_local=True)

    '''
    # note the step name change    
    if 'create_uniprot_table' in steps:
        # create uniprot_table_name & uniprot_table_id

        # might need to be adjusted        
        uniprot_schema = retrieve_bq_schema_object(PARAMS,
                                                   table_name=PARAMS['UNIPROT_TABLE'],
                                                   release=PARAMS['UNIPROT_RELEASE'])
        # might need to be adjusted/added
        create_and_load_table_from_tsv(PARAMS,
                                       tsv_file=uniprot_file_name,
                                       table_id=uniprot_table_id,
                                       num_header_rows=1,
                                       schema=uniprot_schema)
        print("UniProt table built!")

    if 'create_refseq_table' in steps:
        # todo it'd be nice to clean this up some--it works, but we tacked stuff on; could be simplified
        print("Building RefSeq mapping table!")

        refseq_id_list = list()
        # add the header row here rather than during schema creation? ['uniprot_id', 'uniprot_review_status', 'gene_symbol', 'refseq_id']
        
        # get uniprot table records (columns: Entry_Name, Reviewed, Gene_Names_primary, RefSeq)
        
        # loop through: 
            - set the following variables:
                uniprot_id = row['Entry_Name']
                status = row['Reviewed']
                gene_symbol = row['Gene_Names_primary']
            - split RefSeq string into a list (ref_seq_str.strip(';').split(';'))
            - if ref seq list is empty, continue to next record
            - for item in refseq list:
                - if '[' in item:
                    - create a new list, further splitting the list (refseq_id_paired.strip("]").split(" ["))
                    - length should always be two, else fatal error
                    - new list idx 0 is refseq_id, idx 1 is uniprot_id
                - else, item is refseq_id
                
                - if refseq_id is found, append this list to refseq_id_list: 
                    - [uniprot_id, status, gene_symbol, refseq_id]
        
        # write refseq_id_list to tsv 
        # upload to bucket
        # create schema object
        # create intermediate table from tsv
        
        # create final filtered refseq table using this query: make_refseq_filtered_status_mapping_query(refseq_table_id)
        
        # can this be added to generic schema?
        add to schema_tags = { "uniprot-version": API_PARAMS['UNIPROT_RELEASE'] }
        
        # update table schema 
        # delete intermediate table
        
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
