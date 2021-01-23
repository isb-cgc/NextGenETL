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

from functools import cmp_to_key

from common_etl.utils import *

API_PARAMS = dict()
BQ_PARAMS = dict()
YAML_HEADERS = ('api_params', 'bq_params', 'steps')


# ***** FUNCTIONS USED BY MULTIPLE PROCESSES

def make_all_studies_query():
    table_name = get_table_name(BQ_PARAMS['STUDIES_TABLE'])
    table_id = get_dev_table_id(table_name, is_metadata=True)

    return """
    SELECT study_id, study_submitter_id, pdc_study_id, study_name, project_submitter_id,
    aliquots_count, cases_count, embargo_date, analytical_fraction
    FROM  `{}`
    """.format(table_id)


def has_table(project, dataset, table_name):
    query = """
    SELECT COUNT(1) AS has_table
    FROM `{}.{}.__TABLES_SUMMARY__`
    WHERE table_id = '{}'
    """.format(project, dataset, table_name)

    res = get_query_results(query)

    for row in res:
        has_table_res = row['has_table']
        return bool(has_table_res)


def has_quant_table(study_submitter_id):
    table_name = get_table_name(BQ_PARAMS['QUANT_DATA_TABLE'], study_submitter_id)
    return has_table(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['DEV_DATASET'], table_name)


def is_currently_embargoed(embargo_date):
    if embargo_date:
        split_embargo_date = embargo_date.split('-')
        current_date = time.strftime("%Y-%m-%d", time.localtime())
        split_current_date = current_date.split('-')

        if split_embargo_date[0] > split_current_date[0]:  # year YYYY
            return True
        elif split_embargo_date[0] == split_current_date[0]:
            if split_embargo_date[1] > split_current_date[1]:  # month MM
                return True
            elif split_embargo_date[1] == split_current_date[1]:
                if split_embargo_date[2] >= split_current_date[2]:  # day DD
                    return True
    return False


def get_table_name(prefix, suffix=None, include_release=True, release=None):
    table_name = prefix

    if suffix:
        table_name += '_' + suffix

    if include_release and not release:
        table_name += '_' + BQ_PARAMS['RELEASE']
    elif release:
        table_name += '_' + release

    return re.sub('[^0-9a-zA-Z_]+', '_', table_name)


def get_file_name(file_extension, prefix, suffix=None, include_release=True, release=None):
    filename = get_table_name(prefix, suffix, include_release, release)
    return "{}.{}".format(filename, file_extension)


def get_table_id(project, dataset, table_name):
    return "{}.{}.{}".format(project, dataset, table_name)


def get_dev_table_id(table_name, is_metadata=False):
    dataset = BQ_PARAMS['DEV_META_DATASET'] if is_metadata else BQ_PARAMS['DEV_DATASET']
    return get_table_id(BQ_PARAMS['DEV_PROJECT'], dataset, table_name)


def build_table_from_jsonl(project, dataset, table_prefix, table_suffix=None):
    print("Building {} table!".format(table_prefix))

    build_start = time.time()

    table_name = get_table_name(table_prefix, table_suffix)
    table_id = get_table_id(project, dataset, table_name)
    console_out("Building {0}... ", (table_id,))

    schema_filename = '{}/{}/{}.json'.format(project, dataset, table_name)
    schema, metadata = from_schema_file_to_obj(BQ_PARAMS, schema_filename)

    jsonl_name = '{}.jsonl'.format(table_name)
    create_and_load_table(BQ_PARAMS, jsonl_name, schema, table_id)

    build_end = time.time() - build_start
    console_out("Table built in {0}!\n", (format_seconds(build_end),))


def build_table_from_tsv(project, dataset, table_prefix, table_suffix=None, backup_table_suffix=None):
    build_start = time.time()

    table_name = get_table_name(table_prefix, table_suffix)
    table_id = get_table_id(project, dataset, table_name)
    schema_filename = '{}/{}/{}.json'.format(project, dataset, table_name)
    schema, metadata = from_schema_file_to_obj(BQ_PARAMS, schema_filename)

    if not schema and not metadata and backup_table_suffix:
        console_out("No schema file found for {}, trying backup ({})", (table_suffix, backup_table_suffix))
        table_name = get_table_name(table_prefix, backup_table_suffix)
        table_id = get_table_id(project, dataset, table_name)
        schema_filename = '{}/{}/{}.json'.format(project, dataset, table_name)
        schema, metadata = from_schema_file_to_obj(BQ_PARAMS, schema_filename)

    # still no schema? return
    if not schema:
        console_out("No schema file found for {}, skipping table.", (table_id,))
        return

    console_out("\nBuilding {0}... ", (table_id,))
    tsv_name = get_file_name('tsv', table_prefix, table_suffix)
    create_and_load_tsv_table(BQ_PARAMS, tsv_name, schema, table_id, BQ_PARAMS['NULL_MARKER'])

    build_end = time.time() - build_start
    console_out("Table built in {0}!\n", (format_seconds(build_end),))


def delete_from_steps(step, steps):
    delete_idx = steps.index(step)
    steps.pop(delete_idx)


def build_jsonl_from_pdc_api(endpoint, request_function, ids_list=None, request_parameters=tuple()):
    """
    usage:
    build_jsonl_from_pdc_api("endpoint_name", request_function(), ids_list)
    build_jsonl_from_pdc_api("endpoint_name", request_function())
    """
    joined_record_list = list()

    print("\nAppending data from {} endpoint.".format(endpoint))

    if ids_list:
        for idx, id_entry in enumerate(ids_list):
            combined_request_parameters = request_parameters + (id_entry,)
            joined_record_list += request_data_from_pdc_api(endpoint, request_function, combined_request_parameters)
            if len(ids_list) < 100:
                print("Added data for {}. Total record count: {}".format(id_entry, len(joined_record_list)))
            elif len(joined_record_list) % 1000 == 0 and len(joined_record_list) != 0:
                print("{} records appended.".format(len(joined_record_list)))
    else:
        joined_record_list = request_data_from_pdc_api(endpoint, request_function, request_parameters)
        print("Collected {} records.".format(len(joined_record_list)))

    jsonl_filename = get_file_name('jsonl', API_PARAMS['ENDPOINT_SETTINGS'][endpoint]['output_name'])
    local_filepath = get_scratch_fp(BQ_PARAMS, jsonl_filename)
    write_list_to_jsonl(local_filepath, joined_record_list)

    upload_to_bucket(BQ_PARAMS, local_filepath, delete_local=True)


def request_data_from_pdc_api(endpoint, request_body_function, request_parameters=None):
    is_paginated = API_PARAMS['ENDPOINT_SETTINGS'][endpoint]['is_paginated']
    payload_key = API_PARAMS['ENDPOINT_SETTINGS'][endpoint]['payload_key']

    def append_api_response_data():
        api_response = get_graphql_api_response(API_PARAMS, graphql_request_body)

        response_body = api_response['data'] if not is_paginated else api_response['data'][endpoint]

        for record in response_body[payload_key]:
            record_list.append(record)

        return response_body['pagination']['pages'] if 'pagination' in response_body else None

    record_list = list()

    if not is_paginated:
        if request_parameters:
            # * operator unpacks tuple for use as positional function args
            graphql_request_body = request_body_function(*request_parameters)
        else:
            graphql_request_body = request_body_function()

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

        if not total_pages:
            has_fatal_error("API did not return a value for total pages, but is_paginated set to True.")

        while page < total_pages:
            offset += limit
            page += 1

            paginated_request_params = request_parameters + (offset, limit)
            graphql_request_body = request_body_function(*paginated_request_params)
            new_total_pages = append_api_response_data()

            if new_total_pages != total_pages:
                has_fatal_error("Page count change mid-ingestion (from {} to {})".format(total_pages, new_total_pages))

    return record_list


# ***** STUDY TABLE CREATION FUNCTIONS

def make_all_programs_query():
    return """{allPrograms{
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
                    acquisition_type
                } 
            }
        }}"""


def make_study_query(study_id):
    return """{{ study 
    (study_id: \"{}\") {{ 
        study_submitter_id 
        study_name 
        disease_type 
        primary_site 
        analytical_fraction 
        experiment_type 
        cases_count 
        aliquots_count
        embargo_date 
    }} }}
    """.format(study_id)


def create_studies_dict(json_res):
    studies = []

    for program in json_res['data']['allPrograms']:
        program_id = program['program_id']
        program_submitter_id = program['program_submitter_id']
        program_name = program['name']
        program_start_date = program['start_date']
        program_end_date = program['end_date']
        program_manager = program['program_manager']

        for project in program['projects']:
            project_id = project['project_id']
            project_submitter_id = project['project_submitter_id']
            project_name = project['name']

            for study in project['studies']:
                study_dict = study.copy()
                json_res = get_graphql_api_response(API_PARAMS, make_study_query(study_dict['study_id']))
                study_metadata = json_res['data']['study'][0]

                for k, v in study_metadata.items():
                    study_dict[k] = v

                study_dict['program_id'] = program_id
                study_dict['program_submitter_id'] = program_submitter_id
                study_dict['program_name'] = program_name
                study_dict['program_start_date'] = program_start_date
                study_dict['program_end_date'] = program_end_date
                study_dict['program_manager'] = program_manager
                study_dict['project_id'] = project_id
                study_dict['project_submitter_id'] = project_submitter_id
                study_dict['project_name'] = project_name

                for k, v in study_dict.items():
                    if not v:
                        study_dict[k] = None

                studies.append(study_dict)

    return studies


# ***** BIOSPECIMEN TABLE CREATION FUNCTIONS

def make_biospecimen_per_study_query(study_id):
    return '''
    {{ biospecimenPerStudy( study_id: \"{}\") {{
        aliquot_id sample_id case_id aliquot_submitter_id sample_submitter_id case_submitter_id 
        aliquot_status case_status sample_status project_name sample_type disease_type primary_site pool taxon
    }}
    }}'''.format(study_id)


def make_unique_biospecimen_query(dup_table_id):
    return """
            SELECT DISTINCT * 
            FROM `{}`
            """.format(dup_table_id)


def make_biospec_query(bio_table_id, csa_table_id):
    return """
    SELECT a.case_id, a.study_id, a.sample_id, a.aliquot_id, b.aliquot_run_metadata_id
        FROM `{}` AS a
        LEFT JOIN `{}` AS b
        ON a.aliquot_id = b.aliquot_id
        AND a.sample_id = b.sample_id
        AND a.case_id = b.case_id
        GROUP BY a.case_id, a.study_id, a.sample_id, a.aliquot_id, b.aliquot_run_metadata_id
    """.format(bio_table_id, csa_table_id)


def make_biospec_count_query(biospec_table_id, csa_table_id):
    return """
        SELECT bio_study_count, bio_case_count, bio_sample_count, bio_aliquot_count, csa_aliquot_run_count 
        FROM ( 
          SELECT COUNT(DISTINCT aliquot_run_metadata_id) AS csa_aliquot_run_count
          FROM `{}`) 
        AS csa, 
        ( 
          SELECT COUNT(DISTINCT case_id) AS bio_case_count,
                 COUNT(DISTINCT study_id) AS bio_study_count,
                 COUNT(DISTINCT sample_id) AS bio_sample_count,
                 COUNT(DISTINCT aliquot_id) AS bio_aliquot_count
          FROM `{}`) 
        AS bio
    """.format(csa_table_id, biospec_table_id)


def build_biospecimen_tsv(study_ids_list, biospecimen_tsv):
    console_out("Building biospecimen tsv!")

    print("{} studies total".format(len(study_ids_list)))

    with open(biospecimen_tsv, 'w') as bio_fh:
        bio_fh.write(create_tsv_row(['aliquot_id',
                                     'sample_id',
                                     'case_id',
                                     'study_id',
                                     'aliquot_submitter_id',
                                     'sample_submitter_id',
                                     'case_submitter_id',
                                     'aliquot_status',
                                     'case_status',
                                     'sample_status',
                                     'project_name',
                                     'sample_type',
                                     'disease_type',
                                     'primary_site',
                                     'pool',
                                     'taxon'],
                                    null_marker=BQ_PARAMS['NULL_MARKER']))

        for study in study_ids_list:
            json_res = get_graphql_api_response(API_PARAMS, make_biospecimen_per_study_query(study['study_id']))

            aliquots_cnt = study['aliquots_count']
            res_size = len(json_res['data']['biospecimenPerStudy'])

            has_quant_tbl = has_quant_table(study['study_submitter_id'])

            console_out("study_id: {}, study_submitter_id: {}, has_quant_table: {}, "
                        "aliquots_count: {}, api result size: {}",
                        (study['study_id'], study['study_submitter_id'], has_quant_tbl, aliquots_cnt, res_size))

            for biospecimen in json_res['data']['biospecimenPerStudy']:
                # create_tsv_row([], BQ_PARAMS['NULL_MARKER'])
                bio_fh.write(create_tsv_row([biospecimen['aliquot_id'],
                                             biospecimen['sample_id'],
                                             biospecimen['case_id'],
                                             study['study_id'],
                                             biospecimen['aliquot_submitter_id'],
                                             biospecimen['sample_submitter_id'],
                                             biospecimen['case_submitter_id'],
                                             biospecimen['aliquot_status'],
                                             biospecimen['case_status'],
                                             biospecimen['sample_status'],
                                             biospecimen['project_name'],
                                             biospecimen['sample_type'],
                                             biospecimen['disease_type'],
                                             biospecimen['primary_site'],
                                             biospecimen['pool'],
                                             biospecimen['taxon']],
                                            null_marker=BQ_PARAMS['NULL_MARKER']))


# ***** GENE TABLE CREATION FUNCTIONS

def make_gene_symbols_per_study_query(pdc_study_id):
    # todo make function to build these names
    table_name = get_table_name(BQ_PARAMS['QUANT_DATA_TABLE'], pdc_study_id, BQ_PARAMS['RELEASE'])
    table_id = get_dev_table_id(table_name)

    return """
        SELECT DISTINCT(gene_symbol)
        FROM `{}`
    """.format(table_id)


def make_gene_query(gene_name):
    return '''
    {{ 
        geneSpectralCount(gene_name: \"{}\") {{
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
    table_name = get_table_name(BQ_PARAMS['SWISSPROT_TABLE'], release=BQ_PARAMS['UNIPROT_RELEASE'])
    swissprot_table_id = get_dev_table_id(table_name, is_metadata=True)
    return """
    SELECT swissprot_id 
    FROM `{}`
    """.format(swissprot_table_id)


def add_gene_symbols_per_study(pdc_study_id, gene_symbol_set):
    table_name = get_table_name(BQ_PARAMS['QUANT_DATA_TABLE'], pdc_study_id, BQ_PARAMS['RELEASE'])

    if has_table(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['DEV_DATASET'], table_name):
        results = get_query_results(make_gene_symbols_per_study_query(pdc_study_id))

        for row in results:
            gene_symbol_set.add(row['gene_symbol'])


def build_gene_symbol_list(studies_list):
    console_out("Building gene symbol tsv!")
    gene_symbol_set = set()

    for study in studies_list:
        table_name = get_table_name(BQ_PARAMS['QUANT_DATA_TABLE'], study['pdc_study_id'], BQ_PARAMS['RELEASE'])

        if has_table(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['DEV_DATASET'], table_name):
            add_gene_symbols_per_study(study['pdc_study_id'], gene_symbol_set)
            console_out("- Added {}, current count: {}", (study['pdc_study_id'], len(gene_symbol_set)))
        else:
            console_out("- No table for {}, skipping.", (study['pdc_study_id'],))

    gene_symbol_list = list(sorted(gene_symbol_set))
    return gene_symbol_list


def build_gene_tsv(gene_symbol_list, gene_tsv, append=False):
    compare_uniprot_ids = cmp_to_key(sort_uniprot_by_age)
    swissprot_set = {row[0] for row in get_query_results(make_swissprot_query())}

    gene_symbol_set = set(gene_symbol_list)
    gene_tsv_exists = os.path.exists(gene_tsv)

    if append:
        console_out("Resuming geneSpectralCount API calls... ", end='')

        if gene_tsv_exists:
            with open(gene_tsv, 'r') as tsv_file:
                gene_reader = csv.reader(tsv_file, delimiter='\t')
                next(gene_reader)
                saved_genes = {row[1] for row in gene_reader}

            gene_symbol_set = gene_symbol_set - saved_genes

        remaining_genes = len(gene_symbol_set)
        call_count_str = "{} gene API calls remaining".format(remaining_genes)
        call_count_str += "--skipping step." if not remaining_genes else "."
        console_out("{}", (call_count_str,))

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
                console_out("No geneSpectralCount data found for {0}", (gene_symbol,))
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
                console_out("Added {0} genes", (count,))


# API_PARAMS['UNIPROT_MAPPING_FILE'], API_PARAMS['UNIPROT_MAPPING_FP']
# API_PARAMS['SWISSPROT_OUTPUT_FILE'], API_PARAMS['SWISSPROT_FTP_FP']
def download_from_uniprot_ftp(local_file, server_fp, type_str):
    console_out("Creating {} tsv... ", (type_str,), end="")

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

    console_out(" done!")


def is_uniprot_accession_number(id_str):
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


# ***** CASES SAMPLES ALIQUOTS TABLE CREATION FUNCTIONS

def make_cases_aliquots_query(offset, limit):
    return '''{{ 
        paginatedCasesSamplesAliquots(offset:{0} limit:{1}) {{ 
            total casesSamplesAliquots {{
                case_id 
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


def build_cases_aliquots_jsonl(csa_jsonl_fp):
    offset = 0
    limit = API_PARAMS['PAGINATED_LIMIT']
    page = 1

    cases_aliquots = list()

    csa_res = get_graphql_api_response(API_PARAMS, make_cases_aliquots_query(offset, limit))

    total_pages = csa_res['data']['paginatedCasesSamplesAliquots']['pagination']['pages']

    print("Retrieved api response for page {} of {}.\n{}".format(
        page, total_pages, csa_res['data']['paginatedCasesSamplesAliquots']['pagination']))

    for case in csa_res['data']['paginatedCasesSamplesAliquots']['casesSamplesAliquots']:
        cases_aliquots.append(case)

    while page <= total_pages:
        page += 1
        offset = offset + limit

        csa_res = get_graphql_api_response(API_PARAMS, make_cases_aliquots_query(offset, limit))

        print("Retrieved api response for page {} of {}.\n{}".format(
            page, total_pages, csa_res['data']['paginatedCasesSamplesAliquots']['pagination']))

        for case in csa_res['data']['paginatedCasesSamplesAliquots']['casesSamplesAliquots']:
            cases_aliquots.append(case)

        print("Appended data to dict! New size: {}".format(len(cases_aliquots)))

    write_list_to_jsonl(csa_jsonl_fp, cases_aliquots)


# ***** QUANT DATA MATRIX TABLE CREATION FUNCTIONS

def make_quant_data_matrix_query(study_submitter_id, data_type):
    return '{{ quantDataMatrix(study_submitter_id: \"{}\" data_type: \"{}\") }}'.format(study_submitter_id, data_type)


def make_proteome_quant_table_query(study):
    quant_table_name = "{}_{}_{}".format(BQ_PARAMS['QUANT_DATA_TABLE'], study, BQ_PARAMS['RELEASE'])
    quant_table_id = get_dev_table_id(quant_table_name)

    case_aliquot_table_name = '{}_{}'.format(BQ_PARAMS['CASE_ALIQUOT_TABLE'], BQ_PARAMS['RELEASE'])
    case_aliquot_table_id = get_dev_table_id(case_aliquot_table_name, is_metadata=True)

    gene_table_name = '{}_{}'.format(BQ_PARAMS['GENE_TABLE'], BQ_PARAMS['RELEASE'])
    gene_table_id = get_dev_table_id(gene_table_name, is_metadata=True)

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
            console_out("Quant API returns non-standard aliquot_run_metadata_id entry: {}", (el,))
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
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(BQ_PARAMS['WORKING_BUCKET'], prefix=BQ_PARAMS['WORKING_BUCKET_DIR'])
    files = set()

    for blob in blobs:
        filename = blob.name.split('/')[-1]
        files.add(filename)

    return files


def get_proteome_studies(studies_list):
    proteome_studies_list = list()

    for study in studies_list:
        if study['analytical_fraction'] == "Proteome":
            proteome_studies_list.append(study)

    return proteome_studies_list


def get_quant_table_name(study, is_final=True):
    analytical_fraction = study['analytical_fraction']

    if not is_final:
        return get_table_name(BQ_PARAMS['QUANT_DATA_TABLE'], study['pdc_study_id'], BQ_PARAMS['RELEASE'])
    else:
        study_name = study['study_name']
        study_name = study_name.replace(analytical_fraction, "")
        study_name = change_study_name_to_table_name_format(study_name)

        return "_".join([BQ_PARAMS['QUANT_DATA_TABLE'],
                         analytical_fraction.lower(),
                         study_name,
                         BQ_PARAMS['DATA_SOURCE'],
                         BQ_PARAMS['RELEASE']])


def change_study_name_to_table_name_format(study_name):
    study_name = study_name.replace("-", " ")
    study_name = study_name.replace("   ", " ")
    study_name = study_name.replace("  ", " ")

    study_name_list = study_name.split(" ")
    new_study_name_list = list()

    for name in study_name_list:
        if not name.isupper():
            name = name.lower()

        if name:
            new_study_name_list.append(name)

    return "_".join(new_study_name_list)


# ***** FILE METADATA TABLE CREATION FUNCTIONS

def make_files_per_study_query(study_id):
    return """
    {{ filesPerStudy (pdc_study_id: \"{}\") {{
            study_id 
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
    return """
    SELECT file_id
    FROM `{}`
    ORDER BY file_id
    """.format(table_id)


def make_file_metadata_query(file_id):
    return """
    {{ fileMetadata(file_id: \"{}\") {{
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
    table_name = BQ_PARAMS['FILE_PDC_METADATA_TABLE'] + '_' + BQ_PARAMS['RELEASE']
    table_id = get_dev_table_id(table_name, is_metadata=True)

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
    file_metadata_table_name = BQ_PARAMS['FILE_PDC_METADATA_TABLE'] + '_' + BQ_PARAMS['RELEASE']
    file_metadata_table_id = get_dev_table_id(file_metadata_table_name, is_metadata=True)
    file_per_study_table_name = BQ_PARAMS['FILES_PER_STUDY_TABLE'] + '_' + BQ_PARAMS['RELEASE']
    file_per_study_table_id = get_dev_table_id(file_per_study_table_name, is_metadata=True)

    return """
    SELECT fps.file_id, fps.file_submitter_id, 
        fps.study_id, fps.pdc_study_id, fps.study_name, fps.study_submitter_id, 
        fpm.study_run_metadata_id, fpm.study_run_metadata_submitter_id,
        fps.file_format, fps.file_type, fps.data_category, fps.file_size, 
        fpm.fraction_number, fpm.experiment_type, fpm.plex_or_dataset_name, fpm.analyte, fpm.instrument, 
        fps.md5sum, fps.url, "open" AS `access`
    FROM `{}` AS fps
    FULL JOIN `{}` AS fpm
        ON fpm.file_id = fps.file_id
    GROUP BY fps.file_id, fps.file_submitter_id, 
        fps.study_id, fps.pdc_study_id, fps.study_name, fps.study_submitter_id, 
        fpm.study_run_metadata_id, fpm.study_run_metadata_submitter_id,
        fps.file_format, fps.file_type, fps.data_category, fps.file_size, 
        fpm.fraction_number, fpm.experiment_type, fpm.plex_or_dataset_name, fpm.analyte, fpm.instrument, 
        fps.md5sum, fps.url
    """.format(file_per_study_table_id, file_metadata_table_id)


def build_per_study_file_jsonl(study_ids_list):
    jsonl_start = time.time()
    file_list = []

    for study in study_ids_list:
        study_id = study['pdc_study_id']
        print("Retrieving for {}: {}".format(study_id, study['study_submitter_id']))
        files_res = get_graphql_api_response(API_PARAMS, make_files_per_study_query(study_id), fail_on_error=False)

        if 'errors' in files_res:
            error_message = files_res['errors'][0]['message']
            print("**{} not included due to error: {}".format(study_id, error_message))
        if 'data' in files_res:
            study_file_count = 0

            for file_row in files_res['data']['filesPerStudy']:
                if 'signedUrl' in file_row and 'url' in file_row['signedUrl']:
                    url = file_row['signedUrl']['url']
                    file_row['url'] = url.split("?")[0]
                else:
                    file_row['url'] = None

                file_row.pop('signedUrl', None)

                study_file_count += 1
                file_list.append(file_row)

            print("{} files retrieved.\n".format(study_file_count))
        else:
            print("No data returned by per-study file query for {}\n".format(study_id))

    files_per_study_jsonl_file = get_file_name('jsonl', BQ_PARAMS['FILES_PER_STUDY_TABLE'])
    files_per_study_jsonl_path = get_scratch_fp(BQ_PARAMS, files_per_study_jsonl_file)

    write_list_to_jsonl(files_per_study_jsonl_path, file_list)
    upload_to_bucket(BQ_PARAMS, files_per_study_jsonl_path)

    jsonl_end = time.time() - jsonl_start

    console_out("Per-study file metadata jsonl file created in {0}!\n", (format_seconds(jsonl_end),))


def get_file_ids():
    table_name = get_table_name(BQ_PARAMS['FILES_PER_STUDY_TABLE'])
    table_id = get_dev_table_id(table_name, is_metadata=True)
    return get_query_results(make_file_id_query(table_id))  # todo fix back


def build_file_pdc_metadata_jsonl(file_ids):
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

    file_metadata_jsonl_file = get_file_name('jsonl', BQ_PARAMS['FILE_PDC_METADATA_TABLE'])
    file_metadata_jsonl_path = get_scratch_fp(BQ_PARAMS, file_metadata_jsonl_file)

    write_list_to_jsonl(file_metadata_jsonl_path, file_metadata_list)
    upload_to_bucket(BQ_PARAMS, file_metadata_jsonl_path)

    jsonl_end = time.time() - jsonl_start
    console_out("File PDC metadata jsonl file created in {0}!\n", (format_seconds(jsonl_end),))


# ***** CASE METADATA TABLE CREATION FUNCTIONS

def make_cases_query():
    return """{{
        allCases {{
            case_id 
            case_submitter_id 
            project_submitter_id 
             primary_site 
             externalReferences {{ 
                external_reference_id 
                reference_resource_shortname 
                reference_resource_name 
                reference_entity_location 
            }}
        }}
    }}"""


"""
def build_cases_jsonl():
    jsonl_start = time.time()

    cases_list = []
    cases_res = get_graphql_api_response(API_PARAMS, make_cases_query())

    for case_row in cases_res['data']['allCases']:
        cases_list.append({
            "case_id": case_row['case_id'],
            "case_submitter_id": case_row['case_submitter_id'],
            "project_submitter_id": case_row['project_submitter_id'],
            "disease_type": case_row['disease_type'],
            "primary_site": case_row['primary_site']
        })

    cases_jsonl_file = get_file_name('jsonl', BQ_PARAMS['CASES_TABLE'])
    cases_jsonl_path = get_scratch_fp(BQ_PARAMS, cases_jsonl_file)

    write_list_to_jsonl(cases_jsonl_path, cases_list)

    upload_to_bucket(BQ_PARAMS, cases_jsonl_path)

    jsonl_end = time.time() - jsonl_start
    console_out("Cases jsonl file created in {0}!\n", (format_seconds(jsonl_end),))
"""

def get_cases_data():
    cases_table = get_table_name(BQ_PARAMS['CASES_TABLE'])
    table_id = get_table_id(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['DEV_META_DATASET'], cases_table)

    return """
        SELECT * 
        FROM `{}`
    """.format(table_id)


"""
def build_case_metadata_jsonl(cases_list):
    print("building case metadata jsonl")
    jsonl_start = time.time()
    case_metadata_list = []

    for case in cases_list:
        case_dict = dict()
        meta_cnt = 0

        case_meta_query = make_case_query(case['case_submitter_id'])
        case_meta_res = get_graphql_api_response(API_PARAMS, case_meta_query)

        if 'data' not in case_meta_res or 'case' not in case_meta_res['data']:
            print("Result has an issue: {}".format(case_meta_res))
            # case_dict.update(case)
            continue

        num_case_meta_res = len(case_meta_res['data']['case'])
        res = case_meta_res['data']['case']

        if num_case_meta_res == 0:
            # print(res)
            continue
        elif num_case_meta_res > 1:
            print("results > 2:\n{}".format(res))
            # print(res)
            continue

        case_metadata = res[0]

        if (case_metadata['project_submitter_id'] != case['project_submitter_id'] or
                case_metadata['case_submitter_id'] != case['case_submitter_id'] or
                case_metadata['case_id'] != case['case_id'] or
                case_metadata['primary_site'] != case['primary_site'] or
                case_metadata['disease_type'] != case['disease_type']):
            print("weird, non-matching column data!")
            continue

        case_dict.update(case_metadata)

        meta_cnt += 1
        case_metadata_list.append(case_dict)

        if meta_cnt >= 5:
            print("woot!!")
            exit()

    case_metadata_jsonl_file = get_file_name('jsonl', BQ_PARAMS['CASE_METADATA_TABLE'])
    case_meta_jsonl_path = get_scratch_fp(BQ_PARAMS, case_metadata_jsonl_file)

    write_list_to_jsonl(case_meta_jsonl_path, cases_list)

    upload_to_bucket(BQ_PARAMS, case_meta_jsonl_path)

    jsonl_end = time.time() - jsonl_start
    console_out("Case metadata jsonl file created in {0}!\n", (format_seconds(jsonl_end),))
"""


# ***** CLINICAL TABLE CREATION FUNCTIONS
def make_cases_diagnoses_query(pdc_study_id, offset, limit):
    return ''' {{ 
        paginatedCaseDiagnosesPerStudy(pdc_study_id: "{0}" offset: {1} limit: {2}) {{
            total caseDiagnosesPerStudy {{
                case_id
                case_submitter_id
                disease_type
                primary_site
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
    return """
    {{ paginatedCaseDemographicsPerStudy (pdc_study_id: "{0}" offset: {1} limit: {2}) {{ 
        total caseDemographicsPerStudy {{ 
            case_id 
            case_submitter_id 
            disease_type 
            primary_site 
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
    }} }}
    """.format(pdc_study_id, offset, limit)


def build_cases_diagnoses_jsonl(studies_list):
    cases_diagnoses = list()

    diagnoses_jsonl_file = get_file_name('jsonl', BQ_PARAMS['CLINICAL_DIAGNOSES_TABLE'])
    diagnoses_jsonl_path = get_scratch_fp(BQ_PARAMS, diagnoses_jsonl_file)

    for study in studies_list:
        offset = 0
        page = 1
        limit = API_PARAMS['PAGINATED_LIMIT']

        pdc_study_id = study['pdc_study_id']

        print("\nFor {}:".format(pdc_study_id))

        diagnoses_res = get_graphql_api_response(API_PARAMS, make_cases_diagnoses_query(pdc_study_id, offset, limit))

        total_pages = diagnoses_res['data']['paginatedCaseDiagnosesPerStudy']['pagination']['pages']

        print("Retrieved api response for page {} of {}.\n{}".format(
            page, total_pages, diagnoses_res['data']['paginatedCaseDiagnosesPerStudy']['pagination']))

        for case in diagnoses_res['data']['paginatedCaseDiagnosesPerStudy']['caseDiagnosesPerStudy']:
            cases_diagnoses.append(case)

        page += 1

        while page < total_pages:
            offset = offset + limit

            diagnoses_res = get_graphql_api_response(API_PARAMS, make_cases_diagnoses_query(pdc_study_id, offset, limit))

            print("Retrieved api response for page {} of {}.\n{}".format(
                page, total_pages, diagnoses_res['data']['paginatedCaseDiagnosesPerStudy']['pagination']))

            for case in diagnoses_res['data']['paginatedCaseDiagnosesPerStudy']['caseDiagnosesPerStudy']:
                cases_diagnoses.append(case)

            print("Appended data to dict! New size: {}".format(len(cases_diagnoses)))
            page += 1

        write_list_to_jsonl(diagnoses_jsonl_path, cases_diagnoses)


def main(args):
    start = time.time()
    console_out("PDC script started at {}".format(time.strftime("%x %X", time.localtime())))
    steps = None

    try:
        global API_PARAMS, BQ_PARAMS
        API_PARAMS, BQ_PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        has_fatal_error(str(err), ValueError)

    if 'delete_tables' in steps:
        for table_id in BQ_PARAMS['DELETE_TABLES']:
            delete_bq_table(table_id)
            console_out("Deleted table: {}", (table_id,))

        delete_from_steps('delete_tables', steps)

    if 'build_studies_jsonl' in steps:
        console_out("\nbuild_studies_jsonl started")
        jsonl_start = time.time()

        json_res = get_graphql_api_response(API_PARAMS, make_all_programs_query())
        studies = create_studies_dict(json_res)

        studies_jsonl_file = get_file_name('jsonl', BQ_PARAMS['STUDIES_TABLE'])
        studies_jsonl_path = get_scratch_fp(BQ_PARAMS, studies_jsonl_file)

        write_list_to_jsonl(studies_jsonl_path, studies)
        upload_to_bucket(BQ_PARAMS, studies_jsonl_path)

        jsonl_end = time.time() - jsonl_start
        console_out("\t\t- done, created in {0}!", (format_seconds(jsonl_end),))

        delete_from_steps('build_studies_jsonl', steps)

    if 'build_studies_table' in steps:
        build_table_from_jsonl(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['DEV_META_DATASET'], BQ_PARAMS['STUDIES_TABLE'])

        delete_from_steps('build_studies_table', steps)

    # Don't bother building the study list if we aren't running further steps, exit
    if len(steps) == 0:
        end = time.time() - start
        console_out("Finished program execution in {}!\n", (format_seconds(end),))
        exit()

    studies_list = list()
    excluded_studies_list = list()
    pdc_study_ids = list()

    for study in get_query_results(make_all_studies_query()):
        if is_currently_embargoed(study.get('embargo_date')):
            excluded_studies_list.append((study.get('study_name'), study.get('embargo_date')))
        else:
            studies_list.append(dict(study.items()))

    embargoed_str_list = ["\t- {} (expires {})".format(study, embargo_date)
                          for study, embargo_date in excluded_studies_list]
    embargoed_print_str = "\n".join(embargoed_str_list)
    console_out("\nCurrently embargoed:\n{}\n", (embargoed_print_str,))

    for study in studies_list:
        pdc_study_ids.append(study['pdc_study_id'])

    pdc_study_ids.sort()

    if 'build_biospecimen_tsv' in steps:
        # *** NOTE: DATA MAY BE INCOMPLETE CURRENTLY in PDC API

        biospecimen_tsv_file = get_file_name('tsv', BQ_PARAMS['BIOSPECIMEN_TABLE'], 'duplicates')
        biospecimen_tsv_path = get_scratch_fp(BQ_PARAMS, biospecimen_tsv_file)
        build_biospecimen_tsv(studies_list, biospecimen_tsv_path)
        upload_to_bucket(BQ_PARAMS, biospecimen_tsv_path)

    if 'build_biospecimen_table' in steps:
        # *** NOTE: DATA MAY BE INCOMPLETE CURRENTLY in PDC API

        build_table_from_tsv(BQ_PARAMS['DEV_PROJECT'],
                             BQ_PARAMS['DEV_META_DATASET'],
                             BQ_PARAMS['BIOSPECIMEN_TABLE'],
                             'duplicates')

        dup_table_name = get_table_name(BQ_PARAMS['BIOSPECIMEN_TABLE'], 'duplicates')
        dup_table_id = get_table_id(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['DEV_META_DATASET'], dup_table_name)
        final_table_name = get_table_name(BQ_PARAMS['BIOSPECIMEN_TABLE'])
        final_table_id = get_table_id(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['DEV_META_DATASET'], final_table_name)
        load_table_from_query(BQ_PARAMS, final_table_id, make_unique_biospecimen_query(dup_table_id))

        if has_table(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['DEV_META_DATASET'], final_table_name):
            delete_bq_table(dup_table_id)

    if 'build_per_study_file_jsonl' in steps:
        build_per_study_file_jsonl(studies_list)

    if 'build_per_study_file_table' in steps:
        build_table_from_jsonl(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['DEV_META_DATASET'],
                               BQ_PARAMS['FILES_PER_STUDY_TABLE'])

    if 'build_file_pdc_metadata_jsonl' in steps:
        file_ids = get_file_ids()
        build_file_pdc_metadata_jsonl(file_ids)

    if 'build_file_pdc_metadata_table' in steps:
        build_table_from_jsonl(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['DEV_META_DATASET'],
                               BQ_PARAMS['FILE_PDC_METADATA_TABLE'])

    if 'build_file_associated_entries_table' in steps:
        # Note, this currently only works for aliquots, because there were no entries in the API pull that had entries
        # in the aliquots list, but that didn't have an aliquot_id. If this ever changes, we'll need to adjust,
        # but based on the way I've seen their api work I don't expect that to happen.
        table_name = BQ_PARAMS['FILE_ASSOC_MAPPING_TABLE'] + '_' + BQ_PARAMS['RELEASE']
        full_table_id = get_dev_table_id(table_name, is_metadata=True)
        load_table_from_query(BQ_PARAMS, full_table_id, make_associated_entities_query())

    if 'build_file_combined_table' in steps:
        table_name = BQ_PARAMS['FILE_COMBINED_METADATA_TABLE'] + '_' + BQ_PARAMS['RELEASE']
        full_table_id = get_dev_table_id(table_name, is_metadata=True)
        load_table_from_query(BQ_PARAMS, full_table_id, make_combined_file_metadata_query())

    if 'build_cases_jsonl' in steps:
        build_jsonl_from_pdc_api(endpoint="allCases", request_function=make_cases_query)

    if 'build_cases_table' in steps:
        build_table_from_jsonl(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['DEV_META_DATASET'],
                               BQ_PARAMS['CASE_METADATA_TABLE'])

    if 'build_cases_aliquots_jsonl' in steps:
        build_jsonl_from_pdc_api(endpoint="casesSamplesAliquots", request_function=make_cases_aliquots_query)

        """
        jsonl_start = time.time()

        cases_aliquots_jsonl_file = get_file_name('jsonl', BQ_PARAMS['CASE_ALIQUOT_TABLE'])
        cases_aliquots_jsonl_path = get_scratch_fp(BQ_PARAMS, cases_aliquots_jsonl_file)
        build_cases_aliquots_jsonl(cases_aliquots_jsonl_path)
        upload_to_bucket(BQ_PARAMS, cases_aliquots_jsonl_path)

        jsonl_end = time.time() - jsonl_start
        console_out("Cases Aliquots table jsonl file created in {0}!\n", (format_seconds(jsonl_end),))
        """

    if 'build_cases_aliquots_table' in steps:
        build_table_from_jsonl(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['DEV_META_DATASET'], BQ_PARAMS['CASE_ALIQUOT_TABLE'])

    if 'build_case_diagnoses_jsonl' in steps:
        build_jsonl_from_pdc_api(endpoint="paginatedCaseDiagnosesPerStudy",
                                 request_function=make_cases_diagnoses_query,
                                 ids_list=pdc_study_ids)

    if 'build_case_diagnoses_table' in steps:
        pass

    if 'build_case_demographics_jsonl' in steps:
        build_jsonl_from_pdc_api(endpoint="paginatedCaseDemographicsPerStudy",
                                 request_function=make_cases_demographics_query,
                                 ids_list=pdc_study_ids)

    if 'build_case_demographics_table' in steps:
        pass

    if 'build_quant_tsvs' in steps:
        for study_id_dict in studies_list:
            quant_tsv_file = get_file_name('tsv', BQ_PARAMS['QUANT_DATA_TABLE'], study_id_dict['pdc_study_id'])
            quant_tsv_path = get_scratch_fp(BQ_PARAMS, quant_tsv_file)

            lines_written = build_quant_tsv(study_id_dict, 'log2_ratio', quant_tsv_path)
            console_out("\n{0} lines written for {1}", (lines_written, study_id_dict['study_name']))

            if lines_written > 0:
                upload_to_bucket(BQ_PARAMS, quant_tsv_path)
                console_out("{0} uploaded to Google Cloud bucket!", (quant_tsv_file,))
                os.remove(quant_tsv_path)

    if 'build_quant_tables' in steps:
        console_out("Building quant tables...")
        blob_files = get_quant_files()

        for study_id_dict in studies_list:
            quant_tsv_file = get_file_name('tsv', BQ_PARAMS['QUANT_DATA_TABLE'], study_id_dict['pdc_study_id'])

            if quant_tsv_file not in blob_files:
                console_out('Skipping quant table build for {} (no file found in gs).', (study_id_dict['study_name'],))
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
        mapping_table = get_table_name(BQ_PARAMS['UNIPROT_MAPPING_TABLE'], release=BQ_PARAMS['UNIPROT_RELEASE'])
        table_id = get_dev_table_id(mapping_table, is_metadata=True)

        console_out("\nBuilding {0}... ", (table_id,))
        schema_filename = "/".join(table_id.split(".")) + '.json'
        schema, metadata = from_schema_file_to_obj(BQ_PARAMS, schema_filename)
        data_file = split_file[0] + '_' + BQ_PARAMS['UNIPROT_RELEASE'] + API_PARAMS['UNIPROT_FILE_EXT']
        null = BQ_PARAMS['NULL_MARKER']
        create_and_load_tsv_table(BQ_PARAMS, data_file, schema, table_id, null_marker=null, num_header_rows=0)
        console_out("Uniprot table built!")

    if 'build_swissprot_table' in steps:
        table_name = get_table_name(BQ_PARAMS['SWISSPROT_TABLE'], release=BQ_PARAMS['UNIPROT_RELEASE'])
        table_id = get_dev_table_id(table_name, is_metadata=True)

        console_out("Building {0}... ", (table_id,))
        schema_filename = "/".join(table_id.split(".")) + '.json'
        schema, metadata = from_schema_file_to_obj(BQ_PARAMS, schema_filename)

        data_file = table_name + API_PARAMS['UNIPROT_FILE_EXT']
        null = BQ_PARAMS['NULL_MARKER']
        create_and_load_tsv_table(BQ_PARAMS, data_file, schema, table_id, null_marker=null, num_header_rows=0)
        console_out("Swiss-prot table built!")

    if 'build_gene_tsv' in steps:
        gene_symbol_list = build_gene_symbol_list(studies_list)
        gene_tsv_file = get_file_name('tsv', BQ_PARAMS['GENE_TABLE'])
        gene_tsv_path = get_scratch_fp(BQ_PARAMS, gene_tsv_file)

        build_gene_tsv(gene_symbol_list, gene_tsv_path, append=API_PARAMS['RESUME_GENE_TSV'])
        upload_to_bucket(BQ_PARAMS, gene_tsv_path)

    if 'build_gene_table' in steps:
        gene_tsv_file = get_file_name('tsv', BQ_PARAMS['GENE_TABLE'])
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

        build_table_from_tsv(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['DEV_META_DATASET'], BQ_PARAMS['GENE_TABLE'])

    if 'build_proteome_quant_tables' in steps:
        for study in studies_list:

            # only run the build script for analytes we're currently publishing
            if study['analytical_fraction'] not in BQ_PARAMS["BUILD_ANALYTES"]:
                continue

            pdc_study_id = study['pdc_study_id']
            raw_table_name = get_quant_table_name(study, is_final=False)

            if has_table(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['DEV_DATASET'], raw_table_name):
                final_table_name = get_quant_table_name(study)
                final_table_id = get_dev_table_id(final_table_name)

                load_table_from_query(BQ_PARAMS, final_table_id, make_proteome_quant_table_query(pdc_study_id))

    if 'update_proteome_quant_metadata' in steps:
        dir_path = '/'.join([BQ_PARAMS['BQ_REPO'], BQ_PARAMS['FIELD_DESC_DIR']])
        fields_file = "{}_{}.json".format(BQ_PARAMS['FIELD_DESC_FILE_PREFIX'], BQ_PARAMS['RELEASE'])
        field_desc_fp = get_filepath(dir_path, fields_file)

        with open(field_desc_fp) as field_output:
            descriptions = json.load(field_output)

        for study in get_proteome_studies(studies_list):
            table_name = get_quant_table_name(study)
            table_id = get_dev_table_id(table_name)

            console_out("Updating metadata for {}", (table_id,))
            update_schema(table_id, descriptions)

    if "update_table_metadata" in steps:
        metadata_pdc_dir = BQ_PARAMS['DATA_SOURCE'] + '_' + BQ_PARAMS["RELEASE"]
        rel_path = '/'.join([BQ_PARAMS['BQ_REPO'], BQ_PARAMS['TABLE_METADATA_DIR'], metadata_pdc_dir])
        metadata_fp = get_filepath(rel_path)
        metadata_files = [f for f in os.listdir(metadata_fp) if os.path.isfile(os.path.join(metadata_fp, f))]

        console_out("Updating table metadata:")

        for json_file in metadata_files:
            table_name = json_file.split('.')[-2]
            table_id = get_dev_table_id(table_name)

            if not exists_bq_table(table_id):
                console_out("skipping {} (no bq table found)", (table_id,))
                continue
            else:
                console_out("- {}", (table_id,))

            json_fp = metadata_fp + '/' + json_file

            with open(json_fp) as json_file_output:
                metadata = json.load(json_file_output)
                update_table_metadata(table_id, metadata)

    if "publish_proteome_tables" in steps:
        for study in get_proteome_studies(studies_list):
            table_name = get_quant_table_name(study)
            project_submitter_id = study['project_submitter_id']

            if project_submitter_id not in BQ_PARAMS['PROD_DATASET_MAP']:
                continue

            dataset = BQ_PARAMS['PROD_DATASET_MAP'][project_submitter_id]

            src_table_id = get_dev_table_id(table_name)
            vers_table_id = get_table_id(BQ_PARAMS['PROD_PROJECT'], dataset + '_versioned', table_name)
            curr_table_id = get_table_id(BQ_PARAMS['PROD_PROJECT'], dataset, table_name[:-7] + 'current')

            if exists_bq_table(src_table_id):
                console_out("Publishing {}".format(vers_table_id))
                copy_bq_table(BQ_PARAMS, src_table_id, vers_table_id, replace_table=True)
                console_out("Publishing {}".format(curr_table_id))
                copy_bq_table(BQ_PARAMS, src_table_id, curr_table_id, replace_table=True)

                update_friendly_name(BQ_PARAMS, vers_table_id, is_gdc=False)

                # todo -- next round -- how to change past version to archived, since it isn't version# - 1

    end = time.time() - start
    console_out("Finished program execution in {}!\n", (format_seconds(end),))


if __name__ == '__main__':
    main(sys.argv)
