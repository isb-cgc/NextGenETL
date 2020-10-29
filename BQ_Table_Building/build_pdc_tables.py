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

from common_etl.utils import *

API_PARAMS = dict()
BQ_PARAMS = dict()
YAML_HEADERS = ('api_params', 'bq_params', 'steps')


def map_biospecimen_query(column_id_1, column_id_2):
    table_id = get_table_id(BQ_PARAMS['DEV_PROJECT'],
                            BQ_PARAMS['DEV_META_DATASET'],
                            get_table_name(BQ_PARAMS['BIOSPECIMEN_TABLE']))

    return """
            WITH map_ids AS (
                SELECT {0}, ARRAY_AGG(distinct {1}) AS {1}s
                FROM `{2}`
                GROUP BY {0}
            )
            SELECT {0}, {1}s
            FROM map_ids
    """.format(column_id_1, column_id_2, table_id)


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


def get_study_ids():
    table_id = get_table_id(BQ_PARAMS['DEV_PROJECT'],
                            BQ_PARAMS['DEV_META_DATASET'],
                            get_table_name(BQ_PARAMS['STUDIES_TABLE']))

    return """
    SELECT study_id, study_submitter_id, pdc_study_id, study_name, aliquots_count, cases_count, embargo_date
    FROM  `{}`
    """.format(table_id)


def make_quant_data_matrix_query(study_submitter_id, data_type):
    return '{{ quantDataMatrix(study_submitter_id: \"{}\" data_type: \"{}\") }}'.format(study_submitter_id, data_type)


def build_quant_tsv(study_id_dict, data_type, tsv_fp):
    study_submitter_id = study_id_dict['study_submitter_id']
    study_name = study_id_dict['study_name']
    lines_written = 0

    res_json = get_graphql_api_response(API_PARAMS,
                                        make_quant_data_matrix_query(study_submitter_id, data_type),
                                        fail_on_error=False)

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
    blobs = storage_client.list_blobs(BQ_PARAMS['WORKING_BUCKET'],
                                      prefix=BQ_PARAMS['WORKING_BUCKET_DIR'])
    files = set()

    for blob in blobs:
        filename = blob.name.split('/')[-1]
        files.add(filename)

    return files


def make_gene_name_set_query(proteome_study):
    table_name = get_table_name(BQ_PARAMS['QUANT_DATA_TABLE'], proteome_study, BQ_PARAMS['RELEASE'])
    table_id = get_table_id(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['DEV_DATASET'], table_name)

    return """
        SELECT DISTINCT(gene_symbol)
        FROM `{}`
    """.format(table_id)


def add_gene_names_per_study(proteome_study, gene_set):
    results = get_query_results(make_gene_name_set_query(proteome_study))

    for row in results:
        gene_set.add(row['gene_symbol'])

    return gene_set


def build_proteome_gene_name_list():
    console_out("Building proteome gene name tsv!")

    proteome_studies = API_PARAMS['PROTEOME_STUDIES']
    gene_name_set = set()

    for proteome_study in proteome_studies:
        console_out("Add gene names from {0}... ", (proteome_study,))
        add_gene_names_per_study(proteome_study, gene_name_set)
        console_out("\t\t- new set size: {0}", (len(gene_name_set),))

    gene_name_list = list(gene_name_set)
    gene_name_list.sort()

    return gene_name_list


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


def build_gene_tsv(gene_name_list, gene_tsv, append=False):
    gene_symbol_set = set(gene_name_list)

    gene_tsv_exists = os.path.exists(gene_tsv)

    if append:
        console_out("Resuming geneSpectralCount API calls... ", end='')

        if gene_tsv_exists:
            with open(gene_tsv, 'r') as tsv_file:
                saved_genes = set()
                gene_reader = csv.reader(tsv_file, delimiter='\t')

                passed_first_row = False

                for row in gene_reader:
                    if not passed_first_row:
                        passed_first_row = True
                        continue

                    saved_genes.add(row[1])

            gene_symbol_set = gene_symbol_set - saved_genes

        remaining_genes = len(gene_symbol_set)

        if remaining_genes == 0:
            console_out("{} gene API calls remaining--skipping step.", (remaining_genes,))
            return
        else:
            console_out("{} gene API calls remaining.", (remaining_genes,))

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
                                          'proteins',
                                          'assays'],
                                         null_marker=BQ_PARAMS['NULL_MARKER']))

        count = 0

        no_spectral_count_set = set()
        empty_spectral_count_set = set()

        for gene_symbol in gene_symbol_set:
            count += 1
            json_res = get_graphql_api_response(API_PARAMS, make_gene_query(gene_symbol))
            time.sleep(0.1)  # need a delay to avoid making too many api requests and getting 500 server error

            gene = json_res['data']['geneSpectralCount'][0]

            if not gene:
                console_out("No geneSpectralCount data found for {0}", (gene_symbol,))
                no_spectral_count_set.add(gene_symbol)
                continue
            elif not gene['gene_name']:
                console_out("Empty geneSpectralCount data found for {0}", (gene_symbol,))
                empty_spectral_count_set.add(gene_symbol)
                continue
            else:
                if count % 50 == 0:
                    console_out("Added {0} genes", (count,))

            for key in gene.keys():
                gene[key] = str(gene[key]).strip()

                if not gene[key] or gene[key] == '':
                    gene[key] = 'None'

            authority = ""
            authority_gene_id = ""

            split_authority = gene['authority'].split(':')
            if len(split_authority) > 2:
                has_fatal_error("Authority should split into <= two elements. Actual: {}".format(gene['authority']))
            if len(split_authority) > 0:
                if split_authority[0]:
                    authority = split_authority[0]
            if len(split_authority) > 1:
                if split_authority[1]:
                    authority_gene_id = split_authority[1]

            gene_fh.write(create_tsv_row([gene['gene_id'],
                                          gene['gene_name'],
                                          gene['NCBI_gene_id'],
                                          authority,
                                          authority_gene_id,
                                          gene['description'],
                                          gene['organism'],
                                          gene['chromosome'],
                                          gene['locus'],
                                          gene['proteins'],
                                          gene['assays']],
                                         null_marker=BQ_PARAMS['NULL_MARKER']))


def make_total_cases_aliquots_query():
    return '''
    {{ paginatedCasesSamplesAliquots(offset:{0} limit:{1}) {{ 
    total casesSamplesAliquots {{
    case_id case_submitter_id external_case_id  
    samples {{
    sample_id sample_submitter_id
    aliquots {{ aliquot_id aliquot_submitter_id
    aliquot_run_metadata {{ aliquot_run_metadata_id}}
    }} 
    }}
    }}
    pagination {{ count sort from page total pages size }}
    }}
    }}
    '''.format(0, 1)


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
    limit = API_PARAMS['CSA_LIMIT']
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


def build_biospec_query(bio_table_id, csa_table_id):
    return """
    SELECT a.case_id, a.study_id, a.sample_id, a.aliquot_id, b.aliquot_run_metadata_id
        FROM `{}` AS a
        LEFT JOIN `{}` AS b
        ON a.aliquot_id = b.aliquot_id
        AND a.sample_id = b.sample_id
        AND a.case_id = b.case_id
        GROUP BY a.case_id, a.study_id, a.sample_id, a.aliquot_id, b.aliquot_run_metadata_id
    """.format(bio_table_id, csa_table_id)


def build_biospec_count_query(biospec_table_id, csa_table_id):
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


def build_aliquot_run_query(table_id, case_id, sample_id, aliquot_id):
    return """
        SELECT aliquot_run_metadata_id
        FROM `{}` 
        WHERE case_id = '{}'
        AND sample_id = '{}'
        AND aliquot_id = '{}'    
    """.format(table_id, case_id, sample_id, aliquot_id)


def build_nested_biospecimen_jsonl():
    bio_table_name = get_table_name(BQ_PARAMS['BIOSPECIMEN_TABLE'])
    bio_table_id = get_table_id(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['DEV_META_DATASET'], bio_table_name)
    csa_table_name = get_table_name(BQ_PARAMS['CASE_ALIQUOT_TABLE'])
    csa_table_id = get_table_id(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['DEV_META_DATASET'], csa_table_name)

    case_id_keys_obj = dict()
    biospec_count_res = get_query_results(build_biospec_count_query(bio_table_id, csa_table_id))
    counts = dict()

    for row in biospec_count_res:
        for counts_tuple in list(row.items()):
            key = counts_tuple[0]
            val = counts_tuple[1]
            counts[key] = val

    biospec_res = get_query_results(build_biospec_query(bio_table_id, csa_table_id))
    counts['total_rows'] = biospec_res.total_rows

    i = 0

    for row in biospec_res:
        if i % 500 == 0:
            print("{} of {} rows processed".format(i, counts['total_rows']))
        i += 1

        id_row = dict()

        for id_tuple in list(row.items()):
            key = id_tuple[0]
            val = id_tuple[1]
            id_row[key] = val

        case_id = id_row['case_id']
        study_id = id_row['study_id']
        sample_id = id_row['sample_id']
        aliquot_id = id_row['aliquot_id']
        aliquot_run_metadata_id = id_row['aliquot_run_metadata_id']

        if case_id and case_id not in case_id_keys_obj:
            case_id_keys_obj[case_id] = dict()
        elif not case_id:
            continue

        if study_id and study_id not in case_id_keys_obj[case_id]:
            case_id_keys_obj[case_id][study_id] = dict()
        elif not study_id:
            continue

        if sample_id and sample_id not in case_id_keys_obj[case_id][study_id]:
            case_id_keys_obj[case_id][study_id][sample_id] = dict()
        elif not sample_id:
            continue
        if aliquot_id and aliquot_id not in case_id_keys_obj[case_id][study_id][sample_id]:
            case_id_keys_obj[case_id][study_id][sample_id][aliquot_id] = list()
        elif not aliquot_id:
            continue

        if aliquot_run_metadata_id and aliquot_run_metadata_id not in \
                case_id_keys_obj[case_id][study_id][sample_id][aliquot_id]:
            case_id_keys_obj[case_id][study_id][sample_id][aliquot_id].append(aliquot_run_metadata_id)
        elif not aliquot_run_metadata_id:
            continue
        else:
            print("duplicate entry! case_id_keys_obj[{}][{}][{}][{}] = {}".format(
                case_id, study_id, sample_id, aliquot_id, aliquot_run_metadata_id))

    print("\nBuilding JSON object!\n")
    case_list = []

    for case_id in case_id_keys_obj:
        if case_id:
            study_list = list()

            for study_id in case_id_keys_obj[case_id]:
                if study_id:
                    sample_list = list()

                    for sample_id in case_id_keys_obj[case_id][study_id]:
                        if sample_id:
                            aliquot_list = list()

                            for aliquot_id in case_id_keys_obj[case_id][study_id][sample_id]:
                                if aliquot_id:
                                    aliquot_run_metadata_list = list()

                                    for aliquot_run_metadata_id in \
                                            case_id_keys_obj[case_id][study_id][sample_id][aliquot_id]:
                                        if aliquot_run_metadata_id:
                                            aliquot_run_metadata_list.append({
                                                "aliquot_run_metadata_id": aliquot_run_metadata_id})

                                    aliquot_list.append({"aliquot_id": aliquot_id,
                                                         "aliquot_run_metadata": aliquot_run_metadata_list})
                            sample_list.append({"sample_id": sample_id, "aliquots": aliquot_list})
                    study_list.append({"study_id": study_id, "samples": sample_list})
            case_list.append({'case_id': case_id, 'studies': study_list})

    case_study_sample_aliquot_obj = {
        'total_distinct': {
            'combined_rows': counts['total_rows'],
            'biospec_cases': counts['bio_case_count'],
            'biospec_studies': counts['bio_study_count'],
            'biospec_samples': counts['bio_sample_count'],
            'biospec_aliquots': counts['bio_aliquot_count'],
            'aliquot_run_metadata': counts['csa_aliquot_run_count']
        },
        'data': {
            'cases': case_list
        }
    }

    jsonl_file = get_table_name(BQ_PARAMS['CASE_STUDY_BIOSPECIMEN_TABLE']) + '.jsonl'
    jsonl_fp = get_scratch_fp(BQ_PARAMS, jsonl_file)
    write_list_to_jsonl(jsonl_fp, case_study_sample_aliquot_obj['data']['cases'])
    upload_to_bucket(BQ_PARAMS, jsonl_fp)

    print_nested_biospecimen_statistics(case_study_sample_aliquot_obj['total_distinct'])


def make_files_per_study_query(study_id):
    return """
    {{ filesPerStudy (study_id: \"{}\") {{
            study_id 
            pdc_study_id 
            study_name file_id 
            file_name 
            file_submitter_id 
            file_type md5sum 
            file_location 
            file_size 
            data_category 
            file_format
        }} 
    }}""".format(study_id)


def build_per_study_file_jsonl(study_ids_list):
    jsonl_start = time.time()
    file_list = []

    for study in study_ids_list:
        study_id = study['study_id']
        files_res = get_graphql_api_response(API_PARAMS, make_files_per_study_query(study_id))

        if 'data' in files_res:
            study_file_count = 0

            for file_row in files_res['data']['filesPerStudy']:
                print(file_row)

                study_file_count += 1
                file_list.append(file_row)

            print("{} files retrieved for {}".format(study_file_count, study['study_submitter_id']))
        else:
            print("No data returned by per-study file query for {}".format(study_id))

    per_study_file_jsonl_path = get_scratch_fp(BQ_PARAMS, get_table_name(BQ_PARAMS['TEMP_FILE_TABLE']) + '.jsonl')

    write_list_to_jsonl(per_study_file_jsonl_path, file_list)
    upload_to_bucket(BQ_PARAMS, per_study_file_jsonl_path)

    jsonl_end = time.time() - jsonl_start

    console_out("Per-study file metadata jsonl file created in {0}!\n", (format_seconds(jsonl_end),))


def make_file_id_query(table_id, batch=True):
    # Note -- ROW_NUMBER can be used to resume file metadata jsonl creation, as an index, in WHERE CLAUSE
    # e.g. (WHERE RowNumber BETWEEN 50 AND 60)

    if batch and API_PARAMS['METADATA_BATCH'] and API_PARAMS['METADATA_BATCH_SIZE']:
        start_idx = API_PARAMS['METADATA_OFFSET']
        end_idx = start_idx + API_PARAMS['METADATA_BATCH_SIZE']
        where_clause = "WHERE row_number BETWEEN {} AND {}".format(start_idx, end_idx)
    elif batch and API_PARAMS['METADATA_BATCH']:
        start_idx = API_PARAMS['METADATA_OFFSET']
        where_clause = "WHERE row_number >= {}".format(start_idx)
    else:
        where_clause = ''

    return """
        SELECT ROW_NUMBER() OVER(ORDER BY file_id ASC) 
            AS row_number, file_id 
        FROM `{}`
        {}
        ORDER BY file_id ASC
    """.format(table_id, where_clause)


def make_file_metadata_query(file_id):
    return """
    {{ fileMetadata(file_id: \"{}\") {{
        file_id 
        fraction_number 
        experiment_type 
        plex_or_dataset_name 
        analyte 
        instrument 
        study_run_metadata_submitter_id 
        study_run_metadata_id 
        }} 
    }}    
    """.format(file_id)


def get_file_ids():
    table_name = get_table_name(BQ_PARAMS['TEMP_FILE_TABLE'])
    table_id = get_table_id(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['DEV_META_DATASET'], table_name)
    return get_query_results(make_file_id_query(table_id, batch=False))  # todo fix back


def build_file_metadata_jsonl(file_ids):
    num_files = file_ids.total_rows
    jsonl_start = time.time()
    file_metadata_list = []
    cnt = 0

    for row in file_ids:
        file_id = row['file_id']
        file_metadata_res = get_graphql_api_response(API_PARAMS, make_file_metadata_query(file_id))

        if 'data' in file_metadata_res:
            for metadata_row in file_metadata_res['data']['fileMetadata']:
                file_metadata_list.append(metadata_row)
                cnt += 1

                if cnt % 25 == 0:
                    print("{} of {} files retrieved".format(cnt, num_files))
        else:
            print("No data returned by file metadata query for {}".format(file_id))

    file_metadata_jsonl_path = get_scratch_fp(BQ_PARAMS, get_table_name(BQ_PARAMS['FILES_TABLE']) + '.jsonl')

    write_list_to_jsonl(file_metadata_jsonl_path, file_metadata_list)
    upload_to_bucket(BQ_PARAMS, file_metadata_jsonl_path)

    jsonl_end = time.time() - jsonl_start
    console_out("File metadata jsonl file created in {0}!\n", (format_seconds(jsonl_end),))


def make_cases_query():
    return """ 
    { allCases {
        case_id
        case_submitter_id
        project_submitter_id
        disease_type
        primary_site
        }
    }"""


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

    cases_jsonl_fp = get_scratch_fp(BQ_PARAMS, get_table_name(BQ_PARAMS['CASES_TABLE']) + '.jsonl')
    write_list_to_jsonl(cases_jsonl_fp, cases_list)
    upload_to_bucket(BQ_PARAMS, cases_jsonl_fp)

    jsonl_end = time.time() - jsonl_start
    console_out("Cases jsonl file created in {0}!\n", (format_seconds(jsonl_end),))


def make_case_query(case_submitter_id):
    return """
    {{ case (case_submitter_id: \"{}\") {{
        case_id
        case_submitter_id
        project_submitter_id
        external_case_id
        tissue_source_site_code
        days_to_lost_to_followup
        disease_type
        lost_to_followup
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
    }}
    """.format(case_submitter_id)


def get_cases_data():
    cases_table = get_table_name(BQ_PARAMS['CASES_TABLE'])
    table_id = get_table_id(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['DEV_META_DATASET'], cases_table)

    return """
        SELECT * 
        FROM `{}`
    """.format(table_id)


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

    case_meta_jsonl_fp = get_scratch_fp(BQ_PARAMS, get_table_name(BQ_PARAMS['CASE_METADATA_TABLE']) + '.jsonl')
    write_list_to_jsonl(case_meta_jsonl_fp, cases_list)
    upload_to_bucket(BQ_PARAMS, case_meta_jsonl_fp)

    jsonl_end = time.time() - jsonl_start
    console_out("Case metadata jsonl file created in {0}!\n", (format_seconds(jsonl_end),))


def build_uniprot_tsv(dest_scratch_fp):
    def pop_unwanted(row_list, excluded_idx_list):
        excluded_idx_list.sort(reverse=True)

        for idx in excluded_idx_list:
            row_list.pop(idx)

        return row_list

    def create_tsv_row_filter_wrapped_lines(row_list, null_marker="None"):
        # some of the rows are really continuations of long PubMed article lists, not actually new rows--
        # probably due to field size limit in Python? Or just error in UniProt data
        if ';' in row_list[0]:
            return None

        print_str = ''
        last_idx = len(row_list) - 1

        for i, column in enumerate(row_list):
            if not column:
                column = null_marker

            delimiter = "\t" if i < last_idx else "\n"
            print_str += column + delimiter

        return print_str

    console_out("creating uniprot tsv... ")

    download_from_bucket(BQ_PARAMS, API_PARAMS['UNIPROT_MAPPING_FILE'])

    src_scratch_fp = get_scratch_fp(BQ_PARAMS, API_PARAMS['UNIPROT_MAPPING_FILE'])

    csv.field_size_limit(sys.maxsize)

    ref_keys = API_PARAMS['UNIPROT_MAPPING_KEYS']

    with open(dest_scratch_fp, 'w') as dest_tsv_file:
        unwanted_indices = API_PARAMS['UNIPROT_EXCLUDE_INDICES']

        ref_keys = pop_unwanted(ref_keys, unwanted_indices)
        header = create_tsv_row_filter_wrapped_lines(ref_keys, null_marker=BQ_PARAMS['NULL_MARKER'])
        dest_tsv_file.write(header)

        with open(src_scratch_fp, 'r') as src_tsv_file:
            csv_reader = csv.reader(src_tsv_file, delimiter='\t')

            for row in csv_reader:
                row = pop_unwanted(row, unwanted_indices)
                row_str = create_tsv_row_filter_wrapped_lines(row, null_marker=BQ_PARAMS['NULL_MARKER'])
                dest_tsv_file.write(row_str)

    console_out("\t\t- done!")


def is_uniprot_accession_number(id_str):

    # todo strip off isomer suffix (-1

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


def build_table_from_tsv(project, dataset, table_prefix, table_suffix=None, backup_table_suffix=None):
    build_start = time.time()

    table_name = get_table_name(table_prefix, table_suffix)
    table_id = get_table_id(project, dataset, table_name)
    schema_filename = '{}.json'.format(table_id)
    schema, metadata = from_schema_file_to_obj(BQ_PARAMS, schema_filename)

    if not schema and not metadata and backup_table_suffix:
        console_out("No schema file found for {}, trying backup ({})", (table_suffix, backup_table_suffix))
        table_name = get_table_name(table_prefix, backup_table_suffix)
        table_id = get_table_id(project, dataset, table_name)
        schema_filename = '{}.json'.format(table_id)
        schema, metadata = from_schema_file_to_obj(BQ_PARAMS, schema_filename)

    if not schema:
        console_out("No schema file found for {}, skipping table.", (backup_table_suffix,))
        return

    console_out("Building {0}... ", (table_id,))
    tsv_name = '{}.tsv'.format(table_name)
    create_and_load_tsv_table(BQ_PARAMS, tsv_name, schema, table_id, BQ_PARAMS['NULL_MARKER'])

    build_end = time.time() - build_start
    console_out("Table built in {0}!\n", (format_seconds(build_end),))


def build_table_from_jsonl(project, dataset, table_prefix, table_suffix=None):
    print("Building {} table!".format(table_prefix))

    build_start = time.time()

    table_name = get_table_name(table_prefix, table_suffix)
    table_id = get_table_id(project, dataset, table_name)
    console_out("Building {0}... ", (table_id,))

    schema_filename = '{}.json'.format(table_id)
    schema, metadata = from_schema_file_to_obj(BQ_PARAMS, schema_filename)

    jsonl_name = '{}.jsonl'.format(table_name)
    create_and_load_table(BQ_PARAMS, jsonl_name, schema, table_id)

    build_end = time.time() - build_start
    console_out("Table built in {0}!\n", (format_seconds(build_end),))


def get_table_name(prefix, suffix=None, include_release=True):
    table_name = prefix

    if suffix:
        table_name += '_' + suffix
    if include_release:
        table_name += '_' + BQ_PARAMS['RELEASE']

    return re.sub('[^0-9a-zA-Z_]+', '_', table_name)


def get_table_id(project, dataset, table_name):
    return "{}.{}.{}".format(project, dataset, table_name)


def has_table(project, dataset, table_name):
    query = """
    SELECT COUNT(1) AS has_table
    FROM `{}.{}.__TABLES_SUMMARY__`
    WHERE table_id = '{}'
    """.format(project, dataset, table_name)

    res = get_query_results(query)

    for row in res:
        has_table = row['has_table']
        break

    return bool(has_table)


def has_quant_table(study_submitter_id):
    return has_table(BQ_PARAMS['DEV_PROJECT'],
                     BQ_PARAMS['DEV_DATASET'],
                     get_table_name(BQ_PARAMS['QUANT_DATA_TABLE'], study_submitter_id))


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

    """
    # Desired output (assuming current date is 2020-10-08...)
    assert is_currently_embargoed("") is False
    assert is_currently_embargoed(None) is False
    assert is_currently_embargoed("2019-01-02") is False
    assert is_currently_embargoed("2019-12-31") is False
    assert is_currently_embargoed("2020-09-30") is False
    assert is_currently_embargoed("2020-10-07") is False
    assert is_currently_embargoed("2020-10-08") is True
    assert is_currently_embargoed("2020-10-09") is True
    assert is_currently_embargoed("2021-01-01") is True
    assert is_currently_embargoed("2021-12-31") is True
    """


def print_nested_biospecimen_statistics(counts):
    print_str = """
Biospecimen JSON created. Statistics for total distinct:
    combined rows: {}
    biospecimen cases: {}
    biospecimen studies: {}
    biospecimen aliquots: {}
    paginatedCasesSamplesAliquots - aliquot_run_metadata rows: {}""".format(counts['combined_rows'],
                                                                            counts['biospec_cases'],
                                                                            counts['biospec_studies'],
                                                                            counts['biospec_samples'],
                                                                            counts['biospec_aliquots'],
                                                                            counts['aliquot_run_metadata'])
    print(print_str)


def main(args):
    start = time.time()
    console_out("PDC script started at {}".format(time.strftime("%x %X", time.localtime())))

    try:
        global API_PARAMS, BQ_PARAMS
        API_PARAMS, BQ_PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        has_fatal_error(str(err), ValueError)

    if 'delete_tables' in steps:
        for table_id in BQ_PARAMS['DELETE_TABLES']:
            delete_bq_table(table_id)
            console_out("Deleted table: {}", (table_id,))

    if 'build_studies_jsonl' in steps:
        console_out("Building studies table... ")
        jsonl_start = time.time()

        json_res = get_graphql_api_response(API_PARAMS, make_all_programs_query())
        studies = create_studies_dict(json_res)

        filename = get_table_name(BQ_PARAMS['STUDIES_TABLE']) + '.jsonl'
        studies_fp = get_scratch_fp(BQ_PARAMS, filename)

        write_list_to_jsonl(studies_fp, studies)
        upload_to_bucket(BQ_PARAMS, studies_fp)

        jsonl_end = time.time() - jsonl_start
        console_out("\t\t- done, created in {0}!", (format_seconds(jsonl_end),))

    if 'build_studies_table' in steps:
        build_table_from_jsonl(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['DEV_META_DATASET'], BQ_PARAMS['STUDIES_TABLE'])

    study_ids_list = list()
    study_ids = get_query_results(get_study_ids())

    excluded_studies = []

    for study in study_ids:
        if not is_currently_embargoed(study.get('embargo_date')):
            study_ids_list.append(dict(study.items()))
        else:
            excluded_tuple = (study.get('study_name'), study.get('embargo_date'))
            excluded_studies.append(excluded_tuple)

    console_out("Studies with currently embargoed data (excluded by script):")

    for excluded_tuple in excluded_studies:
        console_out("\t\t- {} (embargo expires {})", excluded_tuple)

    if 'build_quant_tsvs' in steps:
        tsv_start = time.time()

        for study_id_dict in study_ids_list:
            study_submitter_id = study_id_dict['study_submitter_id']
            study_name = study_id_dict['study_name']
            filename = get_table_name(BQ_PARAMS['QUANT_DATA_TABLE'], study_name) + '.tsv'
            quant_tsv_fp = get_scratch_fp(BQ_PARAMS, filename)
            lines_written = build_quant_tsv(study_id_dict, 'log2_ratio', quant_tsv_fp)

            console_out("\n{0} lines written for {1}", (lines_written, study_submitter_id))

            if lines_written == 0:
                continue

            upload_to_bucket(BQ_PARAMS, quant_tsv_fp)
            console_out("{0} uploaded to Google Cloud bucket!", (filename,))
            os.remove(quant_tsv_fp)

        tsv_end = time.time() - tsv_start
        console_out("Quant table tsv files created in {0}!\n", (format_seconds(tsv_end),))

    if 'build_quant_tables' in steps:
        console_out("Building quant tables...")
        blob_files = get_quant_files()

        for study_id_dict in study_ids_list:
            study_name = study_id_dict['study_name']
            study_submitter_id = study_id_dict['study_submitter_id']
            filename = get_table_name(BQ_PARAMS['QUANT_DATA_TABLE'], study_name) + '.tsv'

            if filename not in blob_files:
                console_out('Skipping quant table build for {}\n\t\t- (gs://{}/{}/{} not found).', (
                    study_submitter_id, BQ_PARAMS['WORKING_BUCKET'], BQ_PARAMS['WORKING_BUCKET_DIR'], filename))
            else:
                build_table_from_tsv(BQ_PARAMS['DEV_PROJECT'],
                                     BQ_PARAMS['DEV_DATASET'],
                                     BQ_PARAMS['QUANT_DATA_TABLE'],
                                     study_name)

    if 'update_quant_tables_metadata' in steps:
        for study_id_dict in study_ids_list:
            study_name = study_id_dict['study_name']
            study_submitter_id = study_id_dict['study_submitter_id']
            bio_table_name = get_table_name(BQ_PARAMS['QUANT_DATA_TABLE'], study_name)
            bio_table_id = get_table_id(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['DEV_DATASET'], bio_table_name)
            schema_filename = bio_table_id + '.json'
            schema, table_metadata = from_schema_file_to_obj(BQ_PARAMS, schema_filename)

            if not table_metadata:
                console_out("No schema for {}, skipping.", (study_submitter_id,))
            else:
                console_out("Updating table metadata for {}.", (study_submitter_id,))
                update_table_metadata(bio_table_id, table_metadata)

    if 'build_gene_tsv' in steps:
        gene_name_list = build_proteome_gene_name_list()
        gene_tsv_path = get_scratch_fp(BQ_PARAMS, get_table_name(BQ_PARAMS['GENE_TABLE']) + '.tsv')

        build_gene_tsv(gene_name_list, gene_tsv_path, append=API_PARAMS['RESUME_GENE_TSV'])
        upload_to_bucket(BQ_PARAMS, gene_tsv_path)

    if 'build_gene_table' in steps:
        gene_tsv_path = get_scratch_fp(BQ_PARAMS, get_table_name(BQ_PARAMS['GENE_TABLE']) + '.tsv')

        with open(gene_tsv_path, 'r') as tsv_file:
            gene_reader = csv.reader(tsv_file, delimiter='\t')

            passed_first_row = False
            num_columns = None

            for row in gene_reader:
                if not passed_first_row:
                    num_columns = len(row)
                    passed_first_row = True
                    print(row)
                    continue

                if len(row) != num_columns:
                    print(row)

        build_table_from_tsv(BQ_PARAMS['DEV_PROJECT'],
                             BQ_PARAMS['DEV_META_DATASET'],
                             BQ_PARAMS['GENE_TABLE'])

    if 'modify_gene_table' in steps:
        client = bigquery.Client()

        gene_table_id = get_table_id(BQ_PARAMS['DEV_PROJECT'],
                                     BQ_PARAMS['DEV_META_DATASET'],
                                     get_table_name(BQ_PARAMS['GENE_TABLE']))

        old_gene_table_id = get_table_id(BQ_PARAMS['DEV_PROJECT'],
                                     BQ_PARAMS['DEV_META_DATASET'],
                                     get_table_name(BQ_PARAMS['GENE_TABLE'], 'old'))

        gene_table = client.get_table(gene_table_id)
        prev_gene_table_schema = gene_table.schema
        new_gene_table_schema = prev_gene_table_schema[:]

        uniprot_schema_field = bigquery.SchemaField('uniprot_accession_nums', 'STRING')

        new_gene_table_schema.insert(-2, uniprot_schema_field)

        assert len(new_gene_table_schema) == len(prev_gene_table_schema) + 1

        copy_bq_table(BQ_PARAMS, gene_table_id, old_gene_table_id, replace_table=True)

        gene_table.schema = new_gene_table_schema

        gene_table = client.update_table(gene_table, ["schema"])

        print("new schema: {}".format(gene_table.schema))


    if 'analyze_gene_table' in steps:
        table_name = get_table_name(BQ_PARAMS['GENE_TABLE'])
        table_id = get_table_id(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['DEV_META_DATASET'], table_name)

        query = """
        SELECT gene_name, gene_id, proteins 
        FROM `{}`
        """.format(table_id)

        res = get_query_results(query)

        max_uniprot_count = 0
        count_tally_list = [0] * 70

        for row in res:
            curr_uniprot_id_count = 0

            protein_list = row.get('proteins').split(';')

            for protein in protein_list:
                uniprot_table = get_table_name(BQ_PARAMS['UNIPROT_MAPPING_TABLE'], include_release=False)
                uniprot_table_id = get_table_id(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['DEV_META_DATASET'], uniprot_table)

                if is_uniprot_accession_number(protein):
                    query = """
                    SELECT COUNT(*) as cnt
                    FROM {}
                    WHERE uniprot_kb_accession = '{}'
                    """.format(uniprot_table_id, protein)

                    uniprot_res = get_query_results(query)

                    for uniprot_row in uniprot_res:
                        count = uniprot_row.get('cnt')
                        if count > 0:
                            curr_uniprot_id_count += 1
                        break

            count_tally_list[curr_uniprot_id_count] = count_tally_list[curr_uniprot_id_count] + 1

            print(count_tally_list)

            max_uniprot_count = max(max_uniprot_count, curr_uniprot_id_count)

        print("max uniprot ids: {}".format(max_uniprot_count))

    if 'build_cases_aliquots_jsonl' in steps:
        jsonl_start = time.time()

        csa_jsonl_fp = get_scratch_fp(BQ_PARAMS, get_table_name(BQ_PARAMS['CASE_ALIQUOT_TABLE']) + '.jsonl')
        build_cases_aliquots_jsonl(csa_jsonl_fp)
        upload_to_bucket(BQ_PARAMS, csa_jsonl_fp)

        jsonl_end = time.time() - jsonl_start
        console_out("Cases Aliquots table jsonl file created in {0}!\n", (format_seconds(jsonl_end),))

    if 'build_cases_aliquots_table' in steps:
        # build_table_from_tsv(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['DEV_META_DATASET'], BQ_PARAMS['CASE_ALIQUOT_TABLE'])
        build_table_from_jsonl(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['DEV_META_DATASET'], BQ_PARAMS['CASE_ALIQUOT_TABLE'])

    if 'build_final_quant_tables' in steps:
        csa_table_id = 'isb-project-zero.PDC_metadata.case_aliquot_run_metadata_mapping_2020_09'
        quant_table_id = 'isb-project-zero.PDC.quant_CPTAC_GBM_Discovery_Study_Proteome_2020_09'
        gene_table_id = 'isb-project-zero.PDC_metadata.genes_pdc_api_2020_09'

        combined_query = """
            WITH aliquot_run 
            AS (
              SELECT case_id, samp.sample_id, aliq.aliquot_id, aliq.aliquot_submitter_id, ar.aliquot_run_metadata_id
              FROM `{0}`
              CROSS JOIN UNNEST(samples) AS samp
              CROSS JOIN UNNEST(samp.aliquots) AS aliq
              CROSS JOIN UNNEST(aliq.aliquot_run_metadata) AS ar)
            SELECT  a.case_id, a.sample_id, a.aliquot_id, a.aliquot_submitter_id, 
                    q.aliquot_run_metadata_id, q.study_id, q.gene, q.log2_ratio, 
                    g.gene_id, g.NCBI_gene_id, g.authority, g.description, g.organism, g.chromosome, g.locus, g.proteins, g.assays
            FROM `{1}` q
            LEFT JOIN aliquot_run a
              ON a.aliquot_run_metadata_id = q.aliquot_run_metadata_id
            LEFT JOIN `{2}` g
              ON g.gene_name = q.gene
        """.format(csa_table_id, quant_table_id, gene_table_id)

    if 'build_biospecimen_tsv' in steps:
        # *** NOTE: DATA MAY BE INCOMPLETE CURRENTLY in PDC API

        biospecimen_tsv_path = get_scratch_fp(BQ_PARAMS,
                                              get_table_name(BQ_PARAMS['BIOSPECIMEN_TABLE'], 'duplicates') + '.tsv')
        build_biospecimen_tsv(study_ids_list, biospecimen_tsv_path)
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

    if 'build_aliquot_sample_study_maps' in steps:
        aliq_study_table = get_table_name('map_aliquot_study')
        aliq_study_table_id = get_table_id(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['DEV_META_DATASET'], aliq_study_table)
        load_table_from_query(BQ_PARAMS, aliq_study_table_id, map_biospecimen_query('aliquot_id', 'study_id'))

        samp_study_table = get_table_name('map_sample_study')
        sample_study_table_id = get_table_id(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['DEV_META_DATASET'], samp_study_table)
        load_table_from_query(BQ_PARAMS, sample_study_table_id, map_biospecimen_query('sample_id', 'study_id'))

        sample_aliq_table = get_table_name('map_sample_aliquot')
        sample_aliq_table_id = get_table_id(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['DEV_META_DATASET'], sample_aliq_table)
        load_table_from_query(BQ_PARAMS, sample_aliq_table_id, map_biospecimen_query('sample_id', 'aliquot_id'))

    if 'build_nested_biospecimen_dict_and_jsonl' in steps:
        build_nested_biospecimen_jsonl()

    if 'build_nested_biospecimen_table' in steps:
        build_table_from_jsonl(
            BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['DEV_META_DATASET'], BQ_PARAMS['CASE_STUDY_BIOSPECIMEN_TABLE'])

    if 'build_per_study_file_jsonl' in steps:
        build_per_study_file_jsonl(study_ids_list)

    if 'build_per_study_file_table' in steps:
        build_table_from_jsonl(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['DEV_META_DATASET'], BQ_PARAMS['TEMP_FILE_TABLE'])

    if 'build_file_metadata_jsonl' in steps:
        file_ids = get_file_ids()
        build_file_metadata_jsonl(file_ids)

    if 'build_file_metadata_table' in steps:
        build_table_from_jsonl(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['DEV_META_DATASET'], BQ_PARAMS['FILES_TABLE'])

    if 'build_cases_jsonl' in steps:
        build_cases_jsonl()

    if 'build_cases_table' in steps:
        build_table_from_jsonl(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['DEV_META_DATASET'],
                               BQ_PARAMS['CASE_METADATA_TABLE'])

    if 'build_case_metadata_jsonl' in steps:
        cases_list = list()
        cases_rows = get_query_results(get_cases_data())

        for case_row in cases_rows:
            keys = case_row.keys()

            case_dict = dict()

            for key in keys:
                case_dict[key] = case_row[key]

            cases_list.append(case_dict)

        build_case_metadata_jsonl(cases_list)

    if 'build_case_metadata_table' in steps:
        pass
        # build_table_from_jsonl(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['DEV_META_DATASET'], BQ_PARAMS['CASES_TABLE'])

    if 'build_uniprot_tsv' in steps:
        uniprot_dest_file = BQ_PARAMS['UNIPROT_MAPPING_TABLE'] + '.tsv'
        uniprot_dest_fp = get_scratch_fp(BQ_PARAMS, uniprot_dest_file)
        build_uniprot_tsv(uniprot_dest_fp)
        upload_to_bucket(BQ_PARAMS, uniprot_dest_fp)

    if 'build_uniprot_table' in steps:
        table_name = BQ_PARAMS['UNIPROT_MAPPING_TABLE']
        table_id = get_table_id(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['DEV_META_DATASET'], table_name)
        console_out("Building {0}... ", (table_id,))
        schema_filename = '{}.json'.format(table_id)
        schema, metadata = from_schema_file_to_obj(BQ_PARAMS, schema_filename)
        tsv_name = '{}.tsv'.format(table_name)
        create_and_load_tsv_table(BQ_PARAMS, tsv_name, schema, table_id, null_marker=BQ_PARAMS['NULL_MARKER'])
        console_out("Uniprot table built!")

    end = time.time() - start
    console_out("Finished program execution in {}!\n", (format_seconds(end),))


if __name__ == '__main__':
    main(sys.argv)
