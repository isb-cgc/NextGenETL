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
import math
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
    }} }}
    """.format(study_id)


"""
def get_study_payload(study_id, pdc_study_id, study_submitter_id):
    query_str = ('\"query study ($study_id: String, '
                 '$pdc_study_id: String, '
                 '$study_submitter_id: String) { '
                 'study (study_id: $study_id, '
                 'pdc_study_id: $pdc_study_id, '
                 'study_submitter_id: $study_submitter_id) { '
                 'pdc_study_id '
                 'study_id '
                 'study_submitter_id '
                 'study_name '
                 'disease_type '
                 'primary_site '
                 'cases_count '
                 'aliquots_count '
                 '} '
                 '}\"'
                 )

    study_vars = ("{{   \"study_id\": \"{}\", "
                  "   \"pdc_study_id\": \"{}\", "
                  "   \"study_submitter_id\": \"{}\"}}"
                  ).format(study_id, pdc_study_id, study_submitter_id)

    payload = '{{ \"query\": {}, \"variables\": {} }}'.format(query_str, study_vars)

    return payload
"""


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

                study_query = make_study_query(study_dict['study_id'])

                study_metadata = get_graphql_api_response(API_PARAMS, query=study_query)

                for k, v in study_metadata['data']['study'][0].items():
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


def make_quant_data_matrix_query(study_submitter_id, data_type):
    return '{{ quantDataMatrix(study_submitter_id: \"{}\" data_type: \"{}\") }}'.format(study_submitter_id, data_type)


def get_table_name(prefix, suffix=None):
    if not suffix:
        table_name = "{}_{}".format(prefix, BQ_PARAMS['RELEASE'])
    else:
        table_name = "{}_{}_{}".format(prefix, suffix, BQ_PARAMS['RELEASE'])

    return re.sub('[^0-9a-zA-Z_]+', '_', table_name)


def get_table_id(project, dataset, table_name):
    return "{}.{}.{}".format(project, dataset, table_name)


def get_and_write_quant_data(study_id_dict, data_type, tsv_fp):
    study_submitter_id = study_id_dict['study_submitter_id']
    study_id = study_id_dict['study_id']
    lines_written = 0

    res_json = get_graphql_api_response(API_PARAMS,
                                        query=make_quant_data_matrix_query(study_submitter_id, data_type),
                                        fail_on_error=False)

    if not res_json or not res_json['data']['quantDataMatrix']:
        return lines_written

    aliquot_metadata = list()

    id_row = res_json['data']['quantDataMatrix'].pop(0)
    id_row.pop(0)  # remove gene column header string

    # process first row, which gives us the aliquot ids and idx positions
    for el in id_row:
        split_el = el.split(':')
        aliquot_run_metadata_id = split_el[0]
        aliquot_submitter_id = split_el[1]

        aliquot_metadata.append(
            {
                "study_id": study_id,
                "aliquot_run_metadata_id": aliquot_run_metadata_id,
                "aliquot_submitter_id": aliquot_submitter_id
            }
        )

    # iterate over each gene row and add to the correct aliquot_run obj
    with open(tsv_fp, 'w') as fh:
        fh.write("{}\t{}\t{}\t{}\t{}\n".format(
            'aliquot_run_metadata_id',
            'aliquot_submitter_id',
            'study_id',
            'gene',
            'log2_ratio')
        )

        for row in res_json['data']['quantDataMatrix']:
            gene = row.pop(0)

            for i, log2_ratio in enumerate(row):
                fh.write("{}\t{}\t{}\t{}\t{}\n".format(
                    aliquot_metadata[i]['aliquot_run_metadata_id'],
                    aliquot_metadata[i]['aliquot_submitter_id'],
                    aliquot_metadata[i]['study_id'],
                    gene,
                    log2_ratio)
                )

                lines_written += 1

    return lines_written


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


def get_study_ids():
    table_id = get_table_id(BQ_PARAMS['DEV_PROJECT'],
                            BQ_PARAMS['DEV_META_DATASET'],
                            get_table_name(BQ_PARAMS['STUDIES_TABLE']))

    return """
    SELECT study_id, study_submitter_id, pdc_study_id, aliquots_count, cases_count
    FROM  `{}`
    """.format(table_id)


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
    table_name = "{}_{}_{}".format(BQ_PARAMS['QUANT_DATA_TABLE'], BQ_PARAMS['RELEASE'], proteome_study)
    table_id = '{}.{}.{}'.format(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['DEV_DATASET'], table_name)

    return """
        SELECT DISTINCT(gene)
        FROM `{}`
    """.format(table_id)


def add_gene_names_per_study(proteome_study, gene_set):
    results = get_query_results(make_gene_name_set_query(proteome_study))

    for row in results:
        gene_set.add(row['gene'])

    return gene_set


def build_proteome_gene_name_set():
    console_out("Building proteome gene name tsv!")

    proteome_studies = API_PARAMS['PROTEOME_STUDIES']
    gene_name_set = set()

    for proteome_study in proteome_studies:
        console_out("Add gene names from {0}... ", (proteome_study,), end="")
        add_gene_names_per_study(proteome_study, gene_name_set)
        console_out("new set size: {0}", (len(gene_name_set),))

    return gene_name_set


def update_ron_gene_table():
    '''
    SELECT gene_name
    FROM `isb-project-zero.PDC_metadata.ron_spreadsheet_genes_2020_01`
    WHERE gene_name like '%|%'
    '''


def make_gene_query(gene_name):
    return '''
    {{ 
        geneSpectralCount(gene_name: \"{}\") {{
            gene_name 
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


def build_gene_tsv(gene_name_set, gene_tsv, append=False):
    if append:
        console_out("Resuming geneSpectralCount API calls. ", end='')
        with open(gene_tsv, 'r') as tsv_file:
            saved_genes = set()
            gene_reader = csv.reader(tsv_file, delimiter='\t')

            passed_first_row = False

            for row in gene_reader:
                if not passed_first_row:
                    passed_first_row = True
                    continue

                saved_genes.add(row[0])

        gene_name_set = gene_name_set - saved_genes

        remaining_genes = len(gene_name_set)

        if remaining_genes == 0:
            console_out("{} gene API calls remaining--skipping step.", (remaining_genes,))
            return
        else:
            console_out("{} gene API calls remaining.", (remaining_genes,))

    file_mode = 'a' if append else 'w'

    with open(gene_tsv, file_mode) as gene_fh:
        if not append:
            gene_fh.write("{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n".format('gene_name',
                                                                    'authority',
                                                                    'description',
                                                                    'organism',
                                                                    'chromosome',
                                                                    'locus',
                                                                    'proteins',
                                                                    'assays'))

        count = 0

        no_spectral_count_set = set()
        empty_spectral_count_set = set()

        for gene_name in gene_name_set:
            count += 1
            json_res = get_graphql_api_response(API_PARAMS, make_gene_query(gene_name))
            # time.sleep(1)  # need a delay to avoid making too many api requests and getting 500 server error

            gene = json_res['data']['geneSpectralCount']

            if not gene:
                console_out("No geneSpectralCount data found for {0}", (gene_name,))
                no_spectral_count_set.add(gene_name)
                continue
            elif not gene['gene_name']:
                console_out("Empty geneSpectralCount data found for {0}", (gene_name,))
                empty_spectral_count_set.add(gene_name)
                continue
            else:
                if count % 50 == 0:
                    console_out("Added {0} genes", (count,))

            for key in gene.keys():
                gene[key].strip()

            gene_fh.write("{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n".format(gene['gene_name'],
                                                                    gene['authority'],
                                                                    gene['description'],
                                                                    gene['organism'],
                                                                    gene['chromosome'],
                                                                    gene['locus'],
                                                                    gene['proteins'],
                                                                    gene['assays']))


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
    '''.format(offset, limit)


def build_cases_samples_aliquots_tsv(csa_tsv):
    console_out("Building cases_samples_aliquots tsv!")

    with open(csa_tsv, 'w') as csa_fh:
        csa_fh.write("{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n".format(
            'case_id',
            'case_submitter_id',
            'external_case_id',
            'sample_id',
            'sample_submitter_id',
            'aliquot_id',
            'aliquot_submitter_id',
            'aliquot_run_metadata_id')
        )

    pages_res = get_graphql_api_response(API_PARAMS, make_total_cases_aliquots_query())
    total_rows = pages_res['data']['paginatedCasesSamplesAliquots']['total']
    pages = math.ceil(total_rows / API_PARAMS['CSA_LIMIT'])

    with open(csa_tsv, 'a') as csa_fh:
        for i in range(pages):
            offset = API_PARAMS['CSA_LIMIT'] * i
            console_out("Getting CasesSamplesAliquots results from offset {0}... ", (offset,), end='')

            json_res = get_graphql_api_response(API_PARAMS, make_cases_aliquots_query(offset, API_PARAMS['CSA_LIMIT']))

            for case in json_res['data']['paginatedCasesSamplesAliquots']['casesSamplesAliquots']:
                case_submitter_id = case['case_submitter_id']
                case_id = case['case_id']
                external_case_id = case['external_case_id']

                for sample in case['samples']:
                    sample_submitter_id = sample['sample_submitter_id']
                    sample_id = sample['sample_id']

                    sample_row = "{}\t{}\t{}\t{}\t{}\t".format(case_id,
                                                               case_submitter_id,
                                                               external_case_id,
                                                               sample_id,
                                                               sample_submitter_id)

                    if len(sample['aliquots']) == 0:
                        # no aliquot_id associated with sample_id
                        aliquot_row = "{}\t{}\t{}\n".format("\\N", '\\N', '\\N')
                        run_row = sample_row + aliquot_row
                        csa_fh.write(run_row)
                    else:
                        for aliquots in sample['aliquots']:
                            aliq_row = "{}\t{}\t".format(aliquots['aliquot_id'], aliquots['aliquot_submitter_id'])
                            aliquot_row = sample_row + aliq_row

                            if len(aliquots['aliquot_run_metadata']) == 0:
                                # no aliquot_run_metadata_id associated with aliquot_id
                                run_meta_id_str = "{}\n".format("\\N")
                                run_row = aliquot_row + run_meta_id_str
                                csa_fh.write(run_row)
                            else:
                                for aliquot_run_metadata in aliquots['aliquot_run_metadata']:
                                    run_meta_id_str = "{}\n".format(aliquot_run_metadata['aliquot_run_metadata_id'])
                                    run_row = aliquot_row + run_meta_id_str
                                    csa_fh.write(run_row)

            console_out("written to tsv file.")


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
        bio_fh.write("{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n".format(
            'aliquot_id',
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
            'taxon'
        ))

        for study in study_ids_list:
            json_res = get_graphql_api_response(API_PARAMS, make_biospecimen_per_study_query(study['study_id']))

            aliquots_cnt = study['aliquots_count']
            res_size = len(json_res['data']['biospecimenPerStudy'])

            has_quant_tbl = has_quant_table(study['study_submitter_id'])

            console_out("study_id: {}, study_submitter_id: {}, has_quant_table: {}, "
                        "aliquots_count: {}, api result size: {}",
                        (study['study_id'], study['study_submitter_id'], has_quant_tbl, aliquots_cnt, res_size))

            for biospecimen in json_res['data']['biospecimenPerStudy']:
                bio_fh.write("{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n".format(
                    biospecimen['aliquot_id'],
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
                    biospecimen['taxon']
                ))


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

    file_metadata_jsonl_path = get_scratch_fp(BQ_PARAMS, get_table_name(BQ_PARAMS['FILE_METADATA']) + '.jsonl')

    write_list_to_jsonl(file_metadata_jsonl_path, file_metadata_list)
    upload_to_bucket(BQ_PARAMS, file_metadata_jsonl_path)

    jsonl_end = time.time() - jsonl_start
    console_out("File metadata jsonl file created in {0}!\n", (format_seconds(jsonl_end),))


def build_case_per_file_query(file_id):
    return """    
    {{ casePerFile(file_id:\"{}\") {{ 
        file_id 
        case_id 
        case_submitter_id 
        }}
    }}
    """.format(file_id)


def build_case_per_file_jsonl(file_id_list):
    jsonl_start = time.time()
    case_file_list = []
    has_case_cnt = 0
    null_case_cnt = 0

    print("(had case count, no case count)")
    for file in file_id_list:
        file_id = file['file_id']
        cases_res = get_graphql_api_response(API_PARAMS, build_case_per_file_query(file_id))

        if 'casePerFile' in cases_res['data'] and cases_res['data']['casePerFile']['case_id']:
            has_case_cnt += 1

            for case_file_row in cases_res['data']['casePerFile']:
                print(case_file_row)
                case_file_list.append(case_file_row)
        else:
            null_case_cnt += 1
        print("({}, {})".format(has_case_cnt, null_case_cnt))

    if has_case_cnt == 0:
        if len(case_file_list) > 0:
            print("Error, different counts in build_case_per_file_jsonl.")
        print("No case_id returned in casePerFile! {} file_ids tried".format(str(len(file_id_list))))
        return

    case_per_file_jsonl_fp = get_scratch_fp(BQ_PARAMS, get_table_name(BQ_PARAMS['TEMP_FILE_TABLE']) + '.jsonl')
    write_list_to_jsonl(case_per_file_jsonl_fp, case_file_list)
    upload_to_bucket(BQ_PARAMS, case_per_file_jsonl_fp)

    jsonl_end = time.time() - jsonl_start
    list_size = str(len(case_file_list))

    console_out("casePerFile jsonl created in {0}, {} rows added!\n", (format_seconds(jsonl_end), list_size))


def build_table_from_tsv(project, dataset, table_prefix, table_suffix=None):
    build_start = time.time()

    table_name = get_table_name(table_prefix, table_suffix)
    table_id = get_table_id(project, dataset, table_name)
    console_out("Building {0}... ", (table_id,))

    schema_filename = '{}.json'.format(table_id)
    schema, metadata = from_schema_file_to_obj(BQ_PARAMS, schema_filename)

    tsv_name = '{}.tsv'.format(table_name)
    create_and_load_tsv_table(BQ_PARAMS, tsv_name, schema, table_id)

    build_end = time.time() - build_start
    console_out("Table built in {0}!\n", (format_seconds(build_end),))


def build_table_from_jsonl(project, dataset, table_prefix, table_suffix=None):
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

    try:
        global API_PARAMS, BQ_PARAMS
        API_PARAMS, BQ_PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        has_fatal_error(str(err), ValueError)

    if 'build_studies_jsonl' in steps:
        jsonl_start = time.time()

        json_res = get_graphql_api_response(API_PARAMS, make_all_programs_query())
        studies = create_studies_dict(json_res)

        filename = get_table_name(BQ_PARAMS['STUDIES_TABLE']) + '.jsonl'
        studies_fp = get_scratch_fp(BQ_PARAMS, filename)

        write_list_to_jsonl(studies_fp, studies)
        upload_to_bucket(BQ_PARAMS, studies_fp)

        jsonl_end = time.time() - jsonl_start

        console_out("Studies table jsonl file created in {0}!\n", (format_seconds(jsonl_end),))

    if 'build_studies_table' in steps:
        build_table_from_jsonl(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['DEV_META_DATASET'], BQ_PARAMS['STUDIES_TABLE'])

    study_ids_list = list()
    study_ids = get_query_results(get_study_ids())

    for study in study_ids:
        study_ids_list.append(dict(study.items()))

    if 'build_quant_tsvs' in steps:
        tsv_start = time.time()

        for study_id_dict in study_ids_list:
            study_submitter_id = study_id_dict['study_submitter_id']
            filename = get_table_name(BQ_PARAMS['QUANT_DATA_TABLE'], study_submitter_id) + '.tsv'
            quant_tsv_fp = get_scratch_fp(BQ_PARAMS, filename)
            lines_written = get_and_write_quant_data(study_id_dict, 'log2_ratio', quant_tsv_fp)

            console_out("{0} lines written for {1}", (lines_written, study_submitter_id))

            if lines_written > 0:
                upload_to_bucket(BQ_PARAMS, quant_tsv_fp)
                console_out("{0} uploaded to Google Cloud bucket!", (filename,))  # os.remove(quant_tsv_fp)
                os.remove(quant_tsv_fp)

        tsv_end = time.time() - tsv_start
        console_out("Quant table tsv files created in {0}!\n", (format_seconds(tsv_end),))

    if 'build_quant_tables' in steps:
        console_out("Building quant tables...")
        blob_files = get_quant_files()

        for study_id_dict in study_ids_list:
            study_submitter_id = study_id_dict['study_submitter_id']
            filename = get_table_name(BQ_PARAMS['QUANT_DATA_TABLE'], study_submitter_id) + '.tsv'

            if filename not in blob_files:
                console_out('skipping quant table build for {} (gs://{}/{}/{} not found).', (
                    study_submitter_id, BQ_PARAMS['WORKING_BUCKET'], BQ_PARAMS['WORKING_BUCKET_DIR'], filename))
            else:
                build_table_from_tsv(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['DEV_DATASET'],
                                     BQ_PARAMS['QUANT_DATA_TABLE'], study_submitter_id)

    if 'update_quant_tables_metadata' in steps:
        for study_id_dict in study_ids_list:
            study_submitter_id = study_id_dict['study_submitter_id']
            bio_table_name = get_table_name(BQ_PARAMS['QUANT_DATA_TABLE'], study_submitter_id)
            bio_table_id = get_table_id(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['DEV_DATASET'], bio_table_name)
            schema_filename = bio_table_id + '.json'
            schema, table_metadata = from_schema_file_to_obj(BQ_PARAMS, schema_filename)

            if not table_metadata:
                console_out("No schema for {}, skipping", (study_submitter_id,))
            else:
                console_out("Updating table metadata for {}", (study_submitter_id,))
                update_table_metadata(bio_table_id, table_metadata)

    if 'build_gene_tsv' in steps:
        gene_name_set = build_proteome_gene_name_set()

        gene_tsv_path = get_scratch_fp(BQ_PARAMS, get_table_name(BQ_PARAMS['GENE_TABLE']) + '.tsv')

        build_gene_tsv(gene_name_set, gene_tsv_path, append=API_PARAMS['RESUME_GENE_TSV'])
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

        build_table_from_tsv(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['DEV_META_DATASET'], BQ_PARAMS['GENE_TABLE'])

    if 'build_cases_samples_aliquots_tsv' in steps:
        csa_tsv_path = get_scratch_fp(BQ_PARAMS, get_table_name(BQ_PARAMS['CASE_ALIQUOT_TABLE']) + '.tsv')
        build_cases_samples_aliquots_tsv(csa_tsv_path)
        upload_to_bucket(BQ_PARAMS, csa_tsv_path)

    if 'build_cases_samples_aliquots_table' in steps:
        build_table_from_tsv(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['DEV_META_DATASET'], BQ_PARAMS['CASE_ALIQUOT_TABLE'])

    if 'build_biospecimen_tsv' in steps:
        biospecimen_tsv_path = get_scratch_fp(BQ_PARAMS,
                                              get_table_name(BQ_PARAMS['BIOSPECIMEN_TABLE'], 'duplicates') + '.tsv')
        build_biospecimen_tsv(study_ids_list, biospecimen_tsv_path)
        upload_to_bucket(BQ_PARAMS, biospecimen_tsv_path)

    if 'build_biospecimen_table' in steps:
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
        print("Build per-study file table")
        build_table_from_jsonl(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['DEV_META_DATASET'], BQ_PARAMS['TEMP_FILE_TABLE'])

    if 'build_file_metadata_jsonl' in steps:
        file_ids = get_file_ids()
        build_file_metadata_jsonl(file_ids)

    if 'build_file_metadata_table' in steps:
        print("Build file metadata table")
        build_table_from_jsonl(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['DEV_META_DATASET'], BQ_PARAMS['FILE_METADATA'])

    if 'build_case_per_file_jsonl' in steps:
        file_ids = get_file_ids()
        build_case_per_file_jsonl(file_ids)

    end = time.time() - start
    console_out("Finished program execution in {0}!\n", (format_seconds(end),))


if __name__ == '__main__':
    main(sys.argv)
