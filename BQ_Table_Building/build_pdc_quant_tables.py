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


def make_quant_data_matrix_query(study_submitter_id, data_type):
    return '{{ quantDataMatrix(study_submitter_id: \"{}\" data_type: \"{}\") }}'.format(study_submitter_id, data_type)


def get_table_name(prefix, suffix=None):
    if not suffix:
        table_name = "{}_{}".format(prefix, BQ_PARAMS['RELEASE'])
    else:
        table_name = "{}_{}_{}".format(prefix, BQ_PARAMS['RELEASE'], suffix)

    return re.sub('[^0-9a-zA-Z_]+', '_', table_name)


def get_table_id(project, dataset, table_name):
    return "{}.{}.{}".format(project, dataset, table_name)


def get_and_write_quant_data(study_id_dict, data_type, tsv_fp):
    study_submitter_id = study_id_dict['study_submitter_id']
    study_id = study_id_dict['study_id']
    lines_written = 0

    res_json = get_graphql_api_response(API_PARAMS, query=make_quant_data_matrix_query(study_submitter_id, data_type))

    if not res_json['data']['quantDataMatrix']:
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


def get_study_ids():
    table_id = get_table_id(BQ_PARAMS['DEV_PROJECT'],
                            BQ_PARAMS['DEV_META_DATASET'],
                            get_table_name(BQ_PARAMS['STUDIES_TABLE']))

    return """
    SELECT study_id, study_submitter_id
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

        print(no_spectral_count_set)
        print(empty_spectral_count_set)


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
    pages_res = get_graphql_api_response(API_PARAMS, make_cases_aliquots_query(0, API_PARAMS['CSA_LIMIT']))

    pages = pages_res['data']['paginatedCasesSamplesAliquots']['pagination']['pages']

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

    with open(csa_tsv, 'a') as csa_fh:
        for i in range(pages):
            offset = 100 * i
            console_out("Getting CasesSamplesAliquots results from offset {0}... ", (offset,), end='')

            json_res = get_graphql_api_response(API_PARAMS, make_cases_aliquots_query(offset, API_PARAMS['CSA_LIMIT']))

            for case in json_res['data']['paginatedCasesSamplesAliquots']['casesSamplesAliquots']:
                case_submitter_id = case['case_submitter_id']
                case_id = case['case_id']
                external_case_id = case['external_case_id']

                for sample in case['samples']:
                    sample_submitter_id = sample['sample_submitter_id']
                    sample_id = sample['sample_id']

                    for aliquots in sample['aliquots']:
                        aliquot_submitter_id = aliquots['aliquot_submitter_id']
                        aliquot_id = aliquots['aliquot_id']

                        for aliquot_run_metadata in aliquots['aliquot_run_metadata']:
                            aliquot_run_metadata_id = aliquot_run_metadata[
                                'aliquot_run_metadata_id']

                            row = """{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n""".format(
                                case_id,
                                case_submitter_id,
                                external_case_id,
                                sample_id,
                                sample_submitter_id,
                                aliquot_id,
                                aliquot_submitter_id,
                                aliquot_run_metadata_id)

                            csa_fh.write(row)

            console_out("written to tsv file.")


def make_biospecimen_per_study_query(study_id):
    return '''
    {{ biospecimenPerStudy( study_id: \"{}\") {{
        aliquot_id sample_id case_id aliquot_submitter_id sample_submitter_id case_submitter_id 
        aliquot_status case_status sample_status project_name sample_type disease_type primary_site pool taxon
    }}
    }}'''.format(study_id)


def build_biospecimen_tsv(study_ids_list, biospecimen_tsv):
    console_out("Building biospecimen tsv!")

    with open(biospecimen_tsv, 'w') as bio_fh:
        bio_fh.write("{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n".format(
            'aliquot_id',
            'sample_id',
            'case_id',
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

            for biospecimen in json_res['data']['biospecimenPerStudy']:
                bio_fh.write("{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n".format(
                    biospecimen['aliquot_id'],
                    biospecimen['sample_id'],
                    biospecimen['case_id'],
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


def build_table(project, dataset, table_prefix, table_suffix=None):
    build_start = time.time()

    table_name = get_table_name(table_prefix, table_suffix)
    table_id = get_table_id(project, dataset, table_name)
    console_out("Building {0}... ", (table_id,))

    schema_filename = '{}.json'.format(table_id)
    schema, metadata = from_schema_file_to_obj(BQ_PARAMS, schema_filename)

    tsv_name = '{}.tsv'.format(table_name)
    create_and_load_tsv_table(BQ_PARAMS, tsv_name, schema, table_id)

    build_end = time.time() - build_start
    console_out("Table built in {0}!\n", format_seconds((build_end,)))


def main(args):
    start = time.time()

    try:
        global API_PARAMS, BQ_PARAMS
        API_PARAMS, BQ_PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        has_fatal_error(str(err), ValueError)

    study_ids_list = list()
    study_ids = get_query_results(get_study_ids())

    for study in study_ids:
        study_ids_list.append(dict(study.items()))

    if 'build_quant_tsv' in steps:
        jsonl_start = time.time()

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

        jsonl_end = time.time() - jsonl_start
        console_out("Quant table jsonl files created in {0}!\n", (format_seconds(jsonl_end),))

    if 'build_master_quant_table' in steps:
        blob_files = get_quant_files()

        for study_id_dict in study_ids_list:
            study_submitter_id = study_id_dict['study_submitter_id']
            filename = get_table_name(BQ_PARAMS['QUANT_DATA_TABLE'], study_submitter_id) + '.tsv'

            if filename not in blob_files:
                console_out('{0} not found in gs://{1}/{2}', (filename,
                                                              BQ_PARAMS['WORKING_BUCKET'],
                                                              BQ_PARAMS['WORKING_BUCKET_DIR']))
            else:
                build_table(BQ_PARAMS['DEV_PROJECT'],
                            BQ_PARAMS['DEV_DATASET'],
                            BQ_PARAMS['QUANT_DATA_TABLE'],
                            study_submitter_id)

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

        exit()

        build_table(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['DEV_META_DATASET'], BQ_PARAMS['GENE_TABLE'])

    if 'build_cases_samples_aliquots_tsv' in steps:
        csa_tsv_path = get_scratch_fp(BQ_PARAMS, get_table_name(BQ_PARAMS['CASE_ALIQUOT_TABLE']) + '.tsv')
        build_cases_samples_aliquots_tsv(csa_tsv_path)
        upload_to_bucket(BQ_PARAMS, csa_tsv_path)

    if 'build_cases_samples_aliquots_table' in steps:
        build_table(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['DEV_META_DATASET'], BQ_PARAMS['CASE_ALIQUOT_TABLE'])

    if 'build_biospecimen_tsv' in steps:
        biospecimen_tsv_path = get_scratch_fp(BQ_PARAMS, get_table_name(BQ_PARAMS['BIOSPECIMEN_TABLE']) + '.tsv')
        build_biospecimen_tsv(study_ids_list, biospecimen_tsv_path)
        upload_to_bucket(BQ_PARAMS, biospecimen_tsv_path)

    if 'build_biospecimen_table' in steps:
        build_table(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['DEV_META_DATASET'], BQ_PARAMS['BIOSPECIMEN_TABLE'])

    end = time.time() - start
    console_out("Finished program execution in {0}!\n", format_seconds((end,)))


if __name__ == '__main__':
    main(sys.argv)
