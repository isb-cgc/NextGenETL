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
import os
from common_etl.utils import *

API_PARAMS = dict()
BQ_PARAMS = dict()
YAML_HEADERS = ('api_params', 'bq_params', 'steps')


def make_quant_data_matrix_query(study_submitter_id, data_type):
    return '{{ quantDataMatrix(study_submitter_id: \"{}\" data_type: \"{}\") }}'.format(
        study_submitter_id, data_type)


def get_quant_tsv_filename(study_submitter_id):
    return get_quant_table_name(study_submitter_id) + '.tsv'


def get_quant_table_name(study_submitter_id):
    study_submitter_id = study_submitter_id.replace('- ', '')
    study_submitter_id = study_submitter_id.replace('-', '_')
    filename = '_'.join(study_submitter_id.split(' '))
    return 'quant_' + BQ_PARAMS['RELEASE'] + '_' + filename


def get_and_write_quant_data(study_id_dict, data_type, tsv_fp):
    study_submitter_id = study_id_dict['study_submitter_id']
    study_id = study_id_dict['study_id']
    lines_written = 0

    res_json = get_graphql_api_response(API_PARAMS, query=make_quant_data_matrix_query(
        study_submitter_id, data_type))

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
            {"study_id": study_id, "aliquot_run_metadata_id": aliquot_run_metadata_id,
             "aliquot_submitter_id": aliquot_submitter_id
             })

    # iterate over each gene row and add to the correct aliquot_run obj
    with open(tsv_fp, 'w') as fh:
        fh.write("{}\t{}\t{}\t{}\t{}\n".format('study_id', 'aliquot_run_metadata_id',
                                               'aliquot_submitter_id', 'gene',
                                               'log2_ratio'))
        for row in res_json['data']['quantDataMatrix']:
            gene = row.pop(0)

            for i, log2_ratio in enumerate(row):
                fh.write("{}\t{}\t{}\t{}\t{}\n".format(aliquot_metadata[i]['study_id'],
                                                       aliquot_metadata[i][
                                                           'aliquot_run_metadata_id'],
                                                       aliquot_metadata[i][
                                                           'aliquot_submitter_id'], gene,
                                                       log2_ratio))

                lines_written += 1

    return lines_written


def get_study_ids():
    table_id = '{}.{}.studies_{}'.format(BQ_PARAMS['DEV_PROJECT'],
                                         BQ_PARAMS['DEV_META_DATASET'],
                                         BQ_PARAMS['RELEASE'])

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


def make_gene_set_query(proteome_study):
    table_name = "quant_{}_{}".format(BQ_PARAMS['RELEASE'], proteome_study)
    table_id = '{}.{}.{}'.format(BQ_PARAMS['DEV_PROJECT'], BQ_PARAMS['DEV_DATASET'],
                                 table_name)

    return """
        SELECT DISTINCT(gene)
        FROM `{}`
    """.format(table_id)


def build_gene_set(proteome_study, gene_set):
    results = get_query_results(make_gene_set_query(proteome_study))

    for row in results:
        gene_set.add(row['gene'])

    return gene_set


"""
https://pdc.cancer.gov/graphql?query={ paginatedCasesSamplesAliquots(offset:0 limit: 5) 
{ total casesSamplesAliquots { case_id case_submitter_id external_case_id 
tissue_source_site_code days_to_lost_to_followup disease_type index_date 
lost_to_followup primary_site samples { sample_id sample_submitter_id sample_type 
sample_type_id gdc_sample_id gdc_project_id biospecimen_anatomic_site composition 
current_weight days_to_collection days_to_sample_procurement 
diagnosis_pathologically_confirmed freezing_method initial_weight 
intermediate_dimension is_ffpe longest_dimension method_of_sample_procurement 
oct_embedded pathology_report_uuid preservation_method sample_type_id 
shortest_dimension time_between_clamping_and_freezing 
time_between_excision_and_freezing tissue_type tumor_code tumor_code_id 
tumor_descriptor aliquots { aliquot_id aliquot_submitter_id aliquot_quantity 
aliquot_volume amount analyte_type aliquot_run_metadata { aliquot_run_metadata_id } } } 
} pagination { count sort from page total pages size } } }

"""


def make_gene_query(gene_name):
    return '''{{ geneSpectralCount(gene_name: \"{}\") {{
        gene_id NCBI_gene_id authority description organism 
        chromosome locus proteins assays
    }}
    }}
    '''.format(gene_name)


def make_cases_samples_aliquots_query(offset, limit):
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


def get_cases_samples_aliquots(csa_tsv):
    pages_res = get_graphql_api_response(API_PARAMS, make_cases_samples_aliquots_query(0, API_PARAMS['CSA_LIMIT']))

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

            json_res = get_graphql_api_response(API_PARAMS,
                                                make_cases_samples_aliquots_query(offset, API_PARAMS['CSA_LIMIT']))

            paged_csas = json_res['data']['paginatedCasesSamplesAliquots']
            cases_samples_aliquots = paged_csas['casesSamplesAliquots']

            for case in cases_samples_aliquots:
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
                                case_submitter_id, external_case_id, sample_id,
                                sample_submitter_id, aliquot_id, aliquot_submitter_id,
                                aliquot_run_metadata_id)

                            csa_fh.write(row)
            console_out("written to tsv file.")


def main(args):
    start = time.time()

    try:
        global API_PARAMS, BQ_PARAMS
        API_PARAMS, BQ_PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        has_fatal_error(str(err), ValueError)

    # jsonl_output_file = 'quant_2020_09.jsonl'

    study_ids_list = list()
    study_ids = get_query_results(get_study_ids())

    for study in study_ids:
        study_ids_list.append(dict(study.items()))

    if 'build_quant_tsv' in steps:
        jsonl_start = time.time()

        for study_id_dict in study_ids_list:
            study_submitter_id = study_id_dict['study_submitter_id']
            filename = get_quant_tsv_filename(study_submitter_id)
            quant_tsv_fp = get_scratch_fp(BQ_PARAMS, filename)
            lines_written = get_and_write_quant_data(study_id_dict, 'log2_ratio',
                                                     quant_tsv_fp)

            console_out("\n{0} lines written for {1}",
                        (lines_written, study_submitter_id))

            if lines_written > 0:
                upload_to_bucket(BQ_PARAMS, quant_tsv_fp)
                console_out("{0} uploaded to Google cloud storage!",
                            (filename,))  # os.remove(quant_tsv_fp)
                os.remove(quant_tsv_fp)

        jsonl_end = time.time() - jsonl_start

        console_out("Quant table jsonl files created in {0:0.0f}s!\n", (jsonl_end,))

    if 'build_master_quant_table' in steps:
        blob_files = get_quant_files()

        for study_id_dict in study_ids_list:
            build_start = time.time()

            study_submitter_id = study_id_dict['study_submitter_id']
            filename = get_quant_tsv_filename(study_submitter_id)

            # filename = filename.replace('quant_', '') # todo remove

            if filename not in blob_files:
                print('{} not in gcp storage'.format(filename))
                continue

            table_name = get_quant_table_name(study_submitter_id)
            table_id = get_working_table_id(BQ_PARAMS, table_name)

            schema_filename = '{}.{}.quant_data_{}.json'.format(
                BQ_PARAMS['DEV_PROJECT'],
                BQ_PARAMS['DEV_DATASET'],
                BQ_PARAMS['RELEASE']
            )

            console_out("Building {0}!", (table_id,))
            schema, table_metadata = from_schema_file_to_obj(BQ_PARAMS, schema_filename)
            create_and_load_tsv_table(BQ_PARAMS, filename, schema, table_id)
            build_end = time.time() - build_start

            console_out("Quant table built in {0:0.0f}s!\n", (build_end,))

    if 'build_gene_table' in steps:
        # PDC API returning an error
        # String cannot represent value:
        # <Buffer f9 03 7c ab b8 14 11 e8 90 7f 0a 27 05 22 9b 82>
        proteome_studies = API_PARAMS['PROTEOME_STUDIES']
        gene_set = set()

        for proteome_study in proteome_studies:
            console_out("Add gene set for {0}", (proteome_study,))
            build_gene_set(proteome_study, gene_set)
            console_out("New gene set size: {}", (len(gene_set),))

        for gene_name in gene_set:
            json_res = get_graphql_api_response(API_PARAMS,
                                                make_gene_query(gene_name))

    csa_tsv = get_scratch_fp(BQ_PARAMS, 'cases_samples_aliquots.tsv')

    if 'build_cases_samples_aliquots_tsv' in steps:
        get_cases_samples_aliquots(csa_tsv)
        upload_to_bucket(BQ_PARAMS, csa_tsv)

    if 'build_cases_samples_aliquots_table' in steps:
        build_start = time.time()

        table_name = 'case_aliquot_run_metadata_mapping_' + BQ_PARAMS['RELEASE']
        table_id = "{}.{}.{}".format(
            BQ_PARAMS['DEV_PROJECT'],
            BQ_PARAMS['DEV_META_DATASET'],
            table_name
        )

        schema_filename = '{}.{}.case_aliquot_run_metadata_mapping_{}.json'.format(
            BQ_PARAMS['DEV_PROJECT'],
            BQ_PARAMS['DEV_DATASET'],
            BQ_PARAMS['RELEASE']
        )

        schema, metadata = from_schema_file_to_obj(BQ_PARAMS, schema_filename)
        create_and_load_tsv_table(BQ_PARAMS, csa_tsv, schema, table_id)
        build_end = time.time() - build_start

        console_out("case_aliquot_run_metadata_mapping table built in {0:0.0f}s!\n", (build_end,))

    end = time.time() - start
    if end < 100:
        console_out("Finished program execution in {0:0.0f}s!\n", (end,))
    else:
        console_out("Finished program execution in {0:0.0f}s!\n", (end,))


if __name__ == '__main__':
    main(sys.argv)
