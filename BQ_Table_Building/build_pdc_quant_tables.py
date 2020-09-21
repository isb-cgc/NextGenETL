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


def query_quant_data_matrix(study_submitter_id, data_type):
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

    res_json = get_graphql_api_response(API_PARAMS,
                                        query=query_quant_data_matrix(
                                            study_submitter_id,
                                            data_type))

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

        aliquot_metadata.append({
            "study_id": study_id,
            "aliquot_run_metadata_id": aliquot_run_metadata_id,
            "aliquot_submitter_id": aliquot_submitter_id})

    # iterate over each gene row and add to the correct aliquot_run obj
    with open(tsv_fp, 'w') as fh:
        fh.write("{}\t{}\t{}\t{}\t{}\n".format(
            'study_id',
            'aliquot_run_metadata_id',
            'aliquot_submitter_id',
            'gene',
            'log2_ratio'
            ))
        for row in res_json['data']['quantDataMatrix']:
            gene = row.pop(0)

            for i, log2_ratio in enumerate(row):
                fh.write("{}\t{}\t{}\t{}\t{}\n".format(
                    aliquot_metadata[i]['study_id'],
                    aliquot_metadata[i]['aliquot_run_metadata_id'],
                    aliquot_metadata[i]['aliquot_submitter_id'],
                    gene,
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
    table_id = '{}.{}.{}'.format(BQ_PARAMS['DEV_PROJECT'],
                                 BQ_PARAMS['DEV_DATASET'],
                                 table_name)

    return """
        SELECT gene
        FROM `{}`
    """.format(table_id)


def build_gene_set(proteome_study, gene_set):
    results = get_query_results(make_gene_set_query(proteome_study))

    for gene in results:
        gene_set.add(gene)

    return gene_set


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
            schema_filename = 'isb-project-zero.PDC.quant_data_2020_09.json'
            console_out("Building {0}!", (table_id,))
            schema, table_metadata = from_schema_file_to_obj(BQ_PARAMS, schema_filename)
            create_and_load_tsv_table(BQ_PARAMS, filename, schema, table_id)
            build_end = time.time() - build_start

            console_out("Quant table build completed in {0:0.0f}s!\n", (build_end,))

    if 'build_gene_table' in steps:
        proteome_studies = API_PARAMS['PROTEOME_STUDIES']
        gene_set = set()

        for proteome_study in proteome_studies:
            build_gene_set(proteome_study, gene_set)

        print(gene_set)

    end = time.time() - start
    if end < 100:
        console_out("Finished program execution in {0:0.0f}s!\n", (end,))
    else:
        console_out("Finished program execution in {0:0.0f}s!\n", (end,))


if __name__ == '__main__':
    main(sys.argv)
