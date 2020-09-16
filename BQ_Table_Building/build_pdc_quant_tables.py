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


def get_jsonl_file(bq_params, record_type):
    return "{}_{}.jsonl".format(bq_params['DATA_SOURCE'], record_type)


def query_quant_data_matrix(study_submitter_id, data_type):
    return '{{ quantDataMatrix(study_submitter_id: \"{}\" data_type: \"{}\") }}'.format(
        study_submitter_id, data_type)


def get_quant_tsv_filename(study_submitter_id):
    return get_quant_table_name(study_submitter_id) + '.tsv'


def get_quant_table_name(study_submitter_id):
    filename = '_'.join(study_submitter_id.split(' '))
    return BQ_PARAMS['RELEASE'] + '_' + filename


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
        fh.write("{}\t{}\t{}\t{}\t{}\t\n".format(
            'study_id',
            'aliquot_run_metadata_id',
            'aliquot_submitter_id',
            'gene',
            'log2_ratio'
            ))
        for row in res_json['data']['quantDataMatrix']:
            gene = row.pop(0)

            for i, log2_ratio in enumerate(row):
                fh.write("{}\t{}\t{}\t{}\t{}\t\n".format(
                    aliquot_metadata[i]['study_id'],
                    aliquot_metadata[i]['aliquot_run_metadata_id'],
                    aliquot_metadata[i]['aliquot_submitter_id'],
                    gene,
                    log2_ratio))

                lines_written += 1

    return lines_written


def get_study_ids():
    return """
    SELECT study_id, study_submitter_id
    FROM  `isb-project-zero.PDC_metadata.studies_2020_09`
    """


def get_quant_files():
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(BQ_PARAMS['WORKING_BUCKET'],
                                      prefix=BQ_PARAMS['WORKING_BUCKET_DIR'])

    for blob in blobs:
        print(blob.name)


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
        get_quant_files()

    has_quant_data_list = list()

    '''
    for study_id_dict in study_ids_list:
        filename = get_quant_tsv_filename(study_id_dict['study_submitter_id'])
        quant_jsonl_fp = get_scratch_fp(BQ_PARAMS, filename)
        if 'upload_to_bucket' in steps:
            upload_to_bucket(BQ_PARAMS, quant_jsonl_fp)

        if os.path.exists(quant_jsonl_fp):
            has_quant_data_list.append(study_id_dict['study_submitter_id'])

    
    if 'upload_to_bucket' in steps:
        upload_start = time.time()

        for study_submitter_id in has_quant_data_list:
            filename = get_quant_jsonl_filename(study_submitter_id)
            quant_jsonl_fp = get_scratch_fp(BQ_PARAMS, filename)

            console_out("Uploading {0}!", (filename,))

        upload_end = time.time() - upload_start

        console_out("Quant table jsonl upload completed in {0:0.0f}s!\n", (upload_end,))


    if 'build_master_quant_table' in steps:
        build_start = time.time()

        for study_submitter_id in has_quant_data_list:
            table_name = get_quant_table_name(study_submitter_id)
            table_id = get_working_table_id(BQ_PARAMS, table_name)

            # todo make for each table
            schema_filename = 'isb-project-zero.PDC.quant_data_2020_09.json'
            console_out("Building {0}!", (schema_filename,))

            schema, table_metadata = from_schema_file_to_obj(BQ_PARAMS, schema_filename)

            create_and_load_table(BQ_PARAMS, jsonl_output_file, schema, table_id)
            update_table_metadata(table_id, table_metadata)

        build_end = time.time() - build_start

        console_out("Quant table build completed in {0:0.0f}s!\n", (build_end,))
    '''
    end = time.time() - start
    console_out("Finished program execution in {0:0.0f}s!\n", (end,))


if __name__ == '__main__':
    main(sys.argv)
