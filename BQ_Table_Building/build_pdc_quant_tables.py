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

from common_etl.utils import *

API_PARAMS = dict()
BQ_PARAMS = dict()
YAML_HEADERS = ('api_params', 'bq_params', 'steps')


def get_jsonl_file(bq_params, record_type):
    return "{}_{}.jsonl".format(bq_params['DATA_SOURCE'], record_type)


def query_quant_data_matrix(study_submitter_id, data_type):
    return '{{ quantDataMatrix(study_submitter_id: \"{}\" data_type: \"{}\") }}'.format(
        study_submitter_id, data_type)


def get_and_write_quant_data(study_id_dict, data_type, jsonl_fp):
    study_submitter_id = study_id_dict['study_submitter_id']
    study_id = study_id_dict['study_id']

    res_json = get_graphql_api_response(API_PARAMS,
                                        query=query_quant_data_matrix(study_submitter_id,
                                                                      data_type))

    if not res_json['data']['quantDataMatrix']:
        lines_written = 0
        print("{} lines written for {}.".format(lines_written, study_submitter_id))
        return lines_written

    log2_ratio_list = list()

    id_row = res_json['data']['quantDataMatrix'].pop(0)
    id_row.pop(0)  # remove gene column header string

    # process first row, which gives us the aliquot ids and idx positions
    for i, el in enumerate(id_row):
        split_el = el.split(':')
        aliquot_run_metadata_id = split_el[0]
        aliquot_submitter_id = split_el[1]

        log2_ratio_list.append(
            {"study_id": study_id, "study_submitter_id": study_submitter_id,
             "aliquot_run_metadata_id": aliquot_run_metadata_id,
             "aliquot_submitter_id": aliquot_submitter_id, "log2_ratios": {}
             })

    # iterate over each gene row and add to the correct aliquot_run obj
    for row in res_json['data']['quantDataMatrix']:
        gene = row.pop(0)

        for i, log2_ratio in enumerate(row):
            log2_ratio_list[i]['log2_ratios'][gene] = log2_ratio

    lines_written = 0
    is_first_write = True

    # flatten json to write to jsonl for bq
    for aliquot in log2_ratio_list:
        aliquot_json_list = list()

        log2_ratios = aliquot.pop('log2_ratios')

        for gene, log2_ratio in log2_ratios.items():
            aliquot['gene'] = gene
            aliquot['log2_ratio'] = log2_ratio

            aliquot_json_list.append(aliquot)

        append_list_to_jsonl(file_handler, aliquot_json_list)

        lines_written += len(aliquot_json_list)

    print("{} lines written for {}.".format(lines_written, study_submitter_id))

    return lines_written


def get_study_ids():
    return """
    SELECT study_id, study_submitter_id
    FROM  `isb-project-zero.PDC_metadata.studies_2020_09`
    """


def main(args):
    start = time.time()

    try:
        global API_PARAMS, BQ_PARAMS
        API_PARAMS, BQ_PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        has_fatal_error(str(err), ValueError)

    if 'build_quant_jsonl' in steps:

        file_obj = open(get_scratch_fp(BQ_PARAMS, 'quant_2020_09.jsonl'), 'w')

        study_ids_list = list()

        study_ids = get_query_results(get_study_ids())

        for study in study_ids:
            study_ids_list.append(dict(study.items()))

        lines_written = 0

        for study_id_dict in study_ids_list:
            try:
                lines_written += get_and_write_quant_data(study_id_dict,
                                                          'log2_ratio',
                                                          file_obj)
            except IOError:
                file_obj.close()
                has_fatal_error("Error writing to quant jsonl file.", IOError)

        print("Quant jsonl total lines written: {}".format(lines_written))
        file_obj.close()

    if 'build_master_quant_table' in steps:
        pass

    end = time.time() - start
    console_out("Finished program execution in {0:0.0f}s!\n", (end,))


if __name__ == '__main__':
    main(sys.argv)
