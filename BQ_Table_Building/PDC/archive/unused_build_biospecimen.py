"""
Copyright 2020-2021, Institute for Systems Biology

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

import time
import sys
import json

from common_etl.utils import (format_seconds, has_fatal_error, load_config, construct_table_name, get_query_results,
                              retrieve_bq_schema_object, normalize_value, load_table_from_query, get_filepath,
                              update_schema, publish_table, create_and_upload_schema_for_json, construct_table_id)

from BQ_Table_Building.PDC.pdc_utils import (build_obj_from_pdc_api, build_table_from_jsonl, get_pdc_study_ids,
                                             get_prefix,
                                             write_jsonl_and_upload, update_pdc_table_metadata)

API_PARAMS = dict()
BQ_PARAMS = dict()
YAML_HEADERS = ('api_params', 'bq_params', 'steps')


def make_biospecimen_per_study_query(pdc_study_id):
    """

    Creates a graphQL string for querying the PDC API's biospecimenPerStudy endpoint.
    :return: GraphQL query string
    """
    return '''{{ 
        biospecimenPerStudy( pdc_study_id: \"{}\" acceptDUA: true) {{
            aliquot_id 
            sample_id 
            case_id 
            aliquot_submitter_id 
            sample_submitter_id 
            case_submitter_id 
            project_name 
            sample_type 
            disease_type 
            primary_site 
            pool 
        }}
    }}'''.format(pdc_study_id)


def alter_biospecimen_per_study_objects(json_obj_list, pdc_study_id):
    """

    Passed as a parameter to build_jsonl_from_pdc_api(). Allows for the dataset's json object to be mutated prior
    to writing it to a file.
    :param json_obj_list: list of json objects to mutate
    :param pdc_study_id: pdc study id for this set of json objects
    """

    # todo change how this works -- not dependent on biospecimen

    def get_case_file_count_mapping():
        """

        Gets a dictionary of form {case_id: file_count} using table created during file metadata ingestion;
        derived from associated entity mapping table
        :return: { '<case_id>': '<file_count>'}
        """
        table_name = construct_table_name(API_PARAMS, prefix=BQ_PARAMS['FILE_COUNT_TABLE'])
        table_id = construct_table_id(BQ_PARAMS['DEV_PROJECT'],
                                    dataset=BQ_PARAMS['META_DATASET'],
                                    table_name=table_name)
        case_file_count_query = """
        SELECT case_id, file_id_count
        FROM {}
        """.format(table_id)

        res = get_query_results(case_file_count_query)
        case_file_count_map = dict()

        for case_file_count_row in res:
            case_id = case_file_count_row[0]
            file_count = case_file_count_row[1]
            case_file_count_map[case_id] = file_count

        return case_file_count_map

    case_file_count_map = get_case_file_count_mapping()

    for case in json_obj_list:
        case['pdc_study_id'] = pdc_study_id
        case_id = case['case_id']
        case['file_count'] = case_file_count_map[case_id] if case_id in case_file_count_map else 0


def main(args):
    start_time = time.time()
    print("PDC script started at {}".format(time.strftime("%x %X", time.localtime())))

    steps = None

    try:
        global API_PARAMS, BQ_PARAMS
        API_PARAMS, BQ_PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        has_fatal_error(err, ValueError)

    all_pdc_study_ids = get_pdc_study_ids(API_PARAMS, BQ_PARAMS, include_embargoed_studies=True)

    biospecimen_endpoint = 'biospecimenPerStudy'
    biospecimen_prefix = get_prefix(API_PARAMS, biospecimen_endpoint)

    if 'build_biospecimen_jsonl' in steps:
        per_study_biospecimen_list = build_obj_from_pdc_api(API_PARAMS,
                                                            endpoint=biospecimen_endpoint,
                                                            request_function=make_biospecimen_per_study_query,
                                                            alter_json_function=alter_biospecimen_per_study_objects,
                                                            ids=all_pdc_study_ids,
                                                            insert_id=True)

        create_and_upload_schema_for_json(API_PARAMS, BQ_PARAMS,
                                          record_list=per_study_biospecimen_list,
                                          table_name=biospecimen_prefix,
                                          include_release=True)

        write_jsonl_and_upload(API_PARAMS, BQ_PARAMS, biospecimen_prefix, per_study_biospecimen_list)

    if 'build_biospecimen_table' in steps:
        biospecimen_schema = retrieve_bq_schema_object(API_PARAMS, BQ_PARAMS,
                                                       table_type=biospecimen_prefix)
        build_table_from_jsonl(API_PARAMS, BQ_PARAMS,
                               endpoint=biospecimen_endpoint,
                               infer_schema=True,
                               schema=biospecimen_schema)

    end = time.time() - start_time
    print("Finished program execution in {}!\n".format(format_seconds(end)))


if __name__ == '__main__':
    main(sys.argv)
