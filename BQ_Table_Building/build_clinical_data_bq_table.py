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

import requests
import time
import os
import sys
import copy

# todo infer
from common_etl.utils import (has_fatal_error, infer_data_types, load_config, get_rel_prefix, get_scratch_fp,
                              upload_to_bucket, create_and_load_table, get_working_table_id, format_seconds,
                              write_list_to_jsonl)

API_PARAMS = dict()
BQ_PARAMS = dict()
# used to capture returned yaml config sections
YAML_HEADERS = ('api_params', 'bq_params', 'steps')


# todo yaml config conformance test


def request_data_from_gdc_api(curr_index):
    """ Make a POST API request and return response (if valid).

    :param curr_index: current API poll start position
    :return: response object
    """
    err_list = []

    try:
        request_params = {
            'from': curr_index,
            'size': API_PARAMS['BATCH_SIZE'],
            'expand': ",".join(API_PARAMS['EXPAND_FG_LIST'])  # note: removed list wrapper
        }

        # retrieve and parse a "page" (batch) of case objects
        res = requests.post(url=API_PARAMS['ENDPOINT'], data=request_params)

        # return response body if request was successful
        if res.status_code == requests.codes.ok:
            return res

        err_list.append("{}".format(res.raise_for_status()))

        restart_idx = curr_index
        err_list.append('API request returned status code {}.'.format(res.status_code))

        if BQ_PARAMS['IO_MODE'] == 'a':
            err_list.append('Script is being run in "append" mode. To resume, set '
                            'START_INDEX = {} in yaml config.'.format(restart_idx))
    except requests.exceptions.MissingSchema as err:
        err_list.append(err)

    has_fatal_error("{}\n(HINT: incorrect ENDPOINT url in yaml config?".format(err_list))
    return None


def add_case_fields_to_master_dict(grouped_fields_dict, cases):
    def add_case_field_to_master_dict(record, parent_fg_list):
        if not record:
            return

        field_group_key = ".".join(parent_fg_list)

        for exclude_field in API_PARAMS['EXCLUDE_FIELDS'][field_group_key]:
            if exclude_field in record:
                del record[exclude_field]

        if isinstance(record, list):
            for child_record in record:
                add_case_field_to_master_dict(child_record, parent_fg_list)
        elif isinstance(record, dict):
            for key in record.keys():
                if isinstance(record[key], dict):
                    add_case_field_to_master_dict(record[key], parent_fg_list + [key])
                elif isinstance(record[key], list) and isinstance(record[key][0], dict):
                    add_case_field_to_master_dict(record[key], parent_fg_list + [key])
                else:
                    if field_group_key not in grouped_fields_dict:
                        grouped_fields_dict[field_group_key] = dict()
                    if not isinstance(record[key], list):
                        grouped_fields_dict[field_group_key][key] = None

    for case in cases:
        add_case_field_to_master_dict(case, [API_PARAMS['PARENT_FG']])


def add_missing_fields_to_case(fields_dict, case):
    case_items = dict()

    for key in fields_dict.keys():
        if len(key.split('.')) < 3:
            if key == 'cases.project' or key == 'cases.demographic':
                case_items[key] = dict()
            else:
                case_items[key] = list()

    for fg in sorted(case_items.keys(), reverse=True):
        split_fg = fg.split('.')

        if len(split_fg) == 2:
            if split_fg[1] in case:
                if isinstance(case_items[fg], list):
                    child_records = []
                    for record in case[split_fg[1]]:
                        temp_child_record = copy.deepcopy(fields_dict[fg])
                        temp_child_record.update(record)
                        child_records.append(temp_child_record)
                    case_items[fg] = child_records
                else:
                    temp_child_record = copy.deepcopy(fields_dict[fg])
                    temp_child_record.update(case[split_fg[1]])
            else:
                temp_child_record = copy.deepcopy(fields_dict[fg])

            if isinstance(case_items[fg], list):
                case_items[fg] = [temp_child_record]
            else:
                case_items[fg] = temp_child_record
        elif len(split_fg) == 1:
            parent_fields = {}

            for key, val in case.items():
                if not isinstance(val, list) and not isinstance(val, dict):
                    parent_fields[key] = val

            temp_child_record = copy.deepcopy(fields_dict[fg])
            temp_child_record.update(parent_fields)
            case_items[fg] = temp_child_record

    for key in fields_dict:
        split_key = key.split('.')
        if len(split_key) == 3:
            parent_fg = ".".join(split_key[:2])
            child_fg = split_key[2]

            for parent_record in case_items[parent_fg]:
                temp_child_records = []

                if child_fg not in parent_record or not parent_record[child_fg]:
                    temp_child_record = copy.deepcopy(fields_dict[key])
                    parent_record[child_fg] = [temp_child_record]
                else:
                    child_records = parent_record[child_fg]

                    if child_records:
                        for record in child_records:
                            temp_child_record = copy.deepcopy(fields_dict[key])
                            temp_child_record.update(record)
                            temp_child_records.append(temp_child_record)

                            parent_record[child_fg] = temp_child_records

    temp_case = dict()

    for fg in case_items:
        split_fg = fg.split('.')
        if len(split_fg) == 1:
            temp_case.update(case_items[fg])
        else:
            case_fg = split_fg[1]
            temp_case[case_fg] = case_items[fg]

    return temp_case


def retrieve_and_save_case_records(local_path):
    """Retrieves case records from API and outputs them to a JSONL file, which is later
        used to populate the clinical data BQ table.

    :param local_path: absolute path to data output file
    """
    start_time = time.time()

    cases_list = list()
    total_cases = None
    total_pages = None
    current_index = API_PARAMS['START_INDEX']

    while True:
        response = request_data_from_gdc_api(current_index)
        response_json = response.json()['data']

        # If response doesn't contain pagination, indicates an invalid request.
        if 'pagination' not in response_json:
            has_fatal_error("'pagination' key not found in response json, exiting.", KeyError)

        if not total_pages:
            total_pages = response_json['pagination']['pages']
            print("Total pages: {}".format(total_pages))
            total_cases = response_json['pagination']['total']
            print("Total cases: {}".format(total_cases))

        current_page = response_json['pagination']['page']
        print("Fetching page {}".format(current_page))

        response_cases = response_json['hits']

        assert len(response_cases) > 0, "paginated case result length == 0 \nresult: {}".format(response.json())

        # todo (maybe): could just build program tables here--that'd save a lot of filtering in the other script
        cases_list += response_cases
        current_index += API_PARAMS['BATCH_SIZE']

        if response_json['pagination']['page'] == total_pages:
            break

    grouped_fields_dict = {
        API_PARAMS['PARENT_FG']: dict()
    }

    add_case_fields_to_master_dict(grouped_fields_dict, cases_list)

    for case in cases_list:
        case = add_missing_fields_to_case(grouped_fields_dict, case)

        assert len(case['diagnoses'][0]['treatments'][0]) == len(grouped_fields_dict['cases.diagnoses.treatments'])
        assert len(case['diagnoses'][0]['annotations'][0]) == len(grouped_fields_dict['cases.diagnoses.annotations'])
        assert len(case['follow_ups'][0]['molecular_tests'][0]) == \
               len(grouped_fields_dict['cases.follow_ups.molecular_tests'])
        assert len(case['follow_ups'][0]) == len(grouped_fields_dict['cases.follow_ups'])
        assert len(case['diagnoses'][0]) == len(grouped_fields_dict['cases.diagnoses'])
        assert len(case['exposures'][0]) == len(grouped_fields_dict['cases.exposures'])
        assert len(case['demographic']) == len(grouped_fields_dict['cases.demographic'])
        assert len(case['family_histories'][0]) == len(grouped_fields_dict['cases.family_histories'])

    if BQ_PARAMS['IO_MODE'] == 'w':
        err_str = "jsonl count ({}) not equal to total cases ({})".format(len(cases_list), total_cases)
        assert total_cases == len(cases_list), err_str

    write_list_to_jsonl(local_path, cases_list)

    print("Output jsonl to {} in '{}' mode".format(local_path, BQ_PARAMS['IO_MODE']))
    extract_time = format_seconds(time.time() - start_time)
    print()
    print("Clinical data retrieval complete!")
    file_size = os.stat(local_path).st_size / 1048576.0
    print("\t{:.2f} mb jsonl file size".format(file_size))
    print("\t{} to query API and write to local jsonl file\n".format(extract_time))


def main(args):
    """Script execution function.

    :param args: command-line arguments
    """
    start = time.time()
    steps = []

    try:
        global API_PARAMS, BQ_PARAMS
        API_PARAMS, BQ_PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        has_fatal_error("{}".format(err), ValueError)

    jsonl_output_file = get_rel_prefix(BQ_PARAMS) + "_" + BQ_PARAMS['MASTER_TABLE'] + '.jsonl'
    scratch_fp = get_scratch_fp(BQ_PARAMS, jsonl_output_file)

    if 'retrieve_cases_and_write_to_jsonl' in steps:
        # Hits the GDC api endpoint, outputs data to jsonl file (format required by bq)
        print('Starting GDC API calls!')
        retrieve_and_save_case_records(scratch_fp)

    if 'upload_jsonl_to_cloud_storage' in steps:
        # Insert the generated jsonl file into google storage bucket, for later
        # ingestion by BQ
        print('Uploading jsonl file to cloud storage!')
        # don't remove local file here, using it to create schema object in next step
        upload_to_bucket(BQ_PARAMS, scratch_fp)

    if 'build_bq_table' in steps:
        print('Building BQ Table!')
        table_name = "_".join([get_rel_prefix(BQ_PARAMS), BQ_PARAMS['MASTER_TABLE']])
        table_id = get_working_table_id(BQ_PARAMS, table_name)

        create_and_load_table(BQ_PARAMS, jsonl_output_file, table_id)
        os.remove(scratch_fp)

    end = format_seconds(time.time() - start)
    print("Script executed in (} seconds\n".format(end))


if __name__ == '__main__':
    main(sys.argv)
