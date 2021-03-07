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


def add_case_fields_to_master_dict(master_dict, cases):
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
                    if field_group_key not in master_dict:
                        master_dict[field_group_key] = dict()
                    master_dict[field_group_key][key] = None

    for case in cases:
        add_case_field_to_master_dict(case, [API_PARAMS['PARENT_FG']])

    fg_list = sorted(list(master_dict.keys()))

    dummy_case = dict()

    for fg in fg_list:
        split_fg_list = fg.split(".")

        if len(split_fg_list) == 1:
            for field, value in master_dict[fg].items():
                dummy_case[field] = value
        elif len(split_fg_list) == 2:
            if split_fg_list[1] == 'demographic':
                dummy_case[split_fg_list[1]] = master_dict[fg]
            else:
                dummy_case[split_fg_list[1]] = [master_dict[fg]]
        elif len(split_fg_list) == 3:
            dummy_case[split_fg_list[1]][0][split_fg_list[2]] = [master_dict[fg]]

    return dummy_case


def add_missing_fields_to_case_json(grouped_fields_dict, case):
    print("before:")
    print(case)

    for field_group in grouped_fields_dict:
        current_case_position = case

        # split field group into list and remove 'cases' prefix (here, 'cases' is just the parent level dict)
        case_nested_key = field_group.split(".")[1:]
"""
        for case_fg_key in case_nested_key:
            if isinstance(current_case_position, list):
                for record in current_case_position:


            if case_fg_key not in current_case_position:
                if case_fg_key == 'demographic':
                    current_case_position[case_fg_key] = grouped_fields_dict[field_group]
                else:
                        current_case_position[case_fg_key] = [grouped_fields_dict[field_group]]

            current_case_position = current_case_position[case_fg_key]

        
        fields_for_this_fg = grouped_fields_dict[field_group]

        for field in fields_for_this_fg.keys():
            if field not in current_case_position:
                current_case_position[field] = None
        

    print("after:")
    print(case)
    exit()
"""


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

        if response_json['pagination']['page'] == 3:
            break

        # todo switch back
        if response_json['pagination']['page'] == total_pages:
            break

    grouped_fields_dict = {
        API_PARAMS['PARENT_FG']: dict()
    }

    dummy_case = add_case_fields_to_master_dict(grouped_fields_dict, cases_list)

    for case in cases_list:
        temp_case = copy.deepcopy(dummy_case)
        temp_case.update(case.items())

        if len(case) < len(temp_case):
            print(temp_case)
            exit()

        case = temp_case

    exit()

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
        # Creates a BQ schema python object consisting of nested SchemaField objects
        # print('Creating BQ schema object!')
        # schema = create_bq_schema(scratch_fp)

        # Creates and populates BQ table
        # if not schema:
        #     has_fatal_error('Empty SchemaField object', UnboundLocalError)
        print('Building BQ Table!')

        table_name = "_".join([get_rel_prefix(BQ_PARAMS), BQ_PARAMS['MASTER_TABLE']])
        table_id = get_working_table_id(BQ_PARAMS, table_name)

        create_and_load_table(BQ_PARAMS, jsonl_output_file, table_id)

        os.remove(scratch_fp)

    end = format_seconds(time.time() - start)
    print("Script executed in (} seconds\n".format(end))


if __name__ == '__main__':
    main(sys.argv)
