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
import json

import requests
import time
import os
import sys
import copy

# todo infer
from common_etl.utils import (has_fatal_error, load_config, get_rel_prefix, get_scratch_fp,
                              upload_to_bucket, create_and_load_table, get_working_table_id, format_seconds,
                              write_line_to_jsonl, normalize_value, check_value_type,
                              resolve_type_conflicts)

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


def extract_api_response_json(local_path):
    """Retrieves case records from API and outputs them to a JSONL file, which is later
        used to populate the clinical data BQ table.

    :param local_path: absolute path to data output file
    """
    cases_list = list()
    total_pages = None
    current_index = API_PARAMS['START_INDEX']

    local_json_path = local_path[:-1]

    with open(local_json_path, "w") as file_obj:
        file_obj.write('{"cases": [')

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

            file_obj.write(json.dumps(response_cases))

            cases_list += response_cases
            current_index += API_PARAMS['BATCH_SIZE']

            if response_json['pagination']['page'] == total_pages:
                break
            else:
                file_obj.write(',')

        file_obj.write(']}')

    print("Wrote cases response to json file.")


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

    print("Master field dict created!")


def add_missing_fields_and_normalize_case(fields_dict, case):
    case_items = dict()

    for key in fields_dict.keys():
        if len(key.split('.')) < 3:
            if key == 'cases.project' or key == 'cases.demographic':
                case_items[key] = dict()
            else:
                case_items[key] = list()

    for fg in sorted(case_items.keys(), reverse=True):
        temp_child_record = None
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
                    parent_fields[key] = normalize_value(val)

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


def modify_response_json_and_output_jsonl(local_jsonl_path):
    def assert_output_count(field_group, fields, fgs_to_remove=None):
        fgs_to_remove = set() if not fgs_to_remove else fgs_to_remove
        offset = len(fgs_to_remove)
        actual_cnt = len(fields) - offset
        expected_cnt = len(grouped_fields_dict[field_group])

        try:
            assert actual_cnt == expected_cnt
        except AssertionError:
            expected_field_keys = set(grouped_fields_dict[field_group].keys())
            actual_field_keys = set(fields.keys())
            actual_field_keys |= fgs_to_remove
            not_in_expected_keys = actual_field_keys - expected_field_keys
            not_in_actual_keys = expected_field_keys - actual_field_keys

            print("case: {}\n".format(case))
            print("error for {}".format(field_group))
            print("expected count {} -> actual {} at index {}\n".format(expected_cnt, actual_cnt, index))
            print("expected fields: {}".format(expected_field_keys))
            print("actual fields: {}\n".format(actual_field_keys))
            print("not_in_expected_keys: {}".format(not_in_expected_keys))
            print("not_in_actual_keys: {}\n".format(not_in_actual_keys))
            exit()

    local_json_path = local_jsonl_path[:-1]

    with open(local_json_path, 'r') as json_file:
        cases_json = json.load(json_file)

    cases_list = list()

    for cases_page in cases_json['cases']:
        for case in cases_page:
            cases_list.append(case)

    print("Total cases in local json file: {}".format(len(cases_list)))

    grouped_fields_dict = {
        API_PARAMS['PARENT_FG']: dict()
    }

    add_case_fields_to_master_dict(grouped_fields_dict, cases_list)

    with open(local_jsonl_path, 'w') as jsonl_file_obj:
        for index, case in enumerate(cases_list):
            case = add_missing_fields_and_normalize_case(grouped_fields_dict, cases_list[index])

            case_fgs_to_remove = ('demographic', 'diagnoses', 'exposures', 'family_histories', 'follow_ups', 'project')
            diagnoses_fg_to_remove = ('annotations', 'treatments')
            follow_ups_fg_to_remove = ('molecular_tests',)

            for fg in case_fgs_to_remove:
                assert fg in case and case[fg], "{} field group null for index {}\n".format(fg, index)

            assert_output_count("cases", case, case_fgs_to_remove)
            assert_output_count("cases.exposures", case['exposures'][0])
            assert_output_count("cases.demographic", case['demographic'])
            assert_output_count("cases.family_histories", case['family_histories'][0])
            assert_output_count("cases.project", case['project'])
            assert_output_count("cases.diagnoses", case['diagnoses'][0], diagnoses_fg_to_remove)
            assert_output_count("cases.diagnoses.treatments", case['diagnoses'][0]['treatments'][0])
            assert_output_count("cases.diagnoses.annotations", case['diagnoses'][0]['annotations'][0])
            assert_output_count("cases.follow_ups", case['follow_ups'][0], follow_ups_fg_to_remove)
            assert_output_count("cases.follow_ups.molecular_tests", case['follow_ups'][0]['molecular_tests'][0])

            write_line_to_jsonl(jsonl_file_obj, case)

            if index % 1000 == 0:
                print("{:6d} cases written".format(index))

    file_size = os.stat(local_jsonl_path).st_size / 1048576.0
    print("created jsonl! file size: {:.2f} mb".format(file_size))

    return grouped_fields_dict


def infer_types(record, fields_dict, types_dict, parent_fg_list):
    field_group_key = ".".join(parent_fg_list)

    if isinstance(record, list):
        for child_record in record:
            infer_types(child_record, fields_dict, types_dict, parent_fg_list)
    elif isinstance(record, dict):
        for key in record.keys():
            if isinstance(record[key], dict):
                infer_types(record[key], fields_dict, types_dict, parent_fg_list + [key])
            elif isinstance(record[key], list) and isinstance(record[key][0], dict):
                infer_types(record[key], fields_dict, types_dict, parent_fg_list + [key])
            else:
                if field_group_key not in fields_dict:
                    fields_dict[field_group_key] = dict()
                if not isinstance(record[key], list):
                    field_key = field_group_key + '.' + key
                    if field_key not in types_dict:
                        types_dict[field_key] = set()
                    value_type = check_value_type(record[key])
                    if value_type:
                        types_dict[field_key].add(value_type)


def create_column_data_type_dict(grouped_fields_dict, scratch_fp):
    column_data_types = dict()

    count = 1
    with open(scratch_fp, 'r') as jsonl_file_obj:
        json_str = jsonl_file_obj.readline()

        while jsonl_file_obj and json_str:
            case = json.loads(json_str)
            infer_types(case, grouped_fields_dict, column_data_types, [API_PARAMS['PARENT_FG']])

            json_str = jsonl_file_obj.readline()

            if count % 1000 == 0:
                print(count)
            count += 1

    resolve_type_conflicts(column_data_types)

    return column_data_types


"""
def generate_bq_schema(grouped_fields_dict, column_data_types_dict):
    nested_schema_dict = {}

    fg_list = API_PARAMS["EXPAND_FG_LIST"] + [API_PARAMS["PARENT_FG"]]

    for field_group in sorted(fg_list, reverse=True):
        nested_fields_list = list()
        full_field_name = API_PARAMS["PARENT_FG"] + "." + field_group
        for field_name in grouped_fields_dict[full_field_name].keys():
            full_field_name = full_field_name + "." + field_name

            nested_fields_list.append(
                {
                    "name": full_field_name,
                    "type": column_data_types_dict[full_field_name],
                    "mode": "NULLABLE",
                    "description": ""
                }
            )

        nested_schema_dict[full_field_name] = nested_fields_list

    nested_fields_list = list()

    for field_name in grouped_fields_dict[API_PARAMS["PARENT_FG"]]:
        full_field_name = API_PARAMS["PARENT_FG"] + "." + field_name

        nested_fields_list.append(
            {
                "name": full_field_name,
                "type": column_data_types_dict[full_field_name],
                "mode": "NULLABLE",
                "description": ""
            }
        )

    nested_schema_dict[API_PARAMS["PARENT_FG"]] = nested_fields_list

    repeated_schema_dict = dict()
    schema = list()

    for fg in sorted(nested_schema_dict.keys()):
        split_fg = fg.split('.')

        if len(split_fg) == 1:
            schema = nested_schema_dict[fg]
            continue

        repeated_schema_dict[fg] = {
            "name": split_fg[-1],
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": nested_schema_dict[fg]
        }

    sorted_fgs = sorted(repeated_schema_dict.keys(), reverse=True)

    for column in sorted_fgs:
        nested_schema_fields = repeated_schema_dict.pop(column)
        split_column = column.split('.')
        if len(split_column) == 3:
            parent_column = ".".join(split_column[:-1])

            repeated_schema_dict[parent_column]['fields'].append(nested_schema_fields)
        elif len(split_column) == 2:
            schema.append(nested_schema_fields)

    return schema

def generate_bq_schema(grouped_fields_dict, column_data_types_dict):
    def append_to_nested_schema_dict(full_field_name):
        nested_fields_list.append(
            {
                "name": full_field_name,
                "type": column_data_types_dict[full_field_name],
                "mode": "NULLABLE",
                "description": ""
            }
        )

    nested_schema_dict = {}

    for field_group in sorted(API_PARAMS["EXPAND_FG_LIST"], reverse=True):
        nested_fields_list = list()
        full_field_name = API_PARAMS["PARENT_FG"] + "." + field_group
        for field_name in grouped_fields_dict[full_field_name].keys():
            full_field_name = full_field_name + "." + field_name
            append_to_nested_schema_dict(full_field_name)

        nested_schema_dict[full_field_name] = nested_fields_list

    nested_fields_list = list()

    for field_name in grouped_fields_dict[API_PARAMS["PARENT_FG"]]:
        full_field_name = API_PARAMS["PARENT_FG"] + "." + field_name
        append_to_nested_schema_dict(full_field_name)

    nested_schema_dict[API_PARAMS["PARENT_FG"]] = nested_fields_list

    repeated_schema_dict = dict()
    schema = list()

    for fg in sorted(nested_schema_dict.keys()):
        split_fg = fg.split('.')

        if len(split_fg) == 1:
            schema = nested_schema_dict[fg]
            continue

        repeated_schema_dict[fg] = {
            "name": split_fg[-1],
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": nested_schema_dict[fg]
        }

    sorted_fgs = sorted(repeated_schema_dict.keys(), reverse=True)

    for column in sorted_fgs:
        nested_schema_fields = repeated_schema_dict.pop(column)
        split_column = column.split('.')
        if len(split_column) == 3:
            parent_column = ".".join(split_column[:-1])

            repeated_schema_dict[parent_column]['fields'].append(nested_schema_fields)
        elif len(split_column) == 2:
            schema.append(nested_schema_fields)

    return schema
"""


def generate_bq_schema(grouped_fields_dict, column_data_types_dict):
    def append_to_nested_schema_dict():
        fg_fields_list.append(
            {
                "name": full_field_name.split('.')[-1],
                "type": column_data_types_dict[full_field_name],
                "mode": "NULLABLE",
                "description": ""
            }
        )

    full_field_group_name_list = [API_PARAMS["PARENT_FG"] + "." + fg for fg in API_PARAMS["EXPAND_FG_LIST"]]
    full_field_group_name_list.append(API_PARAMS["PARENT_FG"])

    repeated_schema_dict = dict()
    nested_schema_dict = dict()

    for field_group in sorted(full_field_group_name_list, reverse=True):
        fg_fields_list = list()

        for field_name in grouped_fields_dict[field_group].keys():
            full_field_name = field_group + "." + field_name
            append_to_nested_schema_dict()

        nested_schema_dict[field_group] = fg_fields_list

    schema = list()

    for fg in sorted(nested_schema_dict.keys()):
        split_fg = fg.split('.')

        if len(split_fg) == 1:
            schema = nested_schema_dict[fg]
            continue

        repeated_schema_dict[fg] = {
            "name": split_fg[-1],
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": nested_schema_dict[fg]
        }

    sorted_fgs = sorted(repeated_schema_dict.keys(), reverse=True)

    for column in sorted_fgs:
        nested_schema_fields = repeated_schema_dict.pop(column)
        split_column = column.split('.')
        print(split_column)
        if len(split_column) == 3:
            parent_column = ".".join(split_column[:-1])

            repeated_schema_dict[parent_column]['fields'].append(nested_schema_fields)
        elif len(split_column) == 2:
            schema.append(nested_schema_fields)

    return schema

def get_grouped_fields_dict(local_jsonl_path):
    local_json_path = local_jsonl_path[:-1]

    with open(local_json_path, 'r') as json_file:
        cases_json = json.load(json_file)

    cases_list = list()

    for cases_page in cases_json['cases']:
        for case in cases_page:
            cases_list.append(case)

    grouped_fields_dict = {
        API_PARAMS['PARENT_FG']: dict()
    }

    add_case_fields_to_master_dict(grouped_fields_dict, cases_list)

    return grouped_fields_dict


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

    grouped_fields_dict = None

    if 'extract_api_response_json' in steps:
        # Hits the GDC api endpoint, outputs data to jsonl file (format required by bq)
        print('Starting GDC API calls!')
        extract_api_response_json(scratch_fp)

    if 'generate_jsonl_from_modified_api_json' in steps:
        print('Generating master fields list and adding missing fields to cases!')
        grouped_fields_dict = modify_response_json_and_output_jsonl(scratch_fp)

    if 'upload_jsonl_to_cloud_storage' in steps:
        # Insert the generated jsonl file into google storage bucket, for later
        # ingestion by BQ
        print('Uploading jsonl file to cloud storage!')
        # don't remove local file here, using it to create schema object in next step
        upload_to_bucket(BQ_PARAMS, scratch_fp)

    if 'build_bq_table' in steps:
        if not grouped_fields_dict:
            grouped_fields_dict = get_grouped_fields_dict(scratch_fp)

        column_data_types_dict = create_column_data_type_dict(grouped_fields_dict, scratch_fp)
        schema = generate_bq_schema(grouped_fields_dict, column_data_types_dict)

        print('Building BQ Table!')
        table_name = "_".join([get_rel_prefix(BQ_PARAMS), BQ_PARAMS['MASTER_TABLE']])
        table_id = get_working_table_id(BQ_PARAMS, table_name)

        create_and_load_table(BQ_PARAMS, jsonl_output_file, table_id, schema)
        # os.remove(scratch_fp)

    end = format_seconds(time.time() - start)
    print("Script executed in {}\n".format(end))


if __name__ == '__main__':
    main(sys.argv)
