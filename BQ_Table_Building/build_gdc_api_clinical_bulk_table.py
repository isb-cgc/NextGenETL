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
import json

from common_etl.utils import (has_fatal_error, load_config, get_rel_prefix, get_scratch_fp,
                              upload_to_bucket, create_and_load_table_from_jsonl, format_seconds,
                              check_value_type, resolve_type_conflicts, json_datetime_to_str_converter,
                              construct_table_id, get_filename, write_list_to_jsonl,
                              create_and_upload_schema_for_json, construct_table_name, retrieve_bq_schema_object)

API_PARAMS = dict()
BQ_PARAMS = dict()
# used to capture returned yaml config sections
YAML_HEADERS = ('api_params', 'bq_params', 'steps')


def request_data_from_gdc_api(curr_index):
    """

    Make a POST API request and return response (if valid).
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
    """

    Retrieve API case records and output to a JSONL file (later used to populate the clinical data BQ table).
    :param local_path: absolute path to data output file
    """
    current_index = API_PARAMS['START_INDEX']
    file_mode = 'a' if current_index > 0 else 'w'

    # don't append records to existing file if START_INDEX is set to 0
    if file_mode == 'w' and os.path.exists(local_path):
        os.remove(local_path)

    while True:
        response = request_data_from_gdc_api(current_index)
        response_json = response.json()['data']

        # If response doesn't contain pagination, indicates an invalid request.
        if 'pagination' not in response_json:
            has_fatal_error("'pagination' key not found in response json, exiting.", KeyError)

        total_pages = response_json['pagination']['pages']
        response_cases = response_json['hits']
        assert len(response_cases) > 0, "paginated case result length == 0 \nresult: {}".format(response.json())
        print("Fetched page {} of {}".format(response_json['pagination']['page'], total_pages))

        write_list_to_jsonl(jsonl_fp=local_path, json_obj_list=response_cases, mode=file_mode)
        current_index += API_PARAMS['BATCH_SIZE']

        if response_json['pagination']['page'] == total_pages:
            break

    print("Wrote cases response to jsonl file.")

'''
# todo remove
def add_case_fields_to_master_dict(grouped_fields_dict, cases):
    """
    todo
    :param grouped_fields_dict:
    :param cases:
    :return:
    """
    def add_case_field_to_master_dict(record, parent_fg_list):
        """
        todo
        :param record:
        :param parent_fg_list:
        :return:
        """
        if not record:
            return

        field_group_key = ".".join(parent_fg_list)

        if field_group_key == 'cases.sample_ids' or field_group_key == 'cases.submitter_sample_ids':
            print("foo!")

        for exclude_field in API_PARAMS['EXCLUDE_FIELDS'][field_group_key]:
            if exclude_field in record:
                del record[exclude_field]

        if isinstance(record, list):
            if not isinstance(record[0], dict):
                print(record)
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


# todo remove
def add_missing_fields_and_normalize_case(fields_dict, case):
    """
    todo
    :param fields_dict:
    :param case:
    :return:
    """
    def normalize_value(value):
        """
        todo
        """
        if value in ('NA', 'N/A', 'null', 'None', ''):
            return None
        if value in ('False', 'false', 'FALSE'):
            return False
        if value in ('True', 'true', 'TRUE'):
            return True
        else:
            return value

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


# todo remove
def modify_response_json_and_output_jsonl(local_jsonl_path):
    """
    todo
    :param local_jsonl_path:
    :return:
    """
    def assert_output_count(field_group, fields, fgs_to_remove=None):
        """
        todo
        :param field_group:
        :param fields:
        :param fgs_to_remove:
        :return:
        """
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
            # todo fix this sometime
            if 'sample_ids' in case:
                sample_ids = ", ".join(case['sample_ids'])
                case['sample_ids'] = sample_ids
                submitter_sample_ids = ", ".join(case['submitter_sample_ids'])
                case['submitter_sample_ids'] = submitter_sample_ids

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

            jsonl_file_obj.write(json.dumps(obj=case, default=json_datetime_to_str_converter))
            jsonl_file_obj.write('\n')

            if index % 1000 == 0:
                print("{:6d} cases written".format(index))

    file_size = os.stat(local_jsonl_path).st_size / 1048576.0
    print("created jsonl! file size: {:.2f} mb".format(file_size))

    return grouped_fields_dict


# todo remove
def infer_types(record, fields_dict, types_dict, parent_fg_list):
    """
    todo
    :param record:
    :param fields_dict:
    :param types_dict:
    :param parent_fg_list:
    :return:
    """
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


# todo remove
def create_column_data_type_dict(grouped_fields_dict, scratch_fp):
    """
    todo
    :param grouped_fields_dict:
    :param scratch_fp:
    :return:
    """
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


# todo remove
def get_grouped_fields_dict(local_jsonl_path):
    """
    todo
    :param local_jsonl_path:
    :return:
    """
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


# todo remove
def create_field_mapping_dict():
    """
    todo
    :return:
    """
    res = requests.get(API_PARAMS['ENDPOINT'] + '/_mapping')

    # return response body if request was successful
    if res.status_code != requests.codes.ok:
        res.raise_for_status()

    for row in res:
        print(row)


# todo remove
def generate_bq_schema(grouped_fields_dict, column_data_types_dict):
    """
    todo
    :param grouped_fields_dict:
    :param column_data_types_dict:
    :return:
    """
    def append_to_nested_schema_dict():
        """
        todo
        :return:
        """
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
        if len(split_column) == 3:
            parent_column = ".".join(split_column[:-1])

            repeated_schema_dict[parent_column]['fields'].append(nested_schema_fields)
        elif len(split_column) == 2:
            schema.append(nested_schema_fields)

    return schema
'''


def main(args):
    """

    Script execution function.
    :param args: command-line arguments
    """
    start = time.time()
    steps = []

    try:
        global API_PARAMS, BQ_PARAMS
        API_PARAMS, BQ_PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        has_fatal_error("{}".format(err), ValueError)

    bulk_table_name = construct_table_name(API_PARAMS,
                                           prefix=get_rel_prefix(API_PARAMS),
                                           suffix=BQ_PARAMS['MASTER_TABLE'],
                                           include_release=False)
    bulk_table_id = construct_table_id(project=BQ_PARAMS['DEV_PROJECT'],
                                       dataset=BQ_PARAMS['DEV_DATASET'],
                                       table_name=bulk_table_name)

    jsonl_output_file = get_filename(API_PARAMS,
                                     file_extension='jsonl',
                                     prefix=get_rel_prefix(API_PARAMS),
                                     suffix=BQ_PARAMS['MASTER_TABLE'],
                                     include_release=False)
    scratch_fp = get_scratch_fp(BQ_PARAMS, jsonl_output_file)

    if 'build_and_upload_case_jsonl' in steps:
        # Hit GDC api endpoint, outputs data to jsonl file (format required by bq)
        extract_api_response_json(scratch_fp)
        upload_to_bucket(BQ_PARAMS, scratch_fp)

    if 'create_schema' in steps:
        record_list = list()

        with open(scratch_fp) as jsonl_file:
            while True:
                file_record = jsonl_file.readline()

                if not file_record:
                    break

                file_json_obj = json.loads(file_record)
                record_list.append(file_json_obj)

        print(bulk_table_name)

        create_and_upload_schema_for_json(API_PARAMS, BQ_PARAMS,
                                          record_list=record_list,
                                          table_name=bulk_table_name,
                                          include_release=False)

    if 'build_bq_table' in steps:
        bulk_table_schema = retrieve_bq_schema_object(API_PARAMS, BQ_PARAMS,
                                                      table_name=bulk_table_name,
                                                      include_release=False)

        create_and_load_table_from_jsonl(BQ_PARAMS,
                                         jsonl_file=jsonl_output_file,
                                         table_id=bulk_table_id,
                                         schema=bulk_table_schema)

    end = format_seconds(time.time() - start)
    print("Script executed in {}\n".format(end))


if __name__ == '__main__':
    main(sys.argv)
