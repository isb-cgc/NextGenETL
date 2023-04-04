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
import json
import jsonlines
import difflib

from common_etl.utils import (has_fatal_error, load_config, get_rel_prefix, get_scratch_fp, upload_to_bucket,
                              create_and_load_table_from_jsonl, format_seconds, construct_table_id, get_filename,
                              write_list_to_jsonl, create_and_upload_schema_for_json, construct_table_name,
                              retrieve_bq_schema_object, download_from_bucket, recursively_normalize_field_values,
                              write_line_to_jsonl)

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

        err_list.append(f"{res.raise_for_status()}")

        restart_idx = curr_index
        err_list.append(f'API request returned status code {res.status_code}.')

        if BQ_PARAMS['IO_MODE'] == 'a':
            err_list.append(f"""
            Script is being run in "append" mode. To resume, set START_INDEX = {restart_idx} in yaml config.
            """)
    except requests.exceptions.MissingSchema as err:
        err_list.append(err)

    has_fatal_error(f"{err_list}\n(HINT: incorrect ENDPOINT url in yaml config?")
    return None


def extract_api_response_json(local_path):
    """

    Retrieve API case records and output to a JSONL file (later used to populate the clinical data BQ table).
    :param local_path: absolute path to data output file
    """
    current_index = API_PARAMS['START_INDEX']

    # don't append records to existing file if START_INDEX is set to 0
    if API_PARAMS['START_INDEX'] == 0 and os.path.exists(local_path):
        os.remove(local_path)

    while True:
        response = request_data_from_gdc_api(current_index)
        response_json = response.json()['data']

        # If response doesn't contain pagination, indicates an invalid request.
        if 'pagination' not in response_json:
            has_fatal_error("'pagination' key not found in response json, exiting.", KeyError)

        total_pages = response_json['pagination']['pages']
        response_cases = response_json['hits']
        assert len(response_cases) > 0, f"paginated case result length == 0 \nresult: {response.json()}"
        print(f"Fetched page {response_json['pagination']['page']} of {total_pages}")

        for response_case in response_cases:
            for field in API_PARAMS['EXCLUDE_FIELDS']:
                if '.' not in field and field in response_case:
                    response_case.pop(field)
                else:
                    split_field = field.split('.')
                    if len(split_field) == 2:
                        field_group = split_field[0]
                        field = split_field[1]

                        if field in response_case[field_group]:
                            response_case[field_group].pop(field)

        # always set to append--if starting over, file is manually deleted at start of function
        write_list_to_jsonl(jsonl_fp=local_path, json_obj_list=response_cases, mode='a')
        current_index += API_PARAMS['BATCH_SIZE']

        if response_json['pagination']['page'] == total_pages:
            break

    print("Wrote GDC API response to jsonl file.")


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
        has_fatal_error(f"{err}", ValueError)

    bulk_table_name = construct_table_name(API_PARAMS,
                                           prefix=get_rel_prefix(API_PARAMS),
                                           suffix=BQ_PARAMS['MASTER_TABLE'],
                                           include_release=False)
    bulk_table_id = construct_table_id(project=BQ_PARAMS['DEV_PROJECT'],
                                       dataset=BQ_PARAMS['DEV_DATASET'],
                                       table_name=bulk_table_name)
    raw_jsonl_output_file = get_filename(API_PARAMS,
                                         file_extension='jsonl',
                                         prefix=get_rel_prefix(API_PARAMS) + '_raw',
                                         suffix=BQ_PARAMS['MASTER_TABLE'],
                                         include_release=False)

    raw_scratch_fp = get_scratch_fp(BQ_PARAMS, raw_jsonl_output_file)

    normalized_jsonl_output_file = get_filename(API_PARAMS,
                                                file_extension='jsonl',
                                                prefix=get_rel_prefix(API_PARAMS),
                                                suffix=BQ_PARAMS['MASTER_TABLE'],
                                                include_release=False)

    norm_scratch_fp = get_scratch_fp(BQ_PARAMS, normalized_jsonl_output_file)

    diff_output_file = get_filename(API_PARAMS,
                                    file_extension='txt',
                                    prefix=get_rel_prefix(API_PARAMS) + '_diff',
                                    suffix=BQ_PARAMS['MASTER_TABLE'],
                                    include_release=False)

    diff_scratch_fp = get_scratch_fp(BQ_PARAMS, diff_output_file)

    if 'build_and_upload_case_jsonl' in steps:
        # Hit paginated GDC api endpoint, then write data to local jsonl file
        extract_api_response_json(raw_scratch_fp)

        with jsonlines.open(raw_scratch_fp) as raw_data_jsonl_file:
            for record in raw_data_jsonl_file.iter():
                normalized_record = recursively_normalize_field_values(json_records=record, is_single_record=True)
                write_line_to_jsonl(norm_scratch_fp, normalized_record)

        with open(raw_scratch_fp) as raw_jsonl:
            raw_jsonl_text = raw_jsonl.readlines()

        with open(norm_scratch_fp) as norm_jsonl:
            norm_jsonl_text = norm_jsonl.readlines()

        # Find and print the diff:
        for line in difflib.unified_diff(raw_jsonl_text, norm_jsonl_text,
                                         fromfile=raw_scratch_fp, tofile=norm_scratch_fp, lineterm=''):
            with open(diff_scratch_fp, "w") as diff_file:
                diff_file.write(line)

        # Upload bulk jsonl file to Google Cloud bucket
        upload_to_bucket(BQ_PARAMS, norm_scratch_fp, delete_local=True)
        upload_to_bucket(BQ_PARAMS, diff_scratch_fp, delete_local=True)

    if 'create_schema' in steps:
        print("Inferring column data types and generating schema!")
        record_list = list()

        download_from_bucket(BQ_PARAMS, normalized_jsonl_output_file)

        # Create list of record objects for schema analysis
        with open(norm_scratch_fp) as jsonl_file:
            while True:
                file_record = jsonl_file.readline()

                if not file_record:
                    break

                file_json_obj = json.loads(file_record)
                record_list.append(file_json_obj)

        # Infer column type, generate schema, upload to Google Cloud bucket
        create_and_upload_schema_for_json(API_PARAMS, BQ_PARAMS,
                                          record_list=record_list,
                                          table_name=bulk_table_name,
                                          include_release=False,
                                          delete_local=True)

        os.remove(norm_scratch_fp)

    if 'build_bq_table' in steps:
        # Download schema file from Google Cloud bucket
        bulk_table_schema = retrieve_bq_schema_object(API_PARAMS, BQ_PARAMS,
                                                      table_name=bulk_table_name,
                                                      include_release=False)

        # Load jsonl data into BigQuery table
        create_and_load_table_from_jsonl(BQ_PARAMS,
                                         jsonl_file=normalized_jsonl_output_file,
                                         table_id=bulk_table_id,
                                         schema=bulk_table_schema)

    end = format_seconds(time.time() - start)
    print(f"Script executed in {end}\n")


if __name__ == '__main__':
    main(sys.argv)
