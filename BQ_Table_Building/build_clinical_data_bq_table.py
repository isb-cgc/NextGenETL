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
import sys
import requests
import json
import time
import os
from os.path import expanduser
from common_etl.support import upload_to_bucket
from common_etl.utils import (
    infer_data_types, load_config, generate_bq_schema, collect_field_values,
    create_mapping_dict, create_and_load_table, convert_dict_to_string,
    has_fatal_error
)

API_PARAMS = dict()
BQ_PARAMS = dict()
# used to capture returned yaml config sections
YAML_HEADERS = ('api_and_file_params', 'bq_params', 'steps')


def get_expand_groups():
    if 'EXPAND_FIELD_GROUPS' not in API_PARAMS:
        has_fatal_error('EXPAND_FIELD_GROUPS not in api_params (check yaml config file)')

    return ",".join(API_PARAMS['EXPAND_FIELD_GROUPS'])


def request_from_api(api_params, curr_index):
    """
    Make a POST API request and return response (if valid).
    :param api_params: api params set in yaml config
    :param curr_index: current API poll start position
    :return: response object
    """
    err_list = []

    try:
        request_params = {
            'from': curr_index,
            'size': api_params['BATCH_SIZE'],
            'expand': get_expand_groups()
        }

        # retrieve and parse a "page" (batch) of case objects
        res = requests.post(url=api_params['ENDPOINT'], data=request_params)

        # return response body if request was successful
        if res.status_code == requests.codes.ok:
            return res

        restart_idx = curr_index
        err_list.append(
            'API request returned status code {}.'.format(str(res.status_code)))

        if api_params['IO_MODE'] == 'a':
            err_list.append(
                'Scripts is being run in "append" mode. '
                'To resume without data loss or duplication, set START_INDEX '
                '= {} in your YAML config file.'
                    .format(restart_idx))
    except requests.exceptions.MissingSchema as e:
        err_list.append(str(
            e) + '(Hint: check the ENDPOINT value supplied in yaml config.)')

    has_fatal_error(err_list, res.raise_for_status())


def retrieve_and_output_cases(api_params, bq_params, data_fp):
    """
    Retrieves case records from API and outputs them to a JSONL file,
    which is later used to populate the clinical data BQ table.
    :param api_params: API and file output params, from YAML config
    :param bq_params: BQ params, from YAML config
    :param data_fp: absolute path to data output file
    """
    start_time = time.time()  # for benchmarking
    total_cases_count = 0
    is_last_page = False

    with open(data_fp, api_params['IO_MODE']) as json_output_file:
        curr_index = api_params['START_INDEX']
        while not is_last_page:
            res = request_from_api(api_params, curr_index)

            res_json = res.json()['data']
            cases_json = res_json['hits']

            # Currently, if response doesn't contain this metadata,
            # it indicates an invalid response or request.
            if 'pagination' in res_json:
                batch_record_count = res_json['pagination']['count']
                total_cases_count = res_json['pagination']['total']
                curr_page = res_json['pagination']['page']
                last_page = res_json['pagination']['pages']
            else:
                has_fatal_error(
                    "'pagination' key not found in response json, exiting.",
                    KeyError)

            for case in cases_json:
                if 'days_to_index' in case:
                    print("Found days_to_index!\n{}".format(case))
                case_copy = case.copy()
                for field in api_params['EXCLUDE_FIELDS']:
                    if field in case_copy:
                        case.pop(field)

                no_list_value_case = convert_dict_to_string(case)
                # writing in jsonlines format, as required by BQ
                json.dump(obj=no_list_value_case, fp=json_output_file)
                json_output_file.write('\n')

            if curr_page == last_page or (api_params['MAX_PAGES'] and
                                          curr_page == api_params['MAX_PAGES']):
                is_last_page = True

            print("Inserted page {} of {} ({} records) into jsonlines file"
                  .format(curr_page, last_page, batch_record_count))
            curr_index += batch_record_count

    # calculate processing time and file size
    total_time = time.time() - start_time
    file_size = os.stat(data_fp).st_size / 1048576.0

    print(
        "\nClinical data retrieval complete!"
        "\n\t{} of {} cases retrieved"
        "\n\t{:.2f} mb jsonl file size"
        "\n\t{:.1f} sec to retrieve from GDC API output to jsonl file\n".
        format(curr_index, total_cases_count, file_size, total_time)
    )

    # Insert the generated jsonl file into google storage bucket, for later
    # ingestion by BQ.
    # Not used when working locally.
    # todo remove if
    if not api_params['IS_LOCAL_MODE']:
        jsonl_file = data_fp.split('/')[-1]
        target_blob = bq_params['WORKING_BUCKET_DIR'] + '/' + jsonl_file
        upload_to_bucket(bq_params['WORKING_BUCKET'], target_blob, data_fp)


def create_field_records_dict(field_mapping_dict, field_data_type_dict):
    """
    Generate flat dict containing schema metadata object with fields 'name',
    'type', 'description'
    :param field_mapping_dict:
    :param field_data_type_dict:
    :return: schema fields object dict
    """
    schema_dict = {}

    for key in field_data_type_dict:
        try:
            column_name = field_mapping_dict[key]['name'].split('.')[-1]
            description = field_mapping_dict[key]['description']
        except KeyError:
            # cases.id not returned by mapping endpoint. In such cases,
            # substitute an empty description string.
            column_name = key.split(".")[-1]
            description = ""

        if field_data_type_dict[key]:
            # if script was able to infer a data type using field's values,
            # default to using that type
            field_type = field_data_type_dict[key]
        elif key in field_mapping_dict:
            # otherwise, include type from _mapping endpoint
            field_type = field_mapping_dict[key]['type']
        else:
            # this could happen in the case where a field was added to the
            # cases endpoint with only null values,
            # and no entry for the field exists in mapping
            print(
                "[INFO] Not adding field {} because no type found".format(key))
            continue

        # this is the format for bq schema json object entries
        schema_dict[key] = {
            "name": column_name,
            "type": field_type,
            "description": description
        }

    return schema_dict


def create_bq_schema(api_params, data_fp):
    """
    Generates two dicts (one using data type inference, one using _mapping
    API endpoint.)
    Compares their values and builds a python SchemaField object that's used
    to initialize the db table.
    :param data_fp: path to API data output file (jsonl format)
    :param api_params: dict of YAML api and file config params
    """
    # generate dict containing field mapping results
    field_mapping_dict = create_mapping_dict(api_params['ENDPOINT'])

    with open(data_fp, 'r') as data_file:
        field_dict = dict()

        for line in data_file:
            json_case_obj = json.loads(line)
            for key in json_case_obj:
                field_dict = collect_field_values(field_dict, key,
                                                  json_case_obj, 'cases.')

    field_data_type_dict = infer_data_types(field_dict)

    # create a flattened dict of schema fields
    schema_dict = create_field_records_dict(field_mapping_dict,
                                            field_data_type_dict)

    endpoint_name = api_params['ENDPOINT'].split('/')[-1]

    return generate_bq_schema(schema_dict,
                                   record_type=endpoint_name,
                                   expand_fields_list=get_expand_groups())


def construct_filepath(api_params):
    """
    Construct filepath for temp local or VM output file
    :param api_params: api and file params from yaml config
    :return: output filepath for local machine or VM (depending on
    LOCAL_DEBUG_MODE)
    """
    if api_params['IS_LOCAL_MODE']:
        return api_params['LOCAL_DIR'] + api_params['DATA_OUTPUT_FILE']
    else:
        home = expanduser('~')
        return '/'.join(
            [home, api_params['SCRATCH_DIR'], api_params['DATA_OUTPUT_FILE']])


def main(args):
    start = time.time()
    if len(args) != 2:
        has_fatal_error(
            'Usage: {} <configuration_yaml>".format(args[0])', ValueError)

    # Load the YAML config file
    with open(args[1], mode='r') as yaml_file:
        try:
            # todo uncomment
            # GLOBAL API_PARAMS, BQ_PARAMS
            # API_PARAMS, BQ_PARAMS, steps = load_config(yaml_file, YAML_HEADERS)
            api_params, bq_params, steps = load_config(yaml_file, YAML_HEADERS)
        except ValueError as e:
            has_fatal_error(str(e), ValueError)

    data_fp = construct_filepath(api_params)
    schema = None

    if 'retrieve_and_output_cases' in steps:
        # Hits the GDC api endpoint, outputs data to jsonl file (
        # newline-delineated json, required by BQ)
        print('Starting GDC API calls!')
        retrieve_and_output_cases(api_params, bq_params, data_fp)

    if 'create_bq_schema_obj' in steps:
        # Creates a BQ schema python object consisting of nested SchemaField
        # objects
        print('Creating BQ schema object!')
        schema = create_bq_schema(api_params, data_fp)

    if 'build_bq_table' in steps:
        # Creates and populates BQ table
        if not schema:
            has_fatal_error('Empty SchemaField object', UnboundLocalError)
        print('Building BQ Table!')

        # don't want the entire fp for 2nd param, just the file name
        create_and_load_table(
            bq_params,
            jsonl_rows_file=api_params['DATA_OUTPUT_FILE'],
            schema=schema,
            table_name=api_params['GDC_RELEASE'] + '_clinical_data')

    end = time.time() - start
    print("Script executed in {:.0f} seconds\n".format(end))


if __name__ == '__main__':
    main(sys.argv)
