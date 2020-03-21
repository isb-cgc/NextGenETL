"""
Copyright 2020, Institute for Systems Biology

Permission is hereby granted, free of charge, to any person obtaining a copy of this
software and associated documentation files (the "Software"), to deal in the Software
without restriction, including without limitation the rights to use, copy, modify,
merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies
or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING
BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""
import sys
import requests
import json
import time
import os
from os.path import expanduser
from common_etl.support import upload_to_bucket
from common_etl.utils import infer_data_types, load_config, generate_bq_schema_json, collect_field_values, \
    create_mapping_dict, create_table_from_json_schema

YAML_HEADERS = ('api_and_file_params', 'bq_params', 'steps')
REQUIRED_YAML_PARAMS = ['ENDPOINT', 'EXPAND_FIELD_GROUPS', 'API_BATCH_SIZE', 'START_INDEX', 'MAX_PAGES', 'IO_MODE',
                        'OUTPUT_FILEPATH', 'BQ_AS_BATCH', 'WORKING_PROJECT', 'TARGET_DATASET', 'WORKING_BUCKET',
                        'WORKING_BUCKET_DIR', 'TARGET_TABLE', 'BQ_SCHEMA_FILEPATH']


def request_from_api(params, curr_index):
    try:
        request_params = {
            'from': curr_index,
            'size': params['API_BATCH_SIZE'],
            'expand': params['EXPAND_FIELD_GROUPS']
        }

        # retrieve and parse a "page" (batch) of case objects
        res = requests.post(url=params['ENDPOINT'], data=request_params)

        if res.status_code != 200:
            restart_idx = curr_index

            print('\n[ERROR] API request returned status code {}, exiting script.'.format(str(res.status_code)))
            if params['IO_MODE'] == 'a':
                print('IO_MODE set to append--set START_INDEX to {} to resume api calls and avoid duplicate entries.'
                      .format(restart_idx))
            return None

        return res
    except requests.exceptions.MissingSchema as e:
        print('\n[ERROR] ' + str(e) + '(Hint: check the ENDPOINT value supplied in yaml config.)')
        return None


def retrieve_and_output_cases(params):
    """
    Retrieves case records from API and outputs them to a JSON file,
    which is later used to populate the clinical data BQ table.
    :param params: API and file output params, from YAML configuration file
    """
    start_time = time.time()  # for benchmarking
    total_cases_count = 0
    is_last_page = False

    with open(params['OUTPUT_FILEPATH'], params['IO_MODE']) as json_output_file:
        curr_index = params['START_INDEX']

        print('Starting API calls!')

        while not is_last_page:
            res = request_from_api(params, curr_index)

            if not res:
                exit(1)

            res_json = res.json()['data']
            cases_json = res_json['hits']

            # Currently, if response doesn't contain this metadata, it indicates an invalid response or request.
            if 'pagination' in res_json:
                batch_record_count = res_json['pagination']['count']
                total_cases_count = res_json['pagination']['total']
                curr_page = res_json['pagination']['page']
                last_page = res_json['pagination']['pages']
            else:
                raise TypeError("[ERROR] 'pagination' key not found in response json, exiting.")

            for i in range(len(cases_json)):
                # writing in jsonlines format, as required by BQ
                json.dump(obj=cases_json[i], fp=json_output_file)
                json_output_file.write('\n')

            if curr_page == last_page or (params['MAX_PAGES'] and curr_page == params['MAX_PAGES']):
                is_last_page = True

            print("Inserted page {} of {} ({} records) into jsonlines file"
                  .format(curr_page, last_page, batch_record_count))
            curr_index += batch_record_count  # todo: verify this tweak ok

    # calculate processing time and file size
    total_time = time.time()-start_time
    file_size = os.stat(params['OUTPUT_FILEPATH']).st_size / 1048576.0

    print(
        "\nClinical data retrieval complete! \n\n"
        "RESULTS REPORT \n"
        "------------- \n"
        "{} cases inserted \n"
        "{} cases available \n"
        "{:.3f} mb file size \n"
        "{:.3f} sec to execute script \n".format(curr_index, total_cases_count, file_size, total_time)
    )

    schema_filename = params['OUTPUT_FILEPATH'].split('/')[-1]
    bucket_target_blob = params['WORKING_BUCKET_DIR'] + '/' + schema_filename
    upload_to_bucket(params['WORKING_BUCKET'], bucket_target_blob, params['OUTPUT_FILEPATH'])


def create_field_records_dict(field_mapping_dict, field_data_type_dict):
    """
    Generate flat dict containing schema metadata object with fields 'name', 'type', 'description'
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
            # cases.id not returned by mapping endpoint. In such cases, substitute an empty description string.
            column_name = key.split(".")[-1]
            description = ""

        if field_data_type_dict[key]:
            # if script was able to infer a data type using field's values, default to using that type
            field_type = field_data_type_dict[key]
        elif key in field_mapping_dict:
            # otherwise, include type from _mapping endpoint
            field_type = field_mapping_dict[key]['type']
        else:
            # this could happen in the case where a field was added to the cases endpoint with only null values,
            # and no entry for the field exists in mapping
            print("[ERROR] Not adding field {} because no type found".format(key))
            continue

        # this is the format for bq schema json object entries
        schema_dict[key] = {
            "name": column_name,
            "type": field_type,
            "description": description
        }

    return schema_dict


def create_bq_schema_file(params):
    """
    Creates a BQ schema JSON file.
    :param params: YAML config params
    """
    # generate dict containing field mapping results
    field_mapping_dict = create_mapping_dict(params['ENDPOINT'])

    with open(params['OUTPUT_FILEPATH'], 'r') as data_file:
        json_obj = "{'cases': ["

        line = data_file.readline()

        while line != '':
            json_obj += line + ', '

        json_obj += ']}'

        if not data_file:
            print('[ERROR] Empty result retrieved from dataset file, nothing to generate.')
            exit(1)

    # for each field, generate a set of values used to infer datatype
    field_value_sets = collect_field_values(dict(), 'cases', json_obj, '')
    field_data_type_dict = infer_data_types(field_value_sets)

    # create a flattened dict of schema fields
    schema_dict = create_field_records_dict(field_mapping_dict, field_data_type_dict)

    record_type_name = params['ENDPOINT'].split('/')[-1]

    generate_bq_schema_json(schema_dict, record_type_name, params['EXPAND_FIELD_GROUPS'], params['BQ_SCHEMA_FILEPATH'])


def validate_params(params):
    err_string = ''

    def is_valid_idx_param(yaml_param):
        err_str = ''
        try:
            if not isinstance(params[yaml_param], int) or int(params[yaml_param]) < 0:
                err_str += '[ERROR] Invalid value for {} in yaml config (supplied value: {}). ' \
                          'Value should be a non-negative integer'.format(yaml_param, params[yaml_param])
        except ValueError as e:
            err_str += "\n[ERROR] Non-integer value for {} in yaml config:\n".format(yaml_param)
            err_str += "[ERROR] " + str(e)
        return err_str

    # verify all required params exist in yaml config
    for param in REQUIRED_YAML_PARAMS:
        if param not in params:
            err_string += '[ERROR] Required param {} not found in yaml config.'.format(param)

    # verify that api index-related params are set to non-negative integers
    err_string += is_valid_idx_param('API_BATCH_SIZE')
    err_string += is_valid_idx_param('START_INDEX')
    err_string += is_valid_idx_param('MAX_PAGES')

    # API_BATCH_SIZE must also be positive
    if params['API_BATCH_SIZE'] == 0:
        err_string += '[ERROR] API_BATCH_SIZE in yaml_config should be greater than 0.'

    if err_string:
        print(err_string)
        exit(1)


def convert_filepaths(params):
    """
    Convert to vm-friendly file paths
    :param params: params set in yaml config
    :return: modified params dict
    """
    home = expanduser('~')
    params["OUTPUT_FILEPATH"] = params["OUTPUT_FILEPATH"].replace('~', home)
    params["BQ_SCHEMA_FILEPATH"] = params["BQ_SCHEMA_FILEPATH"].replace('~', home)
    return params


def main(args):
    if len(args) != 2:
        print(" ")
        print(" Usage : {} <configuration_yaml>".format(args[0]))
        return

    # Load the YAML config file
    with open(args[1], mode='r') as yaml_file:
        api_and_file_params, bq_params, steps = load_config(yaml_file, YAML_HEADERS)

    # FIXME: Maybe go back and split params up?
    # FIXME: change file extension to jsonl for clinical dat

    params = api_and_file_params.copy()
    params.update(bq_params)

    # Validate YAML config params
    validate_params(params)
    params = convert_filepaths(params)

    if 'retrieve_and_output_cases' in steps:
        pass
        # Hits the GDC api endpoint, builds a json output data file
        # todo: un-comment these
        # print('Starting GDC API calls!')
        # retrieve_and_output_cases(params)

    if 'create_bq_schema_file' in steps:
        # Creates a BQ schema json file
        print('Creating BQ Schema')
        create_bq_schema_file(params)

    if 'build_bq_table' in steps:
        # Creates and populates BQ table
        print('Building BQ Table {}'.params['TARGET_TABLE'])
        create_table_from_json_schema(params)


if __name__ == '__main__':
    main(sys.argv)
