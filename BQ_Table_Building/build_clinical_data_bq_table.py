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

import requests
import json
import time
import os
from common_etl.utils import infer_data_types, load_config, generate_bq_schema, collect_field_values, \
    create_mapping_dict
from google.cloud import bigquery

YAML_HEADERS = ('params', 'steps')
REQUIRED_YAML_PARAMS = ['ENDPOINT', 'EXPAND_FIELD_GROUPS', 'BATCH_SIZE', 'START_INDEX', 'MAX_PAGES', 'IO_MODE',
                        'OUTPUT_FILEPATH', 'BQ_AS_BATCH', 'BQ_SCHEMA_FILEPATH']


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
        inserted_count = 0

        '''
        Create a single parent array for case objects (if not stripped, it would append an array for each batch). 
        Since it's not constructed in-memory, there's no size limit.
        '''
        json_output_file.write('{"cases": [')

        while not is_last_page:
            try:
                request_params = {
                    'from': params['START_INDEX'],
                    'size': params['BATCH_SIZE'],
                    'expand': params['EXPAND_FIELD_GROUPS']
                }

                # retrieve and parse a "page" (batch) of case objects
                res = requests.post(url=params['ENDPOINT'], data=request_params)

                if res.status_code != 200:
                    restart_idx = inserted_count + params['START_INDEX']

                    print('\n[ERROR] API request returned status code {}, exiting script.'.format(str(res.status_code)))
                    print('This round, script inserted {} records. If IO_MODE is set to append in yaml conig, '
                          'set START_INDEX to {} to resume writing to output file.'.format(inserted_count, restart_idx))
                    exit(1)
            except requests.exceptions.MissingSchema as err:
                print('\n[ERROR] ', end='')
                print(err)
                print('(Hint: check the ENDPOINT value supplied in yaml config.)')
                exit(1)

            res_json = res.json()['data']

            # Currently, if response doesn't contain this metadata, it indicates an invalid response or request.
            if 'pagination' in res_json:
                response_metadata = res_json['pagination']
                total_cases_count = response_metadata['total']
                curr_page = response_metadata['page']
                last_page = response_metadata['pages']
                cases_json = res_json['hits']
            else:
                raise TypeError("[ERROR] 'pagination' key not found in response json, exiting.")

            # iterate over each case object and append to json file
            for i in range(len(cases_json)):
                json.dump(obj=cases_json[i], fp=json_output_file)

                if i < len(cases_json) - 1:
                    json_output_file.write(', ')

            if curr_page != last_page or (params['MAX_PAGES'] and curr_page < params['MAX_PAGES']):
                json_output_file.write(',')

                # increment starting index for next batch of cases
                request_params['from'] += request_params['size']
            else:
                is_last_page = True

                # If this is the last page, append metadata and finish constructing the json object
                json_metadata = {
                    "inserted_count": inserted_count,
                    "total_available_count": response_metadata['total']
                }

                json_output_file.write('], "metadata": ')
                json.dump(obj=json_metadata, fp=json_output_file)
                json_output_file.write('}')

            print("Inserted page {} of {}!".format(curr_page, last_page))
            inserted_count += response_metadata['count']

    # calculate processing time and file size
    total_time = time.time()-start_time
    file_size = os.stat(params['OUTPUT_FILEPATH']).st_size / 1048576.0

    print(
        "\nClinical data retrieval complete. \n\n"
        "RESULTS REPORT \n"
        "------------- \n"
        "{} cases inserted \n"
        "{} cases available \n"
        "{:.3f} mb file size \n"
        "{:.3f} sec to execute script \n".format(inserted_count, total_cases_count, file_size, total_time)
    )


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
        json_obj = json.load(data_file)

        if not json_obj:
            print('[ERROR] Empty result retrieved from dataset file, nothing to generate.')
            exit(1)

    # for each field, generate a set of values used to infer datatype
    field_value_sets = collect_field_values(dict(), 'cases', json_obj, '')
    field_data_type_dict = infer_data_types(field_value_sets)

    # create a flattened dict of schema fields
    schema_dict = create_field_records_dict(field_mapping_dict, field_data_type_dict)

    schema_fp = os.getcwd() + params['BQ_SCHEMA_FILEPATH']
    record_type_name = params['ENDPOINT'].split('/')[-1]

    generate_bq_schema(schema_dict, record_type_name, params['EXPAND_FIELD_GROUPS'], schema_fp)


def validate_params(params):
    def is_valid_idx_param(yaml_param):
        try:
            if not isinstance(params[yaml_param], int) or int(params[yaml_param]) < 0:
                print('[ERROR] Invalid value for {} in yaml config (supplied value: {}). '
                      'Value should be a non-negative integer'.format(yaml_param, params[yaml_param]))
        except ValueError as e:
            print("\n[ERROR] Non-integer value for {} in yaml config:".format(yaml_param))
            print("[ERROR] ", end='')
            print(e)

        return True

    # verify all required params exist in yaml config
    for param in REQUIRED_YAML_PARAMS:
        if param not in params:
            print('[ERROR] Required param {} not found in yaml config.'.format(param))
            exit(1)

    # verify that api index-related params are set to non-negative integers
    if not (is_valid_idx_param('BATCH_SIZE') and is_valid_idx_param('START_INDEX') and is_valid_idx_param('MAX_PAGES')):
        exit(1)

    # BATCH_SIZE must also be positive
    if params['BATCH_SIZE'] == 0:
        print('[ERROR] BATCH_SIZE in yaml_config should be greater than 0.')
        exit(1)

    # confirm that API request url is valid
    try:
        request_params = {
            'from': params['START_INDEX'],
            'size': params['BATCH_SIZE'],
            'expand': params['EXPAND_FIELD_GROUPS']
        }
        res = requests.post(url=params['ENDPOINT'], data=request_params)

        if res.status_code != 200:
            print('API request return status {}, exiting script.'.format(str(res.status_code)))
            exit(1)
    except requests.exceptions.MissingSchema as err:
        print('\n[ERROR] ', end='')
        print(err)
        print('(Hint: check the ENDPOINT value supplied in yaml config.)')
        exit(1)


def main(args):
    if len(args) != 2:
        print(" ")
        print(" Usage : {} <configuration_yaml>".format(args[0]))
        return

    # Load the YAML config file
    with open(args[1], mode='r') as yaml_file:
        params, steps = load_config(yaml_file, YAML_HEADERS)

    # Validate YAML config params
    validate_params(params)

    if 'retrieve_and_output_cases' in steps:
        # Hits the GDC api endpoint, builds a json output data file
        retrieve_and_output_cases(params)

    if 'create_bq_schema_file' in steps:
        # Creates a BQ schema json file
        create_bq_schema_file(params)


if __name__ == '__main__':
    # main(sys.argv)
    main(('build_clinical_data_bq_table.py', '../ConfigFiles/ClinicalBQBuild.yaml'))
