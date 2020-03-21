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
from common_etl.utils import infer_data_types, load_config, generate_bq_schema, collect_field_values, \
    create_mapping_dict, create_and_load_table

YAML_HEADERS = ('api_and_file_params', 'bq_params', 'steps')
API_PARAM_LIST = ['ENDPOINT', 'EXPAND_FIELD_GROUPS', 'BATCH_SIZE', 'START_INDEX', 'MAX_PAGES', 'IO_MODE',
                  'SCRATCH_DIR', 'DATA_OUTPUT_FILE']
BQ_PARAM_LIST = ['BQ_AS_BATCH', 'WORKING_PROJECT', 'TARGET_DATASET', 'WORKING_BUCKET', 'WORKING_BUCKET_DIR',
                 'TARGET_TABLE']
LOCAL_TEST = False

EXCLUDE_FIELDS = ['analyte_ids', 'case_autocomplete', 'portion_ids', 'sample_ids', 'slide_ids', 'submitter_aliquot_ids',
                  'submitter_analyte_ids', 'submitter_portion_ids', 'submitter_sample_ids', 'submitter_slide_ids']


def request_from_api(api_params, curr_index):
    try:
        request_params = {
            'from': curr_index,
            'size': api_params['BATCH_SIZE'],
            'expand': api_params['EXPAND_FIELD_GROUPS']
        }

        # retrieve and parse a "page" (batch) of case objects
        res = requests.post(url=api_params['ENDPOINT'], data=request_params)

        if res.status_code == 200:
            return res
        else:
            restart_idx = curr_index

            print('\n[ERROR] API request returned status code {}, exiting script.'.format(str(res.status_code)))
            if api_params['IO_MODE'] == 'a':
                print('IO_MODE set to append--set START_INDEX to {} to resume api calls and avoid duplicate entries.'
                      .format(restart_idx))
    except requests.exceptions.MissingSchema as e:
        print('\n[ERROR] ' + str(e) + '(Hint: check the ENDPOINT value supplied in yaml config.)')

    exit(1)


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

            # Currently, if response doesn't contain this metadata, it indicates an invalid response or request.
            if 'pagination' in res_json:
                batch_record_count = res_json['pagination']['count']
                total_cases_count = res_json['pagination']['total']
                curr_page = res_json['pagination']['page']
                last_page = res_json['pagination']['pages']
            else:
                raise TypeError("[ERROR] 'pagination' key not found in response json, exiting.")

            for case in cases_json:
                case_copy = case.copy()
                for field in EXCLUDE_FIELDS:
                    if field in case_copy:
                        case.pop(field)

                # modified_case = arrays_to_str_list(case)
                # writing in jsonlines format, as required by BQ
                json.dump(obj=case, fp=json_output_file)
                json_output_file.write('\n')

            if curr_page == last_page or (api_params['MAX_PAGES'] and curr_page == api_params['MAX_PAGES']):
                is_last_page = True

            print("Inserted page {} of {} ({} records) into jsonlines file"
                  .format(curr_page, last_page, batch_record_count))
            curr_index += batch_record_count

    # calculate processing time and file size
    total_time = time.time()-start_time
    file_size = os.stat(data_fp).st_size / 1048576.0

    print(
        "\nClinical data retrieval complete! \n\n"
        "RESULTS REPORT \n"
        "------------- \n"
        "{} cases inserted \n"
        "{} cases available \n"
        "{:.3f} mb file size \n"
        "{:.3f} sec to execute script \n".format(curr_index, total_cases_count, file_size, total_time)
    )

    schema_filename = data_fp.split('/')[-1]
    bucket_target_blob = bq_params['WORKING_BUCKET_DIR'] + '/' + schema_filename

    if not LOCAL_TEST:
        upload_to_bucket(bq_params['WORKING_BUCKET'], bucket_target_blob, data_fp)


def create_field_records_dict(field_mapping_dict, field_data_type_dict, array_fields):
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

        if key in array_fields:
            field_type = "ARRAY<" + field_type + ">"

        # this is the format for bq schema json object entries
        schema_dict[key] = {
            "name": column_name,
            "type": field_type,
            "description": description
        }

    return schema_dict


def create_bq_schema(api_params, data_fp):
    """
    :param data_fp:
    :param api_params: YAML config params
    """
    # generate dict containing field mapping results
    field_mapping_dict = create_mapping_dict(api_params['ENDPOINT'])

    with open(data_fp, 'r') as data_file:
        field_dict = dict()
        array_fields = set()

        for line in data_file:
            json_case_obj = json.loads(line)
            for key in json_case_obj:
                field_dict, array_fields = collect_field_values(field_dict, key, json_case_obj, 'cases.', array_fields)

        print(array_fields)

    field_data_type_dict = infer_data_types(field_dict)

    # create a flattened dict of schema fields
    schema_dict = create_field_records_dict(field_mapping_dict, field_data_type_dict, array_fields)

    endpoint_name = api_params['ENDPOINT'].split('/')[-1]
    
    return generate_bq_schema(schema_dict, endpoint_name, api_params['EXPAND_FIELD_GROUPS'])


def validate_params(api_params, bq_params):
    err_string = ''

    def is_valid_idx_param(yaml_param):
        err_str = ''
        try:
            if not isinstance(api_params[yaml_param], int) or int(api_params[yaml_param]) < 0:
                err_str += '[ERROR] Invalid value for {} in yaml config (supplied value: {}). ' \
                          'Value should be a non-negative integer'.format(yaml_param, api_params[yaml_param])
        except ValueError as e:
            err_str += "\n[ERROR] Non-integer value for {} in yaml config:\n".format(yaml_param)
            err_str += "[ERROR] " + str(e)
        return err_str

    # verify all required params exist in yaml config
    for param in API_PARAM_LIST:
        if param not in api_params:
            err_string += '[ERROR] Required param {} not found in yaml config.'.format(param)
    for param in BQ_PARAM_LIST:
        if param not in bq_params:
            err_string += '[ERROR] Required param {} not found in yaml config.'.format(param)

    # verify that api index-related params are set to non-negative integers
    err_string += is_valid_idx_param('BATCH_SIZE')
    err_string += is_valid_idx_param('START_INDEX')
    err_string += is_valid_idx_param('MAX_PAGES')

    # BATCH_SIZE must also be positive
    if api_params['BATCH_SIZE'] == 0:
        err_string += '[ERROR] BATCH_SIZE in yaml_config should be greater than 0.'

    if err_string:
        print(err_string)
        exit(1)


def convert_filepath(file_dir, file_name):
    """
    Convert to vm-friendly file path
    :param file_dir: path to file
    :param file_name: name of file
    :return: filepath
    """
    if LOCAL_TEST:
        return '../temp/' + file_name

    home = expanduser('~')
    return '/'.join([home, file_dir, file_name])


def main(args):
    if len(args) != 2:
        print(" ")
        print(" Usage : {} <configuration_yaml>".format(args[0]))
        return

    # Load the YAML config file
    with open(args[1], mode='r') as yaml_file:
        api_params, bq_params, steps = load_config(yaml_file, YAML_HEADERS)

    # Validate YAML config params
    validate_params(api_params, bq_params)
    data_fp = convert_filepath(api_params['SCRATCH_DIR'], api_params['DATA_OUTPUT_FILE'])
    schema = None

    if 'retrieve_and_output_cases' in steps:
        # Hits the GDC api endpoint, builds a json output data file
        print('Starting GDC API calls!')
        retrieve_and_output_cases(api_params, bq_params, data_fp)

    if 'create_bq_schema_file' in steps:
        # Creates a BQ schema json file
        print('Creating BQ Schema')
        schema = create_bq_schema(api_params, data_fp)
        print(schema)

    if 'build_bq_table' in steps:
        # Creates and populates BQ table
        if not schema:
            print('[ERROR] Empty SchemaField object')
            exit(1)
        print('Building BQ Table')
        create_and_load_table(bq_params, api_params['DATA_OUTPUT_FILE'], schema)


if __name__ == '__main__':
    if LOCAL_TEST:
        main((sys.argv[0], '../temp/ClinicalBQBuild.yaml'))
    else:
        main(sys.argv)
