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
    create_mapping_dict, create_and_load_table, arrays_to_str_list, has_fatal_error, get_program_from_bq

# used to capture returned yaml config sections
YAML_HEADERS = ('api_and_file_params', 'bq_params', 'steps')


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
            'expand': api_params['EXPAND_FIELD_GROUPS']
        }

        # retrieve and parse a "page" (batch) of case objects
        res = requests.post(url=api_params['ENDPOINT'], data=request_params)

        # return response body if request was successful
        if res.status_code == requests.codes.ok:
            return res

        restart_idx = curr_index
        err_list.append('API request returned status code {}.'.format(str(res.status_code)))

        if api_params['IO_MODE'] == 'a':
            err_list.append(
                'Scripts is being run in "append" mode. '
                'To resume without data loss or duplication, set START_INDEX = {} in your YAML config file.'
                    .format(restart_idx))
    except requests.exceptions.MissingSchema as e:
        err_list.append(str(e) + '(Hint: check the ENDPOINT value supplied in yaml config.)')

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

            # Currently, if response doesn't contain this metadata, it indicates an invalid response or request.
            if 'pagination' in res_json:
                batch_record_count = res_json['pagination']['count']
                total_cases_count = res_json['pagination']['total']
                curr_page = res_json['pagination']['page']
                last_page = res_json['pagination']['pages']
            else:
                has_fatal_error("'pagination' key not found in response json, exiting.", KeyError)

            for case in cases_json:
                case_copy = case.copy()
                for field in api_params['EXCLUDE_FIELDS'].split(','):
                    if field in case_copy:
                        case.pop(field)

                no_list_value_case = arrays_to_str_list(case)
                # writing in jsonlines format, as required by BQ
                json.dump(obj=no_list_value_case, fp=json_output_file)
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

    # Insert the generated jsonl file into google storage bucket, for later ingestion by BQ.
    # Not used when working locally.
    if not api_params['IS_LOCAL_MODE']:
        schema_filename = data_fp.split('/')[-1]
        bucket_target_blob = bq_params['WORKING_BUCKET_DIR'] + '/' + schema_filename
        upload_to_bucket(bq_params['WORKING_BUCKET'], bucket_target_blob, data_fp)


def check_clinical_data(clinical_data_fp, api_params):
    counts = {
        'total': 0,
        'no_clinical_fgs': 0
    }

    programs_with_field_group = {
        'none': set()
    }

    no_fg_case_barcodes = {}

    for fg in api_params['EXPAND_FIELD_GROUPS'].split(','):
        counts[fg] = 0
        programs_with_field_group[fg] = set()

    with open(clinical_data_fp, 'r') as file:
        for line in file:
            counts['total'] += 1

            json_line = json.loads(line)
            program_name = get_program_from_bq(json_line['submitter_id'])
            print(program_name)
            print()

            if 'demographic' in json_line:
                counts['demographic'] += 1
                programs_with_field_group['demographic'].add(program_name)
            if 'diagnoses' in json_line:
                diagnoses = json_line['diagnoses'][0]
                counts['diagnoses'] += 1
                programs_with_field_group['diagnoses'].add(program_name)
                if 'annotations' in diagnoses:
                    counts['diagnoses.annotations'] += 1
                    programs_with_field_group['diagnoses.annotations'].add(program_name)
                if 'treatments' in diagnoses.keys():
                    counts['diagnoses.treatments'] += 1
                    programs_with_field_group['diagnoses.treatments'].add(program_name)
            if 'exposures' in json_line:
                counts['exposures'] += 1
                programs_with_field_group['exposures'].add(program_name)
            if 'family_histories' in json_line:
                counts['family_histories'] += 1
                programs_with_field_group['family_histories'].add(program_name)
            if 'follow_ups' in json_line:
                counts['follow_ups'] += 1
                programs_with_field_group['follow_ups'].add(program_name)
                if 'molecular_tests' in json_line['follow_ups'][0]:
                    programs_with_field_group['follow_ups.molecular_tests'].add(program_name)

            # Case has no clinical data field groups in API
            if 'demographic' not in json_line and 'family_histories' not in json_line \
                    and 'exposures' not in json_line and 'diagnoses' not in json_line \
                    and 'follow_ups' not in json_line:
                programs_with_field_group['none'].add(program_name)
                counts['no_clinical_fgs'] += 1

                if program_name not in no_fg_case_barcodes:
                    no_fg_case_barcodes[program_name] = set()
                no_fg_case_barcodes[program_name].add(json_line['submitter_id'])

        # OUTPUT RESULTS
        for fg in api_params['EXPAND_FIELD_GROUPS']:
            print_field_group_check(fg, counts, programs_with_field_group)

        print("\nPrograms with no clinical data:")

        for program in no_fg_case_barcodes:
            no_fg_case_count = len(no_fg_case_barcodes[program])
            print('\n{} has {} cases with no clinical data.'.format(program, str(no_fg_case_count)))
            print('submitter_id (case_barcode) list:')
            print(no_fg_case_barcodes[program])


def print_field_group_check(fg_name, counts, fg_program_list):
    fg_pct = counts[fg_name] / (counts['total'] * 1.0) * 100

    print('For {}:'.format(fg_name))
    print('\tfound in {:.2f}% of cases'.format(fg_pct))
    print('\tprograms with {} field_group: {}'.format(fg_name, str(fg_program_list['fg_name'])))


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
            print("[INFO] Not adding field {} because no type found".format(key))
            continue

        # Note: I could likely go back use ARRAY as a column type. It wasn't working before, and I believe the issue
        # was that I'd set the FieldSchema object's mode to NULLABLE, which I later read is invalid for ARRAY types.
        # But, that'll mean more unnesting for the users. So for now, I've converted these lists of ids into
        # comma-delineated strings of ids.
        # if key in array_fields:
        #    field_type = "ARRAY<" + field_type + ">"

        # this is the format for bq schema json object entries
        schema_dict[key] = {
            "name": column_name,
            "type": field_type,
            "description": description
        }

    return schema_dict


def create_bq_schema(api_params, data_fp):
    """
    Generates two dicts (one using data type inference, one using _mapping API endpoint.)
    Compares their values and builds a python SchemaField object that's used to initialize the db table.
    :param data_fp: path to API data output file (jsonl format)
    :param api_params: dict of YAML api and file config params
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

    field_data_type_dict = infer_data_types(field_dict)

    # create a flattened dict of schema fields
    schema_dict = create_field_records_dict(field_mapping_dict, field_data_type_dict, array_fields)

    endpoint_name = api_params['ENDPOINT'].split('/')[-1]
    
    return generate_bq_schema(schema_dict, endpoint_name, api_params['EXPAND_FIELD_GROUPS'])


def validate_params(api_params, bq_params):
    """
    Validates yaml parameters before beginning to execute the script. This checks for reasonable (though not necessarily
    correct) api request param types and values. It confirms all params are included in specified yaml file.
    :param api_params: dict of api and file related params from user-provided yaml config
    :param bq_params: dict of bq related params from user-provided yaml config
    """
    err_list = []

    def is_valid_idx_param(yaml_param):
        """
        Verifies that index-type params provided are non-negative integer values.
        :param yaml_param: value to verify
        """
        e_list = []

        try:
            if int(api_params[yaml_param]) < 0:
                e_list.append('Invalid value for {} in yaml config (supplied: {}).'
                              .format(yaml_param, type(api_params[yaml_param])))
                e_list.append('Value should be a non-negative integer.')
                has_fatal_error(e_list, ValueError)
        except TypeError as e:
            # triggered by casting an inappropriate type to int for testing
            e_list.append('{} in yaml config should be of type int, not type {}).'
                          .format(yaml_param, type(api_params[yaml_param])))
            e_list.append(str(e))
            has_fatal_error(e_list, TypeError)

    try:
        if api_params['IS_LOCAL_MODE']:
            yaml_template_path = '../ConfigFiles/ClinicalBQBuild.yaml'
        else:
            home = expanduser('~')
            yaml_template_path = home + '/NextGenETL/ConfigFiles/ClinicalBQBuild.yaml'

        with open(yaml_template_path, mode='r') as yaml_file:
            default_api_params, default_bq_params, steps = load_config(yaml_file, YAML_HEADERS)
            default_api_param_keys = [k for k in default_api_params.keys()]
            default_bq_param_keys = [k for k in default_bq_params.keys()]

            # verify all required params exist in yaml config
            for param in default_api_param_keys:
                val = api_params[param]
            for param in default_bq_param_keys:
                val = bq_params[param]
    except FileNotFoundError as e:
        print('Default yaml config file not found, unable to compare with supplied yaml config.\n' + str(e))
    except ValueError as e:
        has_fatal_error(str(e), e)
    except KeyError as e:
        has_fatal_error('Missing param from yaml config file.', e)

    # verify that api index-related params are set to non-negative integers
    is_valid_idx_param('BATCH_SIZE') and is_valid_idx_param('START_INDEX') and is_valid_idx_param('MAX_PAGES')

    # BATCH_SIZE must also be positive
    if api_params['BATCH_SIZE'] == 0:
        has_fatal_error('BATCH_SIZE set to 0 in yaml_config, should be > 0.', ValueError)


def construct_filepath(api_params):
    """
    Construct filepath for temp local or VM output file
    :param api_params: api and file params from yaml config
    :return: output filepath for local machine or VM (depending on LOCAL_DEBUG_MODE)
    """
    if api_params['IS_LOCAL_MODE']:
        return api_params['LOCAL_DIR'] + api_params['DATA_OUTPUT_FILE']
    else:
        home = expanduser('~')
        return '/'.join([home, api_params['SCRATCH_DIR'], api_params['DATA_OUTPUT_FILE']])


def main(args):
    if len(args) != 2:
        has_fatal_error('Usage : {} <configuration_yaml>".format(args[0])', ValueError)

    # Load the YAML config file
    with open(args[1], mode='r') as yaml_file:
        try:
            api_params, bq_params, steps = load_config(yaml_file, YAML_HEADERS)
        except ValueError as e:
            has_fatal_error(str(e), ValueError)

    # Validate YAML config params
    validate_params(api_params, bq_params)

    data_fp = construct_filepath(api_params)
    schema = None

    if 'retrieve_and_output_cases' in steps:
        # Hits the GDC api endpoint, outputs data to jsonl file (newline-delineated json, required by BQ)
        print('Starting GDC API calls!')
        retrieve_and_output_cases(api_params, bq_params, data_fp)

    if 'check_clinical_data' in steps:
        check_clinical_data(data_fp, api_params)

    if 'create_bq_schema_obj' in steps:
        # Creates a BQ schema python object consisting of nested SchemaField objects
        print('Creating BQ schema object!')
        schema = create_bq_schema(api_params, data_fp)

    if 'build_bq_table' in steps:
        # Creates and populates BQ table
        if not schema:
            has_fatal_error('Empty SchemaField object', UnboundLocalError)
        print('Building BQ Table!')

        # don't want the entire fp for 2nd param, just the file name
        create_and_load_table(bq_params, api_params['DATA_OUTPUT_FILE'], schema)


if __name__ == '__main__':
        # my_args = (sys.argv[0], '../temp/ClinicalBQBuild.yaml')

        main(sys.argv)
        # main(my_args)
