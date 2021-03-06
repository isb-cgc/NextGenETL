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
import json
import os
import sys

from google.cloud import bigquery

from common_etl.utils import (has_fatal_error, infer_data_types, load_config, get_rel_prefix, get_scratch_fp,
                              upload_to_bucket, create_and_load_table, get_working_table_id, format_seconds)

API_PARAMS = dict()
BQ_PARAMS = dict()
# used to capture returned yaml config sections
YAML_HEADERS = ('api_params', 'bq_params', 'steps')


##################################################################################
#
#   API calls and data normalization (for BQ table insert)
#
##################################################################################


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
            'expand': ",".join(API_PARAMS['FIELD_GROUPS'])  # note: removed list wrapper
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


def retrieve_and_save_case_records(scratch_fp):
    """Retrieves case records from API and outputs them to a JSONL file, which is later
        used to populate the clinical data BQ table.

    :param scratch_fp: absolute path to data output file
    """

    def convert_dict_to_string(obj):
        """Converts dict/list of primitives or strings to a comma-separated string. Used
        to write data to file.

        :param obj: object to converts
        :return: modified object
        """
        if isinstance(obj, list):
            if not isinstance(obj[0], dict):
                str_list = ', '.join(obj)
                obj = str_list
            else:
                for idx, value in enumerate(obj.copy()):
                    obj[idx] = convert_dict_to_string(value)
        elif isinstance(obj, dict):
            for key in obj:
                obj[key] = convert_dict_to_string(obj[key])
        return obj

    start_time = time.time()  # for benchmarking
    cases_count = 0
    is_last_page = False
    io_mode = BQ_PARAMS['IO_MODE']

    with open(scratch_fp, io_mode) as jsonl_file:
        print("Outputting json objects to {0} in {1} mode".format(scratch_fp, io_mode))
        have_printed_totals = False

        curr_index = API_PARAMS['START_INDEX']
        while not is_last_page:
            res = request_data_from_gdc_api(curr_index)

            res_json = res.json()['data']
            cases_json = res_json['hits']

            # If response doesn't contain pagination, indicates an invalid request.
            if 'pagination' not in res_json:
                has_fatal_error("'pagination' key not found in response json, exiting.",
                                KeyError)

            batch_record_count = res_json['pagination']['count']
            cases_count = res_json['pagination']['total']

            if not have_printed_totals:
                have_printed_totals = True
                print("Total cases for r{0}: {1}".format
                      (BQ_PARAMS['RELEASE'], cases_count))
                print("Batch size: {0}".format(batch_record_count))

            for case in cases_json:
                case_copy = case.copy()

                for field in API_PARAMS['EXCLUDE_FIELDS']:
                    if field in case_copy:
                        case.pop(field)

                no_list_value_case = convert_dict_to_string(case)
                json.dump(obj=no_list_value_case, fp=jsonl_file)
                jsonl_file.write('\n')

            curr_page = res_json['pagination']['page']
            last_page = res_json['pagination']['pages']

            if curr_page == last_page:
                is_last_page = True
            elif API_PARAMS['MAX_PAGES'] and curr_page == API_PARAMS['MAX_PAGES']:
                is_last_page = True

            print("Inserted page {0} of {1} into jsonl file".format(curr_page, last_page))
            curr_index += batch_record_count

    # calculate processing time and file size
    total_time = time.time() - start_time
    file_size = os.stat(scratch_fp).st_size / 1048576.0

    print()
    print("Clinical data retrieval complete!")
    print("\t{} of {} cases retrieved".format(curr_index, cases_count))
    print("\t{:.2f} mb jsonl file size".format(file_size))
    print("\t{:.1f} sec to retrieve from GDC API output to jsonl file".format(total_time))
    print()


##################################################################################
#
#   BQ table creation and data insertion
#
##################################################################################


def create_field_records_dict(field_mappings, field_data_types):
    """ Generate schema dict composed of schema field dicts.

    :param field_mappings: dict of {fields: schema entry dicts}
    :param field_data_types: dict of {fields: data types}
    :return: SchemaField object dict
    """
    # this could use BQ Python API built-in method
    schema_dict = {}

    for field in field_data_types:
        try:
            column_name = field_mappings[field]['name'].split('.')[-1]
            description = field_mappings[field]['description']
        except KeyError:
            # cases.id not returned by mapping endpoint. In such cases,
            # substitute an empty description string.
            column_name = field.split(".")[-1]
            description = ""

        if field_data_types[field]:
            # if script was able to infer a data type using field's values,
            # default to using that type
            field_type = field_data_types[field]
        elif field in field_mappings:
            # otherwise, include type from _mapping endpoint
            field_type = field_mappings[field]['type']
        else:
            # this could happen in the case where a field was added to the
            # cases endpoint with only null values,
            # and no entry for the field exists in mapping
            print("[INFO] Not adding field {0} because no type found".format(field))
            continue

        # this is the format for bq schema json object entries
        schema_dict[field] = {
            "name": column_name,
            "type": field_type,
            "description": description
        }

    return schema_dict


def generate_bq_schema(schema_dict, record_type, expand_fields_list):
    """Generates BigQuery SchemaField list for insertion of case records.

    :param schema_dict: dict of schema fields
    :param record_type: type of field/field group
    :param expand_fields_list: list of field groups included in API request
    :return: list of SchemaFields for case record insertion
    """
    # add fields to a list in order to generate a dict representing nested fields
    field_group_names = [record_type]
    nested_depth = 0

    for field_group in expand_fields_list.split(','):
        nested_field_name = record_type + '.' + field_group
        nested_depth = max(nested_depth, len(nested_field_name.split('.')))
        field_group_names.append(nested_field_name)

    record_lists_dict = {field_grp_name: [] for field_grp_name in field_group_names}
    # add field to correct field grouping list based on full field name
    for field in schema_dict:
        # record_lists_dict key is equal to the parent field components of
        # full field name
        # remove field from period-delimited field group string
        field_group = ".".join(field.split('.')[:-1])

        record_lists_dict[field_group].append(schema_dict[field])

    temp_schema_field_dict = {}

    while nested_depth >= 1:
        for field_group_name in record_lists_dict:
            split_group_name = field_group_name.split('.')

            # builds from max depth inward to avoid iterating through entire schema obj
            # in order to append child field groups. Skip any shallower field groups.
            if len(split_group_name) != nested_depth:
                continue

            schema_field_sublist = []

            for record in record_lists_dict[field_group_name]:
                schema_field = bigquery.SchemaField(name=record['name'],
                                                    field_type=record['type'],
                                                    mode='NULLABLE',
                                                    description=record['description'],
                                                    fields=())

                schema_field_sublist.append(schema_field)

            # remove field from period-delimited field group string
            parent_name = ".".join(field_group_name.split('.')[:-1])

            if field_group_name in temp_schema_field_dict:
                schema_field_sublist += temp_schema_field_dict[field_group_name]

            if parent_name:
                if parent_name not in temp_schema_field_dict:
                    temp_schema_field_dict[parent_name] = list()

                '''
                NOTE: removed get_field_name() wrapper from name argument--that shouldn't be needed and is 
                being relocated from utils.py to build_clinical_data_program_tables.py 
                '''
                schema_field = bigquery.SchemaField(name=field_group_name,
                                                    field_type='RECORD',
                                                    mode='REPEATED',
                                                    description='',
                                                    fields=tuple(schema_field_sublist))

                temp_schema_field_dict[parent_name].append(schema_field)
            else:
                if nested_depth > 1:
                    has_fatal_error("Empty parent_name at level {}".format(nested_depth), ValueError)
                return schema_field_sublist

        nested_depth -= 1

    return None


def create_bq_schema(data_fp):
    """Generates two dicts (one using data type inference, one using _mapping API
    endpoint.) Compares values and builds a SchemaField object, used to
    initialize the bq table.

    :param data_fp: path to API data output file (jsonl format)
    """
    def create_mapping_dict():
        """Creates a dict containing field mappings for given endpoint.
        Note: only differentiates the GDC API's 'long' type (called 'integer' in GDC data
        dictionary) and 'float' type (called 'number' in GDC data dictionary). All others
        typed as string.

        :return: dict of field maps. Each entry contains field name, type, and description
        """
        field_map_dict = {}

        # retrieve mappings json object
        res = requests.get(API_PARAMS['ENDPOINT'] + '/_mapping')
        field_mappings = res.json()['_mapping']

        for field in field_mappings:
            # convert data types from GDC format to formats used in BQ
            if field_mappings[field]['type'] == 'long':
                field_type = 'INTEGER'
            elif field_mappings[field]['type'] == 'float':
                field_type = 'FLOAT'
            else:
                field_type = 'STRING'

            # create json object of field mapping data
            field_map_dict[field] = {
                'name': field.split('.')[-1],
                'type': field_type,
                'description': field_mappings[field]['description']
            }

        return field_map_dict

    def collect_values(fields, field, parent, field_grp_prefix):
        """Recursively inserts sets of values for a given field into return dict (
        used to infer field data type).

        :param fields: A dict of key:value pairs -- {field_name: set(field_values)}
        :param field: field name
        :param parent: dict containing field and it's values
        :param field_grp_prefix: string representation of current location in field hierarchy
        :return: field_dict containing field names and a set of its values
        """
        # If the value of parent_dict[key] is a list at this level, and a dict at the next
        # (or a dict at this level, as seen in second conditional statement),
        # iterate over each list element's dictionary entries. (Sometimes lists are composed
        # of strings rather than dicts, and those are later converted to strings.)
        field_name = field_grp_prefix + field
        new_prefix = field_name + '.'

        if isinstance(parent[field], list) \
                and len(parent[field]) > 0 and isinstance(parent[field][0], dict):
            for dict_item in parent[field]:
                for dict_key in dict_item:
                    fields = collect_values(fields, dict_key, dict_item, new_prefix)
        elif isinstance(parent[field], dict):
            for dict_key in parent[field]:
                fields = collect_values(fields, dict_key, parent[field], new_prefix)
        else:
            if field_name not in fields:
                fields[field_name] = set()

            # This type of list can be converted to a comma-separated value string
            if isinstance(parent[field], list):
                value = ", ".join(parent[field])
            else:
                value = parent[field]

            fields[field_name].add(value)

        return fields

    # generate dict containing field mapping results
    field_mapping_dict = create_mapping_dict()

    with open(data_fp, 'r') as data_file:
        field_dict = dict()

        for line in data_file:
            json_case = json.loads(line)
            for key in json_case:
                field_dict = collect_values(field_dict, key, json_case, 'cases.')

    field_data_type_dict = infer_data_types(field_dict)

    # create a flattened dict of schema fields
    schema_dict = create_field_records_dict(field_mapping_dict, field_data_type_dict)

    endpoint_name = API_PARAMS['ENDPOINT'].split('/')[-1]

    return generate_bq_schema(schema_dict,
                              record_type=endpoint_name,
                              expand_fields_list=",".join(API_PARAMS['FIELD_GROUPS']))  # note: removed list() wrapper


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
