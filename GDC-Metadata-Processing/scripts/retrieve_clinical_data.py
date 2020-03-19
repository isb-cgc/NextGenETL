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

import argparse
import requests
import json
import time
import os
from common_etl.utils import infer_data_types
from google.cloud import bigquery

# todo: meaningful error handling (enabling resume) if the process breaks before data's all appended to the file.


ENDPOINT = 'https://api.gdc.cancer.gov/cases'
EXPAND_FIELD_GROUPS = 'demographic,diagnoses,diagnoses.treatments,diagnoses.annotations,exposures,family_histories'

# removed case_autocomplete, doesn't seem to have any associations in reality

DEFAULT_BATCH_SIZE = 1000
OUTPUT_PATH = '../textFiles/'


def retrieve_and_output_cases(args):
    start_time = time.time() # for benchmarking
    total_cases_count = 0
    is_last_page = False

    fp = OUTPUT_PATH + args.file
    # Set write mode for json output file (append mode used to resume interrupted dataset retrievals)
    io_mode = "a" if args.append else "w"

    request_params = {
        'from': args.start_index,
        'size': args.batch_size,
        'expand': args.expand
    }

    with open(fp, io_mode) as json_output_file:
        inserted_count = 0

        print("\n Appending case objects to {}.".format(OUTPUT_PATH + args.file))

        '''
        Create a single parent array for case objects (if not stripped, it would append an array for each batch). 
        Since it's not constructed in-memory, there's no size limit.
        '''
        json_output_file.write('{"cases": [')

        while not is_last_page:
            # retrieve and parse a "page" (batch) of case objects
            res = requests.post(url=ENDPOINT, data=request_params)
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
            try:
                for i in range(len(cases_json)):
                    json.dump(obj=cases_json[i], fp=json_output_file)

                    if i < len(cases_json) - 1:
                        json_output_file.write(', ')

            except IOError as err:
                print(err)
                exit(-1)

            print("Inserted page {} of {}!".format(curr_page, last_page))

            inserted_count += response_metadata['count']

            if curr_page == last_page or (args.max_pages and curr_page >= args.max_pages):
                is_last_page = True

                # If this is the last page, append metadata and finish constructing the json object
                json_metadata = {
                    "inserted_count": inserted_count,
                    "total_available_count": response_metadata['total']
                }

                json_output_file.write('], "metadata": ')
                json.dump(obj=json_metadata, fp=json_output_file)
                json_output_file.write('}')
            else:
                json_output_file.write(',')

                # increment starting index for next batch of cases
                request_params['from'] += request_params['size']

    # calculate processing time
    total_time = time.time()-start_time

    # report final json file's data size in MB
    file_size = os.stat(fp).st_size / 1048576.0

    output_report(inserted_count, total_cases_count, file_size, total_time)


def generate_bq_nested_schema_field(name, field_list):
    return {
        "name": name,
        "type": "RECORD",
        "mode": "REPEATED",
        "fields": field_list
    }


def generate_clinical_bq_schema(schema_dict):
    demographic_list, diagnoses_list, diagnoses__treatments_list, diagnoses__annotations_list, exposures_list, \
        family_histories_list, cases_list = [], [], [], [], [], [], []

    for field in schema_dict:
        split_field_name = field.split('__')

        if split_field_name[1] == 'diagnoses':
            if split_field_name[2] == 'annotations':
                diagnoses__annotations_list.append(schema_dict[field])
            elif split_field_name[2] == 'treatments':
                diagnoses__treatments_list.append(schema_dict[field])
            else:
                diagnoses_list.append(schema_dict[field])
        elif split_field_name[1] == 'demographic':
            demographic_list.append(schema_dict[field])
        elif split_field_name[1] == 'exposures':
            exposures_list.append(schema_dict[field])
        elif split_field_name[1] == 'family_histories':
            family_histories_list.append(schema_dict[field])
        else:
            cases_list.append(schema_dict[field])

    diagnoses_list.append(generate_bq_nested_schema_field('annotations', diagnoses__annotations_list))
    diagnoses_list.append(generate_bq_nested_schema_field('treatments', diagnoses__treatments_list))
    cases_list.append(generate_bq_nested_schema_field('diagnoses', diagnoses_list))
    cases_list.append(generate_bq_nested_schema_field('demographic', demographic_list))
    cases_list.append(generate_bq_nested_schema_field('exposures', exposures_list))
    cases_list.append(generate_bq_nested_schema_field('family_histories', family_histories_list))

    with open('../../SchemaFiles/clinical_schema.json', 'w') as schema_file:
        json.dump(cases_list, schema_file)


def compile_field_values(field_dict, key, parent_dict, prefix):
    """
    Steps:

    - Open JSON File externally
    - First function pass hands in an empty dict, json_obj['cases'], 'cases__'

    - If the value at key is a dict, add key string to prefix. Iterate over value's keys
    - If the value at key is a list, add key string to prefix. Nested for loop to iterate over value's keys
    - If the value at key is a primitive:
        - make field name by concatenating prefix and key string. Check field_dict for membership. If not found, create
        dict entry with key == field name string and value == to set().
        - Add value to field_dict set.
    """
    if isinstance(parent_dict[key], list) and isinstance(parent_dict[key][0], dict):
        for dict_item in parent_dict[key]:
            for dict_key in dict_item:
                field_dict = compile_field_values(field_dict, dict_key, dict_item, prefix + key + "__")
    elif isinstance(parent_dict[key], dict):
        for dict_key in parent_dict[key]:
            field_dict = compile_field_values(field_dict, dict_key, parent_dict[key], prefix + key + "__")
    else:
        field_name = prefix + key

        if field_name not in field_dict:
            field_dict[field_name] = set()

        if isinstance(parent_dict[key], list):
            value = ", ".join(parent_dict[key])
        else:
            value = parent_dict[key]

        field_dict[field_name].add(value)

    return field_dict


def generate_mapping_endpoint_dict(from_file=False, file_path=None):
    field_mapping_dict = {}

    if from_file:
        with open(file_path, 'r') as json_file:
            json_obj = json.load(json_file)
            field_mappings = json_obj['_mapping']
    else:
        res = requests.get(ENDPOINT + '/_mapping')
        field_mappings = res.json()['_mapping']

    for field in field_mappings:
        field_name = "__".join(field.split('.'))

        if field_mappings[field]['type'] == 'long':
            field_type = 'integer'
        elif field_mappings[field]['type'] == 'float':
            field_type = 'float'
        else:
            field_type = 'string'

        field_mapping_dict[field_name] = {
            'name': field.split('__')[-1],
            'type': field_type,
            'description': field_mappings[field]['description']
        }

    return field_mapping_dict


def generate_schema_fields_dict(field_mapping_dict, field_data_type_dict):
    schema_dict = {}

    for key in field_data_type_dict:
        try:
            column_name = field_mapping_dict[key]['name'].split('.')[-1]
            description = field_mapping_dict[key]['description']
        except KeyError:
            # for some reason, cases.id isn't in the mapping endpoint, this handles the lack of description
            column_name = key.split("__")[-1]
            description = ""

        if field_data_type_dict[key]:
            field_type = field_data_type_dict[key]
        elif key in field_mapping_dict:
            field_type = field_mapping_dict[key]['type']
        else:
            # this would happen if there were a field added to the cases endpoint that didn't hold any values
            # and wasn't actually added to the _mappings.
            print("Not adding field {} because no type found".format(key))
            continue

        if field_type == 'integer':
            bq_type = "INT64"
        elif field_type == 'float':
            bq_type = "FLOAT64"
        elif field_type == 'string':
            bq_type = 'STRING'
        else:
            print("[ERROR] No type defined in schema for field {}".format(key))
            bq_type = None

        schema_dict[key] = {
            "name": column_name,
            "type": bq_type,
            "description": description
        }

    return schema_dict


def generate_schema(args):
    field_mapping_dict = generate_mapping_endpoint_dict()

    with open(OUTPUT_PATH + args.file) as data_file:
        json_obj = json.load(data_file)

    field_value_sets = compile_field_values(dict(), 'cases', json_obj, '')
    field_data_type_dict = infer_data_types(field_value_sets)

    schema_dict = generate_schema_fields_dict(field_mapping_dict, field_data_type_dict)
    generate_clinical_bq_schema(schema_dict)


def output_report(inserted_count, total_cases_count, file_size, total_time):
    print(
        "\nClinical data retrieval complete. \n\n"
        "RESULTS REPORT \n"
        "------------- \n"
        "{} cases inserted \n"
        "{} cases available \n"
        "{:.3f} mb -- size of output json filefile size \n"
        "{:.3f} sec -- time to process data\n".format(inserted_count, total_cases_count, file_size, total_time))


def main(args):
    pass
    # todo: args should be set in the yaml config
    # todo: field_groups should too
    # todo: these functions should be called based on 'steps' in yaml

    # get all case records from API
    # retrieve_and_output_cases(args)
    generate_schema(args)
    # generate_clinical_bq_schema(args)
    # check_for_field_values(args)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Query the GDC cases endpoint to retrieve clinical data")

    parser.add_argument("-i", "--start_index", type=int, default=0,
                        help="Starting index (helpful for resuming interrupted retrieval).")
    parser.add_argument("-s", "--batch_size", type=int, default=DEFAULT_BATCH_SIZE,
                        help="Number of records to retrieve per batch request.")
    parser.add_argument("-e", "--expand", default=EXPAND_FIELD_GROUPS,
                        help="List of 'expand' field groups to retrieve (comma-delineated, no spaces).")
    parser.add_argument("-a", "--append", action='store_true',
                        help="Append new results to existing json file. (Overwrites data by default.)")
    parser.add_argument("-f", "--file", type=str, default='clinical_data.json',
                        help="name of json file to which to write/append retrieved data.")
    parser.add_argument("-p", "--max_pages", type=int, default=0,
                        help="Max number of pages to retrieve before exiting (helpful for testing).")

    kwargs = parser.parse_args()

    main(kwargs)
