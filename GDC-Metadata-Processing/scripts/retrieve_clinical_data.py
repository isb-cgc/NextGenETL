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
import pprint
import json
import time
import os
from common_etl.utils import flatten_json, infer_data_types
from google.cloud import bigquery

# todo: meaningful error handling (enabling resume) if the process breaks before data's all appended to the file.


ENDPOINT = 'https://api.gdc.cancer.gov/cases'
EXPAND_FIELD_GROUPS = 'demographic,diagnoses,diagnoses.treatments,diagnoses.annotations,exposures,family_histories'
PARENT_FIELD_GROUPS = 'demographic,diagnoses,exposures,family_histories,case_id,created_datetime,\
days_to_index,days_to_lost_to_followup,diagnosis_ids,disease_type,index_date,lost_to_followup,primary_site,state,\
submitter_diagnosis_ids,submitter_id,updated_datetime'

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


def filter_mappings_by_field_group(args):
    field_map = {}
    res = requests.get(ENDPOINT + '/_mapping')

    field_group_list = args.field_groups.split(',')

    field_mappings = res.json()['_mapping']

    for field in field_mappings:
        top_level_grouping = field.split('.')[1]

        if top_level_grouping in field_group_list:
            field_map[field] = field_mappings[field]

    return field_map


def generate_bq_schema_field(name, field_type, metadata):
    if field_type == 'integer':
        bq_type = "INT64"
    elif field_type == 'float':
        bq_type = "FLOAT64"
    elif field_type == 'string':
        bq_type = 'STRING'
    else:
        print("[ERROR] No type defined in schema for field {}".format(name))
        bq_type = None

    return {
        "name": name,
        "type": bq_type,
        "description": metadata['description']
    }


def generate_bq_nested_schema_field(name, field_list):
    return {
        "name": name,
        "type": "RECORD",
        "mode": "REPEATED",
        "fields": field_list
    }


def generate_clinical_bq_schema(args):
    demographic_list, diagnoses_list, diagnoses__treatments_list, diagnoses__annotations_list, exposures_list, \
        family_histories_list, cases_list = [], [], [], [], [], [], []

    flattened_json = check_for_field_values(args)
    field_type_dict = infer_data_types(flattened_json)

    filtered_field_mappings = filter_mappings_by_field_group(args)

    for field in filtered_field_mappings.keys():
        field_metadata = filtered_field_mappings[field]

        split_field_name = field_metadata['field'].split('.')
        flat_field_name = "__".join(split_field_name)

        try:
            field_schema = generate_bq_schema_field(split_field_name[-1],
                                                    field_type_dict[flat_field_name],
                                                    field_metadata)
        except KeyError:
            print("[ERROR] flat_field_name: {}, split_field_name: {}, field: {}"
                  .format(flat_field_name, split_field_name[-1], field))

        if split_field_name[0] == 'diagnoses':
            if split_field_name[1] == 'annotations':
                diagnoses__annotations_list.append(field_schema)
            elif split_field_name[1] == 'treatments':
                diagnoses__treatments_list.append(field_schema)
            else:
                diagnoses_list.append(field_schema)
        elif split_field_name[0] == 'demographic':
            demographic_list.append(field_schema)
        elif split_field_name[0] == 'exposures':
            exposures_list.append(field_schema)
        elif split_field_name[0] == 'family_histories':
            family_histories_list.append(field_schema)
        else:
            cases_list.append(field_schema)

    diagnoses_list.append(generate_bq_nested_schema_field('annotations', diagnoses__annotations_list))
    diagnoses_list.append(generate_bq_nested_schema_field('treatments', diagnoses__treatments_list))
    cases_list.append(generate_bq_nested_schema_field('diagnoses', diagnoses_list))
    cases_list.append(generate_bq_nested_schema_field('demographic', demographic_list))
    cases_list.append(generate_bq_nested_schema_field('exposures', exposures_list))
    cases_list.append(generate_bq_nested_schema_field('family_histories', family_histories_list))

    with open('../../SchemaFiles/clinical_schema.json', 'w') as schema_file:
        json.dump(cases_list, schema_file)


def collect_field_vals(cases_field_dict, key, value):
    if not isinstance(value, list) and not isinstance(value, dict):
        if key not in cases_field_dict:
            cases_field_dict[key] = set()
        cases_field_dict[key].add(value)

    return cases_field_dict


def check_for_field_values(args):
    json_file_path = OUTPUT_PATH + args.file

    cases_field_dict = {}
    with open(json_file_path, 'r') as json_file:
        json_obj = json.load(json_file)

        for case in json_obj['cases']:
            for key in case.keys():
                cases_field_dict = collect_field_vals(cases_field_dict, key, case[key])

            if 'demographic' in case:
                for key in case['demographic']:
                    cases_field_dict = collect_field_vals(
                        cases_field_dict, 'demographic__' + key, case['demographic'][key])

            if 'diagnoses' in case:
                for diagnosis in case['diagnoses']:
                    for key in diagnosis:
                        cases_field_dict = collect_field_vals(cases_field_dict, 'diagnoses__' + key, diagnosis[key])

                        if 'annotations' in diagnosis:
                            for annotation in diagnosis['annotations']:
                                for annotation_key in annotation:
                                    cases_field_dict = collect_field_vals(
                                        cases_field_dict,
                                        'diagnoses__annotations__' + annotation_key,
                                        annotation[annotation_key])

                        if 'treatments' in diagnosis:
                            for treatment in diagnosis['treatments']:
                                for treatment_key in treatment:
                                    cases_field_dict = collect_field_vals(
                                        cases_field_dict, 'diagnoses__treatments__' + treatment_key,
                                        treatment[treatment_key])

            if 'family_histories' in case:
                for family_history in case['family_histories']:
                    for key in family_history:
                        cases_field_dict = collect_field_vals(
                            cases_field_dict, 'family_histories__' + key, family_history[key])

            if 'exposures' in case:
                for exposure in case['exposures']:
                    for key in exposure:
                        cases_field_dict = collect_field_vals(
                            cases_field_dict, 'exposures__' + key, exposure[key])

    return cases_field_dict


def pprint_json(data):
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(data)


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
    # todo: args should be set in the yaml config
    # todo: field_groups should too
    # todo: these functions should be called based on 'steps' in yaml

    # get all case records from API
    # retrieve_and_output_cases(args)

    generate_clinical_bq_schema(args)
    # check_for_field_values(args)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Query the GDC cases endpoint to retrieve clinical data")

    parser.add_argument("-i", "--start_index", type=int, default=0,
                        help="Starting index (helpful for resuming interrupted retrieval).")
    parser.add_argument("-s", "--batch_size", type=int, default=DEFAULT_BATCH_SIZE,
                        help="Number of records to retrieve per batch request.")
    parser.add_argument("-e", "--expand", default=EXPAND_FIELD_GROUPS,
                        help="List of 'expand' field groups to retrieve (comma-delineated, no spaces).")
    parser.add_argument("-g", "--field_groups", default=PARENT_FIELD_GROUPS,
                        help="List of top-level field groups. Retrieves all child fields, even those nested.")
    parser.add_argument("-a", "--append", action='store_true',
                        help="Append new results to existing json file. (Overwrites data by default.)")
    parser.add_argument("-f", "--file", type=str, default='clinical_data.json',
                        help="name of json file to which to write/append retrieved data.")
    parser.add_argument("-p", "--max_pages", type=int, default=0,
                        help="Max number of pages to retrieve before exiting (helpful for testing).")

    kwargs = parser.parse_args()

    main(kwargs)
