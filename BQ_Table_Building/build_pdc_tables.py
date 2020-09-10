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
import time
import requests
import json
from common_etl.utils import *

API_PARAMS = dict()
BQ_PARAMS = dict()
YAML_HEADERS = ('api_params', 'bq_params', 'steps')


def run_query(endpoint, query):
    request = requests.post(endpoint + query)
    if request.status_code == 200:
        return request.json()
    else:
        raise Exception("Query failed to run by returning code of {}. {}"
                        .format(request.status_code, query))


def get_all_progs_query():
    return """{allPrograms{
            program_id
            program_submitter_id
            name
            start_date
            end_date
            program_manager
            projects {
                project_id
                project_submitter_id
                name
                studies {
                    pdc_study_id
                    study_id
                    study_submitter_id
                    submitter_id_name
                    analytical_fraction
                    experiment_type
                    acquisition_type
                } 
            }
        }}"""


def get_study_payload(study_id, pdc_study_id, study_submitter_id):
    query_str = ('\"query study ($study_id: String, '
                 '$pdc_study_id: String, '
                 '$study_submitter_id: String) { '
                 'study (study_id: $study_id, '
                 'pdc_study_id: $pdc_study_id, '
                 'study_submitter_id: $study_submitter_id) { '
                 'pdc_study_id '
                 'study_id '
                 'study_submitter_id '
                 'study_name '
                 'study_shortname '
                 'disease_type '
                 'primary_site '
                 'cases_count '
                 'aliquots_count '
                 '} '
                 '}\"'
                 )

    study_vars = ("{{   \"study_id\": \"{}\", "
                  "   \"pdc_study_id\": \"{}\", "
                  "   \"study_submitter_id\": \"{}\"}}"
                  ).format(study_id, pdc_study_id, study_submitter_id)

    payload = '{{ \"query\": {}, \"variables\": {} }}'.format(query_str, study_vars)

    return payload


def get_quant_log2_data(submitter_id):
    return ('{ quantDataMatrix(study_submitter_id: \"'
            + submitter_id + '\" data_type: \"log2_ratio\") }')


def get_graphql_api_response(api_params, query=None, payload=None):
    headers = {'Content-Type': 'application/json'}

    endpoint = api_params['ENDPOINT']

    if query and not payload:
        req_body = {'query': query}
        response = requests.post(endpoint, headers=headers, json=req_body)
    elif payload and not query:
        response = requests.post(endpoint, headers=headers,  data=payload)
    else:
        has_fatal_error("Must specify either query OR payload (not both) "
                        "in get_graphql_api_response.", SyntaxError)

    if not response.ok:
        has_fatal_error("Invalid response from endpoint {}\n Status code: {}"
                        .format(endpoint, response.raise_for_status()))

    return response.json()


def create_studies_dict(json_res):
    studies = []

    for program in json_res['data']['allPrograms']:
        program_id = program['program_id']
        program_submitter_id = program['program_submitter_id']
        program_name = program['name']
        program_start_date = program['start_date']
        program_end_date = program['end_date']
        program_manager = program['program_manager']

        for project in program['projects']:
            project_id = project['project_id']
            project_submitter_id = project['project_submitter_id']
            project_name = project['name']

            for study in project['studies']:
                study_dict = study.copy()

                study_payload = get_study_payload(study_dict['study_id'],
                                                  study_dict['pdc_study_id'],
                                                  study_dict['study_submitter_id'])

                study_metadata = get_graphql_api_response(API_PARAMS,
                                                          payload=study_payload)

                for entry in study_metadata['data']['study']:
                    for field, val in entry.items():
                        study_dict[field] = val

                print("Processing metadata for {}".format(study_dict['study_name']))

                primary_site_list = study_dict.pop('primary_site').split(';')
                disease_type_list = study_dict.pop('disease_type').split(';')

                study_dict['primary_site'] = ', '.join(primary_site_list)
                study_dict['disease_type'] = ', '.join(disease_type_list)

                study_dict['program_id'] = program_id
                study_dict['program_submitter_id'] = program_submitter_id
                study_dict['program_name'] = program_name
                study_dict['program_start_date'] = program_start_date
                study_dict['program_end_date'] = program_end_date
                study_dict['program_manager'] = program_manager

                study_dict['project_id'] = project_id
                study_dict['project_submitter_id'] = project_submitter_id
                study_dict['project_name'] = project_name

                studies.append(study_dict)

    return studies


def main(args):
    start = time.time()

    # Load YAML configuration
    if len(args) != 2:
        has_fatal_error("Usage: {} <configuration_yaml>".format(args[0]), ValueError)

    with open(args[1], mode='r') as yaml_file:
        steps = []

        try:
            global API_PARAMS, BQ_PARAMS
            API_PARAMS, BQ_PARAMS, steps = load_config(yaml_file, YAML_HEADERS)
        except ValueError as err:
            has_fatal_error(str(err), ValueError)

    if 'build_studies_table' in steps:
        print("Building studies table...")
        studies_start = time.time()

        json_res = get_graphql_api_response(API_PARAMS, get_all_progs_query())
        studies = create_studies_dict(json_res)
        studies_fp = get_scratch_path(BQ_PARAMS, BQ_PARAMS['STUDIES_JSONL'])

        write_obj_list_to_jsonl(BQ_PARAMS, studies_fp, studies)
        upload_to_bucket(BQ_PARAMS, BQ_PARAMS['STUDIES_JSONL'])

        table_name = "_".join(['studies', str(BQ_PARAMS['RELEASE'])])
        table_id = get_working_table_id(BQ_PARAMS, table_name)

        schema_filename = "{}.{}.{}_{}.json".format(
            BQ_PARAMS['DEV_PROJECT'],
            BQ_PARAMS['DEV_DATASET'],
            'studies',
            str(BQ_PARAMS['RELEASE']))

        schema, table_metadata = from_schema_file_to_obj(BQ_PARAMS, schema_filename)

        create_and_load_table(BQ_PARAMS, BQ_PARAMS['STUDIES_JSONL'], schema, table_id)
        update_bq_table(table_id, table_metadata)

        studies_end = time.time() - studies_start
        print("Completed in {:0.0f}s!\n".format(studies_end))

    end = time.time() - start
    print("Finished program execution in {:0.0f}s!\n".format(end))


if __name__ == '__main__':
    main(sys.argv)
