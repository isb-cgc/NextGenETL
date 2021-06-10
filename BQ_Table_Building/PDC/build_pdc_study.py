"""
Copyright 2020-2021, Institute for Systems Biology

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
import time
import json

from common_etl.utils import (get_filepath, format_seconds, get_graphql_api_response, has_fatal_error, load_config,
                              get_query_results, get_filename, create_and_load_table_from_jsonl)
from BQ_Table_Building.PDC.pdc_utils import (build_obj_from_pdc_api, build_table_from_jsonl, write_jsonl_and_upload,
                                             get_prefix, update_table_schema_from_generic_pdc)

API_PARAMS = dict()
BQ_PARAMS = dict()
YAML_HEADERS = ('api_params', 'bq_params', 'steps')


def get_project_metadata():
    project_metadata_path = f"{BQ_PARAMS['BQ_REPO']}/{BQ_PARAMS['PROJECT_STUDY_METADATA_DIR']}"
    project_metadata_fp = get_filepath(f"{project_metadata_path}/{BQ_PARAMS['PROJECT_METADATA_FILE']}")

    with open(project_metadata_fp, 'r') as fh:
        return json.load(fh)


def get_study_friendly_names():
    project_metadata_path = f"{BQ_PARAMS['BQ_REPO']}/{BQ_PARAMS['PROJECT_STUDY_METADATA_DIR']}"
    study_metadata_fp = get_filepath(f"{project_metadata_path}/{BQ_PARAMS['STUDY_FRIENDLY_NAME_FILE']}")

    with open(study_metadata_fp, 'r') as fh:
        return json.load(fh)


def make_all_programs_query():
    """
    Creates a graphQL string for querying the PDC API's allPrograms endpoint.
    :return: GraphQL query string
    """
    return """{
        allPrograms (acceptDUA: true) {
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
        }
    }"""


def make_study_query(pdc_study_id):
    """
    Creates a graphQL string for querying the PDC API's study endpoint.
    :return: GraphQL query string
    """
    return f"""{{ 
        study (pdc_study_id: \"{pdc_study_id}\" acceptDUA: true) {{ 
            study_name
            disease_type
            primary_site
            embargo_date
        }} 
    }}"""


def alter_all_programs_json(all_programs_json_obj):
    """
    This function is passed as a parameter to build_jsonl_from_pdc_api(). It allows for the json object to be mutated
    prior to writing it to a file.
    :param all_programs_json_obj: list of json objects to mutate
    """
    project_metadata = get_project_metadata()
    study_friendly_names = get_study_friendly_names()

    temp_programs_json_obj_list = list()

    for program in all_programs_json_obj:
        program['program_name'] = program.pop("name", None)
        print(f"Processing {program['program_name']}")
        projects = program.pop("projects", None)
        for project in projects:
            project['project_name'] = project.pop("name", None)

            # Per past discussion, the retrospective is excluded here
            if project['project_submitter_id'] == 'CPTAC2 Retrospective':
                project['project_submitter_id'] = 'CPTAC-2'

            if project['project_submitter_id'] not in project_metadata:
                project_metadata_path = f"{BQ_PARAMS['BQ_REPO']}/{BQ_PARAMS['PROJECT_STUDY_METADATA_DIR']}"
                project_metadata_fp = get_filepath(f"{project_metadata_path}/{BQ_PARAMS['PROJECT_METADATA_FILE']}")
                project['project_short_name'] = None
                project['program_short_name'] = None
                project['project_friendly_name'] = None
                project['program_labels'] = None
                print(f"""\n**Unmapped project_submitter_id: {project['project_submitter_id']}. 
                      Add project metadata to {project_metadata_fp} and rerun study workflow.\n""")
            else:
                project_shortname_mapping = project_metadata[project['project_submitter_id']]
                project['project_short_name'] = project_shortname_mapping['PROJECT_SHORT_NAME']
                project['program_short_name'] = project_shortname_mapping['PROGRAM_SHORT_NAME']
                project['project_friendly_name'] = project_shortname_mapping['PROJECT_FRIENDLY_NAME']
                project['program_labels'] = project_shortname_mapping['PROGRAM_LABELS']

            studies = project.pop("studies", None)
            for study in studies:
                # add study friendly name from yaml mapping
                study['study_friendly_name'] = study_friendly_names[study['pdc_study_id']]

                # grab a few add't fields from study endpoint
                json_res = get_graphql_api_response(API_PARAMS, make_study_query(study['pdc_study_id']))
                study_metadata = json_res['data']['study'][0]

                # ** unpacks each dictionary's items without altering program and project
                study_obj = {**program, **project, **study, **study_metadata}

                # normalize empty strings (turn into null)
                for k, v in study_obj.items():
                    if not v:
                        study_obj[k] = None

                temp_programs_json_obj_list.append(study_obj)

    all_programs_json_obj.clear()
    all_programs_json_obj.extend(temp_programs_json_obj_list)


def main(args):
    start_time = time.time()
    print(f"PDC study metadata script started at {time.strftime('%x %X', time.localtime())}")

    steps = None

    try:
        global API_PARAMS, BQ_PARAMS
        API_PARAMS, BQ_PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        has_fatal_error(err, ValueError)

    if 'build_studies_jsonl' in steps:
        joined_record_list = build_obj_from_pdc_api(API_PARAMS,
                                                    endpoint='allPrograms',
                                                    request_function=make_all_programs_query,
                                                    alter_json_function=alter_all_programs_json)

        write_jsonl_and_upload(API_PARAMS, BQ_PARAMS,
                               prefix=get_prefix(API_PARAMS, 'allPrograms'),
                               joined_record_list=joined_record_list)
    if 'build_studies_table' in steps:
        build_table_from_jsonl(API_PARAMS, BQ_PARAMS,
                               endpoint='allPrograms',
                               infer_schema=True)

    if 'publish_studies_table' in steps:
        source_table_name = f"{get_prefix(API_PARAMS, 'allPrograms')}_{API_PARAMS['RELEASE']}"
        source_table_id = f"{BQ_PARAMS['DEV_PROJECT']}.{BQ_PARAMS['META_DATASET']}.{source_table_name}"

        update_table_schema_from_generic_pdc(API_PARAMS, BQ_PARAMS, table_id=source_table_id)


    end = time.time() - start_time
    print(f"Finished program execution in {format_seconds(end)}!\n")


if __name__ == '__main__':
    main(sys.argv)
