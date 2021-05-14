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

from common_etl.utils import (format_seconds, get_graphql_api_response, has_fatal_error, load_config)
from BQ_Table_Building.PDC.pdc_utils import (build_obj_from_pdc_api, build_table_from_jsonl)


API_PARAMS = dict()
BQ_PARAMS = dict()
YAML_HEADERS = ('api_params', 'bq_params', 'steps')


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
    return """{{ 
        study (pdc_study_id: \"{}\" acceptDUA: true) {{ 
            study_name
            disease_type
            primary_site
            embargo_date
        }} 
    }}""".format(pdc_study_id)


def alter_all_programs_json(all_programs_json_obj):
    """
    This function is passed as a parameter to build_jsonl_from_pdc_api(). It allows for the json object to be mutated
    prior to writing it to a file.
    :param all_programs_json_obj: list of json objects to mutate
    """
    temp_programs_json_obj_list = list()

    for program in all_programs_json_obj:
        program['program_name'] = program.pop("name", None)
        print("Processing {}".format(program['program_name']))
        projects = program.pop("projects", None)
        for project in projects:
            project['project_name'] = project.pop("name", None)

            if project['project_submitter_id'] == 'CPTAC2 Retrospective':
                project['project_submitter_id'] = 'CPTAC-2'

            studies = project.pop("studies", None)
            for study in studies:
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
    print("PDC study metadata script started at {}".format(time.strftime("%x %X", time.localtime())))

    steps = None

    try:
        global API_PARAMS, BQ_PARAMS
        API_PARAMS, BQ_PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        has_fatal_error(err, ValueError)

    if 'build_studies_jsonl' in steps:
        build_obj_from_pdc_api(API_PARAMS, endpoint='allPrograms', request_function=make_all_programs_query,
                               alter_json_function=alter_all_programs_json)

    if 'build_studies_table' in steps:
        build_table_from_jsonl(API_PARAMS, BQ_PARAMS,
                               endpoint='allPrograms',
                               infer_schema=True)

    end = time.time() - start_time
    print("Finished program execution in {}!\n".format(format_seconds(end)))


if __name__ == '__main__':
    main(sys.argv)
