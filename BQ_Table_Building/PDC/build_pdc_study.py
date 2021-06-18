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
                              load_table_from_query, publish_table)
from BQ_Table_Building.PDC.pdc_utils import (build_obj_from_pdc_api, build_table_from_jsonl, write_jsonl_and_upload,
                                             get_prefix, update_table_schema_from_generic_pdc, get_publish_table_ids,
                                             find_most_recent_published_table_id)

API_PARAMS = dict()
BQ_PARAMS = dict()
YAML_HEADERS = ('api_params', 'bq_params', 'steps')


def make_all_programs_query():
    """
    Create a graphQL string for querying the PDC API's allPrograms endpoint.
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
    Create a graphQL string for querying the PDC API's study endpoint.
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


def get_project_metadata():
    """
    Load project metadata from BQEcosystem/MetadataMappings/pdc_project_metadata.json as dict.
    :return dict of project dicts of the following form. Key equals PDC field "project_submitter_id."
    Example project dict:
        { "CPTAC-TCGA": {
            "project_short_name": "CPTAC_TCGA",
            "project_friendly_name": "CPTAC-TCGA",
            "program_short_name": "TCGA",
            "program_labels": "cptac2; tcga"
        }
    """
    metadata_mappings_path = f"{BQ_PARAMS['BQ_REPO']}/{BQ_PARAMS['PROJECT_STUDY_METADATA_DIR']}"
    project_metadata_fp = get_filepath(f"{metadata_mappings_path}/{BQ_PARAMS['PROJECT_METADATA_FILE']}")

    with open(project_metadata_fp, 'r') as fh:
        return json.load(fh)


def get_study_friendly_names():
    """
    Load study friendly names json file (from BQEcosystem/MetadataMappings/pdc_study_friendly_name_map.json) as dict.
    :return: dict of { "pdc_study_id": "STUDY FRIENDLY NAME" } strings
    """
    metadata_mappings_path = f"{BQ_PARAMS['BQ_REPO']}/{BQ_PARAMS['PROJECT_STUDY_METADATA_DIR']}"
    study_metadata_fp = get_filepath(f"{metadata_mappings_path}/{BQ_PARAMS['STUDY_FRIENDLY_NAME_FILE']}")

    with open(study_metadata_fp, 'r') as fh:
        return json.load(fh)


def check_project_mapping_data(project_shortname_map, project_submitter_id):
    """
    Check that all necessary user-supplied data is included in BQEcosystem/MetadataMappings/pdc_project_metadata.json.
    :param project_shortname_map: dict representation of project objects found in pdc_project_metadata.json
    :param project_submitter_id: project submitter id key, used to look up project metadata in project_shortname_map
    """
    project_metadata_path = f"{BQ_PARAMS['BQ_REPO']}/{BQ_PARAMS['PROJECT_STUDY_METADATA_DIR']}"
    project_metadata_fp = get_filepath(f"{project_metadata_path}/{BQ_PARAMS['PROJECT_METADATA_FILE']}")

    project_mapping_keys_list = ['project_short_name', 'program_short_name', 'project_friendly_name', 'program_labels']

    for mapping_key in project_mapping_keys_list:
        if mapping_key not in project_shortname_map:
            has_fatal_error(f"""
                *** {mapping_key} not in mapping for {project_submitter_id}. Add to {project_metadata_fp} and rerun.
            """)
        elif not project_shortname_map[mapping_key]:
            has_fatal_error(f"""
                *** {mapping_key} is blank for {project_submitter_id}. Add to {project_metadata_fp} and rerun.
            """)


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
            print(f" - {project['project_name']}")

            # Per past discussion, the retrospective is excluded here
            if project['project_submitter_id'] == 'CPTAC2 Retrospective':
                project['project_submitter_id'] = 'CPTAC-2'

            project_metadata_path = f"{BQ_PARAMS['BQ_REPO']}/{BQ_PARAMS['PROJECT_STUDY_METADATA_DIR']}"
            project_metadata_fp = get_filepath(f"{project_metadata_path}/{BQ_PARAMS['PROJECT_METADATA_FILE']}")

            if project['project_submitter_id'] not in project_metadata:
                has_fatal_error(f"""
                    *** Unmapped project_submitter_id: {project['project_submitter_id']}. 
                    Add project metadata to {project_metadata_fp} and rerun study workflow.
                """)
            else:
                project_shortname_map = project_metadata[project['project_submitter_id']]

                check_project_mapping_data(project_shortname_map=project_shortname_map,
                                           project_submitter_id=project['project_submitter_id'])

                project['project_short_name'] = project_shortname_map['project_short_name']
                project['program_short_name'] = project_shortname_map['program_short_name']
                project['project_friendly_name'] = project_shortname_map['project_friendly_name']
                project['program_labels'] = project_shortname_map['program_labels']

            studies = project.pop("studies", None)
            for study in studies:
                # add study friendly name from yaml mapping
                if study['pdc_study_id'] not in study_friendly_names:
                    metadata_mappings_path = f"{BQ_PARAMS['BQ_REPO']}/{BQ_PARAMS['PROJECT_STUDY_METADATA_DIR']}"
                    study_names_fp = get_filepath(f"{metadata_mappings_path}/{BQ_PARAMS['STUDY_FRIENDLY_NAME_FILE']}")

                    has_fatal_error(f"""
                        *** Unmapped study friendly name for {study['pdc_study_id']}. 
                        Add study friendly name to BQEcosystem path {study_names_fp} and rerun study workflow.
                    """)

                study['study_friendly_name'] = study_friendly_names[study['pdc_study_id']]

                # grab a few additional fields from study endpoint
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
        build_table_from_jsonl(API_PARAMS, BQ_PARAMS, endpoint='allPrograms', infer_schema=True)

        raw_table_name = f"{get_prefix(API_PARAMS, 'allPrograms')}_{API_PARAMS['RELEASE']}"
        raw_table_id = f"{BQ_PARAMS['DEV_PROJECT']}.{BQ_PARAMS['META_DATASET']}.{raw_table_name}"

        final_table_name = f"{BQ_PARAMS['STUDIES_TABLE']}_{API_PARAMS['RELEASE']}"
        final_table_id = f"{BQ_PARAMS['DEV_PROJECT']}.{BQ_PARAMS['META_DATASET']}.{final_table_name}"

        ordered_query = f"""
        SELECT embargo_date, study_name, study_submitter_id, submitter_id_name, pdc_study_id, study_id, 
            study_friendly_name, analytical_fraction, disease_type, primary_site, acquisition_type, experiment_type,
            project_id, project_submitter_id, project_name, project_short_name, project_friendly_name,
            program_id, program_submitter_id, program_name, program_short_name, program_manager, program_labels,
            start_date, end_date
        FROM {raw_table_id}
        ORDER BY pdc_study_id
        """

        load_table_from_query(BQ_PARAMS, table_id=final_table_id, query=ordered_query)

        update_table_schema_from_generic_pdc(API_PARAMS, BQ_PARAMS, table_id=final_table_id)

    if 'publish_studies_table' in steps:
        source_table_name = f"{BQ_PARAMS['STUDIES_TABLE']}_{API_PARAMS['RELEASE']}"
        source_table_id = f"{BQ_PARAMS['DEV_PROJECT']}.{BQ_PARAMS['META_DATASET']}.{source_table_name}"

        publish_table(API_PARAMS, BQ_PARAMS,
                      public_dataset=BQ_PARAMS['PUBLIC_META_DATASET'],
                      source_table_id=source_table_id,
                      get_publish_table_ids=get_publish_table_ids,
                      find_most_recent_published_table_id=find_most_recent_published_table_id,
                      overwrite=True)

    end = time.time() - start_time
    print(f"Finished program execution in {format_seconds(end)}!\n")


if __name__ == '__main__':
    main(sys.argv)
