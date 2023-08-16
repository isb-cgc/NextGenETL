"""
Copyright 2023, Institute for Systems Biology

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

from cda_bq_etl.bq_helpers import load_table_from_query, publish_table, update_table_schema_from_generic, \
    exists_bq_table, query_and_retrieve_result
from cda_bq_etl.utils import load_config, has_fatal_error, create_dev_table_id, format_seconds

PARAMS = dict()
YAML_HEADERS = ('params', 'steps')


def get_pdc_projects_list():
    """
    Return current list of PDC projects (pulled from study metadata table in BQEcosystem repo).
    """
    def make_all_studies_query() -> str:
        studies_table_id = f"{PARAMS['DEV_PROJECT']}.{PARAMS['DEV_METADATA_DATASET']}.studies_{PARAMS['RELEASE']}"

        if not exists_bq_table(studies_table_id):
            has_fatal_error("Studies table for release {} does not exist. "
                            "Run studies build script prior to running this script.")

        return f"""
            SELECT * 
            FROM `{studies_table_id}` 
        """

    studies_result = query_and_retrieve_result(make_all_studies_query())

    studies_list = list()

    for study in studies_result:
        studies_list.append(dict(study.items()))

    projects_list = list()
    projects_set = set()

    for study in studies_list:
        if study['project_short_name'] not in projects_set:
            projects_list.append({
                'project_friendly_name': study['project_friendly_name'],
                'project_short_name': study['project_short_name'],
                'project_submitter_id': study['project_submitter_id'],
                'program_short_name': study['program_short_name']
            })

            projects_set.add(study['project_short_name'])

    return projects_list


def make_project_per_sample_file_query(project_submitter_id):
    studies_table_id = f"{PARAMS['DEV_PROJECT']}.{PARAMS['DEV_METADATA_DATASET']}.studies_{PARAMS['RELEASE']}"

    return f"""
        WITH file_instruments AS (
            SELECT file_id, 
                STRING_AGG(DISTINCT instrument, ';') AS instruments
            FROM `{create_dev_table_id(PARAMS, 'file_instrument')}`
            GROUP BY file_id
        )

        SELECT f.file_id,
            c.case_id,
            c.case_submitter_id,
            s.sample_id,
            s.sample_submitter_id,
            s.sample_type,
            study.project_short_name,
            study.project_submitter_id,
            study.program_short_name,
            study.program_name,
            f.data_category,
            f.experiment_type,
            f.file_type,
            f.file_size,
            f.file_format,
            fi.instruments AS instrument,
            f.file_name,
            f.file_location,
            "open" AS `access`
        FROM `{create_dev_table_id(PARAMS, 'file')}` f
        JOIN `{create_dev_table_id(PARAMS, 'file_case_id')}` fc
            ON fc.file_id = f.file_id
        JOIN `{create_dev_table_id(PARAMS, 'case')}` c
            ON c.case_id = fc.case_id
        JOIN `{create_dev_table_id(PARAMS, 'sample_case_id')}` sc
            ON sc.case_id = c.case_id
        JOIN `{create_dev_table_id(PARAMS, 'sample')}` s
            ON s.sample_id = sc.sample_id
        JOIN `{create_dev_table_id(PARAMS, 'file_study_id')}` fs
            ON fs.file_id = f.file_id
        JOIN `{studies_table_id}` study
            ON study.study_id = fs.study_id
        JOIN file_instruments fi
            ON fi.file_id = f.file_id
        WHERE study.project_submitter_id = '{project_submitter_id}'
    """


def main(args):
    try:
        global PARAMS
        PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        has_fatal_error(err, ValueError)

    start_time = time.time()

    projects_list = get_pdc_projects_list(PARAMS)

    if 'create_project_tables' in steps:
        for project in projects_list:
            project_table_name = f"{PARAMS['TABLE_NAME']}_{project['project_short_name']}_{PARAMS['RELEASE']}"
            project_table_id = f"{PARAMS['DEV_PROJECT']}.{PARAMS['DEV_SAMPLE_DATASET']}.{project_table_name}"

            load_table_from_query(params=PARAMS,
                                  table_id=project_table_id,
                                  query=make_project_per_sample_file_query(project['project_submitter_id']))

            schema_tags = dict()

            if 'program_label' in project:
                schema_tags['program-name-lower'] = project['program_label'].lower().strip()

                generic_table_metadata_file = PARAMS['GENERIC_TABLE_METADATA_FILE']
            elif 'program_label_0' in project and 'program_label_1' in project:
                schema_tags['program-name-0-lower'] = project['program_label_0'].lower().strip()
                schema_tags['program-name-1-lower'] = project['program_label_1'].lower().strip()

                generic_table_metadata_file = PARAMS['GENERIC_TABLE_METADATA_FILE_2_PROGRAM']
            else:
                has_fatal_error(f"No program labels found for {project['project_submitter_id']}.")
                exit()  # just used to quiet PyCharm warnings, not needed

            schema_tags['project-name'] = project['project_short_name'].strip()
            schema_tags['friendly-project-name-upper'] = project['project_friendly_name'].upper().strip()

            update_table_schema_from_generic(params=PARAMS,
                                             table_id=project_table_id,
                                             schema_tags=schema_tags,
                                             metadata_file=generic_table_metadata_file)
    if 'publish_tables' in steps:
        for project in projects_list:
            project_name = project['project_short_name']
            program_name = project['program_short_name']

            project_table_name = f"{PARAMS['TABLE_NAME']}_{project_name}_{PARAMS['RELEASE']}"
            project_table_id = f"{PARAMS['DEV_PROJECT']}.{PARAMS['DEV_SAMPLE_DATASET']}.{project_table_name}"

            current_table_name = f"{PARAMS['TABLE_NAME']}_{project_name}_{PARAMS['DC_SOURCE']}_current"
            current_table_id = f"{PARAMS['PROD_PROJECT']}.{program_name}.{current_table_name}"

            versioned_table_name = f"{PARAMS['TABLE_NAME']}_{project_name}_{PARAMS['DC_SOURCE']}_{PARAMS['DC_RELEASE']}"
            versioned_table_id = f"{PARAMS['PROD_PROJECT']}.{program_name}_versioned.{versioned_table_name}"

            publish_table(params=PARAMS,
                          source_table_id=project_table_id,
                          current_table_id=current_table_id,
                          versioned_table_id=versioned_table_id)

    end_time = time.time()

    print(f"Script completed in: {format_seconds(end_time - start_time)}")


if __name__ == "__main__":
    main(sys.argv)
