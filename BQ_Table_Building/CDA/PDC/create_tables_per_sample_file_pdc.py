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

from cda_bq_etl.bq_helpers import create_table_from_query, update_table_schema_from_generic, query_and_retrieve_result
from cda_bq_etl.data_helpers import initialize_logging
from cda_bq_etl.utils import load_config, create_dev_table_id, format_seconds, create_per_sample_table_id

PARAMS = dict()
YAML_HEADERS = ('params', 'steps')


def get_pdc_projects_metadata_list():
    """
    Return current list of PDC projects (pulled from study metadata table in BQEcosystem repo).
    """
    def make_all_studies_query() -> str:
        studies_table_id = f"{PARAMS['DEV_PROJECT']}.{PARAMS['DEV_METADATA_DATASET']}.studies_{PARAMS['RELEASE']}"

        return f"""
            SELECT distinct project_short_name, 
            project_friendly_name, 
            project_submitter_id, 
            program_short_name, 
            program_labels
            FROM `{studies_table_id}`
        """

    projects_result = query_and_retrieve_result(make_all_studies_query())

    projects_list = list()

    for project in projects_result:
        projects_list.append(dict(project))

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
        start_time = time.time()

        global PARAMS
        PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        sys.exit(err)

    log_file_time = time.strftime('%Y.%m.%d-%H.%M.%S', time.localtime())
    log_filepath = f"{PARAMS['LOGFILE_PATH']}.{log_file_time}"
    logger = initialize_logging(log_filepath)

    projects_list = get_pdc_projects_metadata_list()

    if 'create_project_tables' in steps:
        logger.info("Entering create_project_tables")

        for project in projects_list:
            project_table_base_name = f"{project['project_short_name']}_{PARAMS['TABLE_NAME']}"
            project_table_id = create_per_sample_table_id(PARAMS, project_table_base_name)

            create_table_from_query(params=PARAMS,
                                    table_id=project_table_id,
                                    query=make_project_per_sample_file_query(project['project_submitter_id']))

            schema_tags = dict()

            if 'program_labels' not in project:
                logger.critical(f"No program labels found for {project['project_submitter_id']}.")
                sys.exit(-1)

            program_label_list = project['program_labels'].split('; ')

            if len(program_label_list) == 0:
                logger.critical(f"No program labels found for {project['project_submitter_id']}.")
                sys.exit(-1)
            elif len(program_label_list) == 1:
                schema_tags['program-name-lower'] = program_label_list[0].lower().strip()
                generic_table_metadata_file = PARAMS['GENERIC_TABLE_METADATA_FILE']
            elif len(program_label_list) == 2:
                schema_tags['program-name-0-lower'] = program_label_list[0].lower().strip()
                schema_tags['program-name-1-lower'] = program_label_list[1].lower().strip()
                generic_table_metadata_file = PARAMS['GENERIC_TABLE_METADATA_FILE_2_PROGRAM']
            else:
                logger.critical(f"Too many program labels found for {project['project_submitter_id']}.")
                sys.exit(-1)

            schema_tags['project-name'] = project['project_short_name'].strip()
            schema_tags['friendly-project-name-upper'] = project['project_friendly_name'].upper().strip()

            update_table_schema_from_generic(params=PARAMS,
                                             table_id=project_table_id,
                                             schema_tags=schema_tags,
                                             metadata_file=generic_table_metadata_file)
    """
    if 'publish_tables' in steps:
        logger.info("Entering publish_tables")

        for project in projects_list:
            project_name = project['project_short_name']
            program_name = project['program_short_name']

            project_table_name = f"{PARAMS['TABLE_NAME']}_{project_name}_{PARAMS['RELEASE']}"
            project_table_id = f"{PARAMS['DEV_PROJECT']}.{PARAMS['DEV_SAMPLE_DATASET']}.{project_table_name}"

            current_table_name = f"{PARAMS['TABLE_NAME']}_{project_name}_{PARAMS['NODE']}_current"
            current_table_id = f"{PARAMS['PROD_PROJECT']}.{program_name}.{current_table_name}"

            versioned_table_name = f"{PARAMS['TABLE_NAME']}_{project_name}_{PARAMS['NODE']}_{PARAMS['RELEASE']}"
            versioned_table_id = f"{PARAMS['PROD_PROJECT']}.{program_name}_versioned.{versioned_table_name}"

            publish_table(params=PARAMS,
                          source_table_id=project_table_id,
                          current_table_id=current_table_id,
                          versioned_table_id=versioned_table_id)
    """

    end_time = time.time()

    logger.info(f"Script completed in: {format_seconds(end_time - start_time)}")


if __name__ == "__main__":
    main(sys.argv)
