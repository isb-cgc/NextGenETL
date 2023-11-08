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
import logging

from cda_bq_etl.data_helpers import initialize_logging
from cda_bq_etl.utils import load_config, create_dev_table_id, format_seconds, create_clinical_table_id, \
    create_metadata_table_id
from cda_bq_etl.bq_helpers import (create_table_from_query, get_pdc_projects_metadata, get_project_level_schema_tags,
                                   update_table_schema_from_generic, query_and_retrieve_result)

PARAMS = dict()
YAML_HEADERS = ('params', 'steps')


def find_missing_fields(include_trivial_columns: bool = False):
    """
    Get list of columns from CDA table, compare to column order and excluded column lists in yaml config (TABLE_PARAMS),
    output any missing columns in either location.
    :param include_trivial_columns: Optional; if True, will list columns that are not found in yaml config even if they
                                    have only null values in the dataset
    """
    def make_column_query():
        full_table_name = create_dev_table_id(PARAMS, table_name).split('.')[2]

        return f"""
            SELECT column_name
            FROM {PARAMS['DEV_PROJECT']}.{PARAMS['DEV_RAW_DATASET']}.INFORMATION_SCHEMA.COLUMNS
            WHERE table_name = '{full_table_name}' 
        """

    def make_column_values_query():
        return f"""
            SELECT DISTINCT {column}
            FROM {create_dev_table_id(PARAMS, table_name)}
            WHERE {column} IS NOT NULL
        """

    logger = logging.getLogger('base_script')
    logger.info("Scanning for missing fields in config yaml!")

    has_missing_columns = False

    for table_name in PARAMS['TABLE_PARAMS'].keys():
        result = query_and_retrieve_result(make_column_query())

        cda_columns_set = set()

        for row in result:
            cda_columns_set.add(row[0])

        columns_set = set(PARAMS['TABLE_PARAMS'][table_name]['column_order'])
        excluded_columns_set = set()

        if PARAMS['TABLE_PARAMS'][table_name]['excluded_columns'] is not None:
            excluded_columns_set = set(PARAMS['TABLE_PARAMS'][table_name]['excluded_columns'])

        # join into one set
        all_columns_set = columns_set | excluded_columns_set

        deprecated_columns = all_columns_set - cda_columns_set
        missing_columns = cda_columns_set - all_columns_set

        non_trivial_columns = set()

        for column in missing_columns:
            result = query_and_retrieve_result(make_column_values_query())
            result_list = list(result)

            if len(result_list) > 0:
                non_trivial_columns.add(column)

        trivial_columns = missing_columns - non_trivial_columns

        if len(deprecated_columns) > 0 or len(non_trivial_columns) > 0 \
                or (len(trivial_columns) > 0 and include_trivial_columns):
            logger.info(f"For {table_name}:")

            if len(deprecated_columns) > 0:
                logger.info(f"Columns no longer found in CDA: {sorted(deprecated_columns)}")
            if len(trivial_columns) > 0 and include_trivial_columns:
                logger.info(f"Trivial (only null) columns missing from TABLE_PARAMS: {sorted(trivial_columns)}")
            if len(non_trivial_columns) > 0:
                logger.error(f"Non-trivial columns missing from TABLE_PARAMS: {sorted(non_trivial_columns)}")
                has_missing_columns = True

    if has_missing_columns:
        logger.critical("Missing columns found (see above output). Please take the following steps, then restart:")
        logger.critical(" - add columns to TABLE_PARAMS in yaml config")
        logger.critical(" - confirm column description is provided in BQEcosystem/TableFieldUpdates.")
        sys.exit(-1)
    else:
        logger.info("No missing fields!")


def find_project_tables(projects_list: list[str]) -> dict[str, set[str]]:
    """
    Creates per-program dict of tables to be created.
    :return: dict in the form { <program-name>: {set of standalone tables} }
    """
    def make_projects_with_multiple_ids_per_case_sql() -> str:
        return f"""
            WITH projects AS (
                SELECT DISTINCT proj.project_submitter_id
                FROM `{create_dev_table_id(PARAMS, table_metadata['mapping_table'])}` base_mapping_table
                JOIN `{create_dev_table_id(PARAMS, 'case_project_id')}` case_proj
                    ON base_mapping_table.case_id = case_proj.case_id
                JOIN `{create_dev_table_id(PARAMS, 'project')}` proj
                    ON case_proj.project_id = proj.project_id
                GROUP BY base_mapping_table.case_id, case_proj.project_id
                HAVING COUNT(base_mapping_table.case_id) > 1
            )

            SELECT DISTINCT project_submitter_id
            FROM projects
            """

    table_dict = PARAMS['TABLE_PARAMS']

    logger = logging.getLogger('base_script')
    # Create program set for base clinical tables -- will include every program with clinical cases
    tables_per_project_dict = dict()

    if projects_list is None:
        logger.critical("No programs found, exiting.")
        sys.exit(-1)

    print(projects_list)

    for base_project in projects_list:
        tables_per_project_dict[base_project] = {'case'}

    # Create set of programs for each mapping table type,
    # required when a single case has multiple rows for a given field group (e.g. multiple diagnoses or follow-ups)
    for table_name, table_metadata in table_dict.items():
        if table_name == 'case':
            continue

        # create the query and retrieve results
        result = query_and_retrieve_result(sql=make_projects_with_multiple_ids_per_case_sql())

        if result is None:
            logger.error("SQL result is none for query: ")
            logger.debug(make_projects_with_multiple_ids_per_case_sql())
            sys.exit(-1)

        for project_row in result:
            tables_per_project_dict[project_row[0]].add(table_name)

    return tables_per_project_dict


def make_clinical_table_query(project_submitter_id: str) -> str:
    # get all cases for project submitter id
    # get clinical data, demographics and diagnoses
    #
    return f"""
        SELECT c.case_id,
            c.case_submitter_id,
            study.project_short_name,
            study.project_submitter_id,
            study.program_short_name,
            study.program_name
        FROM `{create_dev_table_id(PARAMS, 'case')}` c
        JOIN `{create_dev_table_id(PARAMS, 'case_study_id')}` cs
            ON c.case_id = cs.case_id
        JOIN `{create_metadata_table_id(PARAMS, "studies")}` study
            ON cs.study_id = study.study_id
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

    projects_list = get_pdc_projects_metadata(PARAMS)

    # todo add find missing fields

    if 'find_missing_fields' in steps:
        # logger.info("Finding missing fields")
        # find_missing_fields()
        logger.info("Passing missing fields")
    if 'create_project_tables' in steps:
        logger.info("Entering create_project_tables")

        tables_per_project_dict = find_project_tables(projects_list)
        logger.debug(tables_per_project_dict)

        for project in projects_list:
            project_table_base_name = f"{project['project_short_name']}_{PARAMS['TABLE_NAME']}"
            project_table_id = create_clinical_table_id(PARAMS, project_table_base_name)

            create_table_from_query(params=PARAMS,
                                    table_id=project_table_id,
                                    query=make_clinical_table_query(project['project_submitter_id']))

            schema_tags = get_project_level_schema_tags(PARAMS, project['project_submitter_id'])

            if 'program-name-1-lower' in schema_tags:
                generic_table_metadata_file = PARAMS['GENERIC_TABLE_METADATA_FILE_2_PROGRAM']
            else:
                generic_table_metadata_file = PARAMS['GENERIC_TABLE_METADATA_FILE']

            update_table_schema_from_generic(params=PARAMS,
                                             table_id=project_table_id,
                                             schema_tags=schema_tags,
                                             metadata_file=generic_table_metadata_file)
    end_time = time.time()

    logger.info(f"Script completed in: {format_seconds(end_time - start_time)}")


if __name__ == "__main__":
    main(sys.argv)
