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

from cda_bq_etl.data_helpers import initialize_logging
from cda_bq_etl.utils import (load_config, create_dev_table_id, format_seconds, create_clinical_table_id,
                              create_metadata_table_id)
from cda_bq_etl.bq_helpers import (create_table_from_query, get_pdc_projects_metadata, get_project_level_schema_tags,
                                   update_table_schema_from_generic, query_and_retrieve_result, find_missing_columns)

PARAMS = dict()
YAML_HEADERS = ('params', 'steps')


def project_needs_supplemental_diagnosis_table(project_submitter_id: str) -> bool:
    """
    Tests within a given project: does any case record have > 1 associated diagnosis records?
    :param project_submitter_id: PDC project submitter id
    :return: True if project requires a supplemental diagnosis table; False otherwise
    """
    def make_multiple_diagnosis_count_query() -> str:
        return f"""
            WITH project_ids AS (
                SELECT distinct s.project_id
                FROM `{create_metadata_table_id(PARAMS, 'studies')}` s
                WHERE s.project_submitter_id = '{project_submitter_id}'
            )

            SELECT c.case_id, COUNT(c.case_id) as case_id_count 
            FROM `{create_dev_table_id(PARAMS, 'case_project_id')}` cp
            JOIN project_ids pid
              ON cp.project_id = pid.project_id
            JOIN `{create_dev_table_id(PARAMS, 'case')}` c
              ON cp.case_id = c.case_id
            LEFT JOIN `{create_dev_table_id(PARAMS, 'case_diagnosis_id')}` cdiag
              ON cp.case_id = cdiag.case_id
            LEFT JOIN `{create_dev_table_id(PARAMS, 'diagnosis')}` diag
              ON cdiag.diagnosis_id = diag.diagnosis_id
            GROUP BY case_id
            HAVING case_id_count > 1
        """

    result = query_and_retrieve_result(make_multiple_diagnosis_count_query())

    if result.total_rows == 0:
        return False
    else:
        return True


def filter_null_columns(project_dict: dict[str, str], table_type: str, columns: list[str]) -> list[str]:
    """
    Filter out columns with only null values.
    :param project_dict: dict containing project metadata
    :param table_type: type of clinical table (case, demographic, diagnosis)
    :param columns: full column list from yaml config
    :return: list of non-null columns
    """
    def make_count_column_query() -> str:
        count_sql_str = ''

        for col in columns:
            count_sql_str += f'\nSUM(CASE WHEN this_table.{col} is null THEN 0 ELSE 1 END) AS {col}_count, '

        # remove extra comma (due to looping) from end of string
        count_sql_str = count_sql_str[:-2]

        make_filter_null_columns_sql = f"""
            WITH project_ids AS (
                SELECT distinct s.project_id
                FROM `{create_metadata_table_id(PARAMS, 'studies')}` s
                WHERE s.project_submitter_id = '{project_dict['project_submitter_id']}'
            )
            
            SELECT {count_sql_str}
            FROM `{create_dev_table_id(PARAMS, table_type)}` this_table 
        """

        if table_type == 'case':
            make_filter_null_columns_sql += f"""
                JOIN `{create_dev_table_id(PARAMS, 'case_project_id')}` cp
                    ON this_table.case_id = cp.case_id
                JOIN project_ids pid
                    ON cp.project_id = pid.project_id
            """
        elif table_type == 'demographic' or table_type == 'diagnosis':
            make_filter_null_columns_sql += f"""
                JOIN `{create_dev_table_id(PARAMS, f"{table_type}_project_id")}` dp
                    ON this_table.{table_type}_id = dp.{table_type}_id
                JOIN project_ids pid
                    ON dp.project_id = pid.project_id
                JOIN `{create_dev_table_id(PARAMS, table_type)}` d
                    ON dp.{table_type}_id = d.{table_type}_id
            """

        return make_filter_null_columns_sql

    column_count_result = query_and_retrieve_result(sql=make_count_column_query())

    non_null_columns = list()

    for row in column_count_result:
        # get columns for field group
        for column in columns:
            count = row.get(f"{column}_count")

            if count is not None and count > 0:
                non_null_columns.append(column)

    return non_null_columns


def make_clinical_table_query(project: dict[str, str], non_null_column_dict: dict[str, list[str]]) -> str:
    """
    Output sql used to create project clinical table
    :param project: project metadata dict
    :param non_null_column_dict: set of columns with non-trivial values (to include in table)
    :return: sql string used to create clinical table
    """
    select_list = list()
    for table_type, column_list in non_null_column_dict.items():
        for column in column_list:
            select_list.append(f"`{table_type}`.{column}")

    # insert project_submitter_id
    select_list.insert(2, "project.project_submitter_id")

    select_str = ''

    for column in select_list:
        select_str += f"{column}, "

    # remove last comma
    select_str = select_str[:-2]

    if 'diagnosis' in non_null_column_dict:
        diagnosis_sql = f"""
            LEFT JOIN {create_dev_table_id(PARAMS, 'case_diagnosis_id')} cdiag
                ON `case`.case_id = cdiag.case_id
            LEFT JOIN {create_dev_table_id(PARAMS, 'diagnosis')} diagnosis
                ON cdiag.diagnosis_id = diagnosis.diagnosis_id
        """
    else:
        diagnosis_sql = ''

    return f"""
        WITH project_ids AS (
            SELECT distinct s.project_id
            FROM `{create_metadata_table_id(PARAMS, 'studies')}` s
            WHERE s.project_submitter_id = '{project['project_submitter_id']}'
        )

        SELECT {select_str}
        FROM {create_dev_table_id(PARAMS, 'case_project_id')} cp
        JOIN project_ids pid
            ON cp.project_id = pid.project_id
        JOIN {create_dev_table_id(PARAMS, 'case')} `case`
            ON cp.case_id = `case`.case_id
        JOIN {create_dev_table_id(PARAMS, 'project')} project
            ON cp.project_id = project.project_id
        LEFT JOIN {create_dev_table_id(PARAMS, 'case_demographic_id')} cdemo
            ON `case`.case_id = cdemo.case_id
        LEFT JOIN {create_dev_table_id(PARAMS, 'demographic')} demographic
            ON cdemo.demographic_id = demographic.demographic_id
        {diagnosis_sql}
    """


def make_diagnosis_table_query(project: dict[str], diagnosis_columns) -> str:
    """
    Output sql used to create project supplemental diagnosis table.
    :param project: project metadata dict
    :param diagnosis_columns: diagnosis columns with non-trivial values (to include in table)
    :return: sql string used to create supplemental diagnosis table
    """
    select_str = """
        `case`.case_id,
        `case`.case_submitter_id,
        project.project_submitter_id,
    """

    for column in diagnosis_columns:
        select_str += f"diagnosis.{column}, "

    select_str = select_str[:-2]

    return f"""
        WITH project_ids AS (
            SELECT distinct s.project_id
            FROM `{create_metadata_table_id(PARAMS, 'studies')}` s
            WHERE s.project_submitter_id = '{project['project_submitter_id']}'
        )

        SELECT {select_str}
        FROM {create_dev_table_id(PARAMS, 'case_project_id')} cp
        JOIN project_ids pid
            ON cp.project_id = pid.project_id
        JOIN {create_dev_table_id(PARAMS, 'case')} `case`
            ON cp.case_id = `case`.case_id
        JOIN {create_dev_table_id(PARAMS, 'project')} project
            ON cp.project_id = project.project_id
        LEFT JOIN {create_dev_table_id(PARAMS, 'case_diagnosis_id')} cdiag
            ON `case`.case_id = cdiag.case_id
        LEFT JOIN {create_dev_table_id(PARAMS, 'diagnosis')} diagnosis
            ON cdiag.diagnosis_id = diagnosis.diagnosis_id
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

    if 'find_missing_fields' in steps:
        logger.info("Finding missing columns")
        find_missing_columns(PARAMS)
    if 'create_project_tables' in steps:
        logger.info("Entering create_project_tables")

        for project in projects_list:
            clinical_table_base_name = f"{project['project_short_name']}_{PARAMS['TABLE_NAME']}"
            clinical_table_id = create_clinical_table_id(PARAMS, clinical_table_base_name)

            # only used for some projects
            diagnosis_table_base_name = f"{clinical_table_base_name}_diagnosis"
            diagnosis_table_id = create_clinical_table_id(PARAMS, diagnosis_table_base_name)
            has_diagnosis_table = project_needs_supplemental_diagnosis_table(project['project_submitter_id'])

            non_null_column_dict = dict()

            for table_type, table_metadata in PARAMS['TABLE_PARAMS'].items():
                # create a dict of non-null columns for each project and clinical data type (case, demo, diag)
                non_null_columns = filter_null_columns(project_dict=project,
                                                       table_type=table_type,
                                                       columns=table_metadata['column_order'])
                non_null_column_dict[table_type] = non_null_columns

            # does this project require a supplementary diagnosis table (multiple diagnoses for single case)?
            if not has_diagnosis_table:
                # no cases have multiple diagnoses, so create combined clinical table
                create_table_from_query(params=PARAMS,
                                        table_id=clinical_table_id,
                                        query=make_clinical_table_query(project, non_null_column_dict))
            else:
                # case(s) in this project have multiple diagnoses, so make clinical and diagnosis tables

                # pop off the diagnosis columns -- these will be included in supplementary table
                diagnosis_columns = non_null_column_dict.pop('diagnosis')

                # create main clinical table
                create_table_from_query(params=PARAMS,
                                        table_id=clinical_table_id,
                                        query=make_clinical_table_query(project, non_null_column_dict))

                # create supplementary diagnosis table
                create_table_from_query(PARAMS,
                                        table_id=diagnosis_table_id,
                                        query=make_diagnosis_table_query(project, diagnosis_columns))

            schema_tags = get_project_level_schema_tags(PARAMS, project['project_submitter_id'])

            if 'program-name-1-lower' in schema_tags:
                generic_table_metadata_file = PARAMS['GENERIC_TABLE_METADATA_FILE_2_PROGRAM']
            else:
                generic_table_metadata_file = PARAMS['GENERIC_TABLE_METADATA_FILE']

            update_table_schema_from_generic(params=PARAMS,
                                             table_id=clinical_table_id,
                                             schema_tags=schema_tags,
                                             metadata_file=generic_table_metadata_file)

            if has_diagnosis_table:
                update_table_schema_from_generic(params=PARAMS,
                                                 table_id=diagnosis_table_id,
                                                 schema_tags=schema_tags,
                                                 metadata_file=generic_table_metadata_file)

    end_time = time.time()

    logger.info(f"Script completed in: {format_seconds(end_time - start_time)}")


if __name__ == "__main__":
    main(sys.argv)
