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


def has_supplemental_diagnosis_table(project_id: str) -> bool:
    def make_multiple_diagnosis_count_sql() -> str:
        return f"""
        SELECT c.case_id, COUNT(c.case_id) as case_id_count 
        FROM `{create_dev_table_id(PARAMS, 'case_project_id')}` cp
        JOIN `{create_dev_table_id(PARAMS, 'case')}` c
          ON cp.case_id = c.case_id
        LEFT JOIN `{create_dev_table_id(PARAMS, 'case_diagnosis_id')}` cdiag
          ON cp.case_id = cdiag.case_id
        LEFT JOIN `{create_dev_table_id(PARAMS, 'diagnosis')}` diag
          ON cdiag.diagnosis_id = diag.diagnosis_id
        WHERE cp.project_id = '{project_id}'
        GROUP BY case_id
        HAVING case_id_count > 1
        """

    result = query_and_retrieve_result(make_multiple_diagnosis_count_sql())

    if result.total_rows == 0:
        return False
    else:
        return True


def filter_null_columns(project_id: str, table_type: str, columns: list[str]) -> list[str]:
    def make_count_column_sql() -> str:
        count_sql_str = ''

        for col in columns:
            count_sql_str += f'\nSUM(CASE WHEN this_table.{col} is null THEN 0 ELSE 1 END) AS {col}_count, '

        # remove extra comma (due to looping) from end of string
        count_sql_str = count_sql_str[:-2]

        make_filter_null_columns_sql = f"""
            SELECT {count_sql_str}
            FROM `{create_dev_table_id(PARAMS, table_type)}` this_table 
        """

        if table_type == 'case':
            make_filter_null_columns_sql += f"""
                JOIN `{create_dev_table_id(PARAMS, 'case_project_id')}` cp
                    ON this_table.case_id = cp.case_id
                WHERE cp.project_id = '{project_id}'
            """
        elif table_type == 'demographic' or table_type == 'diagnosis':
            table_type_id = f"{table_type}_id"
            table_type_project_id = f"{table_type}_project_id"

            make_filter_null_columns_sql += f"""
                JOIN `{create_dev_table_id(PARAMS, table_type_project_id)}` dp
                    ON this_table.{table_type_id} = dp.{table_type_id}
                JOIN `{create_dev_table_id(PARAMS, table_type)}` d
                    ON dp.{table_type_id} = d.{table_type_id}
                WHERE dp.project_id = '{project_id}'
            """

        return make_filter_null_columns_sql

    column_count_result = query_and_retrieve_result(sql=make_count_column_sql())

    non_null_columns = list()

    for row in column_count_result:
        # get columns for field group
        for column in columns:
            count = row.get(f"{column}_count")

            if count is not None and count > 0:
                non_null_columns.append(column)

    return non_null_columns


def make_clinical_table_sql(project: dict[str], non_null_column_dict: dict[str, list[str]]) -> str:
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
        SELECT {select_str}
        FROM {create_dev_table_id(PARAMS, 'case_project_id')} cp
        JOIN {create_dev_table_id(PARAMS, 'case')} `case`
            ON cp.case_id = `case`.case_id
        JOIN {create_dev_table_id(PARAMS, 'project')} project
            ON cp.project_id = project.project_id
        LEFT JOIN {create_dev_table_id(PARAMS, 'case_demographic_id')} cdemo
            ON `case`.case_id = cdemo.case_id
        LEFT JOIN {create_dev_table_id(PARAMS, 'demographic')} demographic
            ON cdemo.demographic_id = demographic.demographic_id
        {diagnosis_sql}
        WHERE cp.project_id = '{project['project_id']}'
    """


def make_diagnosis_table_sql(project: dict[str], diagnosis_columns) -> str:
    select_str = """
        `case`.case_id,
        `case`.case_submitter_id,
        project.project_submitter_id,
    """

    for column in diagnosis_columns:
        select_str += f"diagnosis.{column}, "

    select_str = select_str[:-2]

    return f"""    
        SELECT {select_str}
        FROM {create_dev_table_id(PARAMS, 'case_project_id')} cp
        JOIN {create_dev_table_id(PARAMS, 'case')} `case`
            ON cp.case_id = `case`.case_id
        JOIN {create_dev_table_id(PARAMS, 'project')} project
            ON cp.project_id = project.project_id
        LEFT JOIN {create_dev_table_id(PARAMS, 'case_diagnosis_id')} cdiag
            ON `case`.case_id = cdiag.case_id
        LEFT JOIN {create_dev_table_id(PARAMS, 'diagnosis')} diagnosis
            ON cdiag.diagnosis_id = diagnosis.diagnosis_id
        WHERE cp.project_id = '{project['project_id']}'
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
        # logger.info("Finding missing fields")
        logger.info("Skipping missing fields--uncomment before handing off")
        # todo is this definitely working for PDC? seems to be, double check
        # find_missing_fields()
    if 'create_project_tables' in steps:
        logger.info("Entering create_project_tables")

        for project in projects_list:

            clinical_table_base_name = f"{project['project_short_name']}_{PARAMS['TABLE_NAME']}"
            clinical_table_id = create_clinical_table_id(PARAMS, clinical_table_base_name)

            diagnosis_table_base_name = f"{clinical_table_base_name}_diagnosis"
            diagnosis_table_id = create_clinical_table_id(PARAMS, diagnosis_table_base_name)
            has_diagnosis_table = has_supplemental_diagnosis_table(project['project_id'])

            non_null_column_dict = dict()

            for table_type, table_metadata in PARAMS['TABLE_PARAMS'].items():
                columns = table_metadata['column_order']
                non_null_columns = filter_null_columns(project_id=project['project_id'],
                                                       table_type=table_type,
                                                       columns=columns)
                non_null_column_dict[table_type] = non_null_columns

            if not has_diagnosis_table:
                clinical_table_sql = make_clinical_table_sql(project, non_null_column_dict)

                create_table_from_query(params=PARAMS,
                                        table_id=clinical_table_id,
                                        query=clinical_table_sql)

            else:
                diagnosis_columns = non_null_column_dict.pop('diagnosis')

                clinical_table_sql = make_clinical_table_sql(project, non_null_column_dict)

                create_table_from_query(params=PARAMS,
                                        table_id=clinical_table_id,
                                        query=clinical_table_sql)

                diagnosis_table_sql = make_diagnosis_table_sql(project, diagnosis_columns)

                create_table_from_query(PARAMS,
                                        table_id=diagnosis_table_id,
                                        query=diagnosis_table_sql)

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
