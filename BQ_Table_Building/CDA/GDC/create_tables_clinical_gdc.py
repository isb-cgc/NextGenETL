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
import logging
import sys
import time

from cda_bq_etl.data_helpers import initialize_logging
from cda_bq_etl.utils import create_dev_table_id, load_config, format_seconds
from cda_bq_etl.bq_helpers import query_and_retrieve_result, get_project_or_program_list

PARAMS = dict()
YAML_HEADERS = ('params', 'steps')


def find_program_tables(field_groups_dict: dict[str, dict[str, str]]) -> dict[str, set[str]]:
    def make_programs_with_multiple_ids_per_case_sql() -> str:
        if table_vocabulary_dict['parent_field_group'] is not None:
            child_parent_map_table_id = create_dev_table_id(PARAMS, f"{table_vocabulary_dict['join_table_name']}")
            parent_case_map_table_id = create_dev_table_id(PARAMS,
                                                           f"{table_vocabulary_dict['parent_field_group']}_of_case")

            return f"""
                WITH programs AS (
                    SELECT DISTINCT case_proj.project_id
                    FROM `{child_parent_map_table_id}` child_parent
                    JOIN `{parent_case_map_table_id}` parent_case
                        USING ({table_vocabulary_dict['parent_field_group']}_id)
                    JOIN `{create_dev_table_id(PARAMS, 'case_in_project')}` case_proj
                        ON parent_case.case_id = case_proj.case_id
                    GROUP BY child_parent.{table_vocabulary_dict['parent_field_group']}_id, case_proj.project_id
                    HAVING COUNT(child_parent.{table_name}_id) > 1
                )

                SELECT DISTINCT SPLIT(project_id, "-")[0] AS project_short_name
                FROM programs
            """
        else:
            return f"""
                WITH programs AS (
                    SELECT DISTINCT case_proj.project_id
                    FROM `{create_dev_table_id(PARAMS, f"{table_name}_of_case")}` base_table
                    JOIN `{create_dev_table_id(PARAMS, 'case_in_project')}` case_proj
                        ON base_table.case_id = case_proj.case_id
                    GROUP BY base_table.case_id, case_proj.project_id
                    HAVING COUNT(base_table.case_id) > 1
                )

                SELECT DISTINCT SPLIT(project_id, "-")[0] AS project_short_name
                FROM programs
            """

    logger = logging.getLogger('base_script')
    # Create program set for base clinical tables -- will include every program with clinical cases
    programs = get_project_or_program_list(PARAMS)
    tables_per_program_dict = dict()

    if programs is None:
        logger.critical("No programs found, exiting.")
        sys.exit(-1)

    for base_program in programs:
        tables_per_program_dict[base_program] = {PARAMS['MASTER_TABLE']}

    # Create set of programs for each mapping table type,
    # required when a single case has multiple rows for a given field group (e.g. multiple diagnoses or follow-ups)
    for table_name, table_vocabulary_dict in field_groups_dict.items():
        # create the query and retrieve results
        result = query_and_retrieve_result(sql=make_programs_with_multiple_ids_per_case_sql())

        if programs is not None:
            for program_row in result:
                # change certain program names (currently EXCEPTIONAL_RESPONDERS and BEATAML1.0)
                if program_row[0] in PARAMS['ALTER_PROGRAM_NAME_LIST'].keys():
                    program_name = PARAMS['ALTER_PROGRAM_NAME_LIST'][program_row[0]]
                else:
                    program_name = program_row[0]

                tables_per_program_dict[program_name].add(table_name)

    return tables_per_program_dict


def get_field_groups() -> list[str]:
    field_group_list = list()

    for field_group in PARAMS['TSV_FIELD_GROUP_CONFIG'].keys():
        field_group_name = field_group.split('.')[-1]
        field_group_list.append(field_group_name)

    return field_group_list


def find_non_null_columns_by_program(program, field_group):
    def make_count_column_sql() -> str:
        columns = PARAMS['FIELD_CONFIG'][field_group]['column_order']
        mapping_table = PARAMS['FIELD_CONFIG'][field_group]['mapping_table']
        id_key = PARAMS['FIELD_CONFIG'][field_group]['id_key']
        parent_table = PARAMS['FIELD_CONFIG'][field_group]['child_of']

        count_sql_str = ''

        for col in columns:
            count_sql_str += f'\nSUM(CASE WHEN child_table.{col} is null THEN 0 ELSE 1 END) AS {col}_count, '

        # remove extra comma (due to looping) from end of string
        count_sql_str = count_sql_str[:-2]

        if parent_table == 'case':
            return f"""
            SELECT {count_sql_str}
            FROM `{create_dev_table_id(PARAMS, field_group)}` child_table
            JOIN `{create_dev_table_id(PARAMS, mapping_table)}` mapping_table
                ON mapping_table.{id_key} = child_table.{id_key}
            JOIN `{create_dev_table_id(PARAMS, 'case_project_program')}` cpp
                ON mapping_table.case_id = cpp.case_gdc_id
            WHERE cpp.program_name = '{program}'
            """
        elif parent_table:
            parent_mapping_table = PARAMS['FIELD_CONFIG'][parent_table]['mapping_table']
            parent_id_key = PARAMS['FIELD_CONFIG'][parent_table]['id_key']

            return f"""
            SELECT {count_sql_str}
            FROM `{create_dev_table_id(PARAMS, field_group)}` child_table
            JOIN `{create_dev_table_id(PARAMS, mapping_table)}` mapping_table
                ON mapping_table.{id_key} = child_table.{id_key}
            JOIN `{create_dev_table_id(PARAMS, parent_table)}` parent_table
                ON parent_table.{parent_id_key} = mapping_table.{parent_id_key}
            JOIN `{create_dev_table_id(PARAMS, parent_mapping_table)}` parent_mapping_table
                ON parent_mapping_table.{parent_id_key} = parent_table.{parent_id_key}
            JOIN `{create_dev_table_id(PARAMS, 'case_project_program')}` cpp
                ON parent_mapping_table.case_id = cpp.case_gdc_id
            WHERE cpp.program_name = '{program}'
            """
        else:
            pass
            # handle project and case field groups

    column_count_result = query_and_retrieve_result(sql=make_count_column_sql())

    non_null_columns = list()

    for row in column_count_result:
        # get columns for field group
        for column in PARAMS['FIELD_CONFIG'][field_group]['column_order']:
            count = row.get(f"{column}_count")

            if count is not None and count > 0:
                non_null_columns.append(column)

    return non_null_columns


def create_base_clinical_table_for_program():
    pass
    # get list of mapping tables to flatten into clinical -- exclude those in tables_per_program_dict
    # create query that contains all the columns for each included field group
    # don't include columns that are null for every case within the given program


def find_missing_fields(include_trivial_columns: bool = False):
    # get list of columns from CDA table
    # compare to table order and excluded column lists in FIELD_CONFIG[table]
    # any missing columns? print
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

    has_missing_columns = False

    for table_name in PARAMS['FIELD_CONFIG'].keys():
        result = query_and_retrieve_result(make_column_query())

        cda_columns_set = set()

        for row in result:
            cda_columns_set.add(row[0])

        # columns should either be listed in column order or excluded columns in FIELD_CONFIG
        included_columns_set = set(PARAMS['FIELD_CONFIG'][table_name]['column_order'])

        if PARAMS['FIELD_CONFIG'][table_name]['excluded_columns'] is not None:
            excluded_columns_set = set(PARAMS['FIELD_CONFIG'][table_name]['excluded_columns'])
        else:
            excluded_columns_set = set()

        # join into one set
        all_columns_set = included_columns_set | excluded_columns_set

        deprecated_columns = all_columns_set - cda_columns_set
        missing_columns = cda_columns_set - all_columns_set

        non_trivial_columns = set()

        for column in missing_columns:
            result = query_and_retrieve_result(make_column_values_query())
            result_list = list(result)

            if len(result_list) > 0:
                non_trivial_columns.add(column)

        trivial_columns = missing_columns - non_trivial_columns

        if len(deprecated_columns) > 0 \
                or (len(trivial_columns) > 0 and include_trivial_columns) \
                or len(non_trivial_columns) > 0:
            logger.info(f"For {table_name}:")

            if len(deprecated_columns) > 0:
                logger.info(f"Columns no longer found in CDA: {deprecated_columns}")
            if len(trivial_columns) > 0 and include_trivial_columns:
                logger.info(f"Trivial (only null) columns missing from FIELD_CONFIG: {trivial_columns}")
            if len(non_trivial_columns) > 0:
                logger.error(f"Non-trivial columns missing from FIELD_CONFIG: {non_trivial_columns}")
                has_missing_columns = True

    if has_missing_columns:
        logger.critical("Missing columns found (see above output). Please take the following steps, then restart:")
        logger.critical(" - add columns to FIELD_CONFIG in yaml config")
        logger.critical(" - confirm column description is provided in BQEcosystem/TableFieldUpdates.")
        sys.exit(-1)


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

    find_missing_fields()

    if 'find_program_tables' in steps:
        # creates dict of programs and base, supplemental tables to be created
        tables_per_program_dict = find_program_tables(PARAMS['TSV_FIELD_GROUP_CONFIG'])

        for program, tables in tables_per_program_dict.items():
            print(f"{program}: {tables}")

    """

    # counts returned may be null if program has no values within a table, e.g. TCGA has no annotation records

    all_program_columns = dict()

    programs = get_project_or_program_list(PARAMS)
    field_groups = get_field_groups()

    for program in programs:
        logger.info(f"Finding columns for {program}")
        program_columns = dict()

        for field_group in field_groups:
            non_null_columns = find_non_null_columns_by_program(program=program, field_group=field_group)
            if len(non_null_columns) > 0:
                program_columns[field_group] = non_null_columns

        all_program_columns[program] = program_columns

    logger.info("*** Non-null columns, by program and field group")
    for program, column_groups in all_program_columns.items():
        logger.info(f"{program}")
        for field_group, columns in column_groups.items():
            logger.info(f"{field_group}: {columns}")
    """

    # steps:
    # use all_program_columns and tables_per_program_dict to stitch together queries to build each program's tables
    # note: case and project fields are still not completed
    # use the FG_CONFIG to order fields by FG and to account for last_keys_in_table
    # use these queries to build the clinical tables

    end_time = time.time()
    logger.info(f"Script completed in: {format_seconds(end_time - start_time)}")


if __name__ == "__main__":
    main(sys.argv)
