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
from typing import Any

from cda_bq_etl.data_helpers import initialize_logging
from cda_bq_etl.utils import create_dev_table_id, load_config, format_seconds
from cda_bq_etl.bq_helpers import query_and_retrieve_result, get_project_or_program_list, create_table_from_query

PARAMS = dict()
YAML_HEADERS = ('params', 'steps')


def find_missing_fields(include_trivial_columns: bool = False):
    # get list of columns from CDA table
    # compare to table order and excluded column lists in TABLE_PARAMS[table]
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
    logger.info("Scanning for missing fields in config yaml!")

    has_missing_columns = False

    for table_name in PARAMS['TABLE_PARAMS'].keys():
        result = query_and_retrieve_result(make_column_query())

        cda_columns_set = set()

        for row in result:
            cda_columns_set.add(row[0])

        # columns should either be listed in column order or excluded columns in TABLE_PARAMS
        included_columns_set = set(PARAMS['TABLE_PARAMS'][table_name]['column_order'])

        if PARAMS['TABLE_PARAMS'][table_name]['excluded_columns'] is not None:
            excluded_columns_set = set(PARAMS['TABLE_PARAMS'][table_name]['excluded_columns'])
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
                logger.info(f"Trivial (only null) columns missing from TABLE_PARAMS: {trivial_columns}")
            if len(non_trivial_columns) > 0:
                logger.error(f"Non-trivial columns missing from TABLE_PARAMS: {non_trivial_columns}")
                has_missing_columns = True

    if has_missing_columns:
        logger.critical("Missing columns found (see above output). Please take the following steps, then restart:")
        logger.critical(" - add columns to TABLE_PARAMS in yaml config")
        logger.critical(" - confirm column description is provided in BQEcosystem/TableFieldUpdates.")
        sys.exit(-1)
    else:
        logger.info("No missing fields!")


def find_program_tables(table_dict: dict[str, dict[str, str]]) -> dict[str, set[str]]:
    def make_programs_with_multiple_ids_per_case_sql() -> str:
        return f"""
            WITH programs AS (
                SELECT DISTINCT case_proj.project_id
                FROM `{create_dev_table_id(PARAMS, table_metadata['mapping_table'])}` base_table
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
    programs = get_project_or_program_list(PARAMS, rename_programs=False)
    tables_per_program_dict = dict()

    if programs is None:
        logger.critical("No programs found, exiting.")
        sys.exit(-1)

    for base_program in programs:
        tables_per_program_dict[base_program] = {'case'}

    # Create set of programs for each mapping table type,
    # required when a single case has multiple rows for a given field group (e.g. multiple diagnoses or follow-ups)
    for table_name, table_metadata in table_dict.items():
        if table_name == 'case' or table_name == 'project':
            continue

        # create the query and retrieve results
        result = query_and_retrieve_result(sql=make_programs_with_multiple_ids_per_case_sql())

        if result is None:
            logger.error("result is none")

        for program_row in result:
            tables_per_program_dict[program_row[0]].add(table_name)

    return tables_per_program_dict


def find_program_non_null_columns_by_table(program):
    def make_count_column_sql() -> str:
        mapping_table = PARAMS['TABLE_PARAMS'][table]['mapping_table']
        id_key = f"{table}_id"
        parent_table = PARAMS['TABLE_PARAMS'][table]['child_of']

        count_sql_str = ''

        for col in columns:
            count_sql_str += f'\nSUM(CASE WHEN this_table.{col} is null THEN 0 ELSE 1 END) AS {col}_count, '

        # remove extra comma (due to looping) from end of string
        count_sql_str = count_sql_str[:-2]

        if table == 'case':
            return f"""
                SELECT {count_sql_str}
                FROM `{create_dev_table_id(PARAMS, table)}` this_table
                JOIN `{create_dev_table_id(PARAMS, 'case_project_program')}` cpp
                    ON this_table.case_id = cpp.case_gdc_id
                WHERE cpp.program_name = '{program}'
            """
        elif table == 'project':
            return f"""
                SELECT {count_sql_str}
                FROM `{create_dev_table_id(PARAMS, table)}` this_table
                JOIN `{create_dev_table_id(PARAMS, 'case_project_program')}` cpp
                    ON this_table.project_id = cpp.project_id
                WHERE cpp.program_name = '{program}'
            """
        elif parent_table == 'case':
            return f"""
            SELECT {count_sql_str}
            FROM `{create_dev_table_id(PARAMS, table)}` this_table
            JOIN `{create_dev_table_id(PARAMS, mapping_table)}` mapping_table
                ON mapping_table.{id_key} = this_table.{id_key}
            JOIN `{create_dev_table_id(PARAMS, 'case_project_program')}` cpp
                ON mapping_table.case_id = cpp.case_gdc_id
            WHERE cpp.program_name = '{program}'
            """
        elif parent_table:
            parent_mapping_table = PARAMS['TABLE_PARAMS'][parent_table]['mapping_table']
            parent_id_key = f"{parent_table}_id"

            return f"""
            SELECT {count_sql_str}
            FROM `{create_dev_table_id(PARAMS, table)}` this_table
            JOIN `{create_dev_table_id(PARAMS, mapping_table)}` mapping_table
                ON mapping_table.{id_key} = this_table.{id_key}
            JOIN `{create_dev_table_id(PARAMS, parent_table)}` parent_table
                ON parent_table.{parent_id_key} = mapping_table.{parent_id_key}
            JOIN `{create_dev_table_id(PARAMS, parent_mapping_table)}` parent_mapping_table
                ON parent_mapping_table.{parent_id_key} = parent_table.{parent_id_key}
            JOIN `{create_dev_table_id(PARAMS, 'case_project_program')}` cpp
                ON parent_mapping_table.case_id = cpp.case_gdc_id
            WHERE cpp.program_name = '{program}'
            """

    non_null_columns_dict = dict()

    for table in PARAMS['TABLE_PARAMS'].keys():
        first_columns = PARAMS['TABLE_PARAMS'][table]['column_order']['first']
        middle_columns = PARAMS['TABLE_PARAMS'][table]['column_order']['middle']
        last_columns = PARAMS['TABLE_PARAMS'][table]['column_order']['last']

        columns = list()

        if first_columns:
            columns.extend(first_columns)
        if middle_columns:
            columns.extend(middle_columns)
        if last_columns:
            columns.extend(last_columns)

        column_count_result = query_and_retrieve_result(sql=make_count_column_sql())

        non_null_columns = list()

        for row in column_count_result:
            # get columns for field group
            for column in columns:
                count = row.get(f"{column}_count")

                if count is not None and count > 0:
                    non_null_columns.append(column)

        non_null_columns_dict[table] = non_null_columns

    return non_null_columns_dict


def create_sql_for_program_tables(program: str, stand_alone_tables: set[str]):
    def make_sql_statement_from_dict() -> str:
        # stitch together query
        sql_query_str = ""

        if table_sql_dict[table]['with']:
            sql_query_str += "WITH "
            sql_query_str += ", ".join(table_sql_dict[table]['with'])
            sql_query_str += '\n'

        if not table_sql_dict[table]['select']:
            logger.critical("No columns found for 'SELECT' clause.")
            sys.exit(-1)

        sql_query_str += "SELECT "
        sql_query_str += ", ".join(table_sql_dict[table]['select'])
        sql_query_str += "\n"

        if not table_sql_dict[table]['from']:
            logger.critical("No columns found for 'FROM' clause.")
            sys.exit(-1)

        sql_query_str += table_sql_dict[table]['from']
        sql_query_str += "\n"

        if table_sql_dict[table]['join']:
            for table_id in table_sql_dict[table]['join'].keys():
                join_type = table_sql_dict[table]['join'][table_id]['join_type']
                left_key = table_sql_dict[table]['join'][table_id]['left_key']
                right_key = table_sql_dict[table]['join'][table_id]['right_key']
                table_alias = table_sql_dict[table]['join'][table_id]['table_alias']
                map_table_alias = table_sql_dict[table]['join'][table_id]['map_table_alias']

                join_str = f"{join_type} JOIN `{table_id}` `{table_alias}` " \
                           f"ON `{table_alias}`.{left_key} = `{map_table_alias}`.{right_key}\n"
                sql_query_str += join_str

        if table_sql_dict[table]['with_join']:
            sql_query_str += table_sql_dict[table]['with_join']

        map_table_alias = PARAMS['TABLE_PARAMS'][table]['mapping_table']

        # filter by program
        sql_query_str += f"WHERE `{map_table_alias}`.case_id in (" \
                         f"SELECT case_gdc_id " \
                         f"FROM `{create_dev_table_id(PARAMS, 'case_project_program')}` " \
                         f"WHERE program_name = '{program}'" \
                         f") "

        return sql_query_str

    def get_table_column_insert_locations() -> dict[str, list[str]]:
        table_column_locations = dict()

        for stand_alone_table in stand_alone_tables:
            table_column_locations[stand_alone_table] = list()

            child_tables = PARAMS['TABLE_PARAMS'][stand_alone_table]['parent_of']

            if not child_tables:
                continue

            for _child_table in child_tables:
                # if child table does not require a stand-alone table and has non-null columns,
                # add to table_column_locations, then check its children as well
                if _child_table not in stand_alone_tables and non_null_column_dict[_child_table]:
                    table_column_locations[stand_alone_table].append(_child_table)

                    grandchild_tables = PARAMS['TABLE_PARAMS'][_child_table]['parent_of']

                    if not grandchild_tables:
                        continue

                    for grandchild_table in grandchild_tables:
                        if grandchild_table not in stand_alone_tables and non_null_column_dict[grandchild_table]:
                            table_column_locations[stand_alone_table].append(grandchild_table)

        return table_column_locations

    def append_columns_to_select_list(alias_table_name: str,
                                      select_column_list: list[str],
                                      table_alias: str = None):
        for select_column in select_column_list:
            select_column_alias = create_sql_alias_with_prefix(table_name=alias_table_name,
                                                               column_name=select_column,
                                                               table_alias=table_alias)
            table_sql_dict[table]['select'].append(select_column_alias)

    logger = logging.getLogger('base_script')

    # this is used to store information for sql query
    table_sql_dict = dict()

    # this mapping and count columns for inserting into table
    mapping_count_columns = get_mapping_and_count_columns(stand_alone_tables)

    # the list of non-null columns by table for this program
    non_null_column_dict = find_program_non_null_columns_by_table(program)

    # where are field groups inserted into the tables?
    table_insert_locations = get_table_column_insert_locations()

    logger.info(f"Creating clinical tables for {program}:")

    for table in stand_alone_tables:
        table_sql_dict[table] = {
            "with": list(),
            "with_join": "",
            "select": list(),
            "from": "",
            "join": dict(),
            "where": ""
        }

        # add first columns to the 'select' sql string
        for column in PARAMS['TABLE_PARAMS'][table]['column_order']['first']:
            table_sql_dict[table]['select'].append(create_sql_alias_with_prefix(table_name=table, column_name=column))

        table_sql_dict[table]['from'] += f"FROM `{create_dev_table_id(PARAMS, table)}` `{table}`"

        # insert mapping columns, if any
        if mapping_count_columns[table]['mapping_columns'] is not None:
            # add mapping id columns to 'select'
            for parent_table in mapping_count_columns[table]['mapping_columns']:
                mapping_table_alias = PARAMS['TABLE_PARAMS'][table]['mapping_table']
                column_select = create_sql_alias_with_prefix(table_name=parent_table,
                                                             column_name=f"{parent_table}_id",
                                                             table_alias=mapping_table_alias)
                table_sql_dict[table]['select'].append(column_select)

                # add mapping table to 'join'
                mapping_table_id = create_dev_table_id(PARAMS, PARAMS['TABLE_PARAMS'][table]['mapping_table'])

                if mapping_table_id not in table_sql_dict[table]['join']:
                    table_sql_dict[table]['join'][mapping_table_id] = {
                        'join_type': 'LEFT',
                        'left_key': f'{table}_id',
                        'right_key': f'{table}_id',
                        'table_alias': mapping_table_alias,
                        'map_table_alias': table
                    }

        # insert count columns, if any
        if mapping_count_columns[table]['count_columns'] is not None:
            for child_table in mapping_count_columns[table]['count_columns']:
                # current table: diagnosis
                # count table: treatment

                table_id_key = f"{table}_id"
                count_id_key = f"{child_table}_id"
                count_prefix = PARAMS['TABLE_PARAMS'][child_table]['prefix']
                count_mapping_table = PARAMS['TABLE_PARAMS'][child_table]['mapping_table']

                with_sql = f"{child_table}_counts AS (" \
                           f"SELECT {table_id_key}, COUNT({count_id_key}) AS {count_prefix}__count " \
                           f"FROM `{create_dev_table_id(PARAMS, count_mapping_table)}` " \
                           f"GROUP BY {table_id_key} " \
                           f") "

                with_join_sql = f"LEFT JOIN {child_table}_counts " \
                                f"ON `{table}`.{table_id_key} = `{child_table}_counts`.{table_id_key}\n"

                table_sql_dict[table]['with'].append(with_sql)
                table_sql_dict[table]['with_join'] += with_join_sql
                table_sql_dict[table]['select'].append(f"{child_table}_counts.{count_prefix}__count")

        middle_columns = PARAMS['TABLE_PARAMS'][table]['column_order']['middle']

        # add filtered middle columns from base table to 'select'
        for column in middle_columns:
            if column in non_null_column_dict[table]:
                column_alias = create_sql_alias_with_prefix(table_name=table, column_name=column)
                table_sql_dict[table]['select'].append(column_alias)

        # add filtered columns from other field groups, using non_null_column_dict and table_insert_locations
        # get list of tables with columns to insert into this table
        additional_tables_to_include = table_insert_locations[table]

        if additional_tables_to_include:
            for add_on_table in additional_tables_to_include:
                add_on_mapping_table_base_name = PARAMS['TABLE_PARAMS'][add_on_table]['mapping_table']
                add_on_mapping_table_id = create_dev_table_id(PARAMS, table_name=add_on_mapping_table_base_name)
                add_on_table_id = create_dev_table_id(PARAMS, table_name=add_on_table)

                # add aliased columns to 'select' list
                filtered_column_list = non_null_column_dict[add_on_table]

                for column in filtered_column_list:
                    column_alias = create_sql_alias_with_prefix(table_name=add_on_table, column_name=column)
                    table_sql_dict[table]['select'].append(column_alias)

                # add mapping table to join dict
                if add_on_mapping_table_id not in table_sql_dict[table]['join']:
                    table_sql_dict[table]['join'][add_on_mapping_table_id] = {
                        'join_type': 'LEFT',
                        'left_key': f'{table}_id',
                        'right_key': f'{table}_id',
                        'table_alias': add_on_mapping_table_base_name,
                        'map_table_alias': table
                    }

                # add data table to join dict
                if add_on_table_id not in table_sql_dict[table]['join']:
                    table_sql_dict[table]['join'][add_on_table_id] = {
                        'join_type': 'LEFT',
                        'left_key': f'{add_on_table}_id',
                        'right_key': f'{add_on_table}_id',
                        'table_alias': add_on_table,
                        'map_table_alias': add_on_mapping_table_base_name
                    }

        last_columns = PARAMS['TABLE_PARAMS'][table]['column_order']['last']

        # add filtered last columns from base table to 'select'
        if last_columns:
            for column in last_columns:
                if column in non_null_column_dict[table]:
                    column_alias = create_sql_alias_with_prefix(table_name=table, column_name=column)
                    table_sql_dict[table]['select'].append(column_alias)

        # generate sql query
        sql_query = make_sql_statement_from_dict()

        if program in PARAMS['ALTER_PROGRAM_NAMES']:
            program_name = PARAMS['ALTER_PROGRAM_NAMES'][program]
        else:
            program_name = program

        clinical_table_name = f"{PARAMS['TABLE_PARAMS'][table]['table_name']}_{program_name}_{PARAMS['RELEASE']}"
        clinical_table_id = f"{PARAMS['DEV_PROJECT']}.{PARAMS['DEV_CLINICAL_DATASET']}.{clinical_table_name}"

        create_table_from_query(PARAMS, table_id=clinical_table_id, query=sql_query)


def create_sql_alias_with_prefix(table_name: str, column_name: str, table_alias: str = None) -> str:
    """
    Create column alias string using table prefix and column name. Uses table_name as table alias.
    :param table_name: table where column is originally located
    :param column_name: column name
    :param table_alias: Optional, override the joined table associated with this column (e.g. when using a view)
    :return: "<column_name> AS <table_prefix>__<column_name>
    """
    if table_alias is None:
        table_alias = table_name

    prefix = PARAMS['TABLE_PARAMS'][table_name]['prefix']

    if prefix is None:
        return f"`{table_alias}`.{column_name}"
    else:
        return f"`{table_alias}`.{column_name} AS {prefix}__{column_name}"


def get_mapping_and_count_columns(program_table_set: set[str]) -> dict[str, dict[str, list[Any]]]:
    column_dict = dict()

    for table_name in program_table_set:
        column_dict[table_name] = {
            'mapping_columns': [],
            'count_columns': []
        }

        # fetch children for table
        children = PARAMS['TABLE_PARAMS'][table_name]['parent_of']

        if children is not None:
            # scan through children--do any make supplemental tables?
            # will need to add a count column to show how many records are available for given row.
            for child in children:
                if child in program_table_set:
                    column_dict[table_name]['count_columns'].append(child)
                else:
                    grandchildren = PARAMS['TABLE_PARAMS'][child]['parent_of']

                    if grandchildren is not None:
                        for grandchild in grandchildren:
                            if grandchild in program_table_set:
                                column_dict[table_name]['count_columns'].append(grandchild)

        parent = PARAMS['TABLE_PARAMS'][table_name]['child_of']

        if parent is not None:
            column_dict[table_name]['mapping_columns'].append(parent)

            grandparent = PARAMS['TABLE_PARAMS'][parent]['child_of']
            if grandparent is not None:
                column_dict[table_name]['mapping_columns'].append(grandparent)

    return column_dict


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

    if 'find_missing_fields' in steps:
        pass
        # todo needs to be refactored to work with change to column order lists
        # find_missing_fields()
    if 'find_program_tables' in steps:
        # creates dict of programs and base, supplemental tables to be created
        tables_per_program_dict = find_program_tables(PARAMS['TABLE_PARAMS'])

        for program, stand_alone_tables in tables_per_program_dict.items():
            logger.info(f"{program}: {stand_alone_tables}")

            create_sql_for_program_tables(program, stand_alone_tables)

    end_time = time.time()
    logger.info(f"Script completed in: {format_seconds(end_time - start_time)}")


if __name__ == "__main__":
    main(sys.argv)
