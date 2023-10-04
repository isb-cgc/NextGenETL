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

        first_columns_set = set()
        middle_columns_set = set()
        last_columns_set = set()
        excluded_columns_set = set()

        # columns should either be listed in column order lists or excluded column list in TABLE_PARAMS
        if PARAMS['TABLE_PARAMS'][table_name]['column_order']['first'] is not None:
            first_columns_set = set(PARAMS['TABLE_PARAMS'][table_name]['column_order']['first'])
        if PARAMS['TABLE_PARAMS'][table_name]['column_order']['middle'] is not None:
            middle_columns_set = set(PARAMS['TABLE_PARAMS'][table_name]['column_order']['middle'])
        if PARAMS['TABLE_PARAMS'][table_name]['column_order']['last'] is not None:
            last_columns_set = set(PARAMS['TABLE_PARAMS'][table_name]['column_order']['last'])
        if PARAMS['TABLE_PARAMS'][table_name]['excluded_columns'] is not None:
            excluded_columns_set = set(PARAMS['TABLE_PARAMS'][table_name]['excluded_columns'])

        # join into one set
        all_columns_set = first_columns_set | middle_columns_set | last_columns_set | excluded_columns_set

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
        # this shouldn't be necessary, as project should always be 1-1 relationship with case_id?
        # if table_name == 'case' or table_name == 'project':
        if table_name == 'case':
            continue

        # create the query and retrieve results
        result = query_and_retrieve_result(sql=make_programs_with_multiple_ids_per_case_sql())

        if result is None:
            logger.error("result is none")

        for program_row in result:
            tables_per_program_dict[program_row[0]].add(table_name)

    return tables_per_program_dict


def create_sql_for_program_tables(program: str, stand_alone_tables: set[str]):
    """
    Create sql query which is used to create GDC clinical tables by analyzing available data as follows:
        - Find non-null columns for each field group, using column lists in TABLE_PARAMS
        - For base clinical and supplemental tables, determine whether mapping or count columns need to be appended.
            - Mapping columns provide id linkages to ancestor tables, if any (e.g. case_id for diagnosis table)
            - Count columns provide number of rows users can expect to find in child supplemental tables
              (e.g. how many diagnosis rows are available for given case_id?)
              These counts are only provided for direct descendants. For instance, if clinical, diagnosis, and
              treatment tables all exist, then clinical displays count for diagnosis rows, and diagnosis displays count
              for treatment rows.
        - Determine where field groups should be appended (if field group is not in its own supplemental table).
          If program only has "clinical" base table, all columns are appended there. However, if supplemental tables
          exist, columns within each field group are appended to their closest ancestor table. For instance, if clinical
          and diagnosis tables exist, treatment columns are appended to diagnosis. If only clinical exists,
          treatment columns are appended to that table.
    Then, construct a dict to store components of SQL query.
    Parse the contents of dict into a SQL query string, and use to create new BQ table.
    :param program: Program for which create tables
    :param stand_alone_tables: list of supplemental tables to create (those which can't be flattened
                               into clinical or parent table)
    """
    def get_mapping_and_count_columns() -> dict[str, dict[str, list[Any]]]:
        column_dict = dict()

        for table_name in stand_alone_tables:
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
                    if child in stand_alone_tables:
                        column_dict[table_name]['count_columns'].append(child)
                    else:
                        grandchildren = PARAMS['TABLE_PARAMS'][child]['parent_of']

                        if grandchildren is not None:
                            for grandchild in grandchildren:
                                if grandchild in stand_alone_tables:
                                    column_dict[table_name]['count_columns'].append(grandchild)

            parent = PARAMS['TABLE_PARAMS'][table_name]['child_of']

            if parent is not None:
                column_dict[table_name]['mapping_columns'].append(parent)

                grandparent = PARAMS['TABLE_PARAMS'][parent]['child_of']
                if grandparent is not None:
                    column_dict[table_name]['mapping_columns'].append(grandparent)

        return column_dict

    def find_program_non_null_columns_by_table():
        def make_count_column_sql() -> str:
            mapping_table = PARAMS['TABLE_PARAMS'][_table]['mapping_table']
            id_key = f"{_table}_id"
            _parent_table = PARAMS['TABLE_PARAMS'][_table]['child_of']

            count_sql_str = ''

            for col in columns:
                count_sql_str += f'\nSUM(CASE WHEN this_table.{col} is null THEN 0 ELSE 1 END) AS {col}_count, '

            # remove extra comma (due to looping) from end of string
            count_sql_str = count_sql_str[:-2]

            if _table == 'case':
                return f"""
                    SELECT {count_sql_str}
                    FROM `{create_dev_table_id(PARAMS, _table)}` this_table
                    JOIN `{create_dev_table_id(PARAMS, 'case_project_program')}` cpp
                        ON this_table.case_id = cpp.case_gdc_id
                    WHERE cpp.program_name = '{program}'
                """
            elif _table == 'project':
                return f"""
                    SELECT {count_sql_str}
                    FROM `{create_dev_table_id(PARAMS, _table)}` this_table
                    JOIN `{create_dev_table_id(PARAMS, 'case_project_program')}` cpp
                        ON this_table.project_id = cpp.project_id
                    WHERE cpp.program_name = '{program}'
                """
            elif _parent_table == 'case':
                return f"""
                SELECT {count_sql_str}
                FROM `{create_dev_table_id(PARAMS, _table)}` this_table
                JOIN `{create_dev_table_id(PARAMS, mapping_table)}` mapping_table
                    ON mapping_table.{id_key} = this_table.{id_key}
                JOIN `{create_dev_table_id(PARAMS, 'case_project_program')}` cpp
                    ON mapping_table.case_id = cpp.case_gdc_id
                WHERE cpp.program_name = '{program}'
                """
            elif _parent_table:
                parent_mapping_table = PARAMS['TABLE_PARAMS'][_parent_table]['mapping_table']
                parent_id_key = f"{_parent_table}_id"

                return f"""
                SELECT {count_sql_str}
                FROM `{create_dev_table_id(PARAMS, _table)}` this_table
                JOIN `{create_dev_table_id(PARAMS, mapping_table)}` mapping_table
                    ON mapping_table.{id_key} = this_table.{id_key}
                JOIN `{create_dev_table_id(PARAMS, _parent_table)}` parent_table
                    ON parent_table.{parent_id_key} = mapping_table.{parent_id_key}
                JOIN `{create_dev_table_id(PARAMS, parent_mapping_table)}` parent_mapping_table
                    ON parent_mapping_table.{parent_id_key} = parent_table.{parent_id_key}
                JOIN `{create_dev_table_id(PARAMS, 'case_project_program')}` cpp
                    ON parent_mapping_table.case_id = cpp.case_gdc_id
                WHERE cpp.program_name = '{program}'
                """

        non_null_columns_dict = dict()

        for _table in PARAMS['TABLE_PARAMS'].keys():
            _first_columns = PARAMS['TABLE_PARAMS'][_table]['column_order']['first']
            _middle_columns = PARAMS['TABLE_PARAMS'][_table]['column_order']['middle']
            _last_columns = PARAMS['TABLE_PARAMS'][_table]['column_order']['last']

            columns = list()

            if _first_columns:
                columns.extend(_first_columns)
            if _middle_columns:
                columns.extend(_middle_columns)
            if _last_columns:
                columns.extend(_last_columns)

            column_count_result = query_and_retrieve_result(sql=make_count_column_sql())

            non_null_columns = list()

            for row in column_count_result:
                # get columns for field group
                for _column in columns:
                    count = row.get(f"{_column}_count")

                    if count is not None and count > 0:
                        non_null_columns.append(_column)

            non_null_columns_dict[_table] = non_null_columns

        return non_null_columns_dict

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

    def append_columns_to_select_list(column_list: list[str],
                                      src_table: str,
                                      table_alias: str = None,
                                      check_filtered: bool = True):
        """
        Create column alias (using source prefix) and append to 'select' list for table_sql_dict[src_table].
        :param column_list: list of columns to append to 'select' list
        :param src_table: table from which the columns originated
        :param table_alias: Optional; alternate column source (used for count columns created by sql 'with' clause)
        :param check_filtered: Optional; if true, only appends columns with non-null values
                               (stored in non_null_column_dict)
        """
        if not column_list:
            return

        for col in column_list:
            # don't add column to select list if it has null values for every row
            if check_filtered and col not in non_null_column_dict[src_table]:
                continue

            if table_alias is None:
                table_alias = src_table

            col_alias = f"`{table_alias}`.{col}"

            prefix = PARAMS['TABLE_PARAMS'][src_table]['prefix']

            if prefix:
                col_alias += f" AS {prefix}__{col}"

            table_sql_dict[table]['select'].append(col_alias)

    # noinspection PyTypeChecker
    def make_sql_statement_from_dict() -> str:
        """
        Create SQL query string using dict of clauses.
        :return: SQL string used to create clinical or clinical supplemental table
        """
        # create 'with' clause string
        with_clause_str = ''
        if table_sql_dict[table]['with']:
            with_sql_list = list()

            for with_statement_dict in table_sql_dict[table]['with']:
                left_table_alias = with_statement_dict['left_table_alias']
                right_table_alias = with_statement_dict['right_table_alias']
                count_column_prefix = with_statement_dict['count_column_prefix']

                count_mapping_table_name = PARAMS['TABLE_PARAMS'][right_table_alias]['mapping_table']
                count_mapping_table_id = create_dev_table_id(params=PARAMS, table_name=count_mapping_table_name)

                with_sql_str = f"{right_table_alias}_counts AS (" \
                               f"SELECT {left_table_alias}_id, COUNT({right_table_alias}_id) " \
                               f"   AS {count_column_prefix}__count " \
                               f"FROM `{count_mapping_table_id}` " \
                               f"GROUP BY {left_table_alias}_id " \
                               f") "

                with_sql_list.append(with_sql_str)

            with_clause_str = "WITH "
            with_clause_str += ", ".join(with_sql_list)
            with_clause_str += '\n'

        if not table_sql_dict[table]['select']:
            logger.critical("No columns found for 'SELECT' clause.")
            sys.exit(-1)

        select_clause_str = "SELECT "
        select_clause_str += ", ".join(table_sql_dict[table]['select'])
        select_clause_str += "\n"

        if not table_sql_dict[table]['from_table']:
            logger.critical("No columns found for 'FROM' clause.")
            sys.exit(-1)

        from_table_alias = table
        from_table_id = create_dev_table_id(PARAMS, from_table_alias)

        from_clause_str = f"FROM `{from_table_id}` `{from_table_alias}`\n"

        join_clause_str = ""
        if table_sql_dict[table]['join']:
            for table_id in table_sql_dict[table]['join'].keys():
                join_type = table_sql_dict[table]['join'][table_id]['join_type']
                join_key = table_sql_dict[table]['join'][table_id]['join_key']
                left_table_alias = table_sql_dict[table]['join'][table_id]['left_table_alias']
                right_table_alias = table_sql_dict[table]['join'][table_id]['right_table_alias']

                join_clause_str += f"{join_type} JOIN `{table_id}` `{right_table_alias}`\n" \
                                   f"   ON `{right_table_alias}`.{join_key} = `{left_table_alias}`.{join_key}\n"

        map_table_alias = PARAMS['TABLE_PARAMS'][table]['mapping_table']

        # filter by program
        where_clause_str = f"WHERE `{map_table_alias}`.case_id in (\n" \
                           f"   SELECT case_gdc_id\n" \
                           f"   FROM `{create_dev_table_id(PARAMS, 'case_project_program')}`\n" \
                           f"   WHERE program_name = '{program}'\n" \
                           f")\n"

        sql_str = with_clause_str + select_clause_str + from_clause_str + join_clause_str + where_clause_str
        # logger.debug(f"SQL for {table}:\n{sql_str}")

        return sql_str

    def make_join_clause_dict(key_name: str, left_table_alias: str, right_table_alias: str) -> dict[str, str]:
        """
        Create join clause statement dict.
        :param key_name: has '_id' appended, and is then used as join key
        :param left_table_alias: left table alias
        :param right_table_alias: right table alias
        :return: join clause dict object
        """
        return {
            'join_type': 'LEFT',
            'join_key': f'{key_name}_id',
            'left_table_alias': left_table_alias,
            'right_table_alias': right_table_alias
        }

    logger = logging.getLogger('base_script')

    # used to store information for sql query
    table_sql_dict = dict()

    # dict of mapping and count columns for all of this program's tables
    mapping_count_columns = get_mapping_and_count_columns()

    # dict of this program's non-null columns, by table
    non_null_column_dict = find_program_non_null_columns_by_table()

    # dict specifying into which table to insert every non-null field group that doesn't get its own supplemental table
    table_insert_locations = get_table_column_insert_locations()
    logger.info(f"Creating clinical tables for {program}:")
    logger.debug()
    logger.debug(table_insert_locations)

    for table in stand_alone_tables:
        # used to construct a sql query that creates one of the program tables
        table_sql_dict[table] = {
            "with": list(),
            "select": list(),
            "join": dict(),
            "where": ""
        }

        # add first columns to the 'select' sql string
        first_columns = PARAMS['TABLE_PARAMS'][table]['column_order']['first']
        append_columns_to_select_list(column_list=first_columns, src_table=table)

        # insert mapping columns, if any
        if mapping_count_columns[table]['mapping_columns'] is not None:
            # add mapping id columns to 'select'
            for parent_table in mapping_count_columns[table]['mapping_columns']:
                append_columns_to_select_list(column_list=[f"{parent_table}_id"],
                                              src_table=parent_table,
                                              table_alias=PARAMS['TABLE_PARAMS'][table]['mapping_table'],
                                              check_filtered=False)

                # add mapping table to 'join'
                mapping_table_id = create_dev_table_id(PARAMS, PARAMS['TABLE_PARAMS'][table]['mapping_table'])

                if mapping_table_id not in table_sql_dict[table]['join']:
                    table_sql_dict[table]['join'][mapping_table_id] = make_join_clause_dict(
                        key_name=table,
                        left_table_alias=table,
                        right_table_alias=PARAMS['TABLE_PARAMS'][table]['mapping_table']
                    )

        # insert count columns, if any
        if mapping_count_columns[table]['count_columns'] is not None:
            for child_table in mapping_count_columns[table]['count_columns']:
                count_prefix = PARAMS['TABLE_PARAMS'][child_table]['prefix']

                # used to create "with clause" in make_sql_statement_from_dict()
                with_dict = {
                    'left_table_alias': table,
                    'right_table_alias': child_table,
                    'count_column_prefix': PARAMS['TABLE_PARAMS'][child_table]['prefix']
                }

                table_sql_dict[table]['with'].append(with_dict)

                with_clause_alias = f"{child_table}_counts"

                # join 'with' clause result in main query
                table_sql_dict[table]['join'][with_clause_alias] = make_join_clause_dict(
                    key_name=table,
                    left_table_alias=table,
                    right_table_alias=with_clause_alias
                )

                # add count column to select list
                table_sql_dict[table]['select'].append(f"{child_table}_counts.{count_prefix}__count")

        # add filtered middle columns from base table to 'select'
        middle_columns = PARAMS['TABLE_PARAMS'][table]['column_order']['middle']
        append_columns_to_select_list(column_list=middle_columns, src_table=table)

        # if clinical base table, append project name and id here, just before
        if table == 'case':
            project_columns = PARAMS['TABLE_PARAMS']['project']['column_order']['first']
            append_columns_to_select_list(column_list=project_columns, src_table='project')

        # add filtered columns from other field groups, using non_null_column_dict and table_insert_locations
        # get list of tables with columns to insert into this table
        additional_tables_to_include = table_insert_locations[table]

        if additional_tables_to_include:
            for add_on_table in additional_tables_to_include:
                # add aliased columns to 'select' list
                append_columns_to_select_list(column_list=non_null_column_dict[add_on_table],
                                              src_table=add_on_table,
                                              check_filtered=False)  # already filtered, no need to check again

                add_on_mapping_table_base_name = PARAMS['TABLE_PARAMS'][add_on_table]['mapping_table']
                add_on_mapping_table_id = create_dev_table_id(PARAMS, table_name=add_on_mapping_table_base_name)
                add_on_table_id = create_dev_table_id(PARAMS, table_name=add_on_table)

                # add mapping table to join dict
                if add_on_mapping_table_id not in table_sql_dict[table]['join']:
                    join_clause_dict = make_join_clause_dict(key_name=table,
                                                             left_table_alias=table,
                                                             right_table_alias=add_on_mapping_table_base_name)
                    table_sql_dict[table]['join'][add_on_mapping_table_id] = join_clause_dict

                # add data table to join dict
                if add_on_table_id not in table_sql_dict[table]['join']:
                    join_clause_dict = make_join_clause_dict(key_name=add_on_table,
                                                             left_table_alias=add_on_mapping_table_base_name,
                                                             right_table_alias=add_on_table)
                    table_sql_dict[table]['join'][add_on_table_id] = join_clause_dict

        # add filtered last columns from base table to 'select'
        last_columns = PARAMS['TABLE_PARAMS'][table]['column_order']['last']
        append_columns_to_select_list(column_list=last_columns, src_table=table)

        # generate sql query
        sql_query = make_sql_statement_from_dict()

        # get altered program name, in case where program name differs in table id due to length or punctuation
        # e.g. BEATAML1.0 -> BEATAML1_0, EXCEPTIONAL_RESPONDERS -> EXC_RESPONDERS
        program_name = PARAMS['ALTER_PROGRAM_NAMES'][program] if program in PARAMS['ALTER_PROGRAM_NAMES'] else program

        clinical_table_name = f"{PARAMS['TABLE_PARAMS'][table]['table_name']}_{program_name}_{PARAMS['RELEASE']}"
        clinical_table_id = f"{PARAMS['DEV_PROJECT']}.{PARAMS['DEV_CLINICAL_DATASET']}.{clinical_table_name}"

        create_table_from_query(PARAMS, table_id=clinical_table_id, query=sql_query)


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
        # todo uncomment
        # find_missing_fields()
    if 'find_program_tables' in steps:
        # creates dict of programs and base, supplemental tables to be created
        tables_per_program_dict = find_program_tables(PARAMS['TABLE_PARAMS'])

        for program, stand_alone_tables in tables_per_program_dict.items():
            logger.debug(f"{program}: {stand_alone_tables}")

            create_sql_for_program_tables(program, stand_alone_tables)

    end_time = time.time()
    logger.info(f"Script completed in: {format_seconds(end_time - start_time)}")


if __name__ == "__main__":
    main(sys.argv)
