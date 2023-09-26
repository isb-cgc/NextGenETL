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


def find_program_tables(table_dict: dict[str, dict[str, str]]) -> dict[str, set[str]]:
    def make_programs_with_multiple_ids_per_case_sql() -> str:
        if table_metadata['child_of'] is not None:
            child_parent_map_table_id = create_dev_table_id(PARAMS, f"{table_metadata['mapping_table']}")
            parent_case_map_table_id = create_dev_table_id(PARAMS,
                                                           f"{table_metadata['child_of']}_of_case")

            return f"""
                WITH programs AS (
                    SELECT DISTINCT case_proj.project_id
                    FROM `{child_parent_map_table_id}` child_parent
                    JOIN `{parent_case_map_table_id}` parent_case
                        USING ({table_metadata['child_of']}_id)
                    JOIN `{create_dev_table_id(PARAMS, 'case_in_project')}` case_proj
                        ON parent_case.case_id = case_proj.case_id
                    GROUP BY child_parent.{table_metadata['child_of']}_id, case_proj.project_id
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
        tables_per_program_dict[base_program] = {'case'}

    # Create set of programs for each mapping table type,
    # required when a single case has multiple rows for a given field group (e.g. multiple diagnoses or follow-ups)
    for table_name, table_metadata in table_dict.items():
        logger.info(table_name)
        if table_name == 'case' or table_name == 'project':
            continue

        # create the query and retrieve results
        result = query_and_retrieve_result(sql=make_programs_with_multiple_ids_per_case_sql())

        if result is None:
            logger.error("result is none")

        for program_row in result:
            # change certain program names (currently EXCEPTIONAL_RESPONDERS and BEATAML1.0)
            if program_row[0] in PARAMS['ALTER_PROGRAM_NAMES'].keys():
                program_name = PARAMS['ALTER_PROGRAM_NAMES'][program_row[0]]
            else:
                program_name = program_row[0]

            tables_per_program_dict[program_name].add(table_name)

    return tables_per_program_dict


def find_non_null_columns_by_program(program, field_group):
    def make_count_column_sql() -> str:
        columns = PARAMS['TABLE_PARAMS'][field_group]['column_order']
        mapping_table = PARAMS['TABLE_PARAMS'][field_group]['mapping_table']
        id_key = f"{field_group}_id"
        parent_table = PARAMS['TABLE_PARAMS'][field_group]['child_of']

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
            parent_mapping_table = PARAMS['TABLE_PARAMS'][parent_table]['mapping_table']
            parent_id_key = f"{parent_table}_id"

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
        for column in PARAMS['TABLE_PARAMS'][field_group]['column_order']:
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


def create_sql_for_program_tables():
    """
    store sql strings in dictionary?
    yes, in two parts.
    table_sql_dict = {
        "clinical": {
            "select": "SELECT ...",
            "from": "FROM ..."
            # here we have to add counts for any child associated tables (not grandchildren)
        },
        "diagnosis": {
            # here we have to add the associated case_id and counts for any child associated tables
        },
        "treatment": {
            # here we have to add the associated diagnosis id and case id
        },
        ...
    }

    I could build the basic tables, then later insert the associated child row counts?
    How are we going to know which tables require which counts.
    What if we build the basic scaffolding as a data structure...
    {
        clinical: {
            diagnosis: {
                treatment: {}
                pathology_detail: {}
            },
            follow_up: {
                molecular_test: {}
            }
        }
    }

    so traversing this forward would tell us that we'd need counts for any keys at a given level.
    traversing back out tells us which mapping keys we need to include in the table.
    case: {
        mapping_keys: {},
        count_keys: {diagnosis, follow_up}
    },
    diagnosis: {
        mapping_keys: {case},
        count_keys: {treatment, pathology_detail}
    },
    treatment: {
        mapping_keys: {case, diagnosis}.
        count_keys: {}
    }

    We can get the key counts from the child join tables. Create WITH statements that we can then use as lookup tables.
    Sometimes these won't be children, they can be grandchildren. clinical can have diag__treat__count, for instance.

    e.g.
    '''
    WITH treatment_counts AS (
        SELECT diagnosis.diagnosis_id, COUNT(treatment_diagnosis.treatment_id) AS treatment_count
        FROM `isb-project-zero.cda_gdc_raw.2023_09_diagnosis` diagnosis
        LEFT JOIN `isb-project-zero.cda_gdc_raw.2023_09_treatment_of_diagnosis` treatment_diagnosis
            USING(diagnosis_id)
        GROUP BY diagnosis_id
    )
    '''

    First we add the primary key to the select statement.
  
    select = "SELECT treatment_id AS diag__treat__treatment_id"

    create_sql_alias_with_prefix() will create the string for the column. It adds the table alias and modifies the
    column name to include a prefix if needed.

    select = f"SELECT {create_sql_alias_with_prefix("treatment", "treatment_id")}"
    from = f"FROM {create_dev_table_id(PARAMS, "treatment")}\n"

    parent_table = TABLE_PARAMS['treatment']['child_of']
    parent_column_alias = create_sql_alias_with_prefix(parent_table, f"{parent_table}_id")}

    select += f", {parent_column_alias}"

    mapping_table = TABLE_PARAMS['treatment']['mapping_table']
    from += f"{create_dev_table_id(PARAMS, mapping_table)} mapping_table"

    Then, if there are any mapping keys, we add them in order of ancestry.
    We also add join to allow for those associations.




    """


def create_sql_alias_with_prefix(table_name: str, column_name: str, table_alias: str = None) -> str:
    """
    Create column alias string using table prefix and column name. Uses table_name as table alias.
    :param table_name: table where column is originally located
    :param column_name: column name
    :param table_alias: Optional, override the joined table associated with this column (e.g. when using a view)
    :return: "<column_name> AS <table_prefix>__<column_name>
    """
    prefix = PARAMS['TABLE_PARAMS'][table_name]['prefix']

    if prefix is not None:
        aliased_column_name = f"{prefix}__{column_name}"

        if table_alias:
            return f"{table_alias}.{column_name} AS {aliased_column_name}"
        else:
            return f"{table_name}.{column_name} AS {aliased_column_name}"
    else:
        if table_alias:
            return f"{table_alias}.{column_name}"
        else:
            return f"{table_name}.{column_name}"


def get_mapping_and_count_columns(program_table_set: set[str]) -> dict[str, list[str]]:
    column_dict = dict()

    for table_name in program_table_set:
        column_dict[table_name] = {
            'mapping_keys': [],
            'count_keys': []
        }

        # fetch children for table
        children = PARAMS['TABLE_PARAMS'][table_name]['parent_of']

        if children is not None:
            # scan through children--do any make supplemental tables?
            # will need to add a count column to show how many records are available for given row.
            for child in children:
                if child in program_table_set:
                    column_dict[table_name]['count_keys'].append(child)
                else:
                    grandchildren = PARAMS['TABLE_PARAMS'][child]['parent_of']

                    if grandchildren is not None:
                        for grandchild in grandchildren:
                            if grandchild in program_table_set:
                                column_dict[table_name]['count_keys'].append(grandchild)

        parent = PARAMS['TABLE_PARAMS'][table_name]['child_of']

        if parent is not None:
            column_dict[table_name]['mapping_keys'].append(parent)

            grandparent = PARAMS['TABLE_PARAMS'][parent]['child_of']
            if grandparent is not None:
                column_dict[table_name]['mapping_keys'].append(grandparent)

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

    # todo add some logging text for this in cases where it doesn't find any issues
    find_missing_fields()

    if 'find_program_tables' in steps:
        # creates dict of programs and base, supplemental tables to be created
        tables_per_program_dict = find_program_tables(PARAMS['TABLE_PARAMS'])

        for program, tables in tables_per_program_dict.items():
            logger.info(f"{program}: {tables}")

            column_dict = get_mapping_and_count_columns(tables)

            for table in column_dict.keys():
                logger.info(f"{table}: {column_dict[table]}")

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
    # use the TABLE_INSERT_ORDER to order fields by FG and to account for last_keys_in_table
    # use these queries to build the clinical tables

    end_time = time.time()
    logger.info(f"Script completed in: {format_seconds(end_time - start_time)}")


if __name__ == "__main__":
    main(sys.argv)
