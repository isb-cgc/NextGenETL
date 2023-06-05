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

from common_etl.support import bq_harness_with_result
from common_etl.utils import load_config, has_fatal_error

API_PARAMS = dict()
BQ_PARAMS = dict()
YAML_HEADERS = ('api_params', 'bq_params', 'steps')


def find_program_tables(field_groups_dict: dict[str, dict[str, str]]) -> dict[str, set[str]]:
    def make_programs_with_cases_sql() -> str:
        # Retrieving programs from this view rather than from the programs table to avoid pulling programs with no
        # clinical case associations, which has happened in the past
        return f"""
        SELECT DISTINCT program_name
        FROM `isb-project-zero.cda_gdc_test.2023_03_case_project_program`

        """

    def make_programs_with_multiple_ids_per_case_sql() -> str:
        parent_field_group = table_vocabulary_dict['first_level_field_group']

        # only has a value if this field group is a child of another (e.g. diagnoses.treatments)
        child_field_group = table_vocabulary_dict['second_level_field_group']

        # mapping tables variously use "of", "from", or "has" for joining names
        table_join_word = table_vocabulary_dict['table_join_word']

        if child_field_group:
            return f"""
                WITH programs AS (
                    SELECT DISTINCT case_proj.project_id
                    FROM `isb-project-zero.cda_gdc_test.2023_03_{child_field_group}_{table_join_word}_{parent_field_group}` parent
                    JOIN `isb-project-zero.cda_gdc_test.2023_03_{parent_field_group}_of_case` child_case
                        USING ({parent_field_group}_id)
                    JOIN `isb-project-zero.cda_gdc_test.2023_03_case_in_project` case_proj
                        ON child_case.case_id = case_proj.case_id
                    GROUP BY parent.{parent_field_group}_id, case_proj.project_id
                    HAVING COUNT(parent.{parent_field_group}_id) > 1
                )

                SELECT DISTINCT SPLIT(project_id, "-")[0] AS project_short_name
                FROM programs
            """
        else:
            return f"""
                WITH programs AS (
                    SELECT DISTINCT case_proj.project_id
                    FROM `isb-project-zero.cda_gdc_test.2023_03_{parent_field_group}_of_case` child_case
                    JOIN `isb-project-zero.cda_gdc_test.2023_03_case_in_project` case_proj
                        ON child_case.case_id = case_proj.case_id
                    GROUP BY child_case.case_id, case_proj.project_id
                    HAVING COUNT(child_case.case_id) > 1
                )

                SELECT DISTINCT SPLIT(project_id, "-")[0] AS project_short_name
                FROM programs
            """

    tables_per_program_dict = dict()

    # Create program set for base clinical tables -- will include every program with clinical cases
    base_programs = bq_harness_with_result(sql=make_programs_with_cases_sql(), do_batch=False, verbose=False)

    if base_programs is not None:
        for base_program in base_programs:
            tables_per_program_dict[base_program[0]] = {'clinical'}

    # Create set of programs for each mapping table type,
    # required when a single case has multiple rows for a given field group (e.g. multiple diagnoses or follow-ups)
    for field_group_name, table_vocabulary_dict in field_groups_dict.items():
        # create the query and retrieve results
        programs = bq_harness_with_result(sql=make_programs_with_multiple_ids_per_case_sql(),
                                          do_batch=False,
                                          verbose=False)

        if programs is not None:
            for program in programs:
                tables_per_program_dict[program[0]].add(field_group_name)

    return tables_per_program_dict


def find_null_columns_by_program(program, field_group):
    def make_count_column_sql() -> str:
        columns = API_PARAMS['FIELD_CONFIG'][field_group]['column_order']
        mapping_table = API_PARAMS['FIELD_CONFIG'][field_group]['mapping_table']
        id_key = API_PARAMS['FIELD_CONFIG'][field_group]['id_key']
        parent_table = API_PARAMS['FIELD_CONFIG'][field_group]['child_of']

        count_sql_str = ''

        for column in columns:
            count_sql_str += f'\nSUM(CASE WHEN child_table.{column} is null THEN 0 ELSE 1 END) AS {column}_count, '

        # remove extra comma (due to looping) from end of string
        count_sql_str = count_sql_str[:-2]

        if parent_table == 'case':
            return f"""
            SELECT {count_sql_str}
            FROM `isb-project-zero.cda_gdc_test.2023_03_{field_group}` child_table
            JOIN `isb-project-zero.cda_gdc_test.2023_03_{mapping_table}` mapping_table
                ON mapping_table.{id_key} = child_table.{id_key}
            JOIN `isb-project-zero.cda_gdc_test.2023_03_case_project_program` cpp
                ON mapping_table.case_id = cpp.case_gdc_id
            WHERE cpp.program_name = '{program}'
            """
        elif parent_table:
            parent_mapping_table = API_PARAMS['FIELD_CONFIG'][parent_table]['mapping_table']
            parent_id_key = API_PARAMS['FIELD_CONFIG'][parent_table]['id_key']

            return f"""
            SELECT {count_sql_str}
            FROM `isb-project-zero.cda_gdc_test.2023_03_{field_group}` child_table
            JOIN `isb-project-zero.cda_gdc_test.2023_03_{mapping_table}` mapping_table
                ON mapping_table.{id_key} = child_table.{id_key}
            JOIN `isb-project-zero.cda_gdc_test.2023_03_{parent_table}` parent_table
                ON parent_table.{parent_id_key} = mapping_table.{parent_id_key}
            JOIN `isb-project-zero.cda_gdc_test.2023_03_{parent_mapping_table}` parent_mapping_table
                ON parent_mapping_table.{parent_id_key} = parent_table.{parent_id_key}
            JOIN `isb-project-zero.cda_gdc_test.2023_03_case_project_program` cpp
                ON parent_mapping_table.case_id = cpp.case_gdc_id
            WHERE cpp.program_name = '{program}'
            """
        else:
            pass
            # handle project and case field groups

    column_count_result = bq_harness_with_result(sql=make_count_column_sql(), do_batch=False, verbose=False)

    non_null_columns = list()

    for row in column_count_result:
        # get columns for field group
        for column in API_PARAMS['FIELD_CONFIG'][field_group]['column_order']:
            count = row.get(f"{column}_count")

            if count is not None and count > 0:
                non_null_columns.append(column)

    return non_null_columns


def create_base_clinical_table_for_program():
    pass
    # get list of mapping tables to flatten into clinical -- exclude those in tables_per_program_dict
    # create query that contains all the columns for each included field group
    # don't include columns that are null for every case within the given program


def main(args):

    try:
        global API_PARAMS, BQ_PARAMS
        API_PARAMS, BQ_PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        has_fatal_error(err, ValueError)

    if 'find_program_tables' in steps:

        tables_per_program_dict = find_program_tables(API_PARAMS['TSV_FIELD_GROUP_CONFIG'])

        # for program, tables in tables_per_program_dict.items():
        #     print(f"{program}: {tables}")

    programs = ['APOLLO', 'BEATAML1.0', 'CDDP_EAGLE', 'CGCI', 'CMI', 'CPTAC', 'CTSP', 'EXCEPTIONAL_RESPONDERS', 'FM',
                'GENIE', 'HCMI', 'MATCH', 'MMRF', 'MP2PRT', 'NCICCR', 'OHSU', 'ORGANOID', 'REBC', 'TARGET', 'TCGA',
                'TRIO', 'VAREPOP', 'WCDT']
    field_groups = ['demographic', 'diagnosis', 'annotation', 'treatment', 'pathology_detail', 'exposure',
                    'family_history', 'follow_up', 'molecular_test']

    # NOTE: counts returned may be null if program has no values within a table, e.g. TCGA has no annotation records

    all_program_columns = dict()

    for program in programs:
        print(f"finding columns for {program}!")
        program_columns = dict()

        for field_group in field_groups:
            non_null_columns = find_null_columns_by_program(program=program, field_group=field_group)
            if len(non_null_columns) > 0:
                program_columns[field_group] = non_null_columns

        all_program_columns[program] = program_columns

    print("\n*** Non-null columns, by program\n")
    for program, column_groups in all_program_columns.items():
        print(f"\n{program}\n")
        for field_group, columns in column_groups:
            print(f"{field_group}: {columns}")

    # steps:
    # Retrieve case ids by program
    # Determine which tables need to be created for each program -- single clinical table, or additional mapping tables?


if __name__ == "__main__":
    main(sys.argv)
