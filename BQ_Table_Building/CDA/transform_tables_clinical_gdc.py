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

    clinical_table_program_mappings = dict()

    # Create program set for base clinical tables -- will include every program with clinical cases
    base_programs = bq_harness_with_result(sql=make_programs_with_cases_sql(), do_batch=False, verbose=False)

    base_program_set = set()

    if base_programs is not None:
        for base_program in base_programs:
            base_program_set.add(base_program[0])

        clinical_table_program_mappings['clinical'] = base_program_set

    # Create set of programs for each mapping table type,
    # required when a single case has multiple rows for a given field group (e.g. multiple diagnoses or follow-ups)
    for field_group_name, table_vocabulary_dict in field_groups_dict.items():
        # create the query and retrieve results
        programs = bq_harness_with_result(sql=make_programs_with_multiple_ids_per_case_sql(),
                                          do_batch=False,
                                          verbose=False)

        program_set = set()

        if programs is not None:
            for program in programs:
                program_set.add(program[0])

            # if result is non-null, add to clinical_table_program_mappings
            if len(program_set) > 0:
                clinical_table_program_mappings[field_group_name] = program_set

    return clinical_table_program_mappings


def main(args):

    try:
        global API_PARAMS, BQ_PARAMS
        API_PARAMS, BQ_PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        has_fatal_error(err, ValueError)

    print(API_PARAMS)

    clinical_table_program_mappings = find_program_tables(API_PARAMS['TSV_FIELD_GROUP_CONFIG'])

    for field_group, programs in clinical_table_program_mappings.items():
        print(f"{field_group}: {sorted(programs)}")

    # steps:
    # Create mappings for column names in CDA and ISB-CGC tables -- make yaml API params file for this, it's too big
    # Add field groups dict to yaml
    # Retrieve case ids by program
    # Determine which tables need to be created for each program -- single clinical table, or additional mapping tables?


if __name__ == "__main__":
    main(sys.argv)
