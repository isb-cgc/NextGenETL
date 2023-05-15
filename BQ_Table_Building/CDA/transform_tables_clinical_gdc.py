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


def find_project_supplemental_tables(field_groups_dict: dict[str, dict[str, str]]):
    def make_projects_with_multiple_ids_per_case_sql() -> str:
        parent_field_group = table_vocabulary_dict['first_level_field_group']

        # only has a value if this field group is a child of another (e.g. diagnoses.treatments)
        child_field_group = table_vocabulary_dict['second_level_field_group']

        # mapping tables variously use "of", "from", or "has" for joining names
        table_join_word = table_vocabulary_dict['table_join_word']

        if child_field_group:
            return f"""
                WITH projects AS (
                    SELECT DISTINCT project_id
                    FROM `isb-project-zero.cda_gdc_test.2023_03_{child_field_group}_{table_join_word}_{parent_field_group}` 
                    JOIN `isb-project-zero.cda_gdc_test.2023_03_{parent_field_group}_of_case` child_case
                        USING ({parent_field_group}_id)
                    JOIN `isb-project-zero.cda_gdc_test.2023_03_case_in_project` case_proj
                        ON child_case.case_id = case_proj.case_id
                    GROUP BY {parent_field_group}_id, project_id
                    HAVING COUNT({parent_field_group}_id) > 1
                )

                SELECT DISTINCT SPLIT(project_id, "-")[0] AS project_short_name
                FROM projects
            """
        else:
            return f"""
                WITH projects AS (
                    SELECT DISTINCT project_id
                    FROM `isb-project-zero.cda_gdc_test.2023_03_{parent_field_group}_of_case` child_case
                    JOIN `isb-project-zero.cda_gdc_test.2023_03_case_in_project` case_proj
                        ON child_case.case_id = case_proj.case_id
                    GROUP BY child_case.case_id, project_id
                    HAVING COUNT(child_case.case_id) > 1
                )

                SELECT DISTINCT SPLIT(project_id, "-")[0] AS project_short_name
                FROM projects
            """

    field_groups_and_projects_with_supplemental_tables = dict()

    for field_group_name, table_vocabulary_dict in field_groups_dict.items():
        # create the query and retrieve results
        projects = bq_harness_with_result(sql=make_projects_with_multiple_ids_per_case_sql(),
                                          do_batch=False,
                                          verbose=False)

        project_set = set()

        if projects is not None:
            for project in projects:
                project_set.add(project[0])

            # if result is non-null, add to projects_with_supplemental_tables
            if len(project_set) > 0:
                field_groups_and_projects_with_supplemental_tables[field_group_name] = project_set

    for field_group, projects in field_groups_and_projects_with_supplemental_tables.items():
        print(f"{field_group}: {projects}")


def main(args):
    field_groups_dict = {
        "demographic": {
            "first_level_field_group": "demographic",
            "second_level_field_group": "",
            "table_join_word": "",
        },
        "diagnoses": {
            "first_level_field_group": "diagnosis",
            "second_level_field_group": "",
            "table_join_word": "",
        },
        "diagnoses.annotations": {
            "first_level_field_group": "diagnosis",
            "second_level_field_group": "annotation",
            "table_join_word": "has",
        },
        "diagnoses.pathology_details": {
            "first_level_field_group": "diagnosis",
            "second_level_field_group": "pathology_details",
            "table_join_word": "of",
        },
        "diagnoses.treatments": {
            "first_level_field_group": "diagnosis",
            "second_level_field_group": "treatment",
            "table_join_word": "of",
        },
        "exposures": {
            "first_level_field_group": "exposure",
            "second_level_field_group": "",
            "table_join_word": "",
        },
        "family_histories": {
            "first_level_field_group": "family_history",
            "second_level_field_group": "",
            "table_join_word": "",
        },
        "follow_ups": {
            "first_level_field_group": "follow_up",
            "second_level_field_group": "",
            "table_join_word": "",
        },
        "follow_ups.molecular_tests": {
            "first_level_field_group": "follow_up",
            "second_level_field_group": "molecular_test",
            "table_join_word": "from",
        },
    }

    find_project_supplemental_tables(field_groups_dict)


if __name__ == "__main__":
    main(sys.argv)