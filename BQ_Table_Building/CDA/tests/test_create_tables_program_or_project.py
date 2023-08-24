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

from google.cloud import bigquery

from BQ_Table_Building.CDA.tests.shared_test_functions import compare_row_counts, compare_id_keys, compare_table_columns
from cda_bq_etl.utils import load_config, has_fatal_error
from cda_bq_etl.bq_helpers import get_project_or_program_list

PARAMS = dict()
YAML_HEADERS = ('params', 'steps')


def main(args):
    try:
        global PARAMS
        PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        has_fatal_error(err, ValueError)

    client = bigquery.Client()

    table_id_tuple_set = set()

    program_set = get_project_or_program_list(PARAMS)

    for program_name in sorted(program_set):
        old_table_name = f"{program_name}_{PARAMS['TABLE_BASE_NAME']}_{PARAMS['DC_SOURCE']}_{PARAMS['DC_RELEASE']}"
        old_table_id = f"{PARAMS['WORKING_PROJECT']}.{PARAMS['OLD_WORKING_DATASET']}.{old_table_name}"

        new_table_name = f"{PARAMS['TABLE_BASE_NAME']}_{program_name}_{PARAMS['RELEASE']}"
        new_table_id = f"{PARAMS['WORKING_PROJECT']}.{PARAMS['TARGET_DATASET']}.{new_table_name}"

        # check for valid hg38 table location
        old_table = client.get_table(table=old_table_id)
        new_table = client.get_table(table=new_table_id)

        if old_table is None or new_table is None:
            if old_table is None:
                print(f"No table found: {old_table_id}")
            if new_table is None:
                print(f"No table found: {new_table_id}")
        else:
            table_id_tuple = (program_name, old_table_id, new_table_id)
            table_id_tuple_set.add(table_id_tuple)

    for table_id_tuple in sorted(table_id_tuple_set):
        program = table_id_tuple[0]
        old_table_id = table_id_tuple[1]
        new_table_id = table_id_tuple[2]

        print("\n\n**********")
        print(f"For program: {program}")
        print("**********\n")

        if 'compare_row_counts' in steps:
            print("\n** Comparing row counts! **\n")

            compare_row_counts(left_table_id=old_table_id,
                               right_table_id=new_table_id)

        if 'compare_table_keys' in steps:
            print("\n** Comparing primary table keys! **")

            compare_id_keys(left_table_id=old_table_id,
                            right_table_id=new_table_id,
                            primary_key=PARAMS['PRIMARY_KEY'])

        if 'compare_table_columns' in steps:
            print("\n** Comparing table columns! **\n")

            if "SECONDARY_KEY" in PARAMS and PARAMS['SECONDARY_KEY'] is not None:
                compare_table_columns(left_table_id=old_table_id,
                                      right_table_id=new_table_id,
                                      column_list=PARAMS["COLUMN_LIST"],
                                      primary_key=PARAMS["PRIMARY_KEY"],
                                      secondary_key=PARAMS['SECONDARY_KEY'])
            else:
                compare_table_columns(left_table_id=old_table_id,
                                      right_table_id=new_table_id,
                                      column_list=PARAMS["COLUMN_LIST"],
                                      primary_key=PARAMS["PRIMARY_KEY"])


if __name__ == "__main__":
    main(sys.argv)
