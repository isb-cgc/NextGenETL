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

from BQ_Table_Building.CDA.tests.shared_test_functions import compare_row_counts, compare_id_keys, \
    compare_table_columns, compare_concat_columns
from cda_bq_etl.utils import load_config, has_fatal_error

PARAMS = dict()
YAML_HEADERS = ('params', 'steps')


def main(args):
    try:
        global PARAMS
        PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        has_fatal_error(err, ValueError)

    if 'LEFT_TABLE_ID' not in PARAMS:
        has_fatal_error("LEFT_TABLE_ID missing from yaml config.")
    if 'RIGHT_TABLE_ID' not in PARAMS:
        has_fatal_error("LEFT_TABLE_ID missing from yaml config.")
    if 'PRIMARY_KEY' not in PARAMS:
        has_fatal_error("PRIMARY_KEY missing from yaml config.")
    if 'COLUMN_LIST' not in PARAMS:
        has_fatal_error("COLUMN_LIST missing from yaml config.")

    if 'compare_row_counts' in steps:
        print("Comparing row counts!\n")
        compare_row_counts(left_table_id=PARAMS['LEFT_TABLE_ID'],
                           right_table_id=PARAMS['RIGHT_TABLE_ID'])
    if 'compare_table_keys' in steps:
        print("Comparing table keys!")
        compare_id_keys(left_table_id=PARAMS['LEFT_TABLE_ID'],
                        right_table_id=PARAMS['RIGHT_TABLE_ID'],
                        primary_key=PARAMS['PRIMARY_KEY'])
    if 'compare_table_columns' in steps:
        print("Comparing table columns!\n")
        secondary_key = PARAMS['SECONDARY_KEY'] if "SECONDARY_KEY" in PARAMS else None

        compare_table_columns(left_table_id=PARAMS['LEFT_TABLE_ID'],
                              right_table_id=PARAMS['RIGHT_TABLE_ID'],
                              column_list=PARAMS["COLUMN_LIST"],
                              primary_key=PARAMS["PRIMARY_KEY"],
                              secondary_key=secondary_key)

        if "CONCAT_COLUMN_LIST" in PARAMS:
            compare_concat_columns(left_table_id=PARAMS['LEFT_TABLE_ID'],
                                   right_table_id=PARAMS['RIGHT_TABLE_ID'],
                                   concat_column_list=PARAMS["CONCAT_COLUMN_LIST"],
                                   primary_key=PARAMS["PRIMARY_KEY"],
                                   secondary_key=secondary_key)
        else:
            print("No concat column list defined, not evaluating that. If this is unintentional, "
                  "please define CONCAT_COLUMN_LIST in yaml config.")


if __name__ == "__main__":
    main(sys.argv)
