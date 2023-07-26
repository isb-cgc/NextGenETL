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

from BQ_Table_Building.CDA_old.tests.shared_functions import compare_row_counts, compare_id_keys, compare_table_columns
from common_etl.utils import load_config, has_fatal_error

API_PARAMS = dict()
BQ_PARAMS = dict()
YAML_HEADERS = ('api_params', 'bq_params', 'steps')


def main(args):
    try:
        global API_PARAMS, BQ_PARAMS
        API_PARAMS, BQ_PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        has_fatal_error(err, ValueError)

    print("Comparing row counts!\n")

    compare_row_counts(old_table_id=BQ_PARAMS['OLD_TABLE_ID'],
                       new_table_id=BQ_PARAMS['NEW_TABLE_ID'])

    print("Comparing table keys!\n")

    compare_id_keys(old_table_id=BQ_PARAMS['OLD_TABLE_ID'],
                    new_table_id=BQ_PARAMS['NEW_TABLE_ID'],
                    primary_key=BQ_PARAMS['PRIMARY_KEY'])

    columns_list = BQ_PARAMS["COLUMNS"]

    print("Comparing table columns!\n")

    compare_table_columns(old_table_id=BQ_PARAMS['OLD_TABLE_ID'],
                          new_table_id=BQ_PARAMS['NEW_TABLE_ID'],
                          primary_key=BQ_PARAMS['PRIMARY_KEY'],
                          columns=columns_list)


if __name__ == "__main__":
    main(sys.argv)
