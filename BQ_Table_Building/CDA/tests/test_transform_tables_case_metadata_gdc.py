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
from typing import Union

from google.cloud.bigquery.table import RowIterator, _EmptyRowIterator

from common_etl.utils import load_config, has_fatal_error

API_PARAMS = dict()
BQ_PARAMS = dict()
YAML_HEADERS = ('api_params', 'bq_params', 'steps')

BQHarnessResult = Union[None, RowIterator, _EmptyRowIterator]


def make_compare_table_column_sql(old_table_id: str, new_table_id: str) -> str:
    return f"""
    (
        SELECT *
        FROM `{old_table_id}`
        EXCEPT DISTINCT 
        SELECT *
        FROM `{new_table_id}`
    )

    UNION ALL

    (
        SELECT *
        FROM `{new_table_id}`
        EXCEPT DISTINCT 
        SELECT *
        FROM `{old_table_id}`
    )
    """


def main(args):
    try:
        global API_PARAMS, BQ_PARAMS
        API_PARAMS, BQ_PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        has_fatal_error(err, ValueError)

    old_table_id = 'isb-cgc-bq.GDC_case_file_metadata_versioned.caseData_r37'
    new_table_id = 'isb-project-zero.cda_gdc_test.case_metadata_2023_03'

    comparison_sql = make_compare_table_column_sql(old_table_id, new_table_id)


if __name__ == "__main__":
    main(sys.argv)
