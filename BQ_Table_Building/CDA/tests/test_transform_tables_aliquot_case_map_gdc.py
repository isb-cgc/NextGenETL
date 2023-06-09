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


def compare_table_columns(old_table_id: str, new_table_id: str, primary_key: str, columns: list[str]):
    def make_compare_table_column_sql(column_name) -> str:
        return f"""
        (
            SELECT {primary_key}, {column_name}
            FROM `{old_table_id}`
            EXCEPT DISTINCT 
            SELECT {primary_key}, {column_name}
            FROM `{new_table_id}`
        )

        UNION ALL

        (
            SELECT {primary_key}, {column_name}
            FROM `{new_table_id}`
            EXCEPT DISTINCT 
            SELECT {primary_key}, {column_name}
            FROM `{old_table_id}`
        )
        """

    for column in columns:
        column_comparison_query = make_compare_table_column_sql(column)

        result = bq_harness_with_result(sql=column_comparison_query, do_batch=False, verbose=False)

        if result.total_rows > 0:
            print(f"\nFound mismatched data for {column}.")
            print(f"{result.total_rows} total records do not match in old and new tables.\n")
        else:
            print(f"{column} column matches in published and new tables!")


def main(args):
    try:
        global API_PARAMS, BQ_PARAMS
        API_PARAMS, BQ_PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        has_fatal_error(err, ValueError)

    columns_list = ['program_name',
                    'project_id',
                    'case_barcode',
                    'sample_gdc_id',
                    'sample_barcode',
                    'sample_type',
                    'sample_type_name',
                    'sample_is_ffpe',
                    'sample_preservation_method',
                    'portion_gdc_id',
                    'portion_barcode',
                    'analyte_gdc_id',
                    'analyte_barcode',
                    'aliquot_gdc_id',
                    'aliquot_barcode']

    compare_table_columns(old_table_id=BQ_PARAMS['OLD_TABLE_ID'],
                          new_table_id=BQ_PARAMS['NEW_TABLE_ID'],
                          primary_key=BQ_PARAMS['PRIMARY_KEY'],
                          columns=columns_list)


if __name__ == "__main__":
    main(sys.argv)
