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

from typing import Any, Union

from google.cloud.bigquery.table import RowIterator

from common_etl.support import bq_harness_with_result

JSONList = list[dict[str, Union[None, str, float, int, bool]]]


def convert_bq_result_to_object_list(result: RowIterator, column_list: list[str]) -> JSONList:
    object_list = list()

    count = 0

    for row in result:
        object_dict = dict()

        for column in column_list:
            object_dict[column] = row.get(column)

        object_list.append(object_dict)
        count += 1
        if count % 50000 == 0:
            print(f"{count} rows added to object.")

    return object_list


def create_program_name_set(api_params, bq_params):
    def make_program_name_set_query():
        return f"""
        SELECT DISTINCT program_name
        FROM `{bq_params['WORKING_PROJECT']}.{bq_params['WORKING_DATASET']}.{api_params['RELEASE']}_case_project_program`
        """

    result = bq_harness_with_result(sql=make_program_name_set_query(), do_batch=False, verbose=False)

    program_name_set = set()

    for row in result:
        program_name_set.add(row[0])

    return program_name_set
