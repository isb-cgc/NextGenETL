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

import csv
import sys

from typing import Union, Optional

from common_etl.support import bq_harness_with_result
from common_etl.utils import download_from_bucket, get_scratch_fp, load_config, has_fatal_error

API_PARAMS = dict()
BQ_PARAMS = dict()
YAML_HEADERS = ('api_params', 'bq_params', 'steps')

ParamsDict = dict[str, Union[str, int, dict, list]]


def retrieve_dataset_columns(version: Optional[str] = None) -> list[list[str]]:
    """
    Retrieve list of columns and tables in a given set, optionally filtering by data version.
    :param str version: version release number to by which to filter
    :rtype: list[list[str]]
    """
    table_column_query = f"""
        SELECT table_name, column_name
        FROM `{BQ_PARAMS['WORKING_PROJECT']}`.{BQ_PARAMS['WORKING_DATASET']}.INFORMATION_SCHEMA.COLUMNS
    """

    table_columns = bq_harness_with_result(sql=table_column_query, do_batch=False, verbose=False)

    if not version:
        return table_columns

    filtered_table_columns = list()

    for column_data in table_columns:
        if column_data[0].startswith(version):
            filtered_table_columns.append(column_data)

    return filtered_table_columns


def get_tables_per_column(table_columns: list[list[str]],
                          multiple_only: bool = False, print_output: bool = False) -> dict[str, list]:
    """
    Get list of tables for each column name (useful for identifying keys).
    :param list[list[str]] table_columns: Table and column names
    :param bool multiple_only: Filter by columns that occur in multiple tables; defaults to False
    :param bool print_output: Print tab-delimited list of columns and table occurrences
    :return: Column names: list of table names
    :rtype: dict[str, list]
    """
    column_dict = dict()

    for column_data in table_columns:
        table_name = column_data[0][8:]
        column_name = column_data[1]

        if column_name not in column_dict:
            column_dict[column_name] = list()

        column_dict[column_name].append(table_name)

    multiple_column_dict = dict()

    for column in sorted(column_dict.keys()):
        if multiple_only:
            if len(column_dict[column]) > 1:
                multiple_column_dict[column] = column_dict[column]

        if print_output:
            print(f"{column}\t{column_dict[column]}")

    if multiple_only:
        return multiple_column_dict
    else:
        return column_dict


def import_current_fields(filename: str, bucket_path: str) -> dict[str, dict[str, list]]:
    """
    Import list of fields in current ISB-CGC workflows from tsv file.
    :param str filename: todo
    :param str bucket_path: Bucket path to current fields file
    :return: Dictionary of fields with endpoint and workflow data.
    :rtype: dict[str, dict[str, list]]
    """
    download_from_bucket(BQ_PARAMS, filename, bucket_path)

    with open(get_scratch_fp(BQ_PARAMS, filename), mode="r") as fields_file:
        tsv_reader = csv.reader(fields_file, delimiter="\t")

        field_dict = dict()

        for row in tsv_reader:
            field_name = row[0]
            field_group = row[1]
            workflow = row[2]

            if field_name not in field_dict:
                field_dict[field_name] = {
                    "endpoint": list(),
                    "workflows": list()
                }

            field_dict[field_name]["endpoint"].append(field_group)
            field_dict[field_name]["workflows"].append(workflow)

        return field_dict


def output_field_column_differences(table_columns: list[list[str]], bucket_path: str, field_file_name: str):
    """
    Find column names not currently found in ISB-CGC workflows.
    :param list[list[str]] table_columns: todo
    :param str bucket_path: Bucket path to current fields file
    :param str field_file_name: Name of tsv file containing current fields
    """
    columns_dict = get_tables_per_column(table_columns)

    field_dict = import_current_fields(filename=field_file_name, bucket_path=bucket_path)

    columns = set(columns_dict.keys())
    fields = set(field_dict.keys())

    columns_not_found = columns - fields
    fields_not_found = fields - columns

    print(f"\nCDA columns not found in current workflows:")
    for column in sorted(columns_not_found):
        print(f"{column}\t{columns_dict[column]}")

    print(f"\nFields not found in CDA table columns:")
    for field in sorted(fields_not_found):
        endpoint = field_dict[field]["endpoint"]
        workflows = field_dict[field]["workflows"]
        print(f"{field}\t{endpoint}\t{workflows}")


def find_columns_not_in_current_workflows(table_columns: list[list[str]], bucket_path: str,
                                          field_file_name: str) -> set[str]:
    """
    Find column names not currently found in ISB-CGC workflows.
    :param dict[str, Union[str, int, dict, list]] BQ_PARAMS: BQ params in YAML config
    :param list[list[str]] table_columns: Table and column names
    :param str bucket_path: Bucket path to current fields file
    :param str field_file_name: Name of tsv file containing current fields
    :return: Set of columns not found in current ISB-CGC workflows
    :rtype: set[str]
    """
    columns_dict = get_tables_per_column(table_columns)
    field_dict = import_current_fields(filename=field_file_name, bucket_path=bucket_path)

    columns = set(columns_dict.keys())
    fields = set(field_dict.keys())

    return columns - fields


def count_non_null_column_values(table_columns: list[list, str]) -> list[tuple[str, str, int, int, float]]:
    """
    Calculate count of non-null values, total rows, and percentage of non-null values for each column.
    :param list[list[str]] table_columns: Table and column names
    :return: List of tuples containing (table name, column name, non-null count, total count, percentage non-null)
    :rtype: list[tuple[str, str, int, int, float]]
    """
    columns_list = list()

    count = 0

    for table_name, column_name in table_columns:
        sql_query = f"""
            SELECT COUNTIF({column_name} IS NOT NULL) as non_null_count, COUNT(*) as total_count
            FROM `{BQ_PARAMS['WORKING_PROJECT']}`.{BQ_PARAMS['WORKING_DATASET']}.{table_name}
        """

        count_result = bq_harness_with_result(sql_query, do_batch=False, verbose=False)

        for count_row in count_result:
            non_null_count = count_row[0]
            total_count = count_row[1]
            percentage = (non_null_count*1.0 / total_count) * 100
            columns_list.append((table_name, column_name, non_null_count, total_count, percentage))
            break

        count += 1

        if count % 100 == 0:
            print(f"Retrieved {count} counts.")

    return columns_list


def append_column_inclusion_status(columns_list: list[tuple[str, str, int, int, float]],
                                   columns_not_found_in_workflow: set[str]
                                   ) -> list[tuple[str, str, int, int, float, bool]]:
    """
    Add column inclusion status (whether column name is currently found in the ISB-CGC workflows) to column list tuples.
    :param list[tuple[str, str, int, int, float]] columns_list: List of tuples containing (table name, column name,
        non-null count, total count, percentage non-null)
    :param set[str] columns_not_found_in_workflow: set of column names not found in ISB-CGC workflows
    :return: List of tuples containing:
        (table name, column name, non-null count, total count, percentage non-null, column inclusion status)
    :rtype: list[tuple[str, str, int, int, float, bool]]
    """
    column_included_list = list()

    for table_name, column_name, non_null_count, total_count, percentage in columns_list:
        included = False if column_name in columns_not_found_in_workflow else True
        column_included_list.append((table_name, column_name, non_null_count, total_count, percentage, included))

    return column_included_list


def output_main_column_analysis(table_columns: list[list[str]], bucket_path: str, field_file_name: str):
    """
    Build tsv print output for analysis spreadsheet, composed of the following columns:
        - table name
        - column name
        - non-null row count
        - total row count
        - percent non-null rows
        - column name included in current workflow?
    :param list[list[str]] table_columns: Table and column names
    :param str bucket_path: Bucket path to current fields file
    :param str field_file_name: Name of tsv file containing current fields
    """
    columns_not_found_in_workflow = find_columns_not_in_current_workflows(table_columns,
                                                                          bucket_path,
                                                                          field_file_name=field_file_name)

    columns_list = count_non_null_column_values(table_columns)

    column_included_list = append_column_inclusion_status(columns_list, columns_not_found_in_workflow)

    print(f"Table\tColumn\tNon-Null Row Count\tTotal Row Count\t% Non-Null Rows\tColumn Found In Current Workflow?")

    for table_name, column_name, non_null_count, total_count, percentage, included in column_included_list:
        print(f"{table_name}\t{column_name}\t{non_null_count}\t{total_count}\t{percentage}\t{included}")


def main(args):
    try:
        global API_PARAMS, BQ_PARAMS
        API_PARAMS, BQ_PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        has_fatal_error(err, ValueError)

    field_file_name = f"{API_PARAMS['DATA_SOURCE']}_current_fields.tsv"

    if 'retrieve_dataset_columns' in steps:
        table_columns = retrieve_dataset_columns(API_PARAMS['RELEASE'])

        column_name_set = set()

        for table_column in table_columns:
            table_name = table_column[0]
            column_name = table_column[1]

            if table_name.endswith('ref') or table_name == f"{API_PARAMS['RELEASE']}_clinical":
                continue

            column_name_set.add(column_name)

        for column_name in sorted(column_name_set):
            print(column_name)

    if 'output_field_column_differences' in steps:
        output_field_column_differences(table_columns, BQ_PARAMS['WORKING_BUCKET_DIR'], field_file_name)

    if 'output_main_column_analysis' in steps:
        output_main_column_analysis(table_columns, BQ_PARAMS['WORKING_BUCKET_DIR'], field_file_name)


if __name__ == "__main__":
    main(sys.argv)
