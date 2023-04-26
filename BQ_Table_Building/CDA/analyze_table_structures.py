import csv
import sys

from typing import Union, Optional

from common_etl.support import bq_harness_with_result
from common_etl.utils import download_from_bucket, get_scratch_fp

ParamsDict = dict[str, Union[str, int, dict, list]]


def retrieve_dataset_columns(bq_params: ParamsDict, version: Optional[str] = None) -> list[list[str]]:
    """
    Retrieve list of columns and tables in a given set, optionally filtering by data version.
    :param dict[str, Union[str, int, dict, list]] bq_params: BQ params from YAML config
    :param str version: Optional, version prefix by which to filter
    :return: List of table and column names
    :rtype: list[list[str]]
    """
    table_column_query = f"""
        SELECT table_name, column_name
        FROM `{bq_params['WORKING_PROJECT']}`.{bq_params['WORKING_DATASET']}.INFORMATION_SCHEMA.COLUMNS
    """

    table_columns = bq_harness_with_result(sql=table_column_query, do_batch=False, verbose=False)

    if not version:
        return table_columns

    filtered_table_columns = list()

    for column_data in table_columns:
        table_version = column_data[0][:7]

        if table_version == version:
            filtered_table_columns.append(column_data)

    return filtered_table_columns


def get_tables_per_column(table_columns: list[list[str]], multiple_only: bool = False,
                          print_output: bool = False) -> dict[str, list]:
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


def import_current_fields(bq_params: ParamsDict, filename: str, bucket_path: str) -> dict[str, dict[str, list]]:
    """
    Import list of fields in current ISB-CGC workflows from tsv file.
    :param dict[str, Union[str, int, dict, list]] bq_params: BQ params from YAML config
    :param str filename: Name of tsv file containing current fields
    :param str bucket_path: Bucket path to current fields file
    :return: Dictionary of fields with endpoint and workflow data.
    :rtype: dict[str, dict[str, list]]
    """
    download_from_bucket(bq_params, filename, bucket_path)

    with open(get_scratch_fp(bq_params, filename), mode="r") as fields_file:
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


def output_field_column_differences(bq_params: ParamsDict, table_columns: list[list[str]], bucket_path: str,
                                    field_file_name: str):
    """
    Find column names not currently found in ISB-CGC workflows.
    :param dict[str, Union[str, int, dict, list]] bq_params: BQ params in YAML config
    :param list[list[str]] table_columns: Table and column names
    :param str bucket_path: Bucket path to current fields file
    :param str field_file_name: Name of tsv file containing current fields
    """
    columns_dict = get_tables_per_column(table_columns)

    field_dict = import_current_fields(bq_params, filename=field_file_name, bucket_path=bucket_path)

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


def find_columns_not_in_current_workflows(bq_params: ParamsDict, table_columns: list[list[str]], bucket_path: str,
                                          field_file_name: str) -> set[str]:
    """
    Find column names not currently found in ISB-CGC workflows.
    :param dict[str, Union[str, int, dict, list]] bq_params: BQ params in YAML config
    :param list[list[str]] table_columns: Table and column names
    :param str bucket_path: Bucket path to current fields file
    :param str field_file_name: Name of tsv file containing current fields
    :return: Set of columns not found in current ISB-CGC workflows
    :rtype: set[str]
    """
    columns_dict = get_tables_per_column(table_columns)
    field_dict = import_current_fields(bq_params, filename=field_file_name, bucket_path=bucket_path)

    columns = set(columns_dict.keys())
    fields = set(field_dict.keys())

    return columns - fields


def count_non_null_column_values(bq_params: ParamsDict,
                                 table_columns: list[list, str]) -> list[tuple[str, str, int, int, float]]:
    """
    Calculate count of non-null values, total rows, and percentage of non-null values for each column.
    :param dict[str, Union[str, int, dict, list]] bq_params: BQ params in YAML config
    :param list[list[str]] table_columns: Table and column names
    :return: List of tuples containing (table name, column name, non-null count, total count, percentage non-null)
    :rtype: list[tuple[str, str, int, int, float]]
    """
    columns_list = list()

    count = 0

    for table_name, column_name in table_columns:
        sql_query = f"""
            SELECT COUNTIF({column_name} IS NOT NULL) as non_null_count, COUNT(*) as total_count
            FROM `{bq_params['WORKING_PROJECT']}`.{bq_params['WORKING_DATASET']}.{table_name}
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


def output_main_column_analysis(bq_params: ParamsDict, table_columns: list[list[str]], bucket_path: str,
                                field_file_name: str):
    """
    Build tsv print output for analysis spreadsheet, composed of the following columns:
        - table name
        - column name
        - non-null row count
        - total row count
        - percent non-null rows
        - column name included in current workflow?
    :param dict[str, Union[str, int, dict, list]] bq_params: BQ params in YAML config
    :param list[list[str]] table_columns: Table and column names
    :param str bucket_path: Bucket path to current fields file
    :param str field_file_name: Name of tsv file containing current fields
    """
    columns_not_found_in_workflow = find_columns_not_in_current_workflows(bq_params,
                                                                          table_columns,
                                                                          bucket_path,
                                                                          field_file_name=field_file_name)

    columns_list = count_non_null_column_values(bq_params, table_columns)

    column_included_list = append_column_inclusion_status(columns_list, columns_not_found_in_workflow)

    print(f"Table\tColumn\tNon-Null Row Count\tTotal Row Count\t% Non-Null Rows\tColumn Found In Current Workflow?")

    for table_name, column_name, non_null_count, total_count, percentage, included in column_included_list:
        print(f"{table_name}\t{column_name}\t{non_null_count}\t{total_count}\t{percentage}\t{included}")


def main(args):
    data_source = "pdc"

    bq_params = {
        "SCRATCH_DIR": "scratch",
        "WORKING_BUCKET": "next-gen-etl-scratch",
        "WORKING_PROJECT": "isb-project-zero",
        "WORKING_DATASET": f"cda_{data_source}_test"
    }
    version = '2023_03'
    bucket_path = 'law/etl/analysis_files'
    field_file_name = f'{data_source}_current_fields.tsv'

    table_columns = retrieve_dataset_columns(bq_params, version)

    output_field_column_differences(bq_params, table_columns, bucket_path, field_file_name)

    # output_main_column_analysis(bq_params, table_columns, bucket_path, field_file_name)


if __name__ == "__main__":
    main(sys.argv)
