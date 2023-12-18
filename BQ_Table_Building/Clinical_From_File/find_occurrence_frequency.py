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
import logging
import sys
import time
import csv

from typing import Union

from cda_bq_etl.data_helpers import initialize_logging
from cda_bq_etl.gcs_helpers import upload_to_bucket
from cda_bq_etl.utils import load_config, format_seconds, get_scratch_fp
from cda_bq_etl.bq_helpers import query_and_retrieve_result


PARAMS = dict()
YAML_HEADERS = ('params', 'steps')

ParamsDict = dict[str, Union[str, int, dict, list]]


def retrieve_dataset_columns(version: str, program: str) -> list[list[str]]:
    """
    Retrieve list of columns and tables in a given set, optionally filtering by data version.
    :param program: todo
    :param str version: version release number to by which to filter
    :rtype: list[list[str]]
    """
    logger = logging.getLogger("base_script")

    table_column_query = f"""
        SELECT table_name, column_name
        FROM `{PARAMS['DEV_PROJECT']}.{PARAMS['DEV_RAW_DATASET']}`.INFORMATION_SCHEMA.COLUMNS
    """

    table_columns = query_and_retrieve_result(sql=table_column_query)

    filtered_table_columns = list()

    count = 0

    for column_data in table_columns:
        version_program = f"{version}_{program}"
        if column_data[0].startswith(version_program):
            filtered_table_columns.append(column_data)
        count += 1

        if count % 100 == 0:
            logger.info(f"Retrieved {count} columns.")

    return filtered_table_columns


def count_non_null_column_values(table_columns: list[list, str]) -> list[tuple[str, str, int, int, float]]:
    """
    Calculate count of non-null values, total rows, and percentage of non-null values for each column.
    :param list[list[str]] table_columns: Table and column names
    :return: List of tuples containing (table name, column name, non-null count, total count, percentage non-null)
    :rtype: list[tuple[str, str, int, int, float]]
    """
    logger = logging.getLogger("base_script")

    columns_list = list()

    count = 0

    for table_name, column_name in table_columns:
        sql_query = f"""
            SELECT COUNTIF({column_name} IS NOT NULL) as non_null_count, COUNT(*) as total_count
            FROM `{PARAMS['DEV_PROJECT']}`.{PARAMS['DEV_RAW_DATASET']}.{table_name}
        """

        count_result = query_and_retrieve_result(sql_query)

        for count_row in count_result:
            non_null_count = count_row[0]
            total_count = count_row[1]
            percentage = (non_null_count*1.0 / total_count) * 100
            columns_list.append((table_name, column_name, non_null_count, total_count, percentage))
            break

        count += 1

        if count % 100 == 0:
            logger.info(f"Retrieved {count} column statistics.")

    return columns_list


def write_frequencies_to_tsv(table_columns: list[list[str]]):
    """
    Build tsv print output for analysis spreadsheet, composed of the following columns:
        - table name
        - column name
        - non-null row count
        - total row count
        - percent non-null rows
        - column name included in current workflow?
    :param list[list[str]] table_columns: Table and column names
    """
    logger = logging.getLogger("base_script")

    columns_list = count_non_null_column_values(table_columns)

    local_program_dir = get_scratch_fp(PARAMS, PARAMS['PROGRAM'])
    tsv_path = f"{local_program_dir}/{PARAMS['TSV_FILE']}"

    with open(tsv_path, "w") as tsv_fh:
        # write header
        tsv_fh.write(f"Table\tColumn\tNon-Null Row Count\tTotal Row Count\t% Non-Null Rows\n")

        count = 0

        for table_name, column_name, non_null_count, total_count, percentage in columns_list:
            tsv_fh.write(f"{table_name}\t{column_name}\t{non_null_count}\t{total_count}\t{percentage}\n")

            count += 1

            if count % 100 == 0:
                logger.info(f"Wrote {count} lines to tsv.")

    upload_to_bucket(PARAMS, scratch_fp=tsv_path)


def main(args):
    try:
        start_time = time.time()

        global PARAMS
        PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        sys.exit(err)

    log_file_time = time.strftime('%Y.%m.%d-%H.%M.%S', time.localtime())
    log_filepath = f"{PARAMS['LOGFILE_PATH']}.{log_file_time}"
    logger = initialize_logging(log_filepath)

    if 'find_occurrence_frequency' in steps:
        table_columns = retrieve_dataset_columns(PARAMS['RELEASE'], PARAMS['PROGRAM'])

        column_name_set = set()

        for table_column in table_columns:
            table_name = table_column[0]
            column_name = table_column[1]

            if table_name.endswith('ref') or table_name == f"{PARAMS['RELEASE']}_clinical":
                continue

            column_name_set.add(column_name)

        write_frequencies_to_tsv(table_columns)

    if 'output_distinct_values' in steps:
        table_columns = retrieve_dataset_columns(PARAMS['RELEASE'], PARAMS['PROGRAM'])




    end_time = time.time()

    logger.info(f"Script completed in: {format_seconds(end_time - start_time)}")


if __name__ == "__main__":
    main(sys.argv)
