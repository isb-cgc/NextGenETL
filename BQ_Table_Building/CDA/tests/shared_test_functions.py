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

from typing import Union

from google.cloud.bigquery.table import RowIterator

from cda_bq_etl.bq_helpers import query_and_retrieve_result
from common_etl.utils import has_fatal_error

BQQueryResult = Union[None, RowIterator]


def compare_id_keys(left_table_id: str, right_table_id: str, primary_key: str, max_result_count: int = 5):
    """
    Compare primary id keys across tables. Missing keys guarantee a data mismatch,
    but don't guarantee that records do align exactly.
    :param left_table_id: left table id
    :param right_table_id: right table id
    :param primary_key: primary key used for this table
    :param max_result_count: max results to display in log; defaults to 5
    """
    def make_compare_id_keys_sql(table_id_1, table_id_2) -> str:
        return f"""
        SELECT {primary_key} 
        FROM {table_id_1}
        WHERE {primary_key} NOT IN (
          SELECT {primary_key} 
          FROM {table_id_2})
        """

    def compare_table_keys(table_1, table_2):
        result = query_and_retrieve_result(sql=make_compare_id_keys_sql(table_1, table_2))

        if not result:
            has_fatal_error(f"Primary key {primary_key} not found in one or both compared tables")
        elif result.total_rows == 0:
            print(f"\nNo missing values for key: {primary_key} in table {table_2}. Great!")
        else:
            print(f"\n{table_1} has {result.total_rows} {primary_key} values which don't exist in {table_2}.")
            print(f"\nExample values:")

            count = 0

            for row in result:
                print(f"{row[0]}")
                count += 1
                if count == max_result_count:
                    break

    # find primary keys in left table that are missing from right table
    compare_table_keys(left_table_id, right_table_id)

    # find primary keys in right table that are missing from left table
    compare_table_keys(right_table_id, left_table_id)
    print()


def compare_row_counts(left_table_id: str, right_table_id: str):
    """
    Compare total row count between tables.
    :param left_table_id: left table id
    :param right_table_id: right table id
    """
    def make_row_count_sql(table_id: str) -> str:
        return f"""
            SELECT COUNT(*) as row_count
            FROM {table_id}
        """

    left_table_row_count_query = make_row_count_sql(left_table_id)
    right_table_row_count_query = make_row_count_sql(right_table_id)

    left_count_result = query_and_retrieve_result(sql=left_table_row_count_query)
    right_count_result = query_and_retrieve_result(sql=right_table_row_count_query)

    left_count = right_count = 0

    if left_count_result.total_rows == 0 or left_count_result.total_rows > 1:
        has_fatal_error(f"Incorrect row count returned for {left_table_id}: {left_count_result.total_rows}")
    elif right_count_result.total_rows == 0 or right_count_result.total_rows > 1:
        has_fatal_error(f"Incorrect row count  returned for {right_table_id}: {right_count_result.total_rows}")

    for row in left_count_result:
        left_count = row[0]

    for row in right_count_result:
        right_count = row[0]

    if left_count == right_count:
        print(f"Same row count between old and new tables: {left_count}")
    else:
        print(f"Mismatched row counts.\n"
              f"Old table row count: {left_count}.\n"
              f"New table row count: {right_count}.\n"
              f"Note: if this is a GDC table which contains merged legacy data, "
              f"this mismatch could be due to dropping records with no DCF file associations.\n")


def compare_table_columns(left_table_id: str,
                          right_table_id: str,
                          column_list: list[str],
                          primary_key: str,
                          secondary_key: str = None,
                          max_display_rows: int = 5):
    """
    Compare left table to right table on a per-column basis.
    Results returned represent data missing from the right table but found in the left.
    :param left_table_id: left table id
    :param right_table_id: right table id
    :param primary_key: primary key, used to match rows across tables
    :param column_list: list of columns to compare across tables (non-concatenated only, as order is not guaranteed)
    :param secondary_key: optional; secondary key used to map data
    :param max_display_rows: maximum result rows to display in output
    """
    def make_compare_table_column_sql(column_name, table_id_1, table_id_2) -> str:
        secondary_key_sql_string = ''

        if secondary_key is not None:
            secondary_key_sql_string = f"{secondary_key},"

        # outputs values from left table that are not found in right table
        return f"""
            SELECT {secondary_key_sql_string} {primary_key}, {column_name}
            FROM `{table_id_1}`
            EXCEPT DISTINCT 
            SELECT {secondary_key_sql_string} {primary_key}, {column_name}
            FROM `{table_id_2}`
            ORDER BY {primary_key}
        """

    def compare_table_column_left_table(table_id_1, table_id_2):
        column_comparison_query = make_compare_table_column_sql(column, table_id_1, table_id_2)

        result = query_and_retrieve_result(sql=column_comparison_query)

        if not result:
            print(f"\nNo results returned. This can mean that there's a column data type mismatch, "
                  f"or that the column name differs.\n")
        elif result.total_rows > 0:
            print(f"\n{result.total_rows} values in {table_id_1} didn't match value found in {table_id_2}.\n")
            print(f"Example values:\n")

            if secondary_key is not None:
                print(f"{primary_key:40} {secondary_key:40} {column}")
            else:
                print(f"{primary_key:40} {column}")

            count = 0

            for row in result:
                primary_key_value = row.get(primary_key)
                column_value = row.get(column)

                if secondary_key is not None:
                    secondary_key_value = row.get(secondary_key)

                    print(f"{primary_key_value:40} {secondary_key_value:40} {column_value}")
                else:
                    print(f"{primary_key_value:40} {column_value}")

                count += 1
                if count == max_display_rows:
                    print()
                    break
        else:
            print(f"No missing values found in {table_id_2}!")

    for column in column_list:
        print(f"\n* For {column}: *")
        compare_table_column_left_table(left_table_id, right_table_id)
        compare_table_column_left_table(right_table_id, left_table_id)
        print()


def compare_concat_columns(left_table_id: str,
                           right_table_id: str,
                           concat_column_list: list[str],
                           primary_key: str,
                           secondary_key: str = None):
    """
    Compare concatenated column values to ensure matching data, as order is not guaranteed in these column strings.
    :param left_table_id: left table id
    :param right_table_id: right table id
    :param concat_column_list: list of columns containing concatenated strings (associated entities, for example)
    :param primary_key: primary key, used to match rows across tables
    :param secondary_key: optional; secondary key used to map data
    """
    def make_concat_column_query(table_id: str) -> str:
        secondary_key_string = ''

        if secondary_key is not None:
            secondary_key_string = f"{secondary_key},"

        concat_columns_str = ", ".join(concat_column_list)

        return f"""
            SELECT {secondary_key_string} {primary_key}, {concat_columns_str}  
            FROM `{table_id}`
        """

    def make_records_dict(query: str) -> dict[str, dict[str, str]]:
        result = query_and_retrieve_result(sql=query)

        records_dict = dict()

        for record_count, record in enumerate(result):
            primary_key_id = record.get(primary_key)
            records_dict_key = primary_key_id

            if secondary_key is not None:
                records_dict_key += f";{record.get(secondary_key)}"

            record_dict = dict()

            for _column in concat_column_list:
                record_dict[_column] = record.get(_column)

            records_dict[records_dict_key] = record_dict

            if record_count % 100000 == 0:
                print(f"{record_count}/{result.total_rows} records added to dict!")

        return records_dict

    left_table_records_dict = make_records_dict(query=make_concat_column_query(left_table_id))
    print("Created dict for left table records!")

    right_table_records_dict = make_records_dict(query=make_concat_column_query(right_table_id))
    print("Created dict for right table records!")

    record_key_set = set(left_table_records_dict.keys())
    record_key_set.update(right_table_records_dict.keys())

    for count, record_id in enumerate(record_key_set):
        correct_records_count = 0

        for column in concat_column_list:
            if record_id not in left_table_records_dict:
                print(f"{record_id} not found in left table.")
                break
            elif record_id not in right_table_records_dict:
                print(f"{record_id} not found in right table.")
                break

            left_column_value = left_table_records_dict[record_id][column]
            right_column_value = right_table_records_dict[record_id][column]

            if left_column_value and right_column_value:
                left_column_value_list = left_column_value.split(';')
                right_column_value_list = right_column_value.split(';')

                left_column_value_set = set(left_column_value_list)
                right_column_value_set = set(right_column_value_list)

                if len(left_column_value_list) != len(right_column_value_list):
                    # if length mismatch, there may be duplicates, so definitely not identical;
                    # set eliminates duplicates, so this is necessary
                    print(f'id {record_id} value mismatch for {column}.')
                    print(f'left column values: {left_column_value} right column values: {right_column_value}\n')
                elif len(left_column_value_set ^ right_column_value_set) > 0:
                    # exclusive or -- values only in exactly one set
                    print(f'id {record_id} value mismatch for {column}.')
                    print(f'left column values: {left_column_value} right column values: {right_column_value}\n')
            elif left_column_value != right_column_value:
                # case in which one or both values are None--if both aren't none, that's a mismatch
                print(f'id {record_id} value mismatch for {column}.')
                print(f'old column values: {left_column_value} new column values: {right_column_value}')
            else:
                correct_records_count += 1

        total_record_count = len(record_key_set)

        if count % 100000 == 0:
            print(f"{count}/{total_record_count} records evaluated")
        elif count == len(record_key_set):
            print(f"Record evaluation complete, {total_record_count} records evaluated")
