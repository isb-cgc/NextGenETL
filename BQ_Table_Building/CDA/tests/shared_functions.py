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


def compare_id_keys(old_table_id: str, new_table_id: str, primary_key: str):
    def make_compare_id_keys_sql(table_id_1, table_id_2) -> str:
        return f"""
        SELECT {primary_key} 
        FROM {table_id_1}
        WHERE {primary_key} NOT IN (
          SELECT {primary_key} 
          FROM {table_id_2})
        """

    def compare_table_keys(table_id_1, table_id_2):
        result = query_and_retrieve_result(sql=make_compare_id_keys_sql(table_id_1, table_id_2))

        if not result:
            has_fatal_error(f"Primary key {primary_key} not found in one or both compared tables")
        elif result.total_rows == 0:
            print(f"\nNo missing values for key: {primary_key}. Great!")
        else:
            print(f"\n{table_id_1} has "
                  f"{result.total_rows} {primary_key} values which don't exist in "
                  f"{table_id_2}.")
            print(f"\nExample values:")

            count = 0

            for row in result:
                print(f"{row[0]}")
                count += 1
                if count == 5:
                    break

    # find primary keys in new table that are missing from old table
    compare_table_keys(new_table_id, old_table_id)

    # find primary keys in old table that are missing from new table
    compare_table_keys(old_table_id, new_table_id)


def compare_row_counts(old_table_id: str, new_table_id: str):
    def make_row_count_sql(table_id: str) -> str:
        return f"""
        SELECT COUNT(*) as row_count
        FROM {table_id}
        """

    old_table_row_count_query = make_row_count_sql(old_table_id)
    new_table_row_count_query = make_row_count_sql(new_table_id)

    old_count_result = query_and_retrieve_result(sql=old_table_row_count_query)
    new_count_result = query_and_retrieve_result(sql=new_table_row_count_query)

    for row in old_count_result:
        old_count = row[0]
        break

    for row in new_count_result:
        new_count = row[0]
        break

    if old_count == new_count:
        print(f"Same row count between old and new tables: {old_count}")
    else:
        print(f"Mismatched row counts.\n"
              f"Old table row count: {old_count}.\n"
              f"New table row count: {new_count}.\n"
              f"Note: if this table contains merged legacy data, "
              "this could be due to dropping records with no DCF file associations.\n")


def compare_table_columns(old_table_id: str,
                          new_table_id: str,
                          primary_key: str,
                          columns: list[str],
                          secondary_key: str = None):
    def make_compare_table_column_sql(column_name) -> str:
        if secondary_key is None:
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
                ORDER BY {primary_key}
            """
        else:
            return f"""
                (
                    SELECT {primary_key}, {secondary_key}, {column_name}
                    FROM `{old_table_id}`
                    EXCEPT DISTINCT 
                    SELECT {primary_key}, {secondary_key}, {column_name}
                    FROM `{new_table_id}`
                )
                UNION ALL
                (
                    SELECT {primary_key}, {secondary_key}, {column_name}
                    FROM `{new_table_id}`
                    EXCEPT DISTINCT 
                    SELECT {primary_key}, {secondary_key}, {column_name}
                    FROM `{old_table_id}`
                )
                ORDER BY {primary_key}
            """

    for column in columns:
        column_comparison_query = make_compare_table_column_sql(column)

        result = query_and_retrieve_result(sql=column_comparison_query)

        if not result:
            print(f"\nNo results returned for {column}. This can mean that there's a column data type mismatch, "
                  f"or that the column name differs.\n")
        elif result.total_rows > 0:
            print(f"\nFound mismatched data for {column}.")
            print(f"{result.total_rows} total records do not match in old and new tables.\n")
            if secondary_key is not None:
                print(f"Example values:\n{primary_key}\t\t\t\t{secondary_key}\t\t\t\t{column}")
            else:
                print(f"Example values:\n{primary_key}\t\t\t\t{column}")

            count = 0

            for row in result:
                if secondary_key is not None:
                    print(f"{row[0]}\t{row[1]}\t{row[2]}")
                else:
                    print(f"{row[0]}\t\t\t\t\t{row[1]}")

                count += 1
                if count == 5:
                    print()
                    break
        else:
            print(f"{column} column matches in published and new tables!")


def compare_concat_columns(old_table_id: str, new_table_id: str, concat_columns: list[str]):
    def make_concat_column_query(table_id: str) -> str:
        concat_columns_str = ", ".join(concat_columns)

        return f"""
            SELECT {concat_columns_str}  
            FROM `{table_id}`
        """

    def make_records_dict(query: str) -> dict[str, dict[str, str]]:
        result = query_and_retrieve_result(sql=query)

        records_dict = dict()

        for record_count, record in enumerate(result):
            file_gdc_id = record.get('file_gdc_id')

            record_dict = dict()

            for _column in concat_columns:
                record_dict[_column] = record.get(_column)

            records_dict[file_gdc_id] = record_dict

            if record_count % 100000 == 0:
                print(f"{record_count} records added to dict!")

        return records_dict

    old_records_dict = make_records_dict(query=make_concat_column_query(old_table_id))
    print("Created dict for old table records!")
    new_records_dict = make_records_dict(query=make_concat_column_query(new_table_id))
    print("Created dict for new table records!")

    for count, record_id in enumerate(old_records_dict.keys()):
        for column in concat_columns:
            old_column_value = old_records_dict[record_id][column]
            new_column_value = new_records_dict[record_id][column]

            if old_column_value and new_column_value:
                old_column_value_set = set(old_column_value.split(';'))
                new_column_value_set = set(new_column_value.split(';'))

                missing_values = old_column_value_set ^ new_column_value_set

                if len(missing_values) > 0:
                    print(f'id {record_id} value mismatch for {column}.')
                    print(f'old column values: {old_column_value} new column values: {new_column_value}')
            else:
                # case in which one or both values are None
                if new_column_value != new_column_value:
                    print(f'id {record_id} value mismatch for {column}.')
                    print(f'old column values: {old_column_value} new column values: {new_column_value}')

        if count % 100000 == 0:
            print(f"{count} records evaluated!")
