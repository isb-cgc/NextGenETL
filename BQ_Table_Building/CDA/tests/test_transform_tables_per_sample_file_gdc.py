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

from google.cloud import bigquery

from common_etl.cda_utils import create_program_name_set
from common_etl.support import bq_harness_with_result
from typing import Union

from google.cloud.bigquery.table import RowIterator, _EmptyRowIterator

from common_etl.utils import load_config, has_fatal_error

API_PARAMS = dict()
BQ_PARAMS = dict()
YAML_HEADERS = ('api_params', 'bq_params', 'steps')

BQHarnessResult = Union[None, RowIterator, _EmptyRowIterator]


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
        result = bq_harness_with_result(sql=make_compare_id_keys_sql(table_id_1, table_id_2),
                                        do_batch=False,
                                        verbose=False)

        if not result:
            has_fatal_error(f"Primary key {primary_key} not found in one or both compared tables")
        elif result.total_rows == 0:
            print(f"\n{table_id_1} has no {primary_key} values that don't exist in {table_id_2}. Great!")
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

    old_count_result = bq_harness_with_result(sql=old_table_row_count_query, do_batch=False, verbose=False)
    new_count_result = bq_harness_with_result(sql=new_table_row_count_query, do_batch=False, verbose=False)

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
        ORDER BY {primary_key}
        """

    for column in columns:
        column_comparison_query = make_compare_table_column_sql(column)

        result = bq_harness_with_result(sql=column_comparison_query, do_batch=False, verbose=False)

        if not result:
            print(f"\nNo results returned for {column}. This can mean that there's a column data type mismatch, "
                  f"or that the column name differs.")
        elif result.total_rows > 0:
            print(f"\nFound mismatched data for {column}.")
            print(f"{result.total_rows} total records do not match in old and new tables.\n")
            print(f"Example values:\n{primary_key}\t\t\t\t{column}")

            count = 0

            for row in result:
                print(f"{row[0]}\t\t\t\t\t{row[1]}")
                count += 1
                if count == 5:
                    break
        else:
            print(f"{column} column matches in published and new tables!")


def main(args):
    try:
        global API_PARAMS, BQ_PARAMS
        API_PARAMS, BQ_PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        has_fatal_error(err, ValueError)

    client = bigquery.Client()

    table_id_tuple_set = set()

    program_set = create_program_name_set(API_PARAMS, BQ_PARAMS)

    for program in sorted(program_set):
        if program == "BEATAML1.0":
            program_name = "BEATAML1_0"
        elif program == "EXCEPTIONAL_RESPONDERS":
            program_name = "EXC_RESPONDERS"
        else:
            program_name = program

        gdc_table_name = f"{program_name}_per_sample_file_metadata_hg38_gdc_{API_PARAMS['GDC_RELEASE']}"
        gdc_table_id = f"{BQ_PARAMS['WORKING_PROJECT']}.GDC_per_sample_file.{gdc_table_name}"
        cda_table_name = f"per_sample_file_metadata_hg38_{program_name}_{API_PARAMS['RELEASE']}"
        cda_table_id = f"{BQ_PARAMS['WORKING_PROJECT']}.{BQ_PARAMS['WORKING_DATASET']}.{cda_table_name}"

        # check for valid hg38 table location
        gdc_table = client.get_table(table=gdc_table_id)
        cda_table = client.get_table(table=cda_table_id)

        if gdc_table is None or cda_table is None:
            if gdc_table is None:
                print(f"No table found: {gdc_table_id}")
            if cda_table is None:
                print(f"No table found: {cda_table_id}")
        else:
            table_id_tuple = (gdc_table_id, cda_table_id)
            table_id_tuple_set.add(table_id_tuple)

    for table_id_tuple in table_id_tuple_set:
        gdc_table_id = table_id_tuple[0]
        cda_table_id = table_id_tuple[1]

        print("Comparing row counts!\n")

        compare_row_counts(old_table_id=gdc_table_id,
                           new_table_id=cda_table_id)

        print("Comparing table keys!\n")

        compare_id_keys(old_table_id=gdc_table_id,
                        new_table_id=cda_table_id,
                        primary_key=BQ_PARAMS['PRIMARY_KEY'])

        columns_list = BQ_PARAMS["COLUMNS"]

        print("Comparing table columns!\n")

        compare_table_columns(old_table_id=gdc_table_id,
                              new_table_id=cda_table_id,
                              primary_key=BQ_PARAMS['PRIMARY_KEY'],
                              columns=columns_list)


if __name__ == "__main__":
    main(sys.argv)
