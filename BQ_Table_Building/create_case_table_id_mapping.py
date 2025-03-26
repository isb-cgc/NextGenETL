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
import time

from google.cloud import bigquery
# from google.cloud.bigquery import SchemaField, Client, LoadJobConfig, QueryJob

from cda_bq_etl.data_helpers import initialize_logging, is_uuid
from cda_bq_etl.utils import load_config, format_seconds
from cda_bq_etl.bq_helpers import create_table_from_query, update_table_schema_from_generic, query_and_retrieve_result

PARAMS = dict()
YAML_HEADERS = ('params', 'steps')


def query_table_for_values(table_id: str):
    return f"""
        SELECT * 
        FROM `{table_id}`
        LIMIT 5
    """


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

    excluded_datasets = {
        "CCLE",
        "CCLE_versioned",
        "COSMIC",
        "COSMIC_versioned",
        "DEPMAP",
        "DEPMAP_versioned",
        "mitelman",
        "mitelman_versioned",
        "reactome",
        "reactome_versioned",
        "supplementary_tables",
        "synthetic_lethality",
        "targetome",
        "targetome_versioned"
    }

    if 'retrieve_datasets' in steps:

        sql = f"""
            SELECT
              table_schema, table_name, creation_time
            FROM
              isb-cgc-bq.`region-us`.INFORMATION_SCHEMA.TABLES;        
        """

        results = query_and_retrieve_result(sql)

        if not results:
            print("No results found")
        else:
            table_list = list()
            current_dataset_dict = dict()
            versioned_dataset_dict = dict()
            for result in results:
                dataset = result.table_schema
                table_id = f"isb-cgc-bq.{dataset}.{result.table_name}"
                creation_time = result.creation_time
                formatted_creation_time = creation_time.strftime('%Y-%m-%d %H:%M:%S')
                # print(f"{table_id}\t{formatted_creation_time}")

                if dataset in excluded_datasets:
                    continue

                if 'versioned' not in dataset:
                    if dataset not in current_dataset_dict:
                        current_dataset_dict[dataset] = list()
                    current_dataset_dict[dataset].append(table_id)
                else:
                    if dataset not in versioned_dataset_dict:
                        versioned_dataset_dict[dataset] = list()
                    versioned_dataset_dict[dataset].append(table_id)

            print("Add tables to list:")

            for dataset, current_datasets in sorted(current_dataset_dict.items()):
                current_datasets.sort()
                # print(f"\n{dataset} tables:")
                for table in current_datasets:
                    table_list.append(table)
                    # print(f"\t{table}")

            """
            for dataset, versioned_datasets in sorted(versioned_dataset_dict.items()):
                versioned_datasets.sort()
                # print(f"\n{dataset} tables:")
                for table in versioned_datasets:
                    table_list.append(table)
                    # print(f"\t{table}")
            """

            table_id_uuid_columns = dict()

            print("Query for potential columns: ")

            for table_id in table_list:
                column_set = set()
                sql = query_table_for_values(table_id)
                results = query_and_retrieve_result(sql)

                for row in results:
                    row_dict = dict(row)
                    for column_name, value in row_dict.items():
                        # - check if value is uuid
                        # - if so, store column name in a set
                        if is_uuid(value):
                            column_set.add(column_name)

                    if column_set:
                        break

                table_id_uuid_columns[table_id] = column_set
                print(f"{table_id}: {sorted(column_set)}")
            print("Output potential columns: ")

            """
            for table_id, column_set in table_id_uuid_columns.items():
                print(f"{table_id} potential columns:")
                for column in sorted(column_set):
                    print(f"\t- {column}")
            """

    end_time = time.time()
    logger.info(f"Script completed in: {format_seconds(end_time - start_time)}")


if __name__ == "__main__":
    main(sys.argv)
