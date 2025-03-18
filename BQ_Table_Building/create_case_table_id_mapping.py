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

from cda_bq_etl.data_helpers import initialize_logging
from cda_bq_etl.utils import load_config, format_seconds
from cda_bq_etl.bq_helpers import create_table_from_query, update_table_schema_from_generic, query_and_retrieve_result

PARAMS = dict()
YAML_HEADERS = ('params', 'steps')


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
            print("table results list:")
            table_list = list()
            dataset_dict = dict()
            for result in results:
                dataset = result.table_schema
                table_id = f"isb-cgc-bq.{dataset}.{result.table_name}"
                creation_time = result.creation_time
                formatted_creation_time = creation_time.strftime('%Y-%m-%d %H:%M:%S')
                print(f"{table_id}\t{formatted_creation_time}")

                if 'versioned' not in dataset:
                    table_list.append(table_id)
                else:
                    if dataset not in dataset_dict:
                        dataset_dict[dataset] = list()
                    dataset_dict[dataset].append(table_id)

            for dataset, table_list in dataset_dict.items():
                table_list.sort(reverse=True)
                print(f"\n{dataset} tables:")
                for table in table_list:
                    print(f"\t{table}")

    end_time = time.time()
    logger.info(f"Script completed in: {format_seconds(end_time - start_time)}")


if __name__ == "__main__":
    main(sys.argv)
