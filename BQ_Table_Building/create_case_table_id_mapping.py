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


def query_table_for_values(table_id: str) -> str:
    return f"""
        SELECT * 
        FROM `{table_id}`
        LIMIT 5
    """


def query_column_names(dataset_id: str, column_list: list[str]) -> str:
    split_dataset_id = dataset_id.split('.')
    project_name = split_dataset_id[0]
    dataset_name = split_dataset_id[1]

    where_clause = "WHERE "

    for column in column_list:
        where_clause += f" column_name = '{column}' OR"

    where_clause = where_clause[:-2]

    return f"""
        SELECT table_name, column_name
        FROM `{project_name}`.{dataset_name}.INFORMATION_SCHEMA.COLUMNS
        {where_clause}
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

    column_list = ['case_gdc_id', 'case_id', 'HTAN_Participant_ID', 'PatientID', 'bcr_patient_uuid']

    if 'retrieve_datasets' in steps:

        sql = f"""
            SELECT
              DISTINCT table_schema
            FROM
              isb-cgc-bq.`region-us`.INFORMATION_SCHEMA.TABLES;        
        """

        results = query_and_retrieve_result(sql)

        if not results:
            print("No results found")
        else:
            table_id_dict = dict()

            for result in results:
                project = "isb-cgc-bq"
                dataset = result.table_schema
                dataset_id = f"{project}.{dataset}"

                column_name_sql = query_column_names(dataset_id, column_list)

                column_results = query_and_retrieve_result(column_name_sql)

                for row in column_results:
                    row_dict = dict(row)
                    table_id = f"{dataset_id}.{row_dict['table_name']}"
                    print(f"{table_id}: {row_dict['column_name']}")

                    if table_id in table_id_dict:
                        print(f"this table is already in table_id_dict: {table_id}")
                    else:
                        table_id_dict[table_id] = table_id



    end_time = time.time()
    logger.info(f"Script completed in: {format_seconds(end_time - start_time)}")


if __name__ == "__main__":
    main(sys.argv)
