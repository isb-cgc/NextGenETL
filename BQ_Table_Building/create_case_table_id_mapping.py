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

from cda_bq_etl.data_helpers import initialize_logging, write_list_to_jsonl_and_upload
from cda_bq_etl.utils import load_config, format_seconds
from cda_bq_etl.bq_helpers import (query_and_retrieve_result, create_and_upload_schema_for_json,
                                   retrieve_bq_schema_object, create_and_load_table_from_jsonl)

PARAMS = dict()
YAML_HEADERS = ('params', 'steps')


def query_table_names() -> str:
    return f"""
        SELECT
          DISTINCT table_schema
        FROM
          isb-cgc-bq.`region-us`.INFORMATION_SCHEMA.TABLES;
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
        SELECT DISTINCT table_name, column_name
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

    if 'retrieve_case_tables' in steps:
        logger.info("Entering retrieve_case_tables. This step takes a minute.")

        results = query_and_retrieve_result(query_table_names())

        table_id_list = list()

        for result in results:
            project = PARAMS['PROD_PROJECT']
            dataset = result.table_schema
            dataset_id = f"{project}.{dataset}"

            column_name_sql = query_column_names(dataset_id, PARAMS['CASE_ID_FIELDS'])

            column_results = query_and_retrieve_result(column_name_sql)

            for row in column_results:
                table_id_dict = dict()

                row_dict = dict(row)
                filtered_table = False

                for keyword in PARAMS['FILTERED_TABLE_KEYWORDS']:
                    if keyword in row_dict['table_name']:
                        filtered_table = True

                if filtered_table:
                    # print(f"{row_dict['table_name']} was filtered")
                    continue

                table_id = f"{dataset_id}.{row_dict['table_name']}"

                if table_id not in table_id_dict:
                    table_id_dict['table_id'] = table_id
                    table_id_dict['column_name'] = row_dict['column_name']
                    print(f"{table_id}\t{row_dict['column_name']}")
                    table_id_list.append(table_id_dict)
                else:
                    print(f"this table is already in table_id_dict: {table_id}")

        write_list_to_jsonl_and_upload(PARAMS,
                                       prefix=PARAMS['TABLE_ID_COLUMN_NAME_TABLE'],
                                       record_list=table_id_list)

        create_and_upload_schema_for_json(PARAMS,
                                          record_list=table_id_list,
                                          table_name=PARAMS['TABLE_ID_COLUMN_NAME_TABLE'],
                                          include_release=True)

    if 'build_table_case_column_table' in steps:
        logger.info("Entering build_table_case_column_table")

        table_name = f"{PARAMS['TABLE_ID_COLUMN_NAME_TABLE']}_{PARAMS['RELEASE']}"
        table_id = f"{PARAMS['DEV_PROJECT']}.{PARAMS['DEV_DATASET']}.{table_name}"

        table_schema = retrieve_bq_schema_object(PARAMS,
                                                 table_name=PARAMS['TABLE_ID_COLUMN_NAME_TABLE'],
                                                 include_release=True)

        # Load jsonl data into BigQuery table
        create_and_load_table_from_jsonl(PARAMS,
                                         jsonl_file=f"{PARAMS['TABLE_ID_COLUMN_NAME_TABLE']}_{PARAMS['RELEASE']}.jsonl",
                                         table_id=table_id,
                                         schema=table_schema)

    if 'find_most_recent_versioned_tables' in steps:
        # - get all the table ids from the table created above
        # - if dataset contains _versioned, find the most recent version
        # - find all base names within dataset
        # - then reverse sort by name; the first table is the most recent

        table_name = f"{PARAMS['TABLE_ID_COLUMN_NAME_TABLE']}_{PARAMS['RELEASE']}"
        table_id = f"{PARAMS['DEV_PROJECT']}.{PARAMS['DEV_DATASET']}.{table_name}"
        sql = f"""
            SELECT *
            FROM `{table_id}`
        """


        results = query_and_retrieve_result(sql=sql)

        current_table_id_dict = dict()
        versioned_table_id_dict = dict()
        versioned_table_id_list = list()
        filtered_table_id_list = list()

        for result in results:
            if '_versioned' not in result.table_id:
                filtered_table_id_dict = dict()
                current_table_id_dict[result.table_id] = result.column_name
                filtered_table_id_dict['table_id'] = result.table_id
                filtered_table_id_dict['column_name'] = result.column_name
                filtered_table_id_list.append(filtered_table_id_dict)
            else:
                versioned_table_id_dict[result.table_id] = result.column_name
                versioned_table_id_list.append(result.table_id)

        print(versioned_table_id_list)

        for current_table_id in current_table_id_dict.keys():
            split_table_id = current_table_id.split('.')
            dataset = split_table_id[1]
            table_name = split_table_id[2]

            if table_name[-8:] == '_current':
                filtered_table_id_dict = dict()

                base_table_name = table_name[:-8]
                temp_versioned_table_list = [s for s in versioned_table_id_list if base_table_name in s and dataset in s]

                temp_versioned_table_list.sort(reverse=True)

                print(f"\n{temp_versioned_table_list}\n")

                if temp_versioned_table_list:
                    most_recent_versioned_table_id = temp_versioned_table_list[0]

                    filtered_table_id_dict['table_id'] = most_recent_versioned_table_id
                    filtered_table_id_dict['column_name'] = versioned_table_id_dict[most_recent_versioned_table_id]
                    filtered_table_id_list.append(filtered_table_id_dict)

                    print(f"Most recent versioned table for {current_table_id}: {most_recent_versioned_table_id}")
                else:
                    print(f"No versioned table for {current_table_id} using base name {base_table_name}")
            else:
                print(f"No versioned table for {current_table_id}, not suffixed with _current")
                continue

        write_list_to_jsonl_and_upload(PARAMS,
                                       prefix=PARAMS['FILTERED_TABLE_ID_COLUMN_NAME_TABLE'],
                                       record_list=filtered_table_id_list)

        create_and_upload_schema_for_json(PARAMS,
                                          record_list=filtered_table_id_list,
                                          table_name=PARAMS['FILTERED_TABLE_ID_COLUMN_NAME_TABLE'],
                                          include_release=True)

    if 'build_filtered_table_case_column_table' in steps:
        logger.info("Entering build_table_case_column_table")

        table_name = f"{PARAMS['FILTERED_TABLE_ID_COLUMN_NAME_TABLE']}_{PARAMS['RELEASE']}"
        table_id = f"{PARAMS['DEV_PROJECT']}.{PARAMS['DEV_DATASET']}.{table_name}"

        table_schema = retrieve_bq_schema_object(PARAMS,
                                                 table_name=PARAMS['FILTERED_TABLE_ID_COLUMN_NAME_TABLE'],
                                                 include_release=True)

        # Load jsonl data into BigQuery table
        jsonl_file = f"{PARAMS['FILTERED_TABLE_ID_COLUMN_NAME_TABLE']}_{PARAMS['RELEASE']}.jsonl"
        create_and_load_table_from_jsonl(PARAMS,
                                         jsonl_file=jsonl_file,
                                         table_id=table_id,
                                         schema=table_schema)

    if 'find_case_ids_for_each_table' in steps:
        table_name = f"{PARAMS['FILTERED_TABLE_ID_COLUMN_NAME_TABLE']}_{PARAMS['RELEASE']}"
        filtered_table_id = f"{PARAMS['DEV_PROJECT']}.{PARAMS['DEV_DATASET']}.{table_name}"

        case_id_mapping_list = list()

        sql = f"""
            SELECT * 
            FROM `{filtered_table_id}`
        """

        results = query_and_retrieve_result(sql=sql)

        for result in results:
            column_name = result.column_name
            case_table_id = result.table_id

            print(f"Finding case ids for {case_table_id}...")

            case_ids_sql = f"""
                SELECT DISTINCT {column_name}
                FROM `{case_table_id}`
            """

            case_id_results = query_and_retrieve_result(sql=case_ids_sql)

            if 'gdc' in case_table_id:
                node = 'gdc'
            elif 'pdc' in case_table_id:
                node = 'pdc'
            else:
                node = None

            for case_id_result in case_id_results:
                case_id_dict = dict()
                case_id = case_id_result[0]

                case_id_dict['case_id'] = case_id
                case_id_dict['case_column'] = column_name
                case_id_dict['table_id'] = case_table_id
                case_id_dict['node'] = node

                case_id_mapping_list.append(case_id_dict)

        write_list_to_jsonl_and_upload(PARAMS,
                                       prefix=PARAMS['CASE_TABLE_MAPPING_TABLE'],
                                       record_list=case_id_mapping_list)

        create_and_upload_schema_for_json(PARAMS,
                                          record_list=case_id_mapping_list,
                                          table_name=PARAMS['CASE_TABLE_MAPPING_TABLE'],
                                          include_release=True)

    if 'build_case_table_mapping_table' in steps:
        logger.info("Entering build_table_case_column_table")

        table_schema = retrieve_bq_schema_object(PARAMS,
                                                 table_name=PARAMS['CASE_TABLE_MAPPING_TABLE'],
                                                 include_release=True)

        table_name = f"{PARAMS['CASE_TABLE_MAPPING_TABLE']}_{PARAMS['RELEASE']}"
        table_id = f"{PARAMS['DEV_PROJECT']}.{PARAMS['DEV_DATASET']}.{table_name}"

        # Load jsonl data into BigQuery table
        jsonl_file = f"{PARAMS['CASE_TABLE_MAPPING_TABLE']}_{PARAMS['RELEASE']}.jsonl"
        create_and_load_table_from_jsonl(PARAMS,
                                         jsonl_file=jsonl_file,
                                         table_id=table_id,
                                         schema=table_schema)

    end_time = time.time()
    logger.info(f"Script completed in: {format_seconds(end_time - start_time)}")


if __name__ == "__main__":
    main(sys.argv)
