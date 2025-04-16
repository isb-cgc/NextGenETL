"""
Copyright 2020-2021, Institute for Systems Biology
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
import os
import shutil
import logging
import sys
import time
from decimal import Decimal

import pandas as pd

from google.cloud import storage
from google.cloud.exceptions import Forbidden
from google.resumable_media import InvalidResponse

from cda_bq_etl.bq_helpers import (create_and_upload_schema_for_tsv, retrieve_bq_schema_object,
                                   create_and_load_table_from_tsv, query_and_retrieve_result, list_tables_in_dataset,
                                   create_and_upload_schema_for_json, create_and_load_table_from_jsonl,
                                   update_table_schema_from_generic, find_most_recent_published_table_id, publish_table)
from cda_bq_etl.gcs_helpers import upload_to_bucket
from cda_bq_etl.data_helpers import (initialize_logging, make_string_bq_friendly, create_normalized_tsv,
                                     write_list_to_jsonl_and_upload, normalize_flat_json_values)
from cda_bq_etl.utils import (format_seconds, load_config, get_scratch_fp, calculate_md5sum, create_dev_table_id)

PARAMS = dict()
YAML_HEADERS = ('params', 'steps')


def make_file_pull_list(program: str, filters: dict[str, str]):
    def make_file_pull_list_query() -> str:
        logger = logging.getLogger('base_script')
        if not filters:
            logger.critical(f"No filters provided for {program}, exiting")
            sys.exit(-1)

        where_clause = "WHERE "

        where_clause_strs = list()

        for column_name, column_value in filters.items():
            where_clause_strs.append(f"{column_name} = '{column_value}'")

        where_clause += " AND ".join(where_clause_strs)

        rel_number = PARAMS['RELEASE'].strip('r')

        return f"""
            SELECT f.file_gdc_id,
               f.file_name,
               f.md5sum,
               f.file_size,
               f.file_state,
               f.project_short_name,
               gs.gdc_file_url_gcs
            FROM `isb-project-zero.cda_gdc_metadata.r{rel_number}_fileData_active` f
            # todo change both ids to published tables
            # FROM `isb-cgc-bq.GDC_case_file_metadata_versioned.fileData_active_r{rel_number}` f
            LEFT JOIN `isb-project-zero.GDC_manifests.rel{rel_number}_GDCfileID_to_GCSurl` gs 
               ON f.file_gdc_id = gs.file_gdc_id 
            {where_clause}
        """

    file_result = query_and_retrieve_result(make_file_pull_list_query())

    file_list = list()

    for row in file_result:
        file_list.append(dict(row))

    return file_list


def create_program_tables_dict() -> dict[str, list[str]]:
    prefix = f"{PARAMS['RELEASE']}_TARGET"

    table_list = list_tables_in_dataset(project_dataset_id="isb-project-zero.clinical_from_files_raw",
                                        filter_terms=prefix)

    project_tables = dict()

    for table in table_list:
        if "_CDE_" in table:
            continue

        project = table.split("_")[2]

        if project not in project_tables:
            project_tables[project] = list()

        project_tables[project].append(table)

    return project_tables


def convert_excel_to_tsv(all_files, header_idx):
    """
    Convert Excel files to CSV files.
    :param all_files: list of all filepaths
    :param header_idx: header row idx
    :return: list of tsv files
    """
    logger = logging.getLogger('base_script')
    tsv_files = []
    for file_path in all_files:
        logger.info(file_path)
        tsv_filepath = '.'.join(file_path.split('.')[0:-1])
        tsv_filepath = f"{tsv_filepath}_raw.tsv"
        excel_data = pd.read_excel(io=file_path,
                                   index_col=None,
                                   header=header_idx,
                                   engine='openpyxl')

        # get rid of funky newline formatting in headers
        excel_data.columns = excel_data.columns.map(lambda x: x.replace('\r', '').replace('\n', ''))
        # get rid of funky newline formatting in cells
        excel_data = excel_data.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=["", ""], regex=True)

        if excel_data.size == 0:
            logger.info(f"*** no rows found in excel file: {file_path}; skipping")
            continue
        df_rows = len(excel_data)
        excel_data.to_csv(tsv_filepath,
                          sep='\t',
                          index=False,
                          na_rep="None")
        with open(tsv_filepath, 'r') as tsv_fh:
            tsv_rows = len(tsv_fh.readlines()) - 1
        if df_rows != tsv_rows:
            logger.info(f"df_rows: {df_rows}, tsv_rows: {tsv_rows}")
        tsv_files.append(tsv_filepath)
    return tsv_files


def create_bq_column_names(tsv_file, header_row_idx):
    """
    Create bq column names. Formats them to be bq compatible.
    Creates a numeric suffix if a duplicate column name exists.
    :param tsv_file: tsv file from which to derive headers.
    :param header_row_idx: row index from which to retrieve headers.
    :return: list of column headers.
    """
    try:
        with open(tsv_file, 'r') as tsv_fh:
            header_row = tsv_fh.readlines()[header_row_idx].strip().split('\t')
    except UnicodeDecodeError:
        with open(tsv_file, 'r', encoding="ISO-8859-1") as tsv_fh:
            header_row = tsv_fh.readlines()[header_row_idx].strip().split('\t')

    final_headers = []

    for i in range(0, len(header_row)):
        column_name = header_row[i].strip()
        column_name = make_string_bq_friendly(column_name)
        column_name = column_name.lower()
        if column_name in final_headers:
            i = 1
            while column_name in final_headers:
                # give the duplicate column name a suffix
                column_name = f"{column_name}_{str(i)}"
        final_headers.append(column_name)
    return final_headers


def create_tsv_with_final_headers(tsv_file, headers, data_start_idx):
    """
    Creates modified tsv with bq-compatible column names.
    Strips additional header column rows
    :param tsv_file: raw tsv file, either downloaded from GDC or created by converting Excel file to tsv.
    :param headers: list of bq-compatible headers.
    :param data_start_idx: starting row index for data (should be 1 for TARGET)
    """
    try:
        with open(tsv_file, 'r') as tsv_fh:
            lines = tsv_fh.readlines()
    except UnicodeDecodeError:
        with open(tsv_file, 'r', encoding="ISO-8859-1") as tsv_fh:
            lines = tsv_fh.readlines()

    with open(tsv_file, 'w') as tsv_fh:
        header_row = "\t".join(headers)
        tsv_fh.write(f"{header_row}\n")
        for i in range(data_start_idx, len(lines)):
            line = lines[i].strip()
            if not line:
                break
            tsv_fh.write(f"{line}\n")


def create_table_name_from_file_name(file_path: str) -> str:
    file_name = file_path.split("/")[-1]
    table_base_name = "_".join(file_name.split('.')[0:-1])
    table_base_name = table_base_name.replace("-", "_").replace(".", "_")
    table_id = create_dev_table_id(PARAMS, table_base_name)
    table_name = table_id.split('.')[-1]
    table_name = table_name.replace("-", "_").replace(".", "_")

    return table_name


def make_file_metadata_query(file_gdc_id: str) -> str:
    rel_number = PARAMS['RELEASE'].strip('r')

    return f"""
        SELECT file_name, project_short_name, updated_datetime
        FROM `isb-project-zero.cda_gdc_metadata.r{rel_number}_fileData_active`
        # todo change to published table
        # FROM `isb-cgc-bq.GDC_case_file_metadata_versioned.fileData_active_r{PARAMS['RELEASE']}`
        WHERE file_gdc_id = '{file_gdc_id}'
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

    logger.info(f"GDC clinical file script started at {time.strftime('%x %X', time.localtime())}")

    program = "TARGET"

    local_program_dir = get_scratch_fp(PARAMS, program)
    local_files_dir = f"{local_program_dir}/files"
    local_schemas_dir = f"{local_program_dir}/schemas"
    file_traversal_list = f"{local_program_dir}/{PARAMS['BASE_FILE_NAME']}_traversal_list_{program}.txt"
    tables_file = f"{local_program_dir}/{PARAMS['RELEASE']}_tables_{program}.txt"

    if 'build_file_list_and_download' in steps:
        # create needed directories if they don't already exist
        logger.info('Entering build_file_list_and_download')

        if os.path.exists(local_program_dir):
            shutil.rmtree(local_program_dir)

        # create needed directories if they don't already exist
        if not os.path.exists(local_program_dir):
            logger.info(f"Creating directory {local_program_dir}")
            os.makedirs(local_program_dir)
        if not os.path.exists(local_files_dir):
            logger.info(f"Creating directory {local_files_dir}")
            os.makedirs(local_files_dir)
        if not os.path.exists(local_schemas_dir):
            logger.info(f"Creating directory {local_schemas_dir}")
            os.makedirs(local_schemas_dir)

        file_pull_list = make_file_pull_list(program, PARAMS['FILTERS'])

        storage_client = storage.Client()

        for file_data in file_pull_list:
            file_name = file_data['file_name']
            file_id = file_data['file_gdc_id']
            gs_uri = file_data['gdc_file_url_gcs']
            md5sum = file_data['md5sum']

            file_path = f"{local_files_dir}/{file_id}__{file_name}"

            logger.info(file_path)

            file_obj = open(file_path, 'wb')

            try:
                storage_client.download_blob_to_file(blob_or_uri=gs_uri, file_obj=file_obj)
                file_obj.close()

                md5sum_actual = calculate_md5sum(file_path)

                if md5sum != md5sum_actual:
                    logger.error(f"md5sum mismatch for {gs_uri}.")
                    logger.error(f"expected {md5sum}, actual {md5sum_actual}")
                    sys.exit(-1)
                else:
                    logger.info(f"md5sums match! Written to {file_path}")

            except InvalidResponse:
                logger.error(f"{gs_uri} request failed; InvalidResponse")
                file_obj.close()
                os.remove(file_path)
            except Forbidden:
                logger.error(f"{gs_uri} request failed; Forbidden")
                file_obj.close()
                os.remove(file_path)

    if 'convert_excel_to_csv' in steps:
        # If file suffix is xlsx or xls, convert to tsv.
        # Then modify traversal list file to point to the newly created tsv files.
        logger.info('Entering convert_excel_to_tsv')

        file_names = os.listdir(local_files_dir)

        all_files = list()

        for file_name in file_names:
            all_files.append(f"{local_files_dir}/{file_name}")

        for excel_file_path in all_files:
            upload_to_bucket(PARAMS, scratch_fp=excel_file_path, delete_local=False)

        all_tsv_files = convert_excel_to_tsv(all_files=all_files, header_idx=PARAMS['HEADER_ROW_IDX'])

        with open(file_traversal_list, mode='w') as traversal_list_file:
            for tsv_file in all_tsv_files:
                traversal_list_file.write(f"{tsv_file}\n")

    if 'normalize_tsv_and_create_schema' in steps:
        logger.info(f"Entering upload_tsv_file_and_schema_to_bucket")

        with open(file_traversal_list, mode='r') as traversal_list_file:
            all_files = traversal_list_file.read().splitlines()

        header_row_idx = PARAMS['HEADER_ROW_IDX']

        for tsv_file_path in all_files:
            # Some files have a different encoding--so if a file can't be decoded in Unicode format,
            # open the file using ISO-8859-1 encoding instead.
            try:
                with open(tsv_file_path, 'r') as tsv_fh:
                    row_count = len(tsv_fh.readlines())
            except UnicodeDecodeError:
                with open(tsv_file_path, 'r', encoding="ISO-8859-1") as tsv_fh:
                    row_count = len(tsv_fh.readlines())

            if row_count <= 1:
                logger.warning(f"*** probably an issue: row count is {row_count} for {tsv_file_path}")

            bq_column_names = create_bq_column_names(tsv_file=tsv_file_path, header_row_idx=header_row_idx)

            create_tsv_with_final_headers(tsv_file=tsv_file_path,
                                          headers=bq_column_names,
                                          data_start_idx=PARAMS['DATA_START_IDX'])

            normalized_tsv_file_path = tsv_file_path.replace("_raw.tsv", ".tsv")

            create_normalized_tsv(tsv_file_path, normalized_tsv_file_path)

            table_name = create_table_name_from_file_name(normalized_tsv_file_path)
            schema_file_name = f"schema_{table_name}.json"
            schema_file_path = f"{local_schemas_dir}/{schema_file_name}"

            create_and_upload_schema_for_tsv(PARAMS,
                                             tsv_fp=normalized_tsv_file_path,
                                             header_row=0,
                                             skip_rows=1,
                                             schema_fp=schema_file_path,
                                             delete_local=True)

            # upload raw and normalized tsv files to google cloud storage
            upload_to_bucket(PARAMS, tsv_file_path, delete_local=True, verbose=False)
            upload_to_bucket(PARAMS, normalized_tsv_file_path, delete_local=True, verbose=False)

    if 'build_raw_tables' in steps:
        logger.info("Entering build_raw_tables")
        with open(file_traversal_list, mode='r') as traversal_list_file:
            all_files = traversal_list_file.read().splitlines()

        table_list = []

        for tsv_file_path in all_files:
            normalized_tsv_file_path = tsv_file_path.replace("_raw.tsv", ".tsv")
            file_name = normalized_tsv_file_path.split("/")[-1]

            table_name = create_table_name_from_file_name(normalized_tsv_file_path)
            table_id = f"{PARAMS['DEV_PROJECT']}.{PARAMS['DEV_RAW_DATASET']}.{table_name}"

            schema_file_name = f"schema_{table_name}.json"

            bq_schema = retrieve_bq_schema_object(PARAMS,
                                                  table_name=table_name,
                                                  schema_filename=schema_file_name,
                                                  schema_dir=local_schemas_dir)
            create_and_load_table_from_tsv(PARAMS,
                                           tsv_file=file_name,
                                           table_id=table_id,
                                           num_header_rows=1,
                                           schema=bq_schema)
            table_list.append(table_id)
        with open(tables_file, 'w') as tables_fh:
            for table_name in table_list:
                tables_fh.write(f"{table_name}\n")

        logger.info(f"Tables created for {program}:")
        for table in table_list:
            logger.info(table)
        logger.info("")

    if 'create_and_upload_merged_jsonl' in steps:
        logger.info(f"Entering create_and_upload_merged_jsonl")
        table_list = list()

        with open(tables_file, mode='r') as tables_fh:
            all_tables = tables_fh.read().splitlines()

        for table_id in all_tables:
            table_name = table_id.split(".")[2]
            file_id = table_name.split("__")[0]
            file_id = file_id.replace(f"{PARAMS['RELEASE']}_", "")
            file_id = file_id.replace("_", "-")

            # look up project_short_name using file uuid

            file_metadata_result = query_and_retrieve_result(make_file_metadata_query(file_gdc_id=file_id))

            for row in file_metadata_result:
                if 'CDE' not in row['file_name'] and 'Supplement' not in row['file_name']:
                    table_list.append({
                        "table_id": table_id,
                        "project_short_name": row['project_short_name'],
                        "file_name": row['file_name'],
                        "updated_datetime": row['updated_datetime']
                    })

        records_dict = dict()

        # values from newer files are included preferentially
        for raw_tables_dict in sorted(table_list, key=lambda d: d['updated_datetime'], reverse=True):
            logger.info(raw_tables_dict['table_id'])

            project_short_name = raw_tables_dict['project_short_name']
            disease_code = raw_tables_dict['project_short_name'].split('-')[1]
            program_name = PARAMS['PROGRAM']
            query_result = query_and_retrieve_result(f"SELECT DISTINCT * FROM `{raw_tables_dict['table_id']}`")

            for row in query_result:
                row_dict = dict(row)

                for column in PARAMS['EXCLUDED_COLUMNS']:
                    if column in row_dict:
                        del row_dict[column]

                row_dict['project_short_name'] = project_short_name
                row_dict['program_name'] = program_name
                target_usi = row_dict.pop('target_usi')
                row_dict['case_barcode'] = target_usi
                row_dict['disease_code'] = disease_code

                int_comparison_columns = ['event_free_survival_time_in_days',
                                          'year_of_last_follow_up',
                                          'overall_survival_time_in_days',
                                          'age_at_diagnosis_in_days']

                if target_usi not in records_dict:
                    records_dict[target_usi] = dict()

                for column, value in row_dict.items():
                    if isinstance(value, str):
                        value = value.strip()

                    # column doesn't exist yet, so add it and its value
                    """
                    if value == 'Induction failure':
                        value = 'Induction Failure'
                    elif value == 'Not done':
                        value = 'Not Done'
                    elif value == 'Death without remission':
                        value = 'Death without Remission'
                    elif value == 'unevaluable':
                        value = 'Unevaluable'
                    """

                    if value is None:
                        continue
                    elif column not in records_dict[target_usi]:
                        if column in int_comparison_columns:
                            value = int(value)
                        records_dict[target_usi][column] = value
                    elif column in int_comparison_columns:
                        existing_value = int(records_dict[target_usi][column])
                        new_value = int(value)

                        if new_value > existing_value:
                            records_dict[target_usi][column] = value
                    elif column in ['disease_code', 'project_short_name']:
                        if value and value not in records_dict[target_usi][column]:
                            records_dict[target_usi][column] += f', {value}'
                    elif value != records_dict[target_usi][column]:
                        # if value not in exempt_list and records_dict[target_usi][column] not in exempt_list:
                        logger.warning(f"Record mismatch for {target_usi} in column {column}: "
                                       f"{value} != {records_dict[target_usi][column]}")

        record_list = list()

        for record in records_dict.values():
            record_list.append(record)

        normalized_record_list = normalize_flat_json_values(record_list)

        write_list_to_jsonl_and_upload(PARAMS, "target_merged", normalized_record_list)

        create_and_upload_schema_for_json(PARAMS,
                                          record_list=normalized_record_list,
                                          table_name='target_merged',
                                          include_release=True)

    if 'create_merged_table' in steps:
        logger.info("Entering create_merged_table")

        final_dataset = f"{PARAMS['DEV_PROJECT']}.{PARAMS['DEV_FINAL_DATASET']}"
        final_table_id = f"{final_dataset}.{PARAMS['RELEASE']}_{PARAMS['MERGED_TABLE_NAME']}"

        # Download schema file from Google Cloud bucket
        table_schema = retrieve_bq_schema_object(PARAMS, table_name='target_merged', include_release=True)

        # Load jsonl data into BigQuery table
        create_and_load_table_from_jsonl(PARAMS,
                                         jsonl_file=f"target_merged_{PARAMS['RELEASE']}.jsonl",
                                         table_id=final_table_id,
                                         schema=table_schema)

        update_table_schema_from_generic(params=PARAMS,
                                         table_id=final_table_id,
                                         metadata_file=PARAMS['METADATA_FILE_SINGLE_PROGRAM'])

    if 'publish_tables' in steps:
        prod_dataset_id = f"{PARAMS['PROD_PROJECT']}.{PARAMS['PROD_DATASET']}"

        dev_dataset = f"{PARAMS['DEV_PROJECT']}.{PARAMS['DEV_FINAL_DATASET']}"
        source_table_id = f"{dev_dataset}.{PARAMS['RELEASE']}_{PARAMS['MERGED_TABLE_NAME']}"

        prod_table_name = f"{PARAMS['PROD_TABLE_PREFIX']}_{PARAMS['NODE']}"
        current_table_id = f"{prod_dataset_id}.{prod_table_name}_current"
        versioned_table_id = f"{prod_dataset_id}_versioned.{prod_table_name}_{PARAMS['RELEASE']}"

        table_ids = {
            "source": source_table_id,
            "current": current_table_id,
            "versioned": versioned_table_id,
            "previous_versioned": find_most_recent_published_table_id(PARAMS, versioned_table_id)
        }

        publish_table(PARAMS, table_ids)

    end_time = time.time()

    logger.info(f"Script completed in: {format_seconds(end_time - start_time)}")


if __name__ == '__main__':
    main(sys.argv)
