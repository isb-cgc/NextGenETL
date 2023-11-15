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
import logging
import os
import shutil
import sys
import time

from google.cloud import storage
from google.cloud.exceptions import Forbidden
from google.resumable_media import InvalidResponse

from cda_bq_etl.bq_helpers import (create_and_upload_schema_for_tsv, retrieve_bq_schema_object,
                                   create_and_load_table_from_tsv, query_and_retrieve_result, list_tables_in_dataset,
                                   get_columns_in_table, create_and_upload_schema_for_json,
                                   create_and_load_table_from_jsonl)
from cda_bq_etl.gcs_helpers import upload_to_bucket, download_from_bucket, download_from_external_bucket
from cda_bq_etl.data_helpers import initialize_logging, make_string_bq_friendly, write_list_to_tsv, \
    create_normalized_tsv, write_list_to_jsonl_and_upload
from cda_bq_etl.utils import format_seconds, get_filepath, load_config, get_scratch_fp, calculate_md5sum, \
    create_dev_table_id

PARAMS = dict()
YAML_HEADERS = ('params', 'steps')



def create_tsv_with_final_headers(tsv_file, headers, data_start_idx):
    """
    Creates modified tsv with bq-compatible column names.
    Strips additional header column rows (e.g. in the case of TCGA, where three column header rows exist.)
    :param tsv_file: raw tsv file, either downloaded from GDC or created by converting excel file to tsv.
    :param headers: list of bq-compatible headers.
    :param data_start_idx: starting row index for data (should be 1 for TARGET and 3 for TCGA)
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

        return f"""
            SELECT f.file_gdc_id,
               f.file_name,
               f.md5sum,
               f.file_size,
               f.file_state,
               gs.file_gdc_url
            FROM `isb-project-zero.GDC_metadata.rel36_fileData_current` f
            LEFT JOIN `isb-project-zero.GDC_manifests.rel36_GDCfileID_to_GCSurl` gs
               ON f.file_gdc_id = gs.file_gdc_id 
            {where_clause}
        """

    file_result = query_and_retrieve_result(make_file_pull_list_query())

    file_list = list()

    for row in file_result:

        file_list.append(dict(row))

    return file_list


def create_table_name_from_file_name(file_path: str) -> str:
    file_name = file_path.split("/")[-1]
    table_base_name = "_".join(file_name.split('.')[0:-1])
    table_base_name = table_base_name.replace("-", "_").replace(".", "_")
    table_id = create_dev_table_id(PARAMS, table_base_name)
    table_name = table_id.split('.')[-1]
    table_name = table_name.replace("-", "_").replace(".", "_")

    return table_name


def create_program_tables_dict() -> dict[str, list[str]]:
    prefix = f"{PARAMS['RELEASE']}_TCGA"

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


def build_a_header(all_files: list[str]) -> list[str]:
    header_values = set()

    for filename in all_files:
        with open(filename, 'r', encoding="ISO-8859-1") as readfile:
            row_idx = 0

            for line in readfile:
                if row_idx < PARAMS["HEADER_ROW_IDX"]:
                    row_idx += 1
                    continue
                else:
                    # if we run into one field that is a pure number, it is no longer a header line
                    header_row = set(line.rstrip('\n').split("\t"))
                    header_values = header_values | header_row
                    break

    header_values_list = sorted(list(header_values))

    return header_values_list


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

    program = "TCGA"

    local_program_dir = get_scratch_fp(PARAMS, program)
    local_files_dir = f"{local_program_dir}/files"
    local_schemas_dir = f"{local_program_dir}/schemas"
    file_traversal_list = f"{local_program_dir}/{PARAMS['BASE_FILE_NAME']}_traversal_list_{program}.txt"
    tables_file = f"{local_program_dir}/{PARAMS['RELEASE']}_tables_{program}.txt"

    if 'build_file_list_and_download' in steps:
        # create needed directories if they don't already exist
        logger.info('build_file_pull_list')

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
            gs_uri = file_data['file_gdc_url']
            md5sum = file_data['md5sum']

            file_path = f"{local_files_dir}/{file_name}"

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
                print(f"{gs_uri} request failed")
                file_obj.close()
                os.remove(file_path)
            except Forbidden:
                print(f"{gs_uri} request failed")
                file_obj.close()
                os.remove(file_path)

    if 'combine_project_tsvs' in steps:
        logger.info('combine_project_tsvs')

        file_names = os.listdir(local_files_dir)
        files_by_type = dict()
        table_types = PARAMS['TABLE_TYPES'].keys()

        for table_type in table_types:
            files_by_type[table_type] = list()

        for file_name in file_names:
            file_path = f"{local_files_dir}/{file_name}"

            if 'nte' in file_name:
                files_by_type['nte'].append(file_path)
                continue
            else:
                for table_type in table_types:
                    if table_type in file_name:
                        files_by_type[table_type].append(file_path)
                        continue

        for type, files in files_by_type.items():
            print(type)

            header_line = build_a_header(files)
            print(header_line)

        exit()



        all_tsv_files = list()

        for file_path in all_files:
            tsv_filepath = '.'.join(file_path.split('.')[0:-1])
            tsv_filepath = f"{tsv_filepath}_raw.tsv"

            with open(file_path, 'r', encoding="ISO-8859-1") as tsv_fh:
                lines = tsv_fh.readlines()

            with open(tsv_filepath, 'w') as tsv_fh:
                for line in lines:
                    tsv_fh.write(f"{line.strip()}\n")

            all_tsv_files.append(tsv_filepath)

        with open(file_traversal_list, mode='w') as traversal_list_file:
            for tsv_file in all_tsv_files:
                traversal_list_file.write(f"{tsv_file}\n")

    if 'normalize_tsv_and_create_schema' in steps:
        logger.info(f"upload_tsv_file_and_schema_to_bucket")

        with open(file_traversal_list, mode='r') as traversal_list_file:
            all_files = traversal_list_file.read().splitlines()

        for tsv_file_path in all_files:
            # The TCGA files have a different encoding--so if a file can't be decoded in Unicode format,
            # open the file using ISO-8859-1 encoding instead.
            try:
                with open(tsv_file_path, 'r') as tsv_fh:
                    row_count = len(tsv_fh.readlines())
            except UnicodeDecodeError:
                with open(tsv_file_path, 'r', encoding="ISO-8859-1") as tsv_fh:
                    row_count = len(tsv_fh.readlines())

            if row_count <= 1:
                logger.warning(f"*** probably an issue: row count is {row_count} for {tsv_file_path}")

            bq_column_names = create_bq_column_names(tsv_file=tsv_file_path, header_row_idx=PARAMS['HEADER_ROW_IDX'])

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
        with open(file_traversal_list, mode='r') as traversal_list_file:
            all_files = traversal_list_file.read().splitlines()

        table_list = []

        for tsv_file_path in all_files:
            normalized_tsv_file_path = tsv_file_path.replace("_raw.tsv", ".tsv")
            file_name = normalized_tsv_file_path.split("/")[-1]

            table_name = create_table_name_from_file_name(normalized_tsv_file_path)
            table_id = f"{PARAMS['DEV_PROJECT']}.{PARAMS['DEV_RAW_DATASET']}.{table_name}"

            if program == "TCGA":
                renamed_table = table_name.replace("nationwidechildrens_org", "TCGA")
                table_id = f"{PARAMS['DEV_PROJECT']}.{PARAMS['DEV_RAW_DATASET']}.{renamed_table}"
                print(f"table renamed to: {table_id}")

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

    if 'analyze_tables' in steps:
        column_dict = dict()

        project_dataset_id = "isb-project-zero.clinical_from_files_raw"

        table_list = list_tables_in_dataset(project_dataset_id=project_dataset_id,
                                            filter_terms=f"{PARAMS['RELEASE']}_TCGA")



        """
        for table_type, table_list in tables_by_type.items():
            table_type_column_counts = dict()
            print(table_type)

            for table_name in table_list:
                table_id = f"{project_dataset_id}.{table_name}"
                column_list = get_columns_in_table(table_id=table_id)

                for column in column_list:
                    if column not in table_type_column_counts:
                        table_type_column_counts[column] = 1
                    else:
                        table_type_column_counts[column] += 1

            for column, count in sorted(table_type_column_counts.items(), key=lambda x: x[1], reverse=True):
                print(f"{column}\t{table_type}\t{count}")
        """

        # target_usi: {column: value, ...}

        for table_type, table_list in files_by_type.items():
            #if table_type == 'ablation':
            #    continue

            print(table_type)
            id_key = PARAMS['TABLE_TYPES'][table_type]['id_key']
            records_dict = dict()

            if table_type == "nte":
                continue

            for table in table_list:
                print(table)
                table_id = f"isb-project-zero.clinical_from_files_raw.{table}"

                sql = f"""
                    SELECT DISTINCT * 
                    FROM `{table_id}`
                """

                result = query_and_retrieve_result(sql)

                for row in result:
                    record_dict = dict(row)
                    id_key_value = record_dict.pop(id_key)

                    if id_key_value not in records_dict:
                        records_dict[id_key_value] = dict()

                    for column, value in record_dict.items():
                        if value is None:
                            continue
                        if column not in records_dict[id_key_value]:
                            records_dict[id_key_value][column] = value
                        else:
                            if records_dict[id_key_value][column] != value:
                                old_value = records_dict[id_key_value][column]

                                print(f"{id_key_value}\t{column}\t{old_value}\t{value}")

            new_table_name = f"{PARAMS['RELEASE']}_TCGA_{table_type}"

            record_json_list = list(records_dict.values())

            jsonl_filename = f"{new_table_name}.jsonl"

            write_list_to_jsonl_and_upload(PARAMS,
                                           new_table_name,
                                           record_json_list,
                                           local_filepath=get_scratch_fp(PARAMS, jsonl_filename))

            create_and_upload_schema_for_json(PARAMS,
                                              record_list=record_json_list,
                                              table_name=new_table_name,
                                              include_release=False)

            # Download schema file from Google Cloud bucket
            table_schema = retrieve_bq_schema_object(PARAMS, table_name=new_table_name, include_release=False)

            table_id = f"{PARAMS['DEV_PROJECT']}.{PARAMS['DEV_DATASET']}.{new_table_name}"

            # Load jsonl data into BigQuery table
            create_and_load_table_from_jsonl(PARAMS,
                                             jsonl_file=f"{new_table_name}.jsonl",
                                             table_id=table_id,
                                             schema=table_schema)

        """
        TODO:
        Create merged table.
        Merge in aliquot fields.
        Update field/table metadata.
        Publish.
        Delete working tables.
        """
    end_time = time.time()

    logger.info(f"Script completed in: {format_seconds(end_time - start_time)}")


if __name__ == '__main__':
    main(sys.argv)
