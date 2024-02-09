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


def make_file_pull_list():
    def make_file_info_query():
        logger = logging.getLogger('base_script')

        if 'FILTERS' not in PARAMS:
            logger.critical(f"No filters provided for {PARAMS['PROGRAM']}, exiting")
            sys.exit(-1)

        where_clause = "WHERE "

        where_clause_strs = list()

        for column_name, column_value in PARAMS['FILTERS'].items():
            where_clause_strs.append(f"{column_name} = '{column_value}'")

        where_clause += " AND ".join(where_clause_strs)

        rel_number = PARAMS['RELEASE'].strip('r')

        return f"""
            SELECT f.file_gdc_id,
               f.file_name,
               f.md5sum,
               f.file_size,
               f.file_state,
               gs.file_gdc_url,
               f.project_short_name
            FROM `isb-project-zero.GDC_metadata.rel{rel_number}_fileData_current` f
            LEFT JOIN `isb-project-zero.GDC_manifests.rel{rel_number}_GDCfileID_to_GCSurl` gs
               ON f.file_gdc_id = gs.file_gdc_id 
            {where_clause}
        """

    file_result = query_and_retrieve_result(make_file_info_query())

    file_list = list()

    for row in file_result:

        file_list.append(dict(row))

    return file_list


def create_table_name_from_file_name(file_path: str) -> str:
    file_name = file_path.split("/")[-1]
    table_base_name = "_".join(file_name.split('.')[0:-1])
    table_base_name = table_base_name.replace("-", "_").replace(".", "_").replace(f"{PARAMS['RELEASE']}_", "")
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

    program = PARAMS['PROGRAM']

    local_program_dir = get_scratch_fp(PARAMS, program)
    local_files_dir = f"{local_program_dir}/files"
    local_concat_dir = f"{local_program_dir}/concat_files"
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
        if not os.path.exists(local_concat_dir):
            logger.info(f"Creating directory {local_concat_dir}")
            os.makedirs(local_concat_dir)
        if not os.path.exists(local_schemas_dir):
            logger.info(f"Creating directory {local_schemas_dir}")
            os.makedirs(local_schemas_dir)

        file_pull_list = make_file_pull_list()

        storage_client = storage.Client()

        for file_data in file_pull_list:
            file_name = file_data['file_name']
            gs_uri = file_data['file_gdc_url']
            md5sum = file_data['md5sum']
            project_short_name = file_data['project_short_name']

            file_path = f"{local_files_dir}/{project_short_name}__{file_name}"

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

        concat_file_paths = list()

        for data_type, files in files_by_type.items():
            logger.info(data_type)
            concat_header_row_list = build_a_header(files)
            concat_header_row_list.append('program')
            concat_header_row_list.append('project_short_name')

            logger.info(concat_header_row_list)

            new_file_path = f"{local_concat_dir}/{PARAMS['RELEASE']}_TCGA_{data_type}_raw.tsv"
            concat_file_paths.append(new_file_path)

            with open(new_file_path, 'w', encoding="ISO-8859-1") as big_tsv_fh:
                header_line = "\t".join(concat_header_row_list)
                big_tsv_fh.write(f"{header_line}\n")

                for file_path in files:
                    with open(file_path, 'r', encoding="ISO-8859-1") as tsv_fh:
                        lines = tsv_fh.readlines()

                        line_cnt = 0

                        for line in lines:
                            if line_cnt == PARAMS['HEADER_ROW_IDX']:
                                header_row_list = line.rstrip('\n').split("\t")
                                line_cnt += 1
                                continue
                            elif line_cnt < PARAMS['DATA_START_IDX']:
                                line_cnt += 1
                                continue

                            record = line.rstrip('\n').split("\t")

                            for column in concat_header_row_list:
                                if column in header_row_list:
                                    value_idx = header_row_list.index(column)
                                    big_tsv_fh.write(f"{record[value_idx]}\t")
                                else:
                                    big_tsv_fh.write("NA\t")

                            # get project_short_name from file path
                            project_short_name = file_path.split('__')[0].split('/')[-1]
                            logger.debug(project_short_name)
                            # add program and project short name to tsv rows
                            big_tsv_fh.write(f"{PARAMS['PROGRAM']}\t")
                            big_tsv_fh.write(f"{project_short_name}\t")
                            big_tsv_fh.write("\n")

        with open(file_traversal_list, mode='w') as traversal_list_file:
            for tsv_file in concat_file_paths:
                traversal_list_file.write(f"{tsv_file}\n")

    if 'normalize_tsv_and_create_schema' in steps:
        logger.info(f"normalize_tsv_and_create_schema")

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

            bq_column_names = create_bq_column_names(tsv_file=tsv_file_path, header_row_idx=0)

            create_tsv_with_final_headers(tsv_file=tsv_file_path,
                                          headers=bq_column_names,
                                          data_start_idx=1)

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

    if 'create_cohort_builder_view' in steps:
        pass

    '''
    if 'output_distinct_values' in steps:
        table_suffixes = ['patient']

        for table_suffix in table_suffixes:
            table_name = f"{PARAMS['RELEASE']}_{PARAMS['PROGRAM']}_{table_suffix}"
            table_id = f"{PARAMS['DEV_PROJECT']}.{PARAMS['DEV_RAW_DATASET']}.{table_name}"

            sql = f"""
                SELECT column_name
                FROM `{PARAMS['DEV_PROJECT']}.{PARAMS['DEV_RAW_DATASET']}`.INFORMATION_SCHEMA.COLUMNS
                WHERE table_name = '{table_name}'
                AND data_type = 'STRING'
            """

            result = query_and_retrieve_result(sql)

            column_list = list()

            for row in result:
                column_list.append(row[0])

            for column in column_list:
                distinct_query = f"""
                SELECT distinct {column}
                FROM {table_id}
                """

                distinct_result = query_and_retrieve_result(distinct_query)

                logger.info(column)

                for row in distinct_result:
                    logger.info(row[0])

                time.sleep(3)

    if 'print_non_null_values' in steps:
        # todo if we use this, should not be hardcoded. Likely candidate for removal.
        sql = f"""
        SELECT *
        FROM `isb-project-zero.clinical_from_files_raw.r36_TCGA_patient`
        WHERE bcr_patient_barcode = 'TCGA-FC-A8O0'
        """

        result = query_and_retrieve_result(sql)
        record = dict()

        for row in result:
            record = dict(row)
            break

        for key, value in record.items():
            if value:
                logger.info(f"{key}: {value}")
    '''

    end_time = time.time()

    logger.info(f"Script completed in: {format_seconds(end_time - start_time)}")


if __name__ == '__main__':
    main(sys.argv)
