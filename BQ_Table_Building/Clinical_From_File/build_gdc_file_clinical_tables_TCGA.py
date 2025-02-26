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
import json
import logging
import os
import shutil
import sys
import time

from google.cloud import storage
from google.cloud.exceptions import Forbidden
from google.resumable_media import InvalidResponse

from cda_bq_etl.bq_helpers import (create_and_upload_schema_for_tsv, retrieve_bq_schema_object,
                                   create_and_load_table_from_tsv, query_and_retrieve_result, create_table_from_query,
                                   update_table_schema_from_generic, create_and_load_table_from_jsonl)
from cda_bq_etl.gcs_helpers import upload_to_bucket
from cda_bq_etl.data_helpers import (initialize_logging, make_string_bq_friendly, create_normalized_tsv,
                                     write_list_to_jsonl_and_upload)
from cda_bq_etl.utils import (format_seconds, get_filepath, load_config, get_scratch_fp, calculate_md5sum,
                              create_dev_table_id)

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
            line_list = lines[i].strip().split('\t')

            if not line_list:
                break

            line = "\t".join(line_list)
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
               gs.gdc_file_url_gcs,
               f.project_short_name
            # todo change to published table ids
            FROM `isb-project-zero.cda_gdc_metadata.r{rel_number}_fileData_active` f
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

"""
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
"""


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


def import_column_names() -> list[str]:
    logger = logging.getLogger('base_script')
    column_desc_fp = f"{PARAMS['BQ_REPO']}/{PARAMS['COLUMN_DESCRIPTION_FILEPATH']}"
    column_desc_fp = get_filepath(column_desc_fp)

    if not os.path.exists(column_desc_fp):
        logger.critical("BQEcosystem column description path not found")
        sys.exit(-1)
    with open(column_desc_fp) as column_output:
        descriptions = json.load(column_output)

        return list(descriptions.keys())


def get_table_columns(table_id: str) -> set[str]:
    table_name = table_id.split(".")[-1]
    table_type = "_".join(table_name.split("_")[2:])

    # get all columns from the table
    # check dict for column name. if doesn't exist, add it
    # add the table type

    column_sql = f"""
                    SELECT column_name
                    FROM `{PARAMS['DEV_PROJECT']}.{PARAMS['DEV_RAW_DATASET']}`.INFORMATION_SCHEMA.COLUMNS
                    WHERE table_name = '{table_name}'
                """

    column_results = query_and_retrieve_result(column_sql)

    column_set = set()

    for row in column_results:
        column_set.add(row[0])

    return column_set


def get_raw_table_ids() -> list[str]:
    program = PARAMS['PROGRAM']

    local_program_dir = get_scratch_fp(PARAMS, program)
    file_traversal_list = f"{local_program_dir}/{PARAMS['BASE_FILE_NAME']}_traversal_list_{program}.txt"

    with open(file_traversal_list, mode='r') as traversal_list_file:
        all_files = traversal_list_file.read().splitlines()

    table_list = []

    for tsv_file_path in all_files:
        normalized_tsv_file_path = tsv_file_path.replace("_raw.tsv", ".tsv")
        table_name = create_table_name_from_file_name(normalized_tsv_file_path)
        table_id = f"{PARAMS['DEV_PROJECT']}.{PARAMS['DEV_RAW_DATASET']}.{table_name}"

        table_list.append(table_id)

    return table_list


def get_renamed_table_ids() -> list[str]:
    raw_table_ids = get_raw_table_ids()
    renamed_table_ids = list()

    for raw_table_id in raw_table_ids:
        raw_table_name = raw_table_id.split(".")[-1]
        renamed_table_id = f"{PARAMS['DEV_PROJECT']}.{PARAMS['DEV_RENAMED_DATASET']}.{raw_table_name}"
        renamed_table_ids.append(renamed_table_id)

    return renamed_table_ids


def get_non_null_column_percentages_by_project(table_id: str) -> dict[str, dict[str, float]]:
    logger = logging.getLogger('base_script')

    project_row_count_sql = f"""
        SELECT project_short_name, count(*)
        FROM `{table_id}`
        GROUP BY project_short_name
    """

    project_row_count_result = query_and_retrieve_result(project_row_count_sql)

    project_row_counts = dict()

    for row in project_row_count_result:
        project_row_counts[row[0]] = row[1]

    non_null_percentage_dict = dict()

    logger.info(f"Retrieving column counts for {table_id}")

    table_name = table_id.split(".")[-1]
    dataset_id = ".".join(table_id.split(".")[0:2])

    for project_short_name, project_count in project_row_counts.items():
        column_null_counts_sql = f"""
            WITH null_count_table AS (
                SELECT column_name, COUNT(1) AS nulls_count
                FROM `{table_id}` AS t,
                UNNEST(REGEXP_EXTRACT_ALL(TO_JSON_STRING(t), r'\"(\\w+)\":null')) column_name
                WHERE project_short_name = '{project_short_name}'
                GROUP BY column_name
            ),
            no_null_columns AS (
                SELECT column_name, 0 AS nulls_count
                FROM `{dataset_id}`.INFORMATION_SCHEMA.COLUMNS
                WHERE table_name = '{table_name}'
                AND column_name NOT IN (
                    SELECT column_name 
                    FROM null_count_table
                )
            )
            SELECT * FROM null_count_table
            UNION ALL
            SELECT * FROM no_null_columns
        """

        null_count_result = query_and_retrieve_result(column_null_counts_sql)

        for row in null_count_result:
            column_name = row[0]
            null_count = row[1]

            null_percentage = (null_count / project_count) * 100
            non_null_percentage = round(100 - null_percentage, 2)
            if column_name not in non_null_percentage_dict:
                non_null_percentage_dict[column_name] = dict()

            non_null_percentage_dict[column_name][project_short_name] = non_null_percentage

    return non_null_percentage_dict


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
            gs_uri = file_data['gdc_file_url_gcs']
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
            concat_header_row_list.append('program_name')
            concat_header_row_list.append('project_short_name')

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
                                # create header row list from tsv file
                                header_row_list = line.rstrip('\n').split("\t")
                                line_cnt += 1
                                continue
                            elif line_cnt < PARAMS['DATA_START_IDX']:
                                # skip extra header rows
                                line_cnt += 1
                                continue

                            record = line.rstrip('\n').split("\t")

                            for column in concat_header_row_list:
                                if column in header_row_list:
                                    value_idx = header_row_list.index(column)
                                    big_tsv_fh.write(f"{record[value_idx]}\t")
                                elif column == 'program_name':
                                    big_tsv_fh.write(f"{PARAMS['PROGRAM']}\t")
                                elif column == 'project_short_name':
                                    project_short_name = file_path.split('__')[0].split('/')[-1]
                                    big_tsv_fh.write(f"{project_short_name}\n")
                                else:
                                    big_tsv_fh.write("NA\t")

                            # get project_short_name from file path
                            # add program and project short name to tsv rows

        with open(file_traversal_list, mode='w') as traversal_list_file:
            for tsv_file in concat_file_paths:
                traversal_list_file.write(f"{tsv_file}\n")

    if 'normalize_tsv_and_create_schema' in steps:
        logger.info(f"normalize_tsv_and_create_schema")

        with open(file_traversal_list, mode='r') as traversal_list_file:
            all_files = traversal_list_file.read().rstrip('\n').splitlines()

        for tsv_file_path in all_files:
            logger.debug(tsv_file_path)
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

    if 'build_renamed_tables' in steps:
        for raw_table_id in get_raw_table_ids():
            column_list = list(get_table_columns(raw_table_id))
            first_column_list = list()

            # reorder columns
            for column in PARAMS['COLUMN_ORDERING']:
                if column in column_list:
                    first_column_list.append(column)
                    column_list.remove(column)

            combined_column_list = first_column_list + column_list

            table_name = raw_table_id.split(".")[-1]
            table_type = "_".join(table_name.split("_")[2:])

            select_columns_str = ""

            # alter the column name if necessary
            for column_name in combined_column_list:
                if column_name in PARAMS['COLUMN_RENAMING']:
                    select_columns_str += f"{column_name} AS {PARAMS['COLUMN_RENAMING'][column_name]}, "
                else:
                    select_columns_str += f"{column_name}, "

            # remove trailing comma
            select_columns_str = select_columns_str[:-2]

            sql = f"""
                SELECT 
                    {select_columns_str}
                FROM `{raw_table_id}`
            """

            destination_table_id = f"{PARAMS['DEV_PROJECT']}.{PARAMS['DEV_RENAMED_DATASET']}.{table_name}"
            create_table_from_query(PARAMS, destination_table_id, sql)

            metadata_file_name = PARAMS['TABLE_TYPES'][table_type]['METADATA_FILE_SINGLE_PROGRAM']

            update_table_schema_from_generic(params=PARAMS,
                                             table_id=destination_table_id,
                                             metadata_file=metadata_file_name,
                                             generate_definitions=True)

    if 'build_column_metadata_table' in steps:
        column_metadata_list = list()

        for table_id in get_renamed_table_ids():
            column_set = get_table_columns(table_id)
            table_name = table_id.split(".")[-1]
            table_type = "_".join(table_name.split("_")[2:])

            non_null_by_project_dict = get_non_null_column_percentages_by_project(table_id)

            for column, metadata in non_null_by_project_dict.items():
                for project_short_name, non_null_percent in metadata.items():
                    column_metadata_list.append({
                        'column_name': column,
                        'table_type': table_type,
                        'project_short_name': project_short_name,
                        'non_null_percent': non_null_percent
                    })

        write_list_to_jsonl_and_upload(PARAMS, 'column_metadata', column_metadata_list)
        metadata_table_name = f"{PARAMS['RELEASE']}_{PARAMS['COLUMN_METADATA_TABLE_NAME']}"
        metadata_table_id = f"{PARAMS['DEV_PROJECT']}.{PARAMS['DEV_FINAL_DATASET']}.{metadata_table_name}_all"
        selected_metadata_table_id = f"{PARAMS['DEV_PROJECT']}.{PARAMS['DEV_FINAL_DATASET']}.{metadata_table_name}"

        create_and_load_table_from_jsonl(PARAMS,
                                         jsonl_file=f"column_metadata_{PARAMS['RELEASE']}.jsonl",
                                         table_id=metadata_table_id)

        selected_column_sql = f"""
            WITH combined_projects AS (
                SELECT ANY_VALUE((SELECT AS STRUCT column_name, table_type FROM UNNEST([t]))).*,
                    STRING_AGG(SPLIT(project_short_name, '-')[1], ', ') project_names
                FROM `{metadata_table_id}` t
                GROUP BY TO_JSON_STRING((SELECT AS STRUCT column_name, table_type FROM UNNEST([t])))
            ), highest_non_null AS (
                SELECT column_name, table_type, MAX(non_null_percent) OVER (
                    PARTITION BY column_name, table_type
                ) AS highest_non_null_percent
                FROM `{metadata_table_id}`
                GROUP BY column_name, table_type, non_null_percent
            ), all_null_percents AS (
                SELECT c.column_name, c.table_type, h.highest_non_null_percent, c.project_names
                FROM combined_projects c
                JOIN highest_non_null h 
                    ON c.column_name = h.column_name 
                    AND c.table_type = h.table_type
                GROUP BY c.column_name, c.table_type, h.highest_non_null_percent, c.project_names
                ORDER BY c.column_name
            )

            SELECT * from all_null_percents
            WHERE highest_non_null_percent >= 50
            ORDER BY table_type, column_name
        """

        create_table_from_query(PARAMS, table_id=selected_metadata_table_id, query=selected_column_sql)

    if 'build_distinct_column_values_table' in steps:
        table_column_value_dict = dict()
        for table_type in PARAMS['TABLE_TYPES']:
            table_column_value_dict[table_type] = dict()

            table_type_name = f"{PARAMS['RELEASE']}_{PARAMS['PROGRAM']}_{table_type}"
            table_type_id = f"{PARAMS['DEV_PROJECT']}.{PARAMS['DEV_FINAL_DATASET']}.{table_type_name}"

            sql = f"""
            SELECT * 
            FROM `{table_type_id}`
            """

            result = query_and_retrieve_result(sql=sql)

            for row in result:
                row_dict = dict(row.items())
                for column_name, value in row_dict.items():
                    if column_name not in table_column_value_dict[table_type]:
                        table_column_value_dict[table_type][column_name] = set()
                    if value:
                        table_column_value_dict[table_type][column_name].add(value)

        values_list = list()

        for table_type, column_dict in table_column_value_dict.items():
            for column_name, value_set in column_dict.items():
                distinct_value_count = len(value_set)
                value_str = ""
                if len(value_set) <= 50:
                    for value in sorted(value_set):
                        value_str += f"{value}, "
                    value_str = value_str[:-2]
                else:
                    i = 0
                    value_str = "*** More than 50 distinct values. Example values: "
                    for value in sorted(value_set):
                        value_str += f"{value}, "
                        i += 1
                        if i == 3:
                            break
                    value_str = value_str[:-2]

                value_dict = {
                    "table_type": table_type,
                    "column_name": column_name,
                    "distinct_non_null_value_count": distinct_value_count,
                    "distinct_non_null_values": value_str
                }

                values_list.append(value_dict)

        write_list_to_jsonl_and_upload(PARAMS, 'column_distinct_values', values_list)
        value_table_name = f"{PARAMS['RELEASE']}_{PARAMS['COLUMN_METADATA_TABLE_NAME']}"
        value_table_id = f"{PARAMS['DEV_PROJECT']}.{PARAMS['DEV_FINAL_DATASET']}.{value_table_name}_distinct_values_temp"

        create_and_load_table_from_jsonl(PARAMS,
                                         jsonl_file=f"column_distinct_values_{PARAMS['RELEASE']}.jsonl",
                                         table_id=value_table_id)

        metadata_table_name = f"{PARAMS['RELEASE']}_{PARAMS['COLUMN_METADATA_TABLE_NAME']}"
        selected_metadata_table_id = f"{PARAMS['DEV_PROJECT']}.{PARAMS['DEV_FINAL_DATASET']}.{metadata_table_name}"

        merged_sql = f"""
            SELECT  m.table_type, 
                    m.column_name, 
                    m.highest_non_null_percent, 
                    m.project_names, 
                    v.distinct_non_null_value_count,
                    v.distinct_non_null_values 
            FROM `{selected_metadata_table_id}` m
            JOIN `{value_table_id}` v
            ON m.table_type = v.table_type 
                AND m.column_name = v.column_name
            ORDER BY m.table_type, m.column_name
        """

        merged_table_id = selected_metadata_table_id + "_with_distinct_values"

        create_table_from_query(params=PARAMS, table_id=merged_table_id, query=merged_sql)

        metadata_file_name = PARAMS['COLUMN_TABLE_METADATA_FILE']

        update_table_schema_from_generic(params=PARAMS,
                                         table_id=destination_table_id,
                                         metadata_file=metadata_file_name,
                                         generate_definitions=True)

    if 'build_selected_column_tables' in steps:
        metadata_table_name = f"{PARAMS['RELEASE']}_{PARAMS['COLUMN_METADATA_TABLE_NAME']}"
        selected_metadata_table_id = f"{PARAMS['DEV_PROJECT']}.{PARAMS['DEV_FINAL_DATASET']}.{metadata_table_name}"

        for table_type in PARAMS['TABLE_TYPES']:

            table_type_name = f"{PARAMS['RELEASE']}_{PARAMS['PROGRAM']}_{table_type}"
            table_type_id = f"{PARAMS['DEV_PROJECT']}.{PARAMS['DEV_RENAMED_DATASET']}.{table_type_name}"
            destination_table_id = f"{PARAMS['DEV_PROJECT']}.{PARAMS['DEV_FINAL_DATASET']}.{table_type_name}"

            column_sql = f"""
                SELECT column_name 
                FROM `{selected_metadata_table_id}`
                WHERE table_type = '{table_type}'
            """

            result = query_and_retrieve_result(sql=column_sql)

            column_list = list()

            for row in result:
                column_name = row[0]
                column_list.append(column_name)

            first_column_list = list()

            for column_name in PARAMS['COLUMN_ORDERING']:
                if column_name in column_list:
                    column_list.remove(column_name)
                    first_column_list.append(column_name)

            column_list = first_column_list + column_list

            select_str = "SELECT "

            for column_name in column_list:
                select_str += f"{column_name}, "

            select_str = select_str[:-2]

            destination_table_sql = f"""
                {select_str}
                FROM `{table_type_id}`
            """

            create_table_from_query(PARAMS, table_id=destination_table_id, query=destination_table_sql)

            metadata_file_name = PARAMS['TABLE_TYPES'][table_type]['METADATA_FILE_SINGLE_PROGRAM']

            update_table_schema_from_generic(params=PARAMS,
                                             table_id=destination_table_id,
                                             metadata_file=metadata_file_name,
                                             generate_definitions=True)

    end_time = time.time()

    logger.info(f"Script completed in: {format_seconds(end_time - start_time)}")


if __name__ == '__main__':
    main(sys.argv)
