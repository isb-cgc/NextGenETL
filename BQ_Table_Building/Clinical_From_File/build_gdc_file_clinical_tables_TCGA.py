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
import sys
import time

from cda_bq_etl.bq_helpers import (create_and_upload_schema_for_tsv, retrieve_bq_schema_object,
                                   create_and_load_table_from_tsv, query_and_retrieve_result, list_tables_in_dataset,
                                   get_columns_in_table)
from cda_bq_etl.gcs_helpers import upload_to_bucket, download_from_bucket, download_from_external_bucket
from cda_bq_etl.data_helpers import initialize_logging, make_string_bq_friendly, write_list_to_tsv, \
    create_normalized_tsv
from cda_bq_etl.utils import format_seconds, get_filepath, load_config, get_scratch_fp, calculate_md5sum, \
    create_dev_table_id

PARAMS = dict()
YAML_HEADERS = ('params', 'steps')


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

    if 'analyze_tables' in steps:
        column_dict = dict()

        project_dataset_id = "isb-project-zero.clinical_from_files_raw"

        table_list = list_tables_in_dataset(project_dataset_id=project_dataset_id,
                                            filter_terms=f"{PARAMS['RELEASE']}_TCGA")

        tables_by_type = dict()
        table_types = PARAMS['TABLE_TYPES'].keys()

        for table_type in table_types:
            tables_by_type[table_type] = list()

        for table_name in table_list:
            if 'nte' in table_name:
                tables_by_type['nte'].append(table_name)
                continue
            else:
                for table_type in table_types:
                    if table_type in table_name:
                        tables_by_type[table_type].append(table_name)
                        continue

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

        for table_type, table_list in tables_by_type.items():
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
                                if isinstance(value, str):
                                    if str(old_value).title() == value.title():
                                        continue

                                if isinstance(value, float) or isinstance(old_value, float):
                                    if float(old_value) == float(value):
                                        continue

                                print(f"{id_key_value}\t{column}\t{old_value}\t{value}")



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
