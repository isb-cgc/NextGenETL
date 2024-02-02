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
                                   create_and_load_table_from_tsv, query_and_retrieve_result, list_tables_in_dataset)
from cda_bq_etl.gcs_helpers import upload_to_bucket, download_from_bucket, download_from_external_bucket
from cda_bq_etl.data_helpers import initialize_logging, make_string_bq_friendly, write_list_to_tsv, \
    create_normalized_tsv
from cda_bq_etl.utils import format_seconds, get_filepath, load_config, get_scratch_fp, calculate_md5sum, \
    create_dev_table_id

PARAMS = dict()
YAML_HEADERS = ('params', 'steps')


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

        file_pull_list = make_file_pull_list(program, PARAMS['FILTERS'])

        print(file_pull_list)
        exit(0)

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
                logger.error(f"{gs_uri} request failed; InvalidResponse")
                file_obj.close()
                os.remove(file_path)
            except Forbidden:
                logger.error(f"{gs_uri} request failed; Forbidden")
                file_obj.close()
                os.remove(file_path)


'''
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

        table_list = list_tables_in_dataset(project_dataset_id="isb-project-zero.clinical_from_files_raw",
                                            filter_terms=f"{PARAMS['RELEASE']}_TARGET")

        print(table_list)
        exit(0)

        table_list = [
            "r36_TARGET_AML_ClinicalData_AML1031_20211201",
            "r36_TARGET_AML_ClinicalData_Discovery_20211201",
            "r36_TARGET_AML_ClinicalData_Validation_20211201",
            "r36_TARGET_AML_ClinicalData_AAML1031_AAML0631_additionalCasesForSortedCellsAndCBExperiment_20220330",
            "r36_TARGET_AML_ClinicalData_LowDepthRNAseq_20220331",
        ]

        records_dict = dict()
        # target_usi: {column: value, ...}

        for table in table_list:
            if 'Supplement' in table or 'CDE' in table:
                continue

            print(table)

            table_id = f"isb-project-zero.clinical_from_files_raw.{table}"
            project = table.split("_")[2]

            sql = f"""
                SELECT DISTINCT * 
                FROM `{table_id}`
            """

            result = query_and_retrieve_result(sql)

            for row in result:
                record_dict = dict(row)
                target_usi = record_dict.pop('target_usi')

                if target_usi not in records_dict:
                    records_dict[target_usi] = dict()

                for column, value in record_dict.items():
                    if value is None:
                        continue
                    if column not in records_dict[target_usi]:
                        records_dict[target_usi][column] = value
                    else:
                        if records_dict[target_usi][column] != value:
                            old_value = records_dict[target_usi][column]
                            if isinstance(value, str):
                                if str(old_value).title() == value.title():
                                    continue

                            if isinstance(value, float) or isinstance(old_value, float):
                                if float(old_value) == float(value):
                                    continue

                            print(f"{target_usi}\t{project}\t{column}\t{records_dict[target_usi][column]}\t{value}")



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
'''

if __name__ == '__main__':
    main(sys.argv)
