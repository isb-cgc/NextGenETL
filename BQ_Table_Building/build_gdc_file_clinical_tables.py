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
import sys
import time
import os
import pandas as pd

from google.cloud import storage

from cda_bq_etl.bq_helpers import (create_and_upload_schema_for_tsv, retrieve_bq_schema_object, 
                                   create_and_load_table_from_tsv)
from cda_bq_etl.gcs_helpers import upload_to_bucket, download_from_bucket, download_from_external_bucket
from cda_bq_etl.data_helpers import initialize_logging, make_string_bq_friendly
from cda_bq_etl.utils import format_seconds, get_filepath, load_config, get_scratch_fp

from common_etl.support import (get_the_bq_manifest, build_file_list, build_pull_list_with_bq_public, BucketPuller)

PARAMS = dict()
YAML_HEADERS = ('params', 'programs', 'steps')


def convert_excel_to_tsv(all_files, header_idx):
    """
    Convert excel files to CSV files.
    :param all_files: list of all filepaths
    :param header_idx: header row idx
    :return: list of tsv files
    """
    logger = logging.getLogger('base_script')
    tsv_files = []
    for file_path in all_files:
        logger.info(file_path)
        tsv_filepath = '.'.join(file_path.split('.')[0:-1])
        tsv_filepath = f"{tsv_filepath}.tsv"
        excel_data = pd.read_excel(io=file_path,
                                   index_col=None,
                                   header=header_idx,
                                   engine='openpyxl')

        # get rid of funky newline formatting in headers
        excel_data.columns = excel_data.columns.map(lambda x: x.replace('\r','').replace('\n', ''))
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


def main(args):
    try:
        start_time = time.time()

        global PARAMS
        PARAMS, programs, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        sys.exit(err)
        
    log_file_time = time.strftime('%Y.%m.%d-%H.%M.%S', time.localtime())
    log_filepath = f"{PARAMS['LOGFILE_PATH']}.{log_file_time}"
    logger = initialize_logging(log_filepath)
    
    logger.info(f"GDC clinical file script started at {time.strftime('%x %X', time.localtime())}")

    if not programs:
        logger.critical("Specify program parameters in YAML.")
        sys.exit(-1)

    base_file_name = PARAMS['BASE_FILE_NAME']

    # run steps in order for each program.
    # NOTE: if step fails part way though, you need to manually empty the scratch directory on the VM.
    # I tried to create an automated step for this, but it's complicated to do in Python
    # todo no it isn't, past self. do eet.
    for program in programs:
        # check to make sure required variables exist in yaml config
        if 'filters' not in programs[program]:
            logger.critical(f"'filters' not in programs section of yaml for {program}")
            sys.exit(-1)
        if 'header_row_idx' not in programs[program]:
            logger.critical(f"'header_row_idx' not in programs section of yaml for {program}")
            sys.exit(-1)
        if 'data_start_idx' not in programs[program]:
            logger.critical(f"'data_start_idx' not in programs section of yaml for {program}")
            sys.exit(-1)
        if 'file_suffix' not in programs[program]:
            logger.critical(f"'file_suffix' not in programs section of yaml for {program}")
            sys.exit(-1)

        logger.info(f"Running script for {program}")
        local_program_dir = get_scratch_fp(PARAMS, program)
        local_files_dir = f"{local_program_dir}/files"
        local_schemas_dir = f"{local_program_dir}/schemas"

        # create needed directories if they don't already exist
        if not os.path.exists(local_program_dir):
            logger.info(f"Creating directory {local_program_dir}")
            os.makedirs(local_program_dir)
        else:
            logger.info(f"Directory {local_program_dir} already exists, didn't create.")

        if not os.path.exists(local_files_dir):
            logger.info(f"Creating directory {local_files_dir}")
            os.makedirs(local_files_dir)
        else:
            logger.info(f"Directory {local_files_dir} already exists, didn't create.")

        if not os.path.exists(local_schemas_dir):
            logger.info(f"Creating directory {local_schemas_dir}")
            os.makedirs(local_schemas_dir)
        else:
            logger.info(f"Directory {local_schemas_dir} already exists, didn't create.")

        local_pull_list = f"{local_program_dir}/{base_file_name}_pull_list_{program}.tsv"
        file_traversal_list = f"{local_program_dir}/{base_file_name}_traversal_list_{program}.txt"
        tables_file = f"{local_program_dir}/{PARAMS['RELEASE']}_tables_{program}.txt"

        # the source metadata files have a different release notation (relXX vs rXX)
        rel_no_prefix = PARAMS['RELEASE'].replace('r', '')
        src_table_release = f"{PARAMS['SRC_TABLE_PREFIX']}{rel_no_prefix}"

        manifest_table_name = f"{PARAMS['RELEASE']}_{program}_manifest"
        manifest_table_id = f"{PARAMS['DEV_PROJECT']}.{PARAMS['DEV_DATASET']}.{manifest_table_name}"

        # final_target_table = f"{PARAMS['RELEASE']}_{program}_clin_files"
        if 'build_manifest_from_filters' in steps:
            # Build a file manifest based on fileData table in GDC_metadata (filename, md5, etc)
            # Write to file, create BQ table
            logger.info('build_manifest_from_filters')

            filter_dict = programs[program]['filters']
            file_table_name = f"{src_table_release}_{PARAMS['FILE_TABLE']}"
            file_table_id = f"{PARAMS['DEV_PROJECT']}.{PARAMS['META_DATASET']}.{file_table_name}"
            manifest_file = f"{local_program_dir}/{base_file_name}_{program}.tsv"
            bucket_tsv = f"{PARAMS['WORKING_BUCKET_DIR']}/{src_table_release}_{base_file_name}_{program}.tsv"

            manifest_success = get_the_bq_manifest(file_table=file_table_id,
                                                   filter_dict=filter_dict,
                                                   max_files=None,
                                                   project=PARAMS['DEV_PROJECT'],
                                                   tmp_dataset=PARAMS['DEV_DATASET'],
                                                   tmp_bq=manifest_table_name,
                                                   tmp_bucket=PARAMS['WORKING_BUCKET'],
                                                   tmp_bucket_file=bucket_tsv,
                                                   local_file=manifest_file,
                                                   do_batch=PARAMS['BQ_AS_BATCH'])
            if not manifest_success:
                logger.critical("Failure generating manifest")
                sys.exit(-1)

        if 'build_pull_list' in steps:
            # Build list of file paths in the GDC cloud, create file and bq table
            logger.info('build_pull_list')
            bq_pull_list_table_name = f"{PARAMS['RELEASE']}_{program}_pull_list"
            indexd_table_name = f"{src_table_release}_{PARAMS['INDEXD_TABLE']}"
            indexd_table_id = f"{PARAMS['DEV_PROJECT']}.{PARAMS['MANIFEST_DATASET']}.{indexd_table_name}"

            success = build_pull_list_with_bq_public(manifest_table=manifest_table_id,
                                                     indexd_table=indexd_table_id,
                                                     project=PARAMS['DEV_PROJECT'],
                                                     tmp_dataset=PARAMS['DEV_DATASET'],
                                                     tmp_bq=bq_pull_list_table_name,
                                                     tmp_bucket=PARAMS['WORKING_BUCKET'],
                                                     tmp_bucket_file=PARAMS['BUCKET_PULL_LIST'],
                                                     local_file=local_pull_list,
                                                     do_batch=PARAMS['BQ_AS_BATCH'])
            if not success:
                logger.warning("Build pull list failed")
                return

        if 'download_from_gdc' in steps:
            # download files from gdc buckets and download to local scratch directory
            logger.info('download_from_gdc')
            with open(local_pull_list, mode='r') as pull_list_file:
                pull_list = pull_list_file.read().splitlines()
            logger.info("Preparing to download %s files from buckets\n" % len(pull_list))

            for gs_uri in sorted(pull_list):
                print(gs_uri)

            bp = BucketPuller(10)
            bp.pull_from_buckets(pull_list, local_files_dir)

        if 'build_file_list' in steps:
            # build list of files in local scratch directories
            logger.info('build_file_list')
            all_files = build_file_list(local_files_dir)

            with open(file_traversal_list, mode='w') as traversal_list:
                for line in all_files:
                    # this is a field description file with very weird formatting (newlines/special formatting
                    # within cells). Doesn't seem like it's worth the trouble to load it.
                    # Will glean the field descriptions from there, however.
                    if program == 'TARGET':
                        if '_CDE_' in line:
                            continue
                    traversal_list.write(f"{line}\n")

        if 'convert_excel_to_csv' in steps:
            # If file suffix is xlsx or xls, convert to tsv.
            # Then modify traversal list file to point to the newly created tsv files.
            logger.info('convert_excel_to_tsv')
            if programs[program]['file_suffix'] == 'xlsx' or programs[program]['file_suffix'] == 'xls':
                with open(file_traversal_list, mode='r') as traversal_list_file:
                    all_files = traversal_list_file.read().splitlines()
                    for excel_file in all_files:
                        upload_to_bucket(PARAMS, scratch_fp=excel_file, delete_local=False)
                    all_files = convert_excel_to_tsv(all_files=all_files,
                                                     header_idx=programs[program]['header_row_idx'])
                with open(file_traversal_list, mode='w') as traversal_list_file:
                    for line in all_files:
                        traversal_list_file.write(f"{line}\n")

        if 'upload_tsv_file_and_schema_to_bucket' in steps:
            logger.info(f"upload_tsv_file_and_schema_to_bucket")
            with open(file_traversal_list, mode='r') as traversal_list_file:
                all_files = traversal_list_file.read().splitlines()
                header_row_idx = programs[program]['header_row_idx']

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
                    logger.info(f"*** probably an issue: row count is {row_count} for {tsv_file_path}")
                bq_column_names = create_bq_column_names(tsv_file=tsv_file_path, header_row_idx=header_row_idx)
                create_tsv_with_final_headers(tsv_file=tsv_file_path,
                                              headers=bq_column_names,
                                              data_start_idx=programs[program]['data_start_idx'])
                file_name = tsv_file_path.split("/")[-1]
                table_base_name = "_".join(file_name.split('.')[0:-1])
                table_name = f"{PARAMS['RELEASE']}_{table_base_name}"
                schema_file_name = f"schema_{table_name}.json"
                schema_file_path = f"{local_schemas_dir}/{schema_file_name}"
                create_and_upload_schema_for_tsv(PARAMS, 
                                                 tsv_fp=tsv_file_path, 
                                                 header_row=0, 
                                                 skip_rows=1, 
                                                 schema_fp=schema_file_path)
                upload_to_bucket(PARAMS, tsv_file_path, delete_local=True)

        if 'build_raw_tables' in steps:
            with open(file_traversal_list, mode='r') as traversal_list_file:
                all_files = traversal_list_file.read().splitlines()
            table_list = []
            for tsv_file_path in all_files:
                file_name = tsv_file_path.split("/")[-1]
                table_base_name = "_".join(file_name.split('.')[0:-1])
                table_name = f"{PARAMS['RELEASE']}_{table_base_name}"
                table_id = f"{PARAMS['DEV_PROJECT']}.{PARAMS['DEV_DATASET']}_raw.{table_name}"
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