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
import logging
import tarfile
import sys
import os
import csv
import shutil
import time

from typing import Union

from cda_bq_etl.gcs_helpers import upload_to_bucket, download_from_bucket, download_from_external_bucket
from cda_bq_etl.utils import get_scratch_fp, load_config, get_filepath, format_seconds, create_dev_table_id
from cda_bq_etl.data_helpers import create_normalized_tsv, initialize_logging
from cda_bq_etl.bq_helpers import retrieve_bq_schema_object, create_and_upload_schema_for_tsv, \
    create_and_load_table_from_tsv, create_table_from_query

PARAMS = dict()
YAML_HEADERS = ('params', 'steps')

ParamsDict = dict[str, Union[str, int, dict, list]]


def extract_tarfile(src_path: str, dest_path: str, print_contents: bool = False, overwrite: bool = False):
    """
    Extract CDA_old .tgz into tsvs.
    :param str src_path: Location of the .tgz file on vm
    :param str dest_path: Location to extract .tgz into
    :param bool print_contents: if True, print contents of archive
    :param bool overwrite: if True, overwrite any existing files in destination path
    """
    tar = tarfile.open(name=src_path, mode="r:gz")

    logger = logging.getLogger('base_script')

    if print_contents:
        logging.info(f"Contents of {src_path}:")
        for tar_info in tar:
            if tar_info.isreg():
                logging.info(f"{tar_info.name}, {tar_info.size} bytes")

    # create file list
    for tar_info in tar:
        if tar_info.isreg():
            if not overwrite and os.path.exists(dest_path + "/" + tar_info.name):
                logger.critical(f"file {tar_info.name} already exists in {dest_path}")
                sys.exit(-1)

    tar.extractall(dest_path)
    tar.close()


def get_data_row_count(filepath: str) -> int:
    """
    Get row count from tsv file.
    :param str filepath: Path to tsv file
    :return: Row count for tsv file
    :rtype: int
    """
    with open(filepath) as file:
        tsv_reader = csv.reader(file, delimiter="\t")
        row_count = sum(1 for _ in tsv_reader) - 1

    return row_count


def scan_directories_and_create_file_dict(dest_path: str) -> tuple[dict[str, list], str]:
    """
    Traverse directories and return a list of file names.
    :param str dest_path: parent directory path
    :return: dictionary of subdirectories and file names; path to subdirectory to traverse
    :rtype: tuple[dict[str, list], str]
    """
    logger = logging.getLogger('base_script')

    top_level_dir = os.listdir(dest_path)
    non_hidden_dir = list()

    for dir_name in top_level_dir:
        if dir_name[0] != '.':
            non_hidden_dir.append(dir_name)

    if len(non_hidden_dir) != 1:
        logger.critical("more than one folder in directory")
        logger.critical(non_hidden_dir)
        sys.exit(-1)

    dest_path = f"{dest_path}/{non_hidden_dir[0]}"

    dir_list = os.listdir(dest_path)
    indices_to_remove = list()

    # exclude hidden files
    for idx, directory in enumerate(dir_list):
        if directory.startswith('__') or directory[0] == '.':
            indices_to_remove.append(idx)

    for idx in reversed(indices_to_remove):
        dir_list.pop(idx)

    dir_file_dict = dict()

    for directory in dir_list:
        file_list = os.listdir(f"{dest_path}/{directory}")

        non_hidden_files = list()

        for file_name in file_list:
            if file_name[0] != '.':
                non_hidden_files.append(file_name)

        non_hidden_files.sort()

        dir_file_dict[directory] = non_hidden_files

    return dir_file_dict, dest_path


def create_table_name(file_name: str) -> str:
    """
    Create table name by making file name lowercase, removing file extension, and converting any additional "." to "_".
    :param str file_name: File name to convert to table name
    :return: Valid BigQuery table name
    :rtype: str
    """
    if PARAMS['NODE'] == 'pdc':
        file_name = file_name.replace(f"{PARAMS['RELEASE']}", "")
        file_name = file_name.lower()
        file_name = PARAMS['RELEASE'] + file_name
    else:
        file_name = file_name.lower()

    split_file_name = file_name.split(".")
    table_name = "_".join(split_file_name[:-1])

    return table_name


def get_normalized_file_names() -> list[str]:
    def delete_empty_tsv_files() -> list[str]:
        new_file_list = list()

        for tsv_file in file_list:
            file_type = tsv_file.split(".")[-1]

            if file_type != "tsv":
                continue

            original_tsv_path = f"{dest_path}/{directory}/{tsv_file}"

            with open(original_tsv_path, 'r') as fp:
                line_count = len(fp.readlines())

                if line_count > 1:
                    new_file_list.append(tsv_file)
                else:
                    logger.info(f"Skipping empty tsv {tsv_file}")

        return new_file_list

    logger = logging.getLogger('base_script')

    dest_path = get_filepath(PARAMS['LOCAL_EXTRACT_DIR'])

    normalized_file_names = list()

    if PARAMS['NODE'] == "pdc":
        dir_file_dict, dest_path = scan_directories_and_create_file_dict(dest_path)

        for directory, file_list in dir_file_dict.items():
            # if directory not in PARAMS['DIRS_TO_KEEP']:
            #    continue
            local_directory = f"{dest_path}/{directory}"

            file_list = delete_empty_tsv_files()

            if file_list:
                directory_normalized_file_names = normalize_files(file_list=file_list, dest_path=local_directory)
                normalized_file_names.extend(directory_normalized_file_names)
    elif PARAMS['NODE'] == "gdc":
        extracted_folder = ".".join(PARAMS['TAR_FILE'].split('.')[:-1])
        dest_path += f"/{extracted_folder}"

        file_list = list()

        for file_name in os.listdir(dest_path):
            if file_name[0] != '.' and file_name[0] != '_':
                file_list.append(file_name)

        file_list.sort()

        normalized_file_names = normalize_files(file_list=file_list, dest_path=dest_path)

    return normalized_file_names


def normalize_files(file_list: list[str], dest_path: str) -> list[str]:
    """
    Create new file containing normalized data from raw data file. Cast ints, convert to null and boolean
    where possible.
    :param list[str] file_list: List of files to normalize
    :param str dest_path: Destination path for normalized file creation
    :return: List of normalized file names
    :rtype: list[str]
    """
    normalized_file_names = list()

    logger = logging.getLogger('base_script')

    for tsv_file in file_list:
        file_type = tsv_file.split(".")[-1]

        if file_type != "tsv":
            continue

        original_tsv_path = f"{dest_path}/{tsv_file}"
        # rename raw file
        raw_tsv_file = f"{PARAMS['RELEASE']}_raw_{tsv_file}"
        raw_tsv_path = f"{dest_path}/{raw_tsv_file}"

        os.rename(src=original_tsv_path, dst=raw_tsv_path)

        normalized_tsv_file = f"{PARAMS['RELEASE']}_{tsv_file}"
        normalized_tsv_path = f"{dest_path}/{normalized_tsv_file}"

        # add file to list, used to generate txt list of files for later table creation
        normalized_file_names.append(f"{normalized_tsv_file}\n")

        # create normalized file list
        logger.info(f"Normalizing {tsv_file}")
        create_normalized_tsv(raw_tsv_path, normalized_tsv_path)

        # upload raw and normalized tsv files to google cloud storage
        upload_to_bucket(PARAMS, raw_tsv_path, delete_local=True, verbose=False)
        upload_to_bucket(PARAMS, normalized_tsv_path, delete_local=True, verbose=False)

        logger.info(f"Successfully uploaded raw and normalized {normalized_tsv_file} files to bucket.")

    return normalized_file_names


def get_schema_filename(tsv_file_name: str) -> str:
    """
    Create schema file name based on tsv file name.
    :param str tsv_file_name: Source file used in schema creation
    :return: Schema file name
    :rtype: str
    """
    logger = logging.getLogger('base_script')

    if PARAMS['NODE'] == 'pdc':
        # formatted like V3_3_Aliquot.aliquot_run_metadata_id.tsv
        # remove "." from file name, as occurs in PDC
        extension = tsv_file_name.split(".")[-1]
        file_name = "_".join(tsv_file_name.split(".")[:-1])
        tsv_file_name = f"{file_name}.{extension}"

        schema_file_name = "_".join(tsv_file_name.split("_")[2:])
        schema_file_name = schema_file_name.split(".")[0]
        schema_file_name = f"{PARAMS['RELEASE']}_schema_{schema_file_name}.json"
    elif PARAMS['NODE'] == 'gdc':
        # formatted like: r37_acl.tsv
        base_file_name = tsv_file_name.split('.')[0]
        base_file_name = base_file_name.replace(f"{PARAMS['RELEASE']}_", "")
        schema_file_name = f"{PARAMS['RELEASE']}_schema_{base_file_name}.json"
    else:
        logger.critical(f"Set up schema filename processing for {PARAMS['NODE']}.")
        sys.exit(-1)

    return schema_file_name


def create_gdc_helper_tables():
    def make_case_project_program_view_query():
        """
        Make SQL query used to create a BigQuery view, merging case ids and barcodes with project and program metadata.
        """
        return f"""
            SELECT 
                case_proj.case_id AS case_gdc_id,
                case_proj.case_id AS case_id,
                c.submitter_id AS case_barcode,
                proj.dbgap_accession_number AS project_dbgap_accession_number,
                proj.project_id, 
                proj.name AS project_name,
                prog.name AS program_name,
                prog.dbgap_accession_number AS program_dbgap_accession_number
            FROM `{create_dev_table_id(PARAMS, 'project')}` proj
            JOIN `{create_dev_table_id(PARAMS, 'project_in_program')}` proj_prog
                ON proj.project_id = proj_prog.project_id
            JOIN `{create_dev_table_id(PARAMS, 'program')}` prog
                ON proj_prog.program_id = prog.program_id
            JOIN `{create_dev_table_id(PARAMS, 'case_in_project')}` case_proj
                ON case_proj.project_id = proj.project_id
            JOIN `{create_dev_table_id(PARAMS, 'case')}` c
                ON c.case_id = case_proj.case_id
        """

    def make_treatment_diagnosis_case_query() -> str:
        return f"""
            SELECT treatment_id, diagnosis_id, case_id
            FROM `{create_dev_table_id(PARAMS, 'treatment_of_diagnosis')}`
            JOIN `{create_dev_table_id(PARAMS, 'diagnosis_of_case')}`
                USING(diagnosis_id)
        """

    def make_pathology_detail_diagnosis_case_query() -> str:
        return f"""
            SELECT pathology_detail_id, diagnosis_id, case_id
            FROM `{create_dev_table_id(PARAMS, 'pathology_detail_of_diagnosis')}`
            JOIN `{create_dev_table_id(PARAMS, 'diagnosis_of_case')}`
                USING(diagnosis_id)
        """

    def make_annotation_diagnosis_case_query() -> str:
        return f"""
            SELECT annotation_id, diagnosis_id, case_id
            FROM `{create_dev_table_id(PARAMS, 'diagnosis_has_annotation')}`
            JOIN `{create_dev_table_id(PARAMS, 'diagnosis_of_case')}`
                USING(diagnosis_id)
        """

    def make_molecular_test_follow_up_case_query() -> str:
        return f"""
            SELECT molecular_test_id, follow_up_id, case_id
            FROM `{create_dev_table_id(PARAMS, 'molecular_test_from_follow_up')}`
            JOIN `{create_dev_table_id(PARAMS, 'follow_up_of_case')}`
                USING(follow_up_id)
        """

    logger = logging.getLogger('base_script')
    logger.info("*** Creating helper tables!")
    logger.info("Making case project program table!")
    create_table_from_query(PARAMS,
                            table_id=create_dev_table_id(PARAMS, 'case_project_program'),
                            query=make_case_project_program_view_query())
    logger.info("Making treatment_diagnosis_case_id_map table!")
    create_table_from_query(PARAMS,
                            table_id=create_dev_table_id(PARAMS, 'treatment_diagnosis_case_id_map'),
                            query=make_treatment_diagnosis_case_query())

    logger.info("Making pathology_detail_diagnosis_case_id_map table!")
    create_table_from_query(PARAMS,
                            table_id=create_dev_table_id(PARAMS, 'pathology_detail_diagnosis_case_id_map'),
                            query=make_pathology_detail_diagnosis_case_query())

    logger.info("Making annotation_diagnosis_case_id_map table!")
    create_table_from_query(PARAMS,
                            table_id=create_dev_table_id(PARAMS, 'annotation_diagnosis_case_id_map'),
                            query=make_annotation_diagnosis_case_query())

    logger.info("Making molecular_test_follow_up_case_id_map table!")
    create_table_from_query(PARAMS,
                            table_id=create_dev_table_id(PARAMS, 'molecular_test_follow_up_case_id_map'),
                            query=make_molecular_test_follow_up_case_query())


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

    index_txt_file_name = f"{PARAMS['RELEASE']}_{PARAMS['NODE']}_file_index.txt"

    if "download_cda_archive_file" in steps:
        logger.info("*** Downloading archive file from bucket!")
        local_tar_dir = get_filepath(PARAMS['LOCAL_TAR_DIR'])

        if not os.path.exists(local_tar_dir):
            os.mkdir(local_tar_dir)

        download_from_external_bucket(uri_path=PARAMS['BLOB_URI_PATH'], dir_path=PARAMS['LOCAL_TAR_DIR'],
                                      filename=PARAMS['TAR_FILE'], project=PARAMS['CDA_BUCKET_PROJECT'])

    if "extract_cda_archive_file" in steps:
        logger.info("*** Extracting archive file!")
        local_tar_dir = get_filepath(PARAMS['LOCAL_TAR_DIR'])

        src_path = f"{local_tar_dir}/{PARAMS['TAR_FILE']}"
        dest_path = get_filepath(PARAMS['LOCAL_EXTRACT_DIR'])

        if os.path.exists(dest_path):
            shutil.rmtree(dest_path)

        extract_tarfile(src_path, dest_path, overwrite=True)

    if "normalize_and_upload_tsvs" in steps:
        logger.info("*** Normalizing and uploading tsvs!")

        normalized_file_names = get_normalized_file_names()

        with open(index_txt_file_name, mode="w", newline="") as txt_file:
            txt_file.writelines(normalized_file_names)

        upload_to_bucket(PARAMS, index_txt_file_name, delete_local=True)

    if "create_schemas" in steps:
        logger.info("*** Creating schemas!")
        # download index file
        download_from_bucket(PARAMS, index_txt_file_name)

        with open(get_scratch_fp(PARAMS, index_txt_file_name), mode="r") as index_file:
            file_names = index_file.readlines()

            file_names.sort()

            for tsv_file_name in file_names:
                tsv_file_name = tsv_file_name.strip()
                download_from_bucket(PARAMS, tsv_file_name)

                schema_file_name = get_schema_filename(tsv_file_name)
                schema_file_path = get_scratch_fp(PARAMS, schema_file_name)
                local_file_path = get_scratch_fp(PARAMS, tsv_file_name)

                create_and_upload_schema_for_tsv(PARAMS,
                                                 tsv_fp=local_file_path,
                                                 header_row=0,
                                                 skip_rows=1,
                                                 schema_fp=schema_file_path,
                                                 delete_local=True)
                os.remove(local_file_path)

    if "create_tables" in steps:
        logger.info("*** Creating tables!")
        download_from_bucket(PARAMS, index_txt_file_name)

        with open(get_scratch_fp(PARAMS, index_txt_file_name), mode="r") as index_file:
            file_names = index_file.readlines()

            file_names.sort()

            for tsv_file_name in file_names:
                tsv_file_name = tsv_file_name.strip()
                tsv_file_path = get_scratch_fp(PARAMS, tsv_file_name)
                download_from_bucket(PARAMS, tsv_file_name)

                schema_file_name = get_schema_filename(tsv_file_name)
                schema_object = retrieve_bq_schema_object(PARAMS, schema_filename=schema_file_name)

                table_name = create_table_name(tsv_file_name)
                table_id = f"{PARAMS['DEV_PROJECT']}.{PARAMS['DEV_RAW_DATASET']}.{table_name}"

                if get_data_row_count(f"{tsv_file_path}") >= 1:
                    create_and_load_table_from_tsv(PARAMS,
                                                   tsv_file=tsv_file_name,
                                                   table_id=table_id,
                                                   num_header_rows=1,
                                                   schema=schema_object)
                else:
                    logger.info(f"No rows found, table not created: {table_id}")

                os.remove(tsv_file_path)

        os.remove(get_scratch_fp(PARAMS, index_txt_file_name))

    if "create_helper_tables" in steps:
        if PARAMS['NODE'] == 'gdc':
            create_gdc_helper_tables()

    end_time = time.time()

    logger.info(f"Script completed in: {format_seconds(end_time - start_time)}")


if __name__ == "__main__":
    main(sys.argv)
