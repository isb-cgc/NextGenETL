import tarfile
import sys
import os
import csv
import shutil
from typing import Union

from common_etl.utils import create_and_load_table_from_tsv, create_and_upload_schema_for_tsv, \
    retrieve_bq_schema_object, upload_to_bucket, create_normalized_tsv, download_from_bucket, get_scratch_fp, \
    get_filepath

ParamsDict = dict[str, Union[str, int, dict, list]]


def extract_tarfile(src_path: str, dest_path: str, print_contents: bool = False, overwrite: bool = False):
    """
    Extract CDA .tgz into tsvs.
    :param str src_path: Location of the .tgz file on vm
    :param str dest_path: Location to extract .tgz into
    :param bool print_contents: if True, print contents of archive
    :param bool overwrite: if True, overwrite any existing files in destination path
    """
    tar = tarfile.open(name=src_path, mode="r:gz")

    if print_contents:
        print(f"\nContents of {src_path}:")
        for tar_info in tar:
            if tar_info.isreg():
                print(f"{tar_info.name}, {tar_info.size} bytes")
        print()

    # create file list
    for tar_info in tar:
        if tar_info.isreg():
            if not overwrite and os.path.exists(dest_path + "/" + tar_info.name):
                print(f"file {tar_info.name} already exists in {dest_path}")
                exit(1)

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

        row_count = -1

        for row in tsv_reader:
            row_count += 1

    return row_count


def scan_directories_and_create_file_dict(dest_path: str) -> tuple[dict[str, list], str]:
    """
    Traverse directories and return a list of file names.
    :param str dest_path: parent directory path
    :return: dictionary of subdirectories and file names; path to subdirectory to traverse
    :rtype: tuple[dict[str, list], str]
    """
    top_level_dir = os.listdir(dest_path)

    if len(top_level_dir) != 1:
        print("Error: more than one folder in directory")
        exit(0)

    dest_path = f"{dest_path}/{top_level_dir[0]}"

    dir_list = os.listdir(dest_path)
    indices_to_remove = list()

    # exclude hidden files
    for idx, directory in enumerate(dir_list):
        if directory.startswith('__'):
            indices_to_remove.append(idx)

    for idx in reversed(indices_to_remove):
        dir_list.pop(idx)

    dir_file_dict = dict()

    for directory in dir_list:
        file_list = os.listdir(f"{dest_path}/{directory}")
        dir_file_dict[directory] = file_list

    return dir_file_dict, dest_path


def create_table_name(file_name: str) -> str:
    """
    Create table name by making file name lowercase, removing file extension, and converting any additional "." to "_".
    :param str file_name: File name to convert to table name
    :return: Valid BigQuery table name
    :rtype: str
    """
    file_name = file_name.lower()
    split_file_name = file_name.split(".")
    table_name = "_".join(split_file_name[:-1])

    return table_name


def normalize_files(api_params: ParamsDict, bq_params: ParamsDict, file_list: list[str], dest_path: str) -> list[str]:
    """
    Create new file containing normalized data from raw data file. Cast ints, convert to null and boolean
    where possible.
    :param dict[str, Union[str, int, dict, list]] api_params: API params from YAML config
    :param dict[str, Union[str, int, dict, list]] bq_params: BQ params from YAML config
    :param list[str] file_list: List of files to normalize
    :param str dest_path: Destination path for normalized file creation
    :return: Normalized file name list
    :rtype: list[str]
    """
    normalized_file_names = list()

    for tsv_file in file_list:
        file_type = tsv_file.split(".")[-1]

        if file_type != "tsv":
            continue

        original_tsv_path = f"{dest_path}/{tsv_file}"
        # rename raw file
        raw_tsv_file = f"{api_params['RELEASE']}_raw_{tsv_file}"
        raw_tsv_path = f"{dest_path}/{raw_tsv_file}"

        os.rename(src=original_tsv_path, dst=raw_tsv_path)

        normalized_tsv_file = f"{api_params['RELEASE']}_{tsv_file}"
        normalized_tsv_path = f"{dest_path}/{normalized_tsv_file}"

        # add file to list, used to generate txt list of files for later table creation
        normalized_file_names.append(f"{normalized_tsv_file}\n")

        # create normalized file list
        print(f"\nNormalizing {tsv_file}")
        create_normalized_tsv(raw_tsv_path, normalized_tsv_path)

        # upload raw and normalized tsv files to google cloud storage
        upload_to_bucket(bq_params, raw_tsv_path, delete_local=True, verbose=False)
        upload_to_bucket(bq_params, normalized_tsv_path, delete_local=True, verbose=False)

        print(f"Successfully uploaded raw and normalized {normalized_tsv_file} files to bucket.")

    return normalized_file_names


def get_schema_filename(api_params: ParamsDict, tsv_file_name: str) -> str:
    """
    Create schema file name based on tsv file name.
    :param dict[str, Union[str, int, dict, list]] api_params: API params from YAML config
    :param str tsv_file_name: Source file used in schema creation
    :return: Schema file name
    :rtype: str
    """
    # remove "." from file name, as in PDC
    extension = tsv_file_name.split(".")[-1]
    file_name = "_".join(tsv_file_name.split(".")[:-1])
    tsv_file_name = f"{file_name}.{extension}"

    schema_file_name = "_".join(tsv_file_name.split("_")[2:])
    schema_file_name = schema_file_name.split(".")[0]
    schema_file_name = f"{api_params['RELEASE']}_schema_{schema_file_name}.json"

    return schema_file_name


def main(args):
    source_dc = "pdc"

    if source_dc == "pdc":
        api_params = {
            "RELEASE": "2023_03",
            "LOCAL_TAR_DIR": "scratch/cda_archive_files",
            "LOCAL_EXTRACT_DIR": "cda_pdc",
            "TAR_FILE": "pdc_2023_03.tgz"
        }
        bq_params = {
            "WORKING_BUCKET": "next-gen-etl-scratch",
            "WORKING_BUCKET_DIR": "law/etl/cda_pdc_test",
            "ARCHIVE_BUCKET_PATH": "law/etl/cda_archive_files",
            "SCRATCH_DIR": "scratch",
            "LOCATION": "US",
            "WORKING_PROJECT": "isb-project-zero",
            "WORKING_DATASET": "cda_pdc_test"
        }
        steps = {
            # "download_cda_archive_file",
            # "extract_cda_archive_file",
            # "normalize_and_upload_tsvs",
            "create_schemas",
            "create_tables"
        }
    elif source_dc == "gdc":
        api_params = {
            "RELEASE": "2023_03",
            "LOCAL_TAR_DIR": "scratch/cda_archive_files",
            "LOCAL_EXTRACT_DIR": "cda_gdc",
            "TAR_FILE": "2023_03_gdc_as_extracted.tgz"
        }
        bq_params = {
            "WORKING_BUCKET": "next-gen-etl-scratch",
            "WORKING_BUCKET_DIR": "law/etl/cda_gdc_test",
            "ARCHIVE_BUCKET_PATH": "law/etl/cda_archive_files",
            "SCRATCH_DIR": "scratch",
            "LOCATION": "US",
            "WORKING_PROJECT": "isb-project-zero",
            "WORKING_DATASET": "cda_gdc_test"
        }
        steps = {
            # "download_cda_archive_file",
            "extract_cda_archive_file",
            "normalize_and_upload_tsvs",
            "create_schemas",
            "create_tables"
        }
    else:
        api_params = {
            "RELEASE": "",
            "LOCAL_TAR_DIR": "",
            "TAR_FILE": ""
        }
        bq_params = {
            "WORKING_BUCKET": "next-gen-etl-scratch",
            "WORKING_BUCKET_DIR": "",
            "ARCHIVE_BUCKET_PATH": "law/etl/cda_archive_files",
            "SCRATCH_DIR": "scratch",
            "LOCATION": "US",
            "WORKING_PROJECT": "isb-project-zero",
            "WORKING_DATASET": ""
        }
        steps = {
            "download_and_extract_cda_archive_file",
            "normalize_and_upload_tsvs",
            "create_schemas",
            "create_tables"
        }

    if "download_cda_archive_file" in steps:
        print("\n*** Downloading archive file from bucket!\n")
        local_tar_dir = get_filepath(api_params['LOCAL_TAR_DIR'])

        if not os.path.exists(local_tar_dir):
            os.mkdir(local_tar_dir)

        download_from_bucket(bq_params,
                             bucket_path=bq_params['ARCHIVE_BUCKET_PATH'],
                             filename=api_params['TAR_FILE'],
                             dir_path=local_tar_dir,
                             timeout=30)

    if "extract_cda_archive_file" in steps:
        print("\n*** Extracting archive file!\n")
        local_tar_dir = get_filepath(api_params['LOCAL_TAR_DIR'])

        src_path = f"{local_tar_dir}/{api_params['TAR_FILE']}"
        dest_path = get_filepath(api_params['LOCAL_EXTRACT_DIR'])

        if os.path.exists(dest_path):
            shutil.rmtree(dest_path)

        extract_tarfile(src_path, dest_path, overwrite=True)

    if "normalize_and_upload_tsvs" in steps:
        print("\n*** Normalizing and uploading tsvs!\n")
        dest_path = get_filepath(api_params['LOCAL_EXTRACT_DIR'])

        normalized_file_names = list()

        if source_dc == "pdc":
            dir_file_dict, dest_path = scan_directories_and_create_file_dict(dest_path)

            for directory, file_list in dir_file_dict.items():
                local_directory = f"{dest_path}/{directory}"

                directory_normalized_file_names = normalize_files(api_params,
                                                                  bq_params,
                                                                  file_list=file_list,
                                                                  dest_path=local_directory)

                normalized_file_names.extend(directory_normalized_file_names)
        elif source_dc == "gdc":
            directory = os.listdir(dest_path)
            dest_path += f"/{directory[0]}"
            file_list = os.listdir(dest_path)

            normalized_file_names = normalize_files(api_params, bq_params, file_list=file_list, dest_path=dest_path)

        index_txt_file_name = f"{api_params['RELEASE']}_{source_dc}_file_index.txt"

        with open(index_txt_file_name, mode="w", newline="") as txt_file:
            txt_file.writelines(normalized_file_names)

        upload_to_bucket(bq_params, index_txt_file_name, delete_local=True)

    if "create_schemas" in steps:
        print("\n*** Creating schemas!\n")
        # download index file
        index_txt_file_name = f"{api_params['RELEASE']}_{source_dc}_file_index.txt"
        download_from_bucket(bq_params, index_txt_file_name)

        with open(get_scratch_fp(bq_params, index_txt_file_name), mode="r") as index_file:
            file_names = index_file.readlines()

            for tsv_file_name in file_names:
                tsv_file_name = tsv_file_name.strip()
                download_from_bucket(bq_params, tsv_file_name)

                schema_file_name = get_schema_filename(api_params, tsv_file_name)
                schema_file_path = get_scratch_fp(bq_params, schema_file_name)
                local_file_path = get_scratch_fp(bq_params, tsv_file_name)

                create_and_upload_schema_for_tsv(api_params,
                                                 bq_params,
                                                 tsv_fp=local_file_path,
                                                 header_row=0,
                                                 skip_rows=1,
                                                 schema_fp=schema_file_path,
                                                 delete_local=True)
                os.remove(local_file_path)

    if "create_tables" in steps:
        print("\n*** Creating tables!\n")
        index_txt_file_name = f"{api_params['RELEASE']}_{source_dc}_file_index.txt"
        download_from_bucket(bq_params, index_txt_file_name)

        with open(get_scratch_fp(bq_params, index_txt_file_name), mode="r") as index_file:
            file_names = index_file.readlines()

            for tsv_file_name in file_names:
                tsv_file_name = tsv_file_name.strip()
                tsv_file_path = get_scratch_fp(bq_params, tsv_file_name)
                download_from_bucket(bq_params, tsv_file_name)

                schema_file_name = get_schema_filename(api_params, tsv_file_name)
                schema_object = retrieve_bq_schema_object(api_params, bq_params, schema_filename=schema_file_name)

                table_name = create_table_name(tsv_file_name)
                table_id = f"{bq_params['WORKING_PROJECT']}.{bq_params['WORKING_DATASET']}.{table_name}"

                if get_data_row_count(f"{tsv_file_path}") >= 1:
                    create_and_load_table_from_tsv(bq_params,
                                                   tsv_file=tsv_file_name,
                                                   table_id=table_id,
                                                   num_header_rows=1,
                                                   schema=schema_object)
                else:
                    print(f"No rows found, table not created: {table_id}")

                os.remove(tsv_file_path)

        os.remove(get_scratch_fp(bq_params, index_txt_file_name))


if __name__ == "__main__":
    main(sys.argv)
