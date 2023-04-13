import tarfile
import sys
import os
import csv
import shutil

from common_etl.utils import create_and_load_table_from_tsv, create_and_upload_schema_for_tsv, \
    retrieve_bq_schema_object, upload_to_bucket, get_column_list_tsv, aggregate_column_data_types_tsv, \
    resolve_type_conflicts, create_schema_object, create_normalized_tsv, download_from_bucket, get_scratch_fp


def extract_tarfile(src_path, dest_path, print_contents=False, overwrite=False):
    """
    todo
    :param src_path:
    :param dest_path:
    :param print_contents:
    :param overwrite:
    :return:
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


def get_tsv_headers(filepath):
    """
    todo
    :param filepath:
    :return:
    """
    with open(filepath) as file:
        tsv_reader = csv.reader(file, delimiter="\t")

        for row in tsv_reader:
            headers = row
            break

    return headers


def get_data_row_count(filepath):
    """
    todo
    :param filepath:
    :return:
    """
    with open(filepath) as file:
        tsv_reader = csv.reader(file, delimiter="\t")

        row_count = -1

        for row in tsv_reader:
            row_count += 1

    return row_count


def scan_directories_and_create_file_dict(dest_path):
    """
    todo
    :param dest_path:
    :return:
    """
    top_level_dir = os.listdir(dest_path)

    if len(top_level_dir) != 1:
        print("Error: more than one folder in directory")
        exit(0)

    dest_path = f"{dest_path}/{top_level_dir[0]}"

    dir_list = os.listdir(dest_path)
    indices_to_remove = list()

    for idx, directory in enumerate(dir_list):
        if directory[:2] == "__":
            indices_to_remove.append(idx)

    for idx in reversed(indices_to_remove):
        dir_list.pop(idx)

    dir_file_dict = dict()

    for directory in dir_list:
        file_list = os.listdir(f"{dest_path}/{directory}")
        dir_file_dict[directory] = file_list

    return dir_file_dict, dest_path


def scan_directories_and_return_headers(dest_path, dir_file_dict):
    """
    todo
    :param dest_path:
    :param dir_file_dict:
    :return:
    """
    for directory, file_list in dir_file_dict.items():
        print(f"\nFor {directory}:")
        for tsv_file in file_list:
            file_path = f"{dest_path}/{directory}/{tsv_file}"
            headers = get_tsv_headers(file_path)
            print(f" - {tsv_file}: {headers}")


def create_table_name(release, file_name):
    """
    todo
    :param release:
    :param file_name:
    :return:
    """
    file_name = file_name.lower()
    split_file_name = file_name.split(".")

    table_name = f"{release}_"
    table_name += "_".join(split_file_name[:-1])

    return table_name


def main(args):
    api_params = {
        "RELEASE": "2023_03",
    }
    bq_params = {
        "WORKING_BUCKET": "next-gen-etl-scratch",
        "WORKING_BUCKET_DIR": "law/etl/cda_pdc_test",
        "SCRATCH_DIR": "scratch",
        "LOCATION": "US"
    }
    steps = {
        # "normalize_and_upload_tsvs",
        "create_schemas"
    }

    src_path = f"/home/lauren/scratch/cda_pdc/pdc_{api_params['RELEASE']}.tgz"
    dest_path = f"/home/lauren/scratch/cda_pdc/pdc_{api_params['RELEASE']}"

    if "normalize_and_upload_tsvs" in steps:

        if os.path.exists(dest_path):
            shutil.rmtree(dest_path)

        extract_tarfile(src_path, dest_path, overwrite=True)

        dir_file_dict, dest_path = scan_directories_and_create_file_dict(dest_path)
        # scan_directories_and_return_headers(dest_path, dir_file_dict)

        normalized_file_names = list()

        for directory, file_list in dir_file_dict.items():
            local_directory = f"{dest_path}/{directory}"

            for tsv_file in file_list:
                original_tsv_path = f"{local_directory}/{tsv_file}"
                # rename raw file
                raw_tsv_file = f"{api_params['RELEASE']}_raw_{tsv_file}"
                raw_tsv_path = f"{local_directory}/{raw_tsv_file}"

                os.rename(src=original_tsv_path, dst=raw_tsv_path)

                normalized_tsv_file = f"{api_params['RELEASE']}_{tsv_file}"
                normalized_tsv_path = f"{local_directory}/{normalized_tsv_file}"

                # add file to list, used to generate txt list of files for later table creation
                normalized_file_names.append(f"{normalized_tsv_file}\n")

                # create normalized file list
                create_normalized_tsv(raw_tsv_path, normalized_tsv_path)

                # upload raw and normalized tsv files to google cloud storage
                upload_to_bucket(bq_params, raw_tsv_path, delete_local=True)
                upload_to_bucket(bq_params, normalized_tsv_path, delete_local=True)

        index_txt_file_name = f"{api_params['RELEASE']}_file_index.txt"

        with open(index_txt_file_name, mode="w", newline="") as txt_file:
            txt_file.writelines(normalized_file_names)

        upload_to_bucket(bq_params, index_txt_file_name, delete_local=True)

    if "create_schemas" in steps:
        # download index file
        index_txt_file_name = f"{api_params['RELEASE']}_file_index.txt"
        download_from_bucket(bq_params, index_txt_file_name)

        with open(get_scratch_fp(bq_params, index_txt_file_name), mode="r") as index_file:
            file_names = index_file.readlines()

            for tsv_file in file_names:
                print(tsv_file)

    """
    # schema_file_name = "_".join(tsv_file.split(".")[:-1])
    # schema_file_name = f"schema_{schema_file_name}.json"
    # schema_file_path = f"{dest_path}/{directory}/{schema_file_name}"

    create_and_upload_schema_for_tsv(api_params, bq_params, tsv_fp=local_file_path,
                                     header_row=0, skip_rows=1, schema_fp=schema_file_path)
    """
    '''
    for directory, file_list in dir_file_dict.items():
        for tsv_file in file_list:
            """
            schema_file_name = "_".join(tsv_file.split(".")[:-1])
            schema_file_name = f"schema_{schema_file_name}.json"

            schema_object = retrieve_bq_schema_object(api_params,
                                                      bq_params,
                                                      schema_filename=schema_file_name)
            """

            table_name = create_table_name(api_params['RELEASE'], tsv_file)
            table_id = f"isb-project-zero.cda_pdc_test.{table_name}"

            if get_data_row_count(f"{dest_path}/{directory}/{tsv_file}") >= 1:
                create_and_load_table_from_tsv(bq_params,
                                               tsv_file=tsv_file,
                                               table_id=table_id,
                                               num_header_rows=1)

    '''


if __name__ == "__main__":
    main(sys.argv)
