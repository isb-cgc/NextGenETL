import tarfile
import sys
import os
import csv

from common_etl.utils import create_and_load_table_from_tsv, create_and_upload_schema_for_tsv, \
    retrieve_bq_schema_object, upload_to_bucket, get_column_list_tsv, aggregate_column_data_types_tsv, \
    resolve_type_conflicts, create_schema_object


def extract_tarfile(src_path, dest_path, print_contents=False, overwrite=False):
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
    with open(filepath) as file:
        tsv_reader = csv.reader(file, delimiter="\t")

        for row in tsv_reader:
            headers = row
            break

    return headers


def scan_directories_and_create_file_dict(dest_path):
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
    for directory, file_list in dir_file_dict.items():
        print(f"\nFor {directory}:")
        for tsv_file in file_list:
            file_path = f"{dest_path}/{directory}/{tsv_file}"
            headers = get_tsv_headers(file_path)
            print(f" - {tsv_file}: {headers}")


def create_table_name(release, file_name):
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

    # src_path = f"/Users/lauren/PycharmProjects/NextGenETL/temp/CDA_tsvs/PDC/pdc_{api_params['RELEASE']}.tgz"
    # dest_path = f"/Users/lauren/PycharmProjects/NextGenETL/scratch/PDC/pdc_{api_params['RELEASE']}"

    src_path = f"/home/lauren/scratch/cda_pdc/pdc_{api_params['RELEASE']}.tgz"
    dest_path = f"/home/lauren/scratch/cda_pdc/pdc_{api_params['RELEASE']}"

    extract_tarfile(src_path, dest_path, overwrite=True)

    dir_file_dict, dest_path = scan_directories_and_create_file_dict(dest_path)
    # scan_directories_and_return_headers(dest_path, dir_file_dict)

    for directory, file_list in dir_file_dict.items():
        for tsv_file in file_list:
            schema_file_name = "_".join(tsv_file.split(".")[:-1])
            schema_file_name = f"schema_{schema_file_name}.json"

            local_file_path = f"{dest_path}/{directory}/{tsv_file}"
            schema_file_path = f"{dest_path}/{directory}/{schema_file_name}"

            upload_to_bucket(bq_params, local_file_path)

            """
            column_headers = get_column_list_tsv(tsv_fp=local_file_path, header_row_index=0)

            data_types_dict = aggregate_column_data_types_tsv(tsv_fp=local_file_path, column_headers=column_headers, skip_rows=1)
            data_type_dict = resolve_type_conflicts(data_types_dict)
            schema_obj = create_schema_object(column_headers, data_type_dict)
            """

            create_and_upload_schema_for_tsv(api_params, bq_params, tsv_fp=local_file_path,
                                             header_row=0, skip_rows=1, schema_fp=schema_file_path)

    for directory, file_list in dir_file_dict.items():
        for tsv_file in file_list:
            schema_file_name = "_".join(tsv_file.split(".")[:-1])
            schema_file_name = f"schema_{schema_file_name}.json"

            schema_object = retrieve_bq_schema_object(api_params,
                                                      bq_params,
                                                      schema_filename=schema_file_name)

            table_name = create_table_name(api_params['RELEASE'], tsv_file)
            table_id = f"isb-project-zero.cda_pdc_test.{table_name}"
            create_and_load_table_from_tsv(bq_params,
                                           tsv_file=tsv_file,
                                           table_id=table_id,
                                           num_header_rows=1,
                                           schema=schema_object)


if __name__ == "__main__":
    main(sys.argv)
