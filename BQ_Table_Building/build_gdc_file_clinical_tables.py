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
import json
import re
import os
import pandas as pd

from common_etl.utils import (get_filepath, format_seconds, get_graphql_api_response, has_fatal_error, load_config,
                              load_table_from_query, publish_table, get_scratch_fp, get_rel_prefix,
                              make_string_bq_friendly)

from common_etl.support import (get_the_bq_manifest, confirm_google_vm, create_clean_target, generic_bq_harness,
                                build_file_list, upload_to_bucket, csv_to_bq, build_pull_list_with_bq_public,
                                BucketPuller, build_combined_schema, delete_table_bq_job, install_labels_and_desc,
                                update_schema_with_dict, generate_table_detail_files, publish_table)

PARAMS = dict()
BQ_PARAMS = dict()
YAML_HEADERS = ('params', 'bq_params', 'programs', 'steps')


def concat_all_files(all_files, one_big_tsv):
    """
    Concatenate all files. Gather up all files and glue them into one big one. We also add columns for the
    `source_file_name` and `source_file_id` (which is the name of the directory it is in).
    WARNING! Currently hardwired to CNV file heading!
    :param all_files: todo
    :param one_big_tsv:
    """
    print(f"building {one_big_tsv}")

    all_fields, per_file = build_a_header(all_files)
    saf = sorted(all_fields)

    with open(one_big_tsv, 'w') as outfile:
        outfile.write('\t'.join(saf))
        outfile.write('\n')

        for filename in all_files:
            key_dict = {}
            skipping = True
            with open(filename, 'r', encoding="ISO-8859-1") as readfile:  # Having a problem with UTF-8
                cols_for_file = per_file[filename]
                for line in readfile:
                    split_line = line.rstrip('\n').split("\t")
                    if split_line[0].startswith("CDE_ID"):
                        skipping = False
                        continue
                    if not skipping:
                        for i in range(len(split_line)):
                            key_dict[cols_for_file[i]] = "" if split_line[i] in PARAMS['NO_DATA_VALUES'] else \
                                split_line[i]

                    write_line = []
                    for col in saf:
                        if col in key_dict:
                            write_line.append(key_dict[col])
                        else:
                            write_line.append("")
                    outfile.write('\t'.join(write_line))
                    outfile.write('\n')


def build_a_header(all_files):
    """
    Build a header for the bioclin files.
    :param all_files: todo
    :return:
    """
    all_fields = set()
    per_file = {}
    for filename in all_files:
        per_file[filename] = []
        with open(filename, 'r', encoding="ISO-8859-1") as readfile:  # Having a problem with UTF-8
            header_lines = []
            for line in readfile:
                # if we run into one field that is a pure number, it is no longer a header line
                split_line = line.rstrip('\n').split("\t")
                header_lines.append(split_line)
                if split_line[0].startswith("CDE_ID"):
                    for i in range(len(split_line)):
                        if split_line[i] == 'CDE_ID:':
                            per_file[filename].append(header_lines[0][i])
                        else:
                            per_file[filename].append(split_line[i])
                    all_fields.update(per_file[filename])
                    break

    return all_fields, per_file


def group_by_suffixes(all_files, file_suffix):
    """
    There are a mixture of files, each with a different schema. Group the files into the different sets
    :param file_suffix:
    :param all_files: todo
    :return:
    """

    full_and_name = []
    names_only = []
    for filename in all_files:
        path, just_name = os.path.split(filename)
        full_and_name.append((filename, just_name))
        names_only.append(just_name)

    prefix = longest_common_prefix(names_only)

    path_suffix = []
    for tup in full_and_name:
        path_suffix.append((tup[0], tup[1][len(prefix):]))

    path_group = []
    groups = set()
    p = re.compile(rf"(^.*)_[a-z]+\.{file_suffix}")

    for tup in path_suffix:
        match = p.match(tup[1])

        group = match.group(1)
        path_group.append((tup[0], group))
        groups.add(group)

    files_by_group = {}

    for file_tup in path_group:
        if file_tup[1] not in files_by_group:
            files_by_group[file_tup[1]] = []
        files_by_group[file_tup[1]].append(file_tup[0])

    return files_by_group


def convert_excel_to_tsv(all_files, local_files_dir, header_idx):
    """
    Convert excel files to CSV files.
    :param all_files: todo
    :param local_files_dir:
    :return:
    """

    tsv_files = []

    for filename in all_files:
        print(filename)
        tsv_filename = '.'.join(filename.split('.')[0:-1])
        tsv_filename = f"{tsv_filename}.tsv"

        excel_data = pd.read_excel(io=filename,
                                   index_col=None,
                                   header=header_idx,
                                   engine='openpyxl')
        excel_data.to_csv(tsv_filename, sep='\t', index=False)

        tsv_files.append(tsv_filename)

    return tsv_files


def convert_tsvs_to_merged_jsonl(all_files, header_row_idx, data_start_idx):
    for tsv_file in all_files:
        json_list = []
        with open(tsv_file) as tsv_fh:
            lines = tsv_fh.readlines()
            headers = lines[header_row_idx].strip().split('\t')

            row_count = len(lines)
            col_count = len(headers)

            for row_idx in range(data_start_idx, row_count):
                row_dict = {}

                split_row = lines[row_idx].strip().split('\t')

                for i in range(0, col_count):
                    column_name = make_string_bq_friendly(headers[i])
                    row_dict[column_name] = split_row[i]

                json_list.append(row_dict)

        for row in json_list:
            print(row)
        exit()


def longest_common_prefix(str1):
    """
    Hat tip to: https://www.w3resource.com/python-exercises/basic/python-basic-1-exercise-70.php
    :param str1: todo
    :return:
    """
    if not str1:
        return ""

    short_str = min(str1, key=len)

    for i, char in enumerate(short_str):
        for other in str1:
            if other[i] != char:
                return short_str[:i]

    return short_str


def main(args):
    start_time = time.time()
    print(f"GDC clinical file script started at {time.strftime('%x %X', time.localtime())}")

    steps = None

    try:
        global PARAMS, BQ_PARAMS
        PARAMS, BQ_PARAMS, programs, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        has_fatal_error(err, ValueError)

    if not programs:
        has_fatal_error("Specify program parameters in YAML.")

    local_files_dir_root = get_filepath(f"{PARAMS['SCRATCH_DIR']}/{PARAMS['LOCAL_FILES_DIR']}")
    base_file_name = PARAMS['BASE_FILE_NAME']

    for program in programs:
        print(f"Running script for {program}")
        local_program_dir = f"{local_files_dir_root}/{program}"
        local_files_dir = f"{local_program_dir}/files"

        if not os.path.exists(local_program_dir):
            os.makedirs(local_program_dir)
        if not os.path.exists(local_files_dir):
            os.makedirs(local_files_dir)

        local_pull_list = f"{local_program_dir}/{base_file_name}_pull_list_{program}.tsv"
        file_traversal_list = f"{local_program_dir}/{base_file_name}_traversal_list_{program}.txt"

        # the source metadata files have a different release notation (relXX vs rXX)
        src_table_release = f"{BQ_PARAMS['SRC_TABLE_PREFIX']}{PARAMS['RELEASE']}"

        # final_target_table = f"{get_rel_prefix(PARAMS)}_{program}_clin_files"

        if 'build_manifest_from_filters' in steps:
            # Build a file manifest based on fileData table in GDC_metadata (filename, md5, etc)
            # Write to file, create BQ table
            print('\nbuild_manifest_from_filters')
            filter_dict = programs[program]['filters']

            file_table_name = f"{src_table_release}_{BQ_PARAMS['FILE_TABLE']}"
            file_table_id = f"{BQ_PARAMS['WORKING_PROJECT']}.{BQ_PARAMS['META_DATASET']}.{file_table_name}"

            manifest_file = f"{local_program_dir}/{base_file_name}_{program}.tsv"

            bucket_tsv = f"{PARAMS['WORKING_BUCKET_DIR']}/{src_table_release}_{base_file_name}_{program}.tsv"
            manifest_table_name = f"{get_rel_prefix(PARAMS)}_{program}_manifest"
            manifest_table_id = f"{BQ_PARAMS['WORKING_PROJECT']}.{BQ_PARAMS['TARGET_DATASET']}.{manifest_table_name}"

            manifest_success = get_the_bq_manifest(file_table=file_table_id,
                                                   filter_dict=filter_dict,
                                                   max_files=None,
                                                   project=BQ_PARAMS['WORKING_PROJECT'],
                                                   tmp_dataset=BQ_PARAMS['TARGET_DATASET'],
                                                   tmp_bq=manifest_table_name,
                                                   tmp_bucket=PARAMS['WORKING_BUCKET'],
                                                   tmp_bucket_file=bucket_tsv,
                                                   local_file=manifest_file,
                                                   do_batch=BQ_PARAMS['BQ_AS_BATCH'])
            if not manifest_success:
                has_fatal_error("Failure generating manifest")

        if 'build_pull_list' in steps:
            # Build list of file paths in the GDC cloud, create file and bq table
            print('\nbuild_pull_list')

            bq_pull_list_table_name = f"{get_rel_prefix(PARAMS)}_{program}_pull_list"
            indexd_table_name = f"{src_table_release}_{BQ_PARAMS['INDEXD_TABLE']}"
            indexd_table_id = f"{BQ_PARAMS['WORKING_PROJECT']}.{BQ_PARAMS['MANIFEST_DATASET']}.{indexd_table_name}"

            success = build_pull_list_with_bq_public(manifest_table=manifest_table_id,
                                                     indexd_table=indexd_table_id,
                                                     project=BQ_PARAMS['WORKING_PROJECT'],
                                                     tmp_dataset=BQ_PARAMS['TARGET_DATASET'],
                                                     tmp_bq=bq_pull_list_table_name,
                                                     tmp_bucket=PARAMS['WORKING_BUCKET'],
                                                     tmp_bucket_file=PARAMS['BUCKET_PULL_LIST'],
                                                     local_file=local_pull_list,
                                                     do_batch=BQ_PARAMS['BQ_AS_BATCH'])
            if not success:
                print("Build pull list failed")
                return

        if 'download_from_gdc' in steps:
            # download files and pull
            print('\ndownload_from_gdc')
            with open(local_pull_list, mode='r') as pull_list_file:
                pull_list = pull_list_file.read().splitlines()
            print("Preparing to download %s files from buckets\n" % len(pull_list))
            bp = BucketPuller(10)
            bp.pull_from_buckets(pull_list, local_files_dir)

        if 'build_file_list' in steps:
            print('\nbuild_file_list')
            all_files = build_file_list(local_files_dir)

            with open(file_traversal_list, mode='w') as traversal_list:
                for line in all_files:
                    traversal_list.write("{}\n".format(line))

        if 'convert_excel_to_csv' in steps:
            if programs[program]['file_suffix'] == 'xlsx' or programs[program]['file_suffix'] == 'xls':

                with open(file_traversal_list, mode='r') as traversal_list_file:
                    all_files = traversal_list_file.read().splitlines()
                    all_files = convert_excel_to_tsv(all_files=all_files,
                                                     local_files_dir=local_files_dir,
                                                     header_idx=programs[program]['header_row_idx'])

        if 'convert_tsvs_to_merged_jsonl' in steps:
            with open(file_traversal_list, mode='r') as traversal_list_file:
                all_files = traversal_list_file.read().splitlines()

            if programs[program]['file_suffix'] == 'xlsx':
                all_tsv_files = []
                for file_name in all_files:
                    tsv_filename = '.'.join(file_name.split('.')[0:-1])
                    tsv_filename = f"{tsv_filename}.tsv"
                    all_tsv_files.append(tsv_filename)
                all_files = all_tsv_files

            convert_tsvs_to_merged_jsonl(all_files,
                                         programs[program]['header_row_idx'],
                                         programs[program]['data_start_idx'])

        """
        I'm going to handle this differently. 
        Not going to try to group by type, since this isn't actually relevant to TARGET as far as I can tell.
        Will merge the files into one giant jsonl file instead. Okay if the files have different schemas, then.
        If file_suffix == xlsx, then convert excel to csv before merging files.
        Merge files.
        Infer schema and upload file and schema to bucket.
        Create BQ table.
        Merge in aliquot fields.
        Update field/table metadata.
        Publish.
        Delete working tables.
        """

        """        

        if 'concat_all_files' in steps:
            print('concat_all_files')
            one_big_tsv = get_scratch_fp(PARAMS, f"{base_file_name}_data_{program}.tsv", )

            for k, v in group_dict.items():
                concat_all_files(v, one_big_tsv.format(k))
        """


if __name__ == '__main__':
    main(sys.argv)
