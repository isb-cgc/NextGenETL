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
                              load_table_from_query, publish_table, get_scratch_fp)

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
                            key_dict[cols_for_file[i]] = "" if split_line[i] in PARAMS['NO_DATA_VALUES'] else split_line[i]

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
        with open(filename, 'r', encoding="ISO-8859-1") as readfile: # Having a problem with UTF-8
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


def group_by_suffixes(all_files):
    """
    There are a mixture of files, each with a different schema. Group the files into the different sets
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

    path_suff = []
    for tup in full_and_name:
        path_suff.append((tup[0], tup[1][len(prefix):]))

    path_group = []
    groups = set()
    p = re.compile('(^.*)_[a-z]+\.txt')
    for tup in path_suff:
        m = p.match(tup[1])
        group = m.group(1)
        path_group.append((tup[0], group))
        groups.add(group)

    files_by_group = {}

    for file_tup in path_group:
        if file_tup[1] not in files_by_group:
            files_by_group[file_tup[1]] = []
        files_by_group[file_tup[1]].append(file_tup[0])

    return files_by_group


def convert_excel_to_csv(all_files, local_files_dir):
    """
    Convert excel files to CSV files.
    :param all_files: todo
    :param local_files_dir:
    :return:
    """
    for filename in all_files:
        print(filename)
        page_dict = pd.read_excel(filename, None)
        print(page_dict.keys())
        _, just_name = os.path.split(filename)
        for k, v in page_dict.items():
            print(f"{local_files_dir}/{k}-{just_name}.tsv \n{v}")
            #v.to_csv("{}/{}-{}.tsv".format(local_files_dir, k, just_name), sep = "\t", index = None, header=True)


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

    local_files_dir = get_filepath(PARAMS['LOCAL_FILES_DIR'])  # todo

    for program in programs:
        one_big_tsv = get_scratch_fp(PARAMS, f"{PARAMS['ONE_BIG_TSV_PREFIX']}_{program}.tsv", )
        manifest_file = get_scratch_fp(PARAMS, f"{PARAMS['MANIFEST_FILE_PREFIX']}_{program}.tsv", )
        local_pull_list = get_scratch_fp(PARAMS, f"{PARAMS['LOCAL_PULL_LIST_PREFIX']}_{program}.tsv", )
        file_traversal_list = get_scratch_fp(PARAMS, f"{PARAMS['FILE_TRAVERSAL_LIST_PREFIX']}_{program}.txt", )
        bucket_tsv = f"{BQ_PARAMS['FILE_TABLE_PREFIX']}_{BQ_PARAMS['BUCKET_TSV_PREFIX']}_{program}.tsv"

        if 'build_manifest_from_filters' in steps:
            print('build_manifest_from_filters')
            filter_dict = None  # todo

            file_table_name = f"{BQ_PARAMS['FILE_TABLE_PREFIX']}{PARAMS['RELEASE']}_{BQ_PARAMS['FILE_DATA_SUFFIX']}"
            file_table_id = f"{BQ_PARAMS['WORKING_PROJECT']}.{BQ_PARAMS['META_DATASET']}.{file_table_name}"

            manifest_success = get_the_bq_manifest(file_table=file_table_id,
                                                   filter_dict=filter_dict,
                                                   max_files=None,
                                                   project=BQ_PARAMS['WORKING_PROJECT'],
                                                   tmp_dataset=BQ_PARAMS['TARGET_DATASET'],
                                                   tmp_bq=BQ_PARAMS['BQ_MANIFEST_TABLE'],
                                                   tmp_bucket=PARAMS['WORKING_BUCKET'],
                                                   tmp_bucket_file=bucket_tsv,
                                                   local_file=manifest_file,
                                                   do_batch=BQ_PARAMS['BQ_AS_BATCH'])
            if not manifest_success:
                has_fatal_error("Failure generating manifest")

        if 'download_from_gdc' in steps:
            print('download_from_gdc')
            with open(local_pull_list, mode='r') as pull_list_file:
                pull_list = pull_list_file.read().splitlines()
            print("Preparing to download %s files from buckets\n" % len(pull_list))
            bp = BucketPuller(10)
            bp.pull_from_buckets(pull_list, local_files_dir)

        if 'build_file_list' in steps:
            print('build_file_list')
            all_files = build_file_list(local_files_dir)
            with open(file_traversal_list, mode='w') as traversal_list:
                for line in all_files:
                    traversal_list.write("{}\n".format(line))

        if 'group_by_type' in steps:
            print('group_by_type')
            print(file_traversal_list)
            with open(file_traversal_list, mode='r') as traversal_list_file:
                all_files = traversal_list_file.read().splitlines()
            group_dict = group_by_suffixes(all_files) # WRITE OUT AS JSON!!

        if 'convert_excel_to_csv' in steps:
            print('convert_excel_to_csv')
            with open(file_traversal_list, mode='r') as traversal_list_file:
                all_files = traversal_list_file.read().splitlines()
            convert_excel_to_csv(all_files, local_files_dir)

        if 'concat_all_files' in steps:
            print('concat_all_files')
            for k, v in group_dict.items():
                concat_all_files(v, one_big_tsv.format(k))


if __name__ == '__main__':
    main(sys.argv)
