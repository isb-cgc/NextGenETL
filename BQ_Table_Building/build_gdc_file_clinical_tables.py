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

from common_etl.utils import (get_filepath, format_seconds, has_fatal_error, load_config, get_rel_prefix,
                              make_string_bq_friendly, create_and_upload_schema_for_tsv, retrieve_bq_schema_object,
                              create_and_load_table_from_tsv, upload_to_bucket, load_table_from_query, publish_table)

from common_etl.support import (get_the_bq_manifest, build_file_list, build_pull_list_with_bq_public, BucketPuller,
                                bq_harness_with_result)

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


'''
def group_by_suffixes(all_files, file_suffix):
    """
    There are a mixture of files, each with a different schema. Group the files into the different sets
    :param file_suffix:
    :param all_files: todo
    :return:
    """

    print(all_files)
    print(file_suffix)

    full_and_name = []
    names_only = []
    for filename in all_files:
        path, just_name = os.path.split(filename)
        full_and_name.append((filename, just_name))
        names_only.append(just_name)

        print(f"{filename}, {just_name}")

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
'''


def convert_excel_to_tsv(all_files, header_idx):
    """
    Convert excel files to CSV files.
    :param all_files: todo
    :param header_idx:
    :return:
    """

    tsv_files = []

    for file_path in all_files:
        print(file_path)
        tsv_filepath = '.'.join(file_path.split('.')[0:-1])
        tsv_filepath = f"{tsv_filepath}.tsv"

        excel_data = pd.read_excel(io=file_path,
                                   index_col=None,
                                   header=header_idx,
                                   engine='openpyxl')

        print(excel_data)

        if excel_data.size == 0:
            print(f"*** no rows found in excel file: {file_path}; skipping")
            continue

        excel_data.to_csv(tsv_filepath, sep='\t', index=False)

        tsv_files.append(tsv_filepath)

    return tsv_files


def create_bq_column_names(tsv_file, header_row_idx, backup_header_row_idx=None):
    with open(tsv_file) as tsv_fh:
        header_row = tsv_fh.readlines()[header_row_idx].strip().split('\t')

    final_headers = []

    for i in range(0, len(header_row)):
        column_name = header_row[i].strip()
        column_name = make_string_bq_friendly(column_name)
        column_name = column_name.lower()

        if column_name in final_headers:
            has_fatal_error(f"Duplicate column name '{column_name}' at idx {i} \nFile: {tsv_file}")

        final_headers.append(column_name)

    return final_headers


'''
def create_bq_column_names(tsv_file, header_row_idx, backup_header_row_idx=None):
    with open(tsv_file) as tsv_fh:
        lines = tsv_fh.readlines()

    headers = lines[header_row_idx].strip().split('\t')

    if not backup_header_row_idx:
        return headers

    backup_headers = lines[backup_header_row_idx].strip().split('\t')
    final_headers = []

    col_count = len(headers)

    for i in range(0, col_count):
        column_name = headers[i].strip()

        if column_name == 'CDE_ID:':
            column_name = backup_headers[i].strip()

        column_name = make_string_bq_friendly(column_name)
        column_name = column_name.lower()

        if column_name not in final_headers:
            final_headers.append(column_name)
        else:
            error = f"""
            Duplicate column name '{column_name}' at idx {i}
            File: {tsv_file}
            """
            has_fatal_error(error)

    return headers
'''


def create_tsv_with_final_headers(tsv_file, headers, data_start_idx):
    with open(tsv_file, 'r') as tsv_fh:
        lines = tsv_fh.readlines()

    with open(tsv_file, 'w') as tsv_fh:
        header_row = "\t".join(headers)
        tsv_fh.write(f"{header_row}\n")

        for i in range(data_start_idx, len(lines)):
            line = lines[i].strip()
            if not line:
                break
            tsv_fh.write(f"{line}\n")

"""
def convert_tsv_to_obj(tsv_file, header_row_idx, data_start_idx, backup_header_row_idx):
    json_list = []

    with open(tsv_file) as tsv_fh:
        lines = tsv_fh.readlines()
        headers = lines[header_row_idx].strip().split('\t')
        backup_headers = lines[backup_header_row_idx].strip().split('\t')

        row_count = len(lines)
        col_count = len(headers)

        for row_idx in range(data_start_idx, row_count):
            row_dict = {}

            split_row = lines[row_idx].strip().split('\t')

            for i in range(0, col_count):
                column_name = headers[i].strip()

                if column_name == 'CDE_ID:':
                    column_name = backup_headers[i].strip()

                column_name = make_string_bq_friendly(column_name)

                if column_name in row_dict:
                    column_name = backup_headers[i].strip()
                    column_name = make_string_bq_friendly(column_name)

                if column_name not in row_dict:
                    row_dict[column_name] = split_row[i].strip()
                else:
                    has_fatal_error(f"duplicate column name: {column_name}")

            json_list.append(row_dict)

    return json_list
"""

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


def group_by_suffixes(all_files):
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

    local_dir_root = get_filepath(f"{PARAMS['SCRATCH_DIR']}")
    base_file_name = PARAMS['BASE_FILE_NAME']

    for program in programs:
        if 'filters' not in programs[program]:
            has_fatal_error(f"'filters' not in programs section of yaml for {program}")
        if 'header_row_idx' not in programs[program]:
            has_fatal_error(f"'header_row_idx' not in programs section of yaml for {program}")
        if 'data_start_idx' not in programs[program]:
            has_fatal_error(f"'data_start_idx' not in programs section of yaml for {program}")
        if 'file_suffix' not in programs[program]:
            has_fatal_error(f"'file_suffix' not in programs section of yaml for {program}")

        print(f"Running script for {program}")
        local_program_dir = f"{local_dir_root}/{program}"
        local_files_dir = f"{local_program_dir}/files"
        local_schemas_dir = f"{local_program_dir}/schemas"

        if not os.path.exists(local_program_dir):
            os.makedirs(local_program_dir)
        if not os.path.exists(local_files_dir):
            os.makedirs(local_files_dir)
        if not os.path.exists(local_schemas_dir):
            os.makedirs(local_schemas_dir)

        local_pull_list = f"{local_program_dir}/{base_file_name}_pull_list_{program}.tsv"
        file_traversal_list = f"{local_program_dir}/{base_file_name}_traversal_list_{program}.txt"
        tables_file = f"{local_program_dir}/{get_rel_prefix(PARAMS)}_tables_{program}.txt"

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

                    # this is a field description file with very weird formatting (newlines/special formatting
                    # within cells). Doesn't seem like it's worth the trouble to load it.
                    # Will glean the field descriptions from there, however.
                    if program == 'TARGET':
                        if '_CDE_' in line:
                            continue

                    traversal_list.write(f"{line}\n")

        if 'convert_excel_to_csv' in steps:
            print('\nconvert_excel_to_tsv')
            if programs[program]['file_suffix'] == 'xlsx' or programs[program]['file_suffix'] == 'xls':
                with open(file_traversal_list, mode='r') as traversal_list_file:
                    all_files = traversal_list_file.read().splitlines()

                    for excel_file in all_files:
                        upload_to_bucket(BQ_PARAMS, scratch_fp=excel_file, delete_local=False)

                    all_files = convert_excel_to_tsv(all_files=all_files,
                                                     header_idx=programs[program]['header_row_idx'])

                with open(file_traversal_list, mode='w') as traversal_list_file:
                    for line in all_files:
                        traversal_list_file.write(f"{line}\n")

        if 'upload_tsv_file_and_schema_to_bucket' in steps:
            print(f"upload_tsv_file_and_schema_to_bucket")
            with open(file_traversal_list, mode='r') as traversal_list_file:
                all_files = traversal_list_file.read().splitlines()

                header_row_idx = programs[program]['header_row_idx']

            if 'backup_header_row_idx' in programs[program]:
                backup_header_row_idx = programs[program]['backup_header_row_idx']
            else:
                backup_header_row_idx = None

            for tsv_file_path in all_files:
                with open(tsv_file_path) as tsv_fh:
                    row_count = len(tsv_fh.readlines())
                    if row_count <= 1:
                        print(f"*** probably an issue: row count is {row_count} for {tsv_file_path}")

                bq_column_names = create_bq_column_names(tsv_file=tsv_file_path,
                                                         header_row_idx=header_row_idx,
                                                         backup_header_row_idx=backup_header_row_idx)

                create_tsv_with_final_headers(tsv_file=tsv_file_path,
                                              headers=bq_column_names,
                                              data_start_idx=programs[program]['data_start_idx'])

                file_name = tsv_file_path.split("/")[-1]
                table_base_name = "_".join(file_name.split('.')[0:-1])
                table_name = f"{get_rel_prefix(PARAMS)}_{table_base_name}"
                schema_file_name = f"schema_{table_name}.json"
                schema_file_path = f"{local_schemas_dir}/{schema_file_name}"

                create_and_upload_schema_for_tsv(PARAMS, BQ_PARAMS,
                                                 table_name=table_name,
                                                 tsv_fp=tsv_file_path,
                                                 header_row=0,
                                                 skip_rows=1,
                                                 row_check_interval=1,
                                                 schema_fp=schema_file_path,
                                                 delete_local=True)

                upload_to_bucket(BQ_PARAMS, tsv_file_path, delete_local=True)
        if 'build_raw_tables' in steps:
            with open(file_traversal_list, mode='r') as traversal_list_file:
                all_files = traversal_list_file.read().splitlines()

            table_list = []

            for tsv_file_path in all_files:
                file_name = tsv_file_path.split("/")[-1]
                table_base_name = "_".join(file_name.split('.')[0:-1])
                table_name = f"{get_rel_prefix(PARAMS)}_{table_base_name}"
                table_id = f"{BQ_PARAMS['WORKING_PROJECT']}.{BQ_PARAMS['TARGET_RAW_DATASET']}.{table_name}"
                schema_file_name = f"schema_{table_name}.json"

                bq_schema = retrieve_bq_schema_object(PARAMS, BQ_PARAMS,
                                                      table_name=table_name,
                                                      schema_filename=schema_file_name,
                                                      schema_dir=local_schemas_dir)

                print(bq_schema)

                create_and_load_table_from_tsv(BQ_PARAMS,
                                               tsv_file=file_name,
                                               table_id=table_id,
                                               num_header_rows=1,
                                               schema=bq_schema)

                table_list.append(table_id)

            with open(tables_file, 'w') as tables_fh:
                for table_name in table_list:
                    tables_fh.write(f"{table_name}\n")

            print(f"\n\nTables created for {program}:")
            for table in table_list:
                print(table)
            print('\n')

        if 'find_duplicates_in_tables' in steps:
            with open(tables_file, 'r') as tables_fh:
                id_key = programs[program]['id_key']

                table_ids = tables_fh.readlines()
                for table_id in table_ids:
                    query = f"""
                        SELECT {id_key}, COUNT({id_key})
                        FROM {table_id}
                        GROUP BY {id_key}
                        HAVING COUNT({id_key}) > 1
                    """

                    result = bq_harness_with_result(sql=query,
                                                    do_batch=BQ_PARAMS['DO_BATCH'],
                                                    verbose=False)

                    print(result)


        """
        Create merged table.
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
