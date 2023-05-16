"""

Copyright 2019, Institute for Systems Biology

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""

'''
Make sure the VM has BigQuery and Storage Read/Write permissions!
'''

import sys
import os
from os.path import expanduser
import yaml
import io
from json import loads as json_loads
from types import SimpleNamespace

from common_etl.utils import check_value_type, get_column_list_tsv, \
                             aggregate_column_data_types_tsv, resolve_type_conflicts

from common_etl.support import update_dir_from_git, get_the_bq_manifest, confirm_google_vm, create_clean_target, \
                               generic_bq_harness, build_file_list, pull_from_buckets, upload_to_bucket, csv_to_bq,\
                               build_pull_list_with_bq, create_schema_hold_list, update_schema_tags,\
                               write_table_schema_with_generic, publish_tables_and_update_schema

def load_config(yaml_config):
    """
    The configuration reader. Parses the YAML configuration into dictionaries

    :param yaml_config: Read in YAML file
    :type yaml_config: TextIO
    :return: Dictionaries with Configurations in YAML file
    :rtype: dict
    """
    yaml_dict = None
    config_stream = io.StringIO(yaml_config)
    try:
        yaml_dict = yaml.load(config_stream, Loader=yaml.FullLoader)
    except yaml.YAMLError as ex:
        print(ex)

    if yaml_dict is None:
        return None, None

    return yaml_dict['files_and_buckets_and_tables'], yaml_dict['steps']

def concat_all_files(all_files, one_big_tsv):
    """
    Concatenate all Files. Gather up all files and glue them into one big one. We also add columns for the
    `source_file_name` and `source_file_id` (which is the name of the directory it is in).
    WARNING! Currently hardwired to CNV file heading!

    :param all_files: List of all files to be glued together
    :type all_files: list
    :param one_big_tsv: Name of glued together output file
    :type one_big_tsv: str
    """
    print(f"building {one_big_tsv}")
    first = True
    with open(one_big_tsv, 'w') as outfile:
        for filename in all_files:
            with open(filename, 'r') as readfile:
                norm_path = os.path.normpath(filename)
                path_pieces = norm_path.split(os.sep)
                file_name = path_pieces[-1]
                gdc_id = path_pieces[-2]
                file_name_pieces = file_name.split('.')
                aliquot_gdc_id = file_name_pieces[1]
                for line in readfile:
                    if not line.startswith('gene_id') or first:
                        outfile.write('GDC_Aliquot' if first else aliquot_gdc_id)
                        outfile.write('\t')
                        outfile.write(line.rstrip('\n'))
                        outfile.write('\t')
                        outfile.write('source_file_name' if first else file_name)
                        outfile.write('\t')
                        outfile.write('source_file_id' if first else gdc_id)
                        outfile.write('\n')
                    first = False

def join_with_aliquot_table(cnv_table, aliquot_table, case_table, target_dataset, dest_table, do_batch):
    """
    Merge Skeleton With Aliquot Data
    Creates the final BQ table by joining the skeleton with the aliquot ID info

    :param cnv_table: Raw Copy Number table name
    :type cnv_table: basestring
    :param aliquot_table: Metadata Aliquot table name
    :type aliquot_table: basestring
    :param case_table: Metadata Case table name
    :type case_table: basestring
    :param target_dataset: Scratch data set name
    :type target_dataset: basestring
    :param dest_table: Name of table to create
    :type dest_table: basestring
    :param do_batch: Run the BQ job in Batch mode?
    :type do_batch: bool
    :return: Whether the query succeeded
    :rtype: bool
    """

    sql = merge_bq_sql(cnv_table, aliquot_table, case_table)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)

def merge_bq_sql(cnv_table, aliquot_table, case_table):
    """
    SQL Code For Final Table Generation

    :param cnv_table: Raw Copy Number table name
    :type cnv_table: basestring
    :param aliquot_table: Metadata Aliquot table name
    :type aliquot_table: basestring
    :param case_table: Metadata Case table name
    :type case_table: basestring
    :return: Sting with query to join the tables together
    :rtype: basestring
    """

    return f'''
            WITH
            a1 AS (SELECT DISTINCT GDC_Aliquot
                   FROM `{cnv_table}`),
            a2 AS (SELECT b.project_id AS project_short_name,
                          b.case_barcode,
                          b.sample_barcode,
                          b.aliquot_barcode,
                          b.case_gdc_id,
                          b.sample_gdc_id,
                          b.aliquot_gdc_id
                   FROM a1
                   JOIN `{aliquot_table}` b ON a1.GDC_Aliquot = b.aliquot_gdc_id),
            a3 AS (SELECT a2.project_short_name,
                          a2.case_barcode,
                          a2.sample_barcode,
                          a2.aliquot_barcode,
                          a2.case_gdc_id,
                          a2.sample_gdc_id,
                          a2.aliquot_gdc_id,
                          b.primary_site
                    FROM a2
                    JOIN `{case_table}` b ON a2.case_gdc_id = b.case_gdc_id)
        SELECT
            project_short_name,
            case_barcode,
            primary_site,
            sample_barcode,
            aliquot_barcode,
            gene_id,
            gene_name,
            chromosome,
            start AS start_pos,
            `end` AS end_pos,
            copy_number,
            min_copy_number,	
            max_copy_number,
            case_gdc_id,
            sample_gdc_id,
            aliquot_gdc_id,
            source_file_id AS file_gdc_id
        FROM a3
        JOIN `{cnv_table}` b ON a3.aliquot_gdc_id = b.GDC_Aliquot
    ''' # todo update major to max etc

def merge_samples_by_aliquot(input_table, output_table, target_dataset, do_batch):
    sql = merge_samples_by_aliquot_sql(input_table)
    return generic_bq_harness(sql, target_dataset, output_table, do_batch, 'TRUE')

def merge_samples_by_aliquot_sql(input_table):
    return f"""
        SELECT
            project_short_name,
            case_barcode,
            primary_site,
            string_agg(distinct sample_barcode, ';') as sample_barcode,
            aliquot_barcode,
            gene_id,
            gene_name,
            chromosome,
            start_pos,
            end_pos,
            copy_number,
            min_copy_number,	
            max_copy_number,
            case_gdc_id,
            string_agg(distinct sample_gdc_id, ';') as sample_gdc_id,
            aliquot_gdc_id,
            file_gdc_id
        FROM
            `{input_table}`
        GROUP BY
            project_short_name,
            case_barcode,
            primary_site,
            aliquot_barcode,
            gene_id,
            gene_name,
            chromosome,
            start_pos,
            end_pos,
            copy_number,
            min_copy_number,	
            max_copy_number,
            case_gdc_id,
            aliquot_gdc_id,
            file_gdc_id
    """

def find_types(file, sample_interval):
    """
    Finds the field type for each column in the file
    :param file: file name
    :type file: basestring
    :param sample_interval:sampling interval, used to skip rows in large datasets; defaults to checking every row
        example: sample_interval == 10 will sample every 10th row
    :type sample_interval: int
    :return: a tuple with a list of [field, field type]
    :rtype: tuple ([field, field_type])
    """
    column_list = get_column_list_tsv(tsv_fp=file, header_row_index=0)
    field_types = aggregate_column_data_types_tsv(file, column_list,
                                                  sample_interval=sample_interval,
                                                  skip_rows=1)
    final_field_types = resolve_type_conflicts(field_types)
    typing_tups = []
    for column in column_list:
        tup = (column, final_field_types[column])
        typing_tups.append(tup)

    return typing_tups

def main(args):
    """
    Main Control Flow
    Note that the actual steps run are configured in the YAML input! This allows you
    to e.g. skip previously run steps.
    """
    if not confirm_google_vm():
        print('This job needs to run on a Google Cloud Compute Engine to avoid storage egress charges [EXITING]')
        return

    if len(args) != 2:
        print(" ")
        print(" Usage : {args[0]} <configuration_yaml>")
        return

    print('job started')

    # Get the YAML config loaded:
    with open(args[1], mode='r') as yaml_file:
        params_dict, steps = load_config(yaml_file.read())
        params = SimpleNamespace(**params_dict)

    # Which release is the workflow running on?
    release = f"r{str(params.RELEASE)}"

    # BQ does not like to be given paths that have "~". So make all local paths absolute:
    home = expanduser("~")

    # Workflow Steps

    if 'pull_table_info_from_git' in steps:
        print('pull_table_info_from_git')
        update_dir_from_git(params.SCHEMA_REPO_LOCAL, params.SCHEMA_REPO_URL, params.SCHEMA_REPO_BRANCH)

    for program in params.PROGRAMS:

        # program mapping
        metadata_mapping = f"{params.SCHEMA_REPO_LOCAL}/{params.METADATA_MAPPINGS}"
        with open(metadata_mapping) as program_mapping:
            mappings = json_loads(program_mapping.read().rstrip())
            bq_dataset = mappings[program]['bq_dataset']

        # Local Files
        local_dir = f"{home}/{params.LOCAL_DIR}/{bq_dataset}"
        local_files_dir = f"{local_dir}/{params.LOCAL_FILES_DIR}"
        one_big_tsv = f"{local_dir}/{params.ONE_BIG_TSV}"
        manifest_file = f"{local_dir}/{params.MANIFEST_FILE}"
        local_pull_list = f"{local_dir}/{params.LOCAL_PULL_LIST}"
        file_traversal_list = f"{local_dir}/{params.FILE_TRAVERSAL_LIST}"
        table_metadata = f"{params.SCHEMA_REPO_LOCAL}/{params.SCHEMA_FILE_NAME}"
        field_list = f"{home}/Gene-Level-CNVR-field_list.json"
        field_desc_fp = f"{params.SCHEMA_REPO_LOCAL}/{params.FIELD_DESC_FILE}"

        # BigQuery Tables
        base_table_name = f"{params.DATA_TYPE}_hg38_gdc"
        pub_curr_name = f"{base_table_name}_current"
        pub_ver_name = f"{base_table_name}_{release}"
        upload_table = f"{program}_{base_table_name}_raw_{release}"
        manifest_table = f"{program}_{base_table_name}_manifest_{release}"
        pull_list_table = f"{program}_{base_table_name}_pull_list_{release}"
        draft_table = f"{program}_{base_table_name}_{release}"

        # Google Bucket Locations
        bucket_target_blob = f'{params.WORKING_BUCKET_DIR}/{release}-{program}-{params.DATA_TYPE}.tsv'

        if 'clear_target_directory' in steps:
            # Best practice is to clear out the directory where the files are going. Don't want anything left over:
            print('clear_target_directory')
            create_clean_target(local_files_dir)

        if 'build_manifest_from_filters' in steps:
            # Use the filter set to build a manifest.
            # print('build_manifest_from_filters')
            max_files = params.MAX_FILES if 'MAX_FILES' in params_dict else None
            bq_filters = [{"access": "open"},
                          {"data_type": "Gene Level Copy Number"},
                          {"program_name": params.PROGRAMS},
                          {"data_category": "Copy Number Variation"}]
            manifest_success = get_the_bq_manifest(params.FILE_TABLE.format(release),
                                                   bq_filters, max_files,
                                                   params.WORKING_PROJECT, params.SCRATCH_DATASET,
                                                   manifest_table, params.WORKING_BUCKET,
                                                   params.BUCKET_MANIFEST_TSV, manifest_file,
                                                   params.BQ_AS_BATCH)
            if not manifest_success:
                sys.exit("Failure generating manifest")

        if 'build_pull_list' in steps:
            # Create a "pull list" with BigQuery of gs:// URLs to pull from DCF
            print('build_pull_list')
            build_pull_list_with_bq(f"{params.WORKING_PROJECT}.{params.SCRATCH_DATASET}.{manifest_table}",
                                    params.INDEXD_BQ_TABLE.format(release),
                                    params.WORKING_PROJECT, params.SCRATCH_DATASET,
                                    pull_list_table,
                                    params.WORKING_BUCKET,
                                    params.BUCKET_PULL_LIST,
                                    local_pull_list, params.BQ_AS_BATCH)

        if 'transfer_from_gdc' in steps:
            # Bring the files to the local dir from DCF GDC Cloud Buckets
            with open(local_pull_list, mode='r') as pull_list_file:
                pull_list = pull_list_file.read().splitlines()
            pull_from_buckets(pull_list, local_files_dir)

            all_files = build_file_list(local_files_dir)
            with open(file_traversal_list, mode='w') as traversal_list:
                for line in all_files:
                    traversal_list.write(f"{line}\n")

        if 'concat_all_files' in steps:
            print('concat_all_files')
            with open(file_traversal_list, mode='r') as traversal_list_file:
                all_files = traversal_list_file.read().splitlines()
            concat_all_files(all_files, one_big_tsv)

        # # todo Is this needed?
        # # if 'check_position_data_type' in steps:
        # #     # Due to the GDC workflow pipeline, some of the chromosome position numbers are converted to
        # #     # scientific notation when the file is written out of R. This steps checks for these inconsistencies
        # #     print('checking the data type of the chromosome position columns')
        # #     check_position_data_type(one_big_tsv)
        #
        # # todo Is this needed? Possibly Not based on
        # # if 'fix_position_data' in steps:
        # #     # This function fixes any scientific notation that was found by the check_position_data_type step. This step
        # #     # can be skipped if no scientific notation was found.
        # #     print('Fixing rows with scientific notation in them')
        # #     fix_position_data_type(one_big_tsv)
        #
        if 'upload_to_bucket' in steps:
            print('upload_to_bucket')
            upload_to_bucket(params.WORKING_BUCKET, bucket_target_blob, one_big_tsv)

        if 'analyze_the_schema' in steps:
            print('analyze_the_schema')
            typing_tups = find_types(one_big_tsv, params.SCHEMA_SAMPLE_SKIPS)

            create_schema_hold_list(typing_tups, field_desc_fp, field_list, True)

        # Create the BQ table from the TSV
        if 'create_bq_from_tsv' in steps:
            print('create_bq_from_tsv')
            bucket_src_url = f'gs://{params.WORKING_BUCKET}/{bucket_target_blob}'
            with open(field_list, mode='r') as schema_list:
                typed_schema = json_loads(schema_list.read())
            csv_to_bq(typed_schema, bucket_src_url, params.SCRATCH_DATASET, upload_table, params.BQ_AS_BATCH)

        if 'add_aliquot_fields' in steps:
            print('add_aliquot_fields')
            full_target_table = f'{params.WORKING_PROJECT}.{params.SCRATCH_DATASET}.{upload_table}'
            success = join_with_aliquot_table(full_target_table, f"{params.ALIQUOT_TABLE}_r{params.RELEASE}",
                                              f"{params.CASE_TABLE}_r{params.RELEASE}",
                                              params.SCRATCH_DATASET, f"{draft_table}_w_metadata",
                                              params.BQ_AS_BATCH)
            if not success:
                print("Join job failed")

        # For CPTAC there are instances where multiple samples are merged into the same aliquot
        # for these cases we join the rows by concatenating the samples with semicolons
        if 'merge_same_aliq_samples' in steps:
            source_table = f"{params.WORKING_PROJECT}.{params.SCRATCH_DATASET}.{draft_table}_w_metadata"
            merge_samples_by_aliquot(source_table, draft_table, params.SCRATCH_DATASET,
                                     params.BQ_AS_BATCH)

        if 'update_table_schema' in steps: # todo
            print("update schema tags")
            updated_schema_tags = update_schema_tags(metadata_mapping, params_dict, program)
            print("update table schema")
            write_table_schema_with_generic(
                f"{params.WORKING_PROJECT}.{params.SCRATCH_DATASET}.{bq_dataset}_{base_table_name}_{release}",
                updated_schema_tags, table_metadata, field_desc_fp)

        # if 'qc_bigquery_tables' in steps: # todo
        #     print("QC BQ table")
        #     print(qc_bq_table_metadata(
        #         f"{params.WORKING_PROJECT}.{params.SCRATCH_DATASET}.{bq_dataset}_{standard_table}_{release}"))

        # if 'publish' in steps: # todo update
        #     print('publish tables')
        #     success = publish_tables_and_update_schema(f"{params.WORKING_PROJECT}.{params.SCRATCH_DATASET}.{bq_dataset}_{standard_table}_{release}",
        #                                                f"{params.PUBLICATION_PROJECT}.{bq_dataset}_versioned.{standard_table}_{release}",
        #                                                f"{params.PUBLICATION_PROJECT}.{bq_dataset}.{standard_table}_current",
        #                                                f"REL {str(params.RELEASE)}",
        #                                                f"{standard_table}")
        #
        # if 'dump_working_tables' in steps:
        #     # todo update
        #     print("dump working tables")
        #     # dump_table_tags = [f"{draft_table}_current", f"{draft_table}_{release}"]
        #     # dump_tables = [params[x] for x in dump_table_tags]
        #     # for table in dump_tables:
        #     #     delete_table_bq_job(params.SCRATCH_DATASET, table)

        print('job completed')

if __name__ == "__main__":
    main(sys.argv)
