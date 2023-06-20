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
                               generic_bq_harness, build_file_list, pull_from_buckets, upload_to_bucket, csv_to_bq, \
                               build_pull_list_with_bq, create_schema_hold_list, update_schema_tags, \
                               write_table_schema_with_generic, qc_bq_table_metadata, publish_tables_and_update_schema


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
                for line in readfile:
                    if not line.startswith('gene_id') or first:
                        outfile.write(line.rstrip('\n'))
                        outfile.write('\t')
                        outfile.write('source_file_name' if first else file_name)
                        outfile.write('\t')
                        outfile.write('source_file_id' if first else gdc_id)
                        outfile.write('\n')
                    first = False


def create_draft_table(cnv_table, file_table, aliquot_table, case_table, gene_table,
                       target_dataset, dest_table, do_batch):
    """
    Merge Skeleton With Aliquot, File, Gene, and Case Data to create the final draft table

    :param cnv_table: Raw Copy Number table name
    :type cnv_table: basestring
    :param file_table: File Metadata Table
    :type file_table: basestring
    :param aliquot_table: Aliquot Metadata table name
    :type aliquot_table: basestring
    :param case_table: Case Metadata table name
    :type case_table: basestring
    :param gene_table: GENCODE gene table name
    :type gene_table: basestring
    :param target_dataset: Scratch dataset name
    :type target_dataset: basestring
    :param dest_table: Name of table to create
    :type dest_table: basestring
    :param do_batch: Run the BQ job in Batch mode?
    :type do_batch: bool
    :return: Whether the query succeeded
    :rtype: bool
    """

    sql = sql_for_draft_table(cnv_table, file_table, aliquot_table, case_table, gene_table)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)


def sql_for_draft_table(cnv_table, file_table, aliquot_table, case_table, gene_table):
    """
    SQL Code For Final Table Generation

    :param cnv_table: Raw Copy Number table name
    :type cnv_table: basestring
    :param file_table: File Metadata Table
    :type file_table: basestring
    :param aliquot_table: Aliquot Metadata table name
    :type aliquot_table: basestring
    :param case_table: Case Metadata table name
    :type case_table: basestring
    :param gene_table: GENCODE gene table name
    :type gene_table: basestring
    :return: Sting with query to join the tables together
    :rtype: basestring
    """

    regex_string1 = r"^\w{8}\-\w{4}\-\w{4}\-\w{4}\-\w{12}"
    regex_string2 = r"\w{8}\-\w{4}\-\w{4}\-\w{4}\-\w{12}$"

    return f'''
            SELECT
              DISTINCT
              b.project_id AS project_short_name,
              b.case_barcode,
              c.primary_site,
              string_agg(distinct b.sample_barcode, ';') as sample_barcode,
              b.aliquot_barcode,
              e.gene_id as Ensembl_gene_id,
              d.gene_id as Ensembl_gene_id_v,
              d.gene_name,
              e.gene_type,
              d.chromosome,
              d.start AS start_pos,
              d.`end` AS end_pos,
              d.copy_number,
              d.min_copy_number,
              d.max_copy_number,
              b.case_gdc_id,
              string_agg(distinct b.sample_gdc_id, ';') as sample_gdc_id,
              b.aliquot_gdc_id,
              a.file_gdc_id
            FROM
              `{file_table}` AS a
            JOIN
              `{aliquot_table}` AS b
            ON
              REGEXP_EXTRACT(a.associated_entities__entity_gdc_id, r'{regex_string1}') = b.aliquot_gdc_id
              OR REGEXP_EXTRACT(a.associated_entities__entity_gdc_id, r'{regex_string2}') = b.aliquot_gdc_id
            JOIN
              `{case_table}` AS c
            ON
              b.case_gdc_id = c.case_gdc_id
            JOIN
              `{cnv_table}` AS d
            ON
              a.file_gdc_id = d.source_file_id
            JOIN `{gene_table}` as e
            ON d.gene_id = e.gene_id_v
            WHERE
              b.sample_type_name NOT LIKE '%Normal%'
              AND b.sample_type_name <> "Granulocytes"
              AND `access` = "open"
              AND a.data_type = "Gene Level Copy Number"
              AND a.data_category = "Copy Number Variation"
              AND a.program_name = "TARGET"
            GROUP BY
              project_short_name,
              b.case_barcode,
              c.primary_site,
              b.aliquot_barcode,
              Ensembl_gene_id,
              Ensembl_gene_id_v,
              d.gene_name,
              e.gene_type,
              d.chromosome,
              start_pos,
              end_pos,
              d.copy_number,
              d.min_copy_number,
              d.max_copy_number,
              b.case_gdc_id,
              b.aliquot_gdc_id,
              a.file_gdc_id
    '''


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
        raw_table = f"{bq_dataset}_{base_table_name}_raw_{release}"
        manifest_table = f"{bq_dataset}_{base_table_name}_manifest_{release}"
        pull_list_table = f"{bq_dataset}_{base_table_name}_pull_list_{release}"
        draft_table = f"{bq_dataset}_{base_table_name}_{release}"
        draft_full_id = f"{params.WORKING_PROJECT}.{params.SCRATCH_DATASET}.{draft_table}"
        pub_ver_full_id = f"{params.PUBLICATION_PROJECT}.{bq_dataset}_versioned.{pub_ver_name}"
        pub_curr_full_id = f"{params.PUBLICATION_PROJECT}.{bq_dataset}.{pub_curr_name}"

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
                          {"program_name": program},
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
            csv_to_bq(typed_schema, bucket_src_url, params.SCRATCH_DATASET, raw_table, params.BQ_AS_BATCH)

        if 'create_draft_table' in steps:
            print('Creating final draft table by joining on extra data')
            full_target_table = f'{params.WORKING_PROJECT}.{params.SCRATCH_DATASET}.{raw_table}'
            success = create_draft_table(full_target_table, params.FILE_TABLE.format(release),
                                         f"{params.ALIQUOT_TABLE}_{release}", f"{params.CASE_TABLE}_{release}",
                                         params.GENE_NAMES_TABLE, params.SCRATCH_DATASET, f"{draft_table}",
                                         params.BQ_AS_BATCH)
            if not success:
                print("Join job failed")

        if 'update_table_schema' in steps:
            print("update schema tags")
            updated_schema_tags = update_schema_tags(metadata_mapping, params_dict, program)
            print("update table schema")
            write_table_schema_with_generic(
                f"{draft_full_id}",
                updated_schema_tags, table_metadata, field_desc_fp)

        if 'qc_bigquery_tables' in steps:  # todo test
            print("QC BQ table")
            print(qc_bq_table_metadata(
                f"{draft_full_id}"))

        if 'publish' in steps:  # todo test
            print('publish tables')
            success = publish_tables_and_update_schema(f"{draft_full_id}",
                                                       f"{pub_ver_full_id}",
                                                       f"{pub_curr_full_id}",
                                                       f"REL {str(params.RELEASE)}",
                                                       f"{base_table_name}")

            if not success:
                print("Publication step did not work")

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
