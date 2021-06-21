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
from git import Repo
from json import loads as json_loads
#from createSchemaP3 import build_schema

from common_etl.utils import get_column_list_tsv, aggregate_column_data_types_tsv, resolve_type_conflicts

from common_etl.support import get_the_bq_manifest, confirm_google_vm, create_clean_target, \
                               generic_bq_harness, build_file_list, upload_to_bucket, csv_to_bq, \
                               build_pull_list_with_bq, BucketPuller, build_combined_schema, \
                               delete_table_bq_job, install_labels_and_desc, update_schema_with_dict, \
                               generate_table_detail_files, compare_two_tables, publish_table, update_status_tag

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
        return None, None, None, None, None

    return yaml_dict['files_and_buckets_and_tables'], yaml_dict['bq_filters'], yaml_dict['update_schema_tables'], \
           yaml_dict['schema_tags'], yaml_dict['steps']

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
                    if not line.startswith('GDC_Aliquot') or first:
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
    Original author: Sheila Reynolds

    :param cnv_table: Raw Copy Number table name
    :type cnv_table: basestring
    :param aliquot_table: Metadata Aliquot table name
    :type aliquot_table: basestring
    :param case_table: Metadata Case table name
    :type case_table: basestring
    :return: Sting with query to join the tables together
    :rtype: basestring
    """
    # todo may need to join on sample_id also

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
                   JOIN `{aliquot_table}` b ON a1.GDC_Aliquot = b.aliquot_gdc_id)
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
            chromosome,
            start AS start_pos,
            `end` AS end_pos,
            num_probes,
            segment_mean,
            case_gdc_id,
            sample_gdc_id,
            aliquot_gdc_id,
            source_file_id AS file_gdc_id
        FROM a3
        JOIN `{cnv_table}` b ON a3.aliquot_gdc_id = b.GDC_Aliquot
        '''

def find_types(file, sample_interval): # may need to add skip_rows later
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

    #
    # Get the YAML config loaded:
    #

    with open(args[1], mode='r') as yaml_file:
        params, bq_filters, update_schema_tables, schema_tags, steps = load_config(yaml_file.read())


    #
    # BQ does not like to be given paths that have "~". So make all local paths absolute:
    #

    home = expanduser("~")
    local_files_dir = f"{home}/{params['LOCAL_FILES_DIR']}"
    one_big_tsv = f"{home}/{params['ONE_BIG_TSV']}"
    manifest_file = f"{home}/{params['MANIFEST_FILE']}"
    local_pull_list = f"{home}/{params['LOCAL_PULL_LIST']}"
    file_traversal_list = f"{home}/{params['FILE_TRAVERSAL_LIST']}"
    hold_schema_dict = f"{home}/{params['HOLD_SCHEMA_DICT']}"
    hold_schema_list = f"{home}/{params['HOLD_SCHEMA_LIST']}"

    # Which release is the workflow running on?
    release = f"r{str(params['RELEASE'])}"

    upload_table = f"{params['PROGRAM']}_{params['DATA_TYPE']}"
    manifest_table = f"{upload_table}_manifest"
    pull_list_table = f"{upload_table}_pull_list"
    draft_table = f"{params['PROGRAM']}_{params['DATA_TYPE']}_{params['BUILD']}_gdc"
    publication_table = f"{params['DATA_TYPE']}_{params['BUILD']}_gdc"

    if params['RELEASE'] < 21 and 'METADATA_REL' not in params:
        print("The input release is before new metadata process, "
              "please specify which release of the metadata to use.")

    metadata_rel = "".join(["r", str(params['METADATA_REL'])]) if 'METADATA_REL' in params else release

    if 'clear_target_directory' in steps:
        print('clear_target_directory')
        create_clean_target(local_files_dir)

    #
    # Use the filter set to build a manifest. Note that if a pull list is
    # provided, these steps can be omitted:
    #

    if 'build_manifest_from_filters' in steps:
        print('build_manifest_from_filters')
        max_files = params['MAX_FILES'] if 'MAX_FILES' in params else None

        manifest_success = get_the_bq_manifest(f"{params['FILEDATA_TABLE']}_{metadata_rel}", bq_filters, max_files,
                                               params['WORKING_PROJECT'], params['SCRATCH_DATASET'],
                                               manifest_table, params['WORKING_BUCKET'],
                                               params['BUCKET_MANIFEST_TSV'], manifest_file,
                                               params['BQ_AS_BATCH'])
        if not manifest_success:
            print("Failure generating manifest")
            return

    #
    # We need to create a "pull list" of gs:// URLs to pull from GDC buckets. If you have already
    # created a pull list, just plunk it in 'LOCAL_PULL_LIST' and skip this step. If creating a pull
    # list, uses BQ as long as you have built the manifest using BQ (that route uses the BQ Manifest
    # table that was created).
    #

    if 'build_pull_list' in steps:
        print('build_pull_list')
        full_manifest = f"{params['WORKING_PROJECT']}.{params['SCRATCH_DATASET']}.{manifest_table}"
        success = build_pull_list_with_bq(full_manifest, params['INDEXD_BQ_TABLE'].format(metadata_rel),
                                          params['WORKING_PROJECT'], params['SCRATCH_DATASET'],
                                          pull_list_table,
                                          params['WORKING_BUCKET'],
                                          params['BUCKET_PULL_LIST'],
                                          local_pull_list, params['BQ_AS_BATCH'])

        if not success:
            print("Build pull list failed")
            return
    #
    # Now hitting GDC cloud buckets. Get the files in the pull list:
    #

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
                traversal_list.write(f"{line}\n")

    if 'concat_all_files' in steps:
        print('concat_all_files')
        with open(file_traversal_list, mode='r') as traversal_list_file:
            all_files = traversal_list_file.read().splitlines()
        concat_all_files(all_files, one_big_tsv)

    #
    # Schemas and table descriptions are maintained in the github repo:
    #

    if 'pull_table_info_from_git' in steps:
        print('pull_table_info_from_git')
        try:
            create_clean_target(params['SCHEMA_REPO_LOCAL'])
            repo = Repo.clone_from(params['SCHEMA_REPO_URL'], params['SCHEMA_REPO_LOCAL'])
            repo.git.checkout(params['SCHEMA_REPO_BRANCH'])
        except Exception as ex:
            print(f"pull_table_info_from_git failed: {str(ex)}")
            return

    for table in update_schema_tables:
        if table == 'current':
            use_schema = params['SCHEMA_FILE_NAME']
            schema_release = 'current'
        else:
            use_schema = params['VER_SCHEMA_FILE_NAME']
            schema_release = release

        if 'process_git_schemas' in steps:
            print('process_git_schema')
            # Where do we dump the schema git repository?
            schema_file = f"{params['SCHEMA_REPO_LOCAL']}/{params['RAW_SCHEMA_DIR']}/{use_schema}"
            full_file_prefix = f"{params['PROX_DESC_PREFIX']}/{draft_table}_{schema_release}"
            # Write out the details
            success = generate_table_detail_files(schema_file, full_file_prefix)
            if not success:
                print("process_git_schemas failed")
                return

        # Customize generic schema to this data program:
        if 'replace_schema_tags' in steps:
            print('replace_schema_tags')
            pn = params['PROGRAM']
            dataset_tuple = (pn, pn.replace(".", "_"))
            tag_map_list = []
            for tag_pair in schema_tags:
                for tag in tag_pair:
                    val = tag_pair[tag]
                    use_pair = {}
                    tag_map_list.append(use_pair)
                    if val.find('~-') == 0 or val.find('~lc-') == 0 or val.find('~lcbqs-') == 0:
                        chunks = val.split('-', 1)
                        if chunks[1] == 'programs':
                            if val.find('~lcbqs-') == 0:
                                rep_val = dataset_tuple[1].lower()  # can't have "." in a tag...
                            else:
                                rep_val = dataset_tuple[0]
                        elif chunks[1] == 'builds':
                            rep_val = params['BUILD']
                        else:
                            raise Exception()
                        if val.find('~lc-') == 0:
                            rep_val = rep_val.lower()
                        use_pair[tag] = rep_val
                    else:
                        use_pair[tag] = val
            #full_file_prefix = "{}/{}".format(params['PROX_DESC_PREFIX'], draft_table.format(schema_release))

        if 'analyze_the_schema' in steps:
            print('analyze_the_schema')
            #typing_tups = build_schema(one_big_tsv, params['SCHEMA_SAMPLE_SKIPS'])
            typing_tups = find_types(one_big_tsv, params['SCHEMA_SAMPLE_SKIPS'])
            print(typing_tups)
            # full_file_prefix = f"{params['PROX_DESC_PREFIX']}/{draft_table}_{schema_release}"
            # schema_dict_loc = f"{full_file_prefix}_schema.json"
            # build_combined_schema(None, schema_dict_loc,
            #                       typing_tups, hold_schema_list, hold_schema_dict)

    bucket_target_blob = f"{params['WORKING_BUCKET_DIR']}/" \
                         f"{params['DATE']}-{params['PROGRAM']}-{params['DATA_TYPE']}.tsv"

    if 'upload_to_bucket' in steps:
        print('upload_to_bucket')
        upload_to_bucket(params['WORKING_BUCKET'], bucket_target_blob, one_big_tsv)

    if 'create_bq_from_tsv' in steps:
        print('create_bq_from_tsv')
        bucket_src_url = f'gs://{params["WORKING_BUCKET"]}/{bucket_target_blob}'
        with open(hold_schema_list, mode='r') as schema_hold_dict:
            typed_schema = json_loads(schema_hold_dict.read())
        csv_to_bq(typed_schema, bucket_src_url, params['SCRATCH_DATASET'], f"{draft_table}_{release}_draft", params['BQ_AS_BATCH'])

    if 'add_aliquot_fields' in steps:
        print('add_aliquot_fields')
        full_target_table = f'{params["WORKING_PROJECT"]}.{params["SCRATCH_DATASET"]}.{draft_table}_{release}_draft'
        success = join_with_aliquot_table(full_target_table, f"{params['ALIQUOT_TABLE']}_{metadata_rel}",
                                          f"{params['CASE_TABLE']}_{metadata_rel}",
                                          params['SCRATCH_DATASET'], f"{draft_table}_{release}",
                                          params['BQ_AS_BATCH'])
        if not success:
            print("Join job failed")

    # Create second table
    if 'create_current_table' in steps:
        source_table = f"{params['WORKING_PROJECT']}.{params['SCRATCH_DATASET']}.{draft_table}_{release}"
        current_dest = f"{params['WORKING_PROJECT']}.{params['SCRATCH_DATASET']}.{draft_table}_current"

        success = publish_table(source_table, current_dest)

        if not success:
            print("create current table failed")
            return

    #
    # Update the per-field descriptions:
    #

    for table in update_schema_tables:
        schema_release = 'current' if table == 'current' else release
        if 'update_field_descriptions' in steps:
            print('update_field_descriptions')
            full_file_prefix = f"{params['PROX_DESC_PREFIX']}/{draft_table}_{schema_release}"
            schema_dict_loc = f"{full_file_prefix}_schema.json"
            schema_dict = {}
            with open(schema_dict_loc, mode='r') as schema_hold_dict:
                full_schema_list = json_loads(schema_hold_dict.read())
            for entry in full_schema_list:
                schema_dict[entry['name']] = {'description': entry['description']}

            success = update_schema_with_dict(params['SCRATCH_DATASET'], f"{draft_table}_{schema_release}", schema_dict)
            if not success:
                print("update_field_descriptions failed")
                return

    #
    # Add description and labels to the target table:
    #

    if 'update_table_description' in steps:
        print('update_table_description')
        full_file_prefix = f"{params['PROX_DESC_PREFIX']}/{draft_table}_{release}"
        success = install_labels_and_desc(params['SCRATCH_DATASET'], f"{draft_table}_{release}", full_file_prefix)
        if not success:
            print("update_table_description failed")
            return

    #
    # compare and remove old current table
    #

    # compare the two tables
    if 'compare_remove_old_current' in steps:
        """
        This step compares the old current table to the old versioned duplicate then deletes the old current table,
        if the two tables are the same
        """
        # Table that is currently in production under the current table dataset that is to be replaced
        old_current_table = f'{params["PUBLICATION_PROJECT"]}.{params["PUBLICATION_DATASET"]}.' \
                            f'{publication_table}_current'
        # Previous versioned table that should match the table in the current dataset
        previous_ver_table = f"{params['PUBLICATION_PROJECT']}.{params['PUBLICATION_DATASET']}_versioned." \
                             f"{publication_table}_{str(params['PREVIOUS_RELEASE'])}"
        # Temporary location to save a copy of the previous table
        table_temp = f'{params["WORKING_PROJECT"]}.{params["SCRATCH_DATASET"]}.' \
                     f'{params["PROGRAM"]}_{publication_table}_{str(params["PREVIOUS_RELEASE"])}_backup'

        print(f'Compare {old_current_table} to {previous_ver_table}')
        # Compare the two previous tables to make sure they are exactly the same
        compare = compare_two_tables(old_current_table, previous_ver_table, params['BQ_AS_BATCH'])
        """
        If the tables are exactly the same, the row count from compare_two_tables should be 0, the query will give the 
        number of rows that are different between the two tables
        """
        num_rows = compare.total_rows

        if num_rows == 0:
            print('the tables are the same')
        else:
            print(f'the tables are NOT the same and differ by {num_rows} rows')

        if not compare:
            print('compare_tables failed')
            return
        # move old table to a temporary location
        elif compare and num_rows == 0:
            print('Move old table to temp location')
            # Save the previous current table to a temporary location
            table_moved = publish_table(old_current_table, table_temp)

            if not table_moved:
                print('Old Table was not moved and will not be deleted')
            # remove old table
            elif table_moved:
                print(f'Deleting old table: {old_current_table}')
                delete_table = delete_table_bq_job(params['PUBLICATION_DATASET'], f"{publication_table}_current",
                                                   params['PUBLICATION_PROJECT'])
                if not delete_table:
                    print('delete table failed')
                    return


    #
    # publish table:
    #

    if 'publish' in steps:
        print('publish tables')
        tables = ['versioned', 'current']

        for table in tables:
            if table == 'versioned':
                print(table)
                source_table = f"{params['WORKING_PROJECT']}.{params['SCRATCH_DATASET']}.{draft_table}_{release}"
                publication_dest = f"{params['PUBLICATION_PROJECT']}.{params['PUBLICATION_DATASET']}_versioned." \
                                   f"{publication_table}_release"
            elif table == 'current':
                print(table)
                source_table = f"{params['WORKING_PROJECT']}.{params['SCRATCH_DATASET']}.{draft_table}_current"
                publication_dest = f"{params['PUBLICATION_PROJECT']}.{params['PUBLICATION_DATASET']}." \
                                   f"{publication_table}_current"
            success = publish_table(source_table, publication_dest)

        if not success:
            print("publish table failed")
            return

    # Update previous versioned table with archived tag
    if 'update_status_tag' in steps:
        print('Update previous table')

        success = update_status_tag("_".join([params['PUBLICATION_DATASET'], 'versioned']),
                                    f"{publication_table}_{str(params['PREVIOUS_RELEASE'])}_archived",
                                    params['PUBLICATION_PROJECT'])

        if not success:
            print("update status tag table failed")
            return

    #
    # Clear out working temp tables:
    #

    if 'dump_working_tables' in steps:
        dump_table_tags = [f"{draft_table}_current", f"{draft_table}_{release}"]
        dump_tables = [params[x] for x in dump_table_tags]
        for table in dump_tables:
            delete_table_bq_job(params['SCRATCH_DATASET'], table)

    # todo add archive tables step

    print('job completed')

if __name__ == "__main__":
    main(sys.argv)

