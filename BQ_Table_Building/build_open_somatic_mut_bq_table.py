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
VM NEEDS 26 GB MEMORY FOR THIS TO NOT DIE IN MERGE MODE, MORE IF DEBUG == True
In merge mode, the job needed 18 G peak to run, hitting the highest levels on UCEC
and UCS (which are processed near the very end). This required running on a n1-highmem-4
(4 vCPUs, 26 GB memory) machine, and took about 40 minutes. When the DEBUG Logging was
enabled, performance was degraded, and I moved to using a n1-highmem-8 (8 vCPUs, 52 GB memory)
machine. Note that only one processor gets used, so that is just increasing the memory.
That took 53 minutes. It appears that DEBUG logging created 37.5 million lines for
reading, 187 million lines for writing.
'''

import sys

import os
import yaml
import gzip
import shutil
import zipfile
import io
from git import Repo
import re
from json import loads as json_loads
from os.path import expanduser
from createSchemaP3 import build_schema

from common_etl.support import create_clean_target, pull_from_buckets, build_file_list, generic_bq_harness, \
                               upload_to_bucket, csv_to_bq, delete_table_bq_job, \
                               build_pull_list_with_bq, update_schema, \
                               build_combined_schema, get_the_bq_manifest, confirm_google_vm, \
                               generate_table_detail_files, customize_labels_and_desc, install_labels_and_desc, \
                               publish_table, update_status_tag, bq_harness_with_result

'''
----------------------------------------------------------------------------------------------
The configuration reader. Parses the YAML configuration into dictionaries
'''


def load_config(yaml_config):
    yaml_dict = None
    config_stream = io.StringIO(yaml_config)
    try:
        yaml_dict = yaml.load(config_stream, Loader=yaml.FullLoader)
    except yaml.YAMLError as ex:
        print(ex)

    if yaml_dict is None:
        return None, None, None, None, None, None

    return (yaml_dict['files_and_buckets_and_tables'], yaml_dict['filters'], yaml_dict['bq_filters'],
            yaml_dict['steps'],  yaml_dict['callers'],
            yaml_dict['schema_tags'])

'''
----------------------------------------------------------------------------------------------
Extract the TCGA Programs We Are Working With From File List
Extract from downloaded file names instead of using a specified list.
'''


def build_program_list(all_files):
    programs = set()
    for filename in all_files:
        info_list = file_info(filename, None)
        programs.add(info_list[0])

    return sorted(programs)


'''
----------------------------------------------------------------------------------------------
Extract the Callers We Are Working With From File List
Extract from downloaded file names, compare to expected list. Answer if they match.
'''


def check_caller_list(all_files, expected_callers):
    expected_set = set(expected_callers)
    callers = set()
    for filename in all_files:
        info_list = file_info(filename, None)
        callers.add(info_list[1])

    return callers == expected_set


'''
----------------------------------------------------------------------------------------------
First BQ Processing: Add Aliquot IDs
The GDC files only provide tumor and normal BAM file IDS. We need aliquots, samples, and
associated barcodes. We get this by first attaching the aliquot IDs using the file table
that provides aliquot UUIDs for BAM files.
'''


def attach_aliquot_ids(maf_table, file_table, target_dataset, dest_table, do_batch):
    sql = attach_aliquot_ids_sql(maf_table, file_table)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)


'''
----------------------------------------------------------------------------------------------
SQL for above
'''


def attach_aliquot_ids_sql(maf_table, file_table):
    return '''
        WITH
        a1 AS (SELECT DISTINCT tumor_bam_uuid, normal_bam_uuid FROM `{0}`),        
        a2 AS (SELECT b.associated_entities__entity_gdc_id AS aliquot_gdc_id_tumor,
                      a1.tumor_bam_uuid,
                      a1.normal_bam_uuid
               FROM a1 JOIN `{1}` AS b ON a1.tumor_bam_uuid = b.file_gdc_id
               WHERE b.associated_entities__entity_type = 'aliquot')
        SELECT 
               c.project_short_name,
               c.case_gdc_id,
               c.associated_entities__entity_gdc_id AS aliquot_gdc_id_normal,
               a2.aliquot_gdc_id_tumor,
               a2.tumor_bam_uuid,
               a2.normal_bam_uuid
        FROM a2 JOIN `{1}` AS c ON a2.normal_bam_uuid = c.file_gdc_id
        WHERE c.associated_entities__entity_type = 'aliquot'
        '''.format(maf_table, file_table)


'''
----------------------------------------------------------------------------------------------
Second BQ Processing: Add Barcodes
With the aliquot UUIDs known, we can now use the aliquot table to glue in sample info
'''


def attach_barcodes(temp_table, aliquot_table, target_dataset, dest_table, do_batch, program, case_table):
    sql = attach_barcodes_sql(temp_table, aliquot_table, program, case_table)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)


'''
----------------------------------------------------------------------------------------------
SQL for above
'''


def attach_barcodes_sql(maf_table, aliquot_table, program, case_table):
    if program == 'TCGA':
        return '''
            WITH
            a1 AS (SELECT a.project_short_name,
                          a.case_gdc_id,
                          b.aliquot_barcode AS aliquot_barcode_tumor,
                          b.sample_barcode AS sample_barcode_tumor,
                          a.aliquot_gdc_id_tumor,
                          a.aliquot_gdc_id_normal,
                          a.tumor_bam_uuid,
                          a.normal_bam_uuid
                   FROM `{0}` AS a JOIN `{1}` AS b ON a.aliquot_gdc_id_tumor = b.aliquot_gdc_id)
            SELECT a1.project_short_name,
                   c.case_barcode,
                   a1.sample_barcode_tumor,
                   c.sample_barcode AS sample_barcode_normal,
                   a1.aliquot_barcode_tumor, 
                   c.aliquot_barcode AS aliquot_barcode_normal,
                   a1.tumor_bam_uuid,
                   a1.normal_bam_uuid
            FROM a1 JOIN `{1}` AS c ON a1.aliquot_gdc_id_normal = c.aliquot_gdc_id
            WHERE c.case_gdc_id = a1.case_gdc_id
            '''.format(maf_table, aliquot_table)
    else:
        return '''
            WITH
            a1 AS (SELECT b.project_id AS project_short_name,
                          a.case_id AS case_gdc_id,
                          b.aliquot_barcode AS aliquot_barcode_tumor,
                          b.sample_barcode AS sample_barcode_tumor,
                          a.Tumor_Aliquot_UUID AS aliquot_gdc_id_tumor,
                          a.Matched_Norm_Aliquot_UUID AS aliquot_gdc_id_normal,
                          a.Start_Position
                FROM
                  `{0}` AS a JOIN `{1}` AS b ON a.Tumor_Aliquot_UUID = b.aliquot_gdc_id),
            a2 AS (SELECT a1.project_short_name,
                          c.case_barcode,
                          a1.sample_barcode_tumor,
                          c.sample_barcode AS sample_barcode_normal,
                          a1.aliquot_barcode_tumor,
                          c.aliquot_barcode AS aliquot_barcode_normal,
                          a1.aliquot_gdc_id_tumor,
                          a1.Start_Position
                FROM a1 JOIN `{1}` AS c ON a1.aliquot_gdc_id_normal = c.aliquot_gdc_id
                WHERE c.case_gdc_id = a1.case_gdc_id)
            SELECT a2.project_short_name,
                   a2.case_barcode,
                   d.primary_site,
                   a2.sample_barcode_tumor,
                   a2.sample_barcode_normal,
                   a2.aliquot_barcode_tumor,
                   a2.aliquot_barcode_normal,
                   a2.aliquot_gdc_id_tumor,
                   a2.Start_Position
            FROM a2 JOIN `{2}` AS d ON a2.case_barcode = d.case_barcode
              
        '''.format(maf_table, aliquot_table, case_table)


'''
----------------------------------------------------------------------------------------------
Final BQ Step: Glue the New Info to the Original Table
All the new info we have pulled together goes in the first columns of the final table
'''


def final_merge(maf_table, barcode_table, target_dataset, dest_table, do_batch, program):
    sql = final_join_sql(maf_table, barcode_table, program)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)


'''
----------------------------------------------------------------------------------------------
SQL for above
'''


def final_join_sql(maf_table, barcodes_table, program):
    if program == 'TCGA':
        return '''
             SELECT a.project_short_name,
                    a.case_barcode,
                    a.primary_site,
                    b.*,
                    a.sample_barcode_tumor,
                    a.sample_barcode_normal,
                    a.aliquot_barcode_tumor, 
                    a.aliquot_barcode_normal,
             FROM `{0}` as a JOIN `{1}` as b ON a.tumor_bam_uuid = b.tumor_bam_uuid
        '''.format(barcodes_table, maf_table)
    else:
        return '''
             SELECT a.project_short_name,
                    a.case_barcode,
                    b.*,
                    a.sample_barcode_tumor,
                    a.sample_barcode_normal,
                    a.aliquot_barcode_tumor, 
                    a.aliquot_barcode_normal,
             FROM `{0}` as a JOIN `{1}` as b 
             ON a.aliquot_gdc_id_tumor = b.Tumor_Aliquot_UUID AND a.Start_Position = b.Start_Position
        '''.format(barcodes_table, maf_table)


'''
----------------------------------------------------------------------------------------------
file_info() function Author: Sheila Reynolds
File name includes important information, e.g. the program name and the caller. Extract that
out along with name and ID.
'''


def file_info(aFile, program):
    norm_path = os.path.normpath(aFile)
    path_pieces = norm_path.split(os.sep)

    if program == 'TCGA':
        file_name = path_pieces[-1]
        file_name_parts = file_name.split('.')
        callerName = file_name_parts[2]
        fileUUID = file_name_parts[3]
    else:
        fileUUID = path_pieces[-2]
        callerName = None

    return ([callerName, fileUUID])


'''
------------------------------------------------------------------------------
Clean header field names
Some field names are not accurately named and as of 2020-08-05, the GDC has said they will not be updated. We decided to 
update the field names to accurately reflect the data within th column.
'''


def clean_header_names(header_line, fields_to_fix, program):
    header_id = header_line.split('\t')
    if program != 'TCGA':
        for header_name in range(len(header_id)):
            for dict in fields_to_fix:
                original, new = next(iter(dict.items()))

                if header_id[header_name] == original:
                    header_id[header_name] = new

    return header_id


'''
------------------------------------------------------------------------------
Separate the Callers into their own columns
The non-TCGA maf files has one column with a semicolon deliminated 
'''


def process_callers(callers_str, callers):
    line_callers = callers_str.rstrip('\n').split(';')
    caller_list = dict.fromkeys(callers, 'No')
    for caller in line_callers:
        is_star = re.search(r'\*', caller)
        if caller.rstrip('*') in caller_list.keys():
            if is_star:
                caller_list[caller.rstrip('*')] = 'Yes*'
            else:
                caller_list[caller] = 'Yes'
    return caller_list


'''
------------------------------------------------------------------------------
Concatenate all Files
'''


def concat_all_files(all_files, one_big_tsv, program, callers, fields_to_fix):
    """
    Concatenate all Files
    Gather up all files and glue them into one big one. The file name and path often include features
    that we want to add into the table. The provided file_info_func returns a list of elements from
    the file path. Note if file is zipped,
    we unzip it, concat it, then toss the unzipped version.
    THIS VERSION OF THE FUNCTION USES THE FIRST LINE OF THE FIRST FILE TO BUILD THE HEADER LINE!
    """
    print("building {}".format(one_big_tsv))
    first = True
    header_id = None
    with open(one_big_tsv, 'w') as outfile:
        for filename in all_files:
            toss_zip = False
            if filename.endswith('.zip'):
                dir_name = os.path.dirname(filename)
                print("Unzipping {}".format(filename))
                with zipfile.ZipFile(filename, "r") as zip_ref:
                    zip_ref.extractall(dir_name)
                use_file_name = filename[:-4]
                toss_zip = True
            elif filename.endswith('.gz'):
                dir_name = os.path.dirname(filename)
                use_file_name = filename[:-3]
                print("Uncompressing {}".format(filename))
                with gzip.open(filename, "rb") as gzip_in:
                    with open(use_file_name, "wb") as uncomp_out:
                        shutil.copyfileobj(gzip_in, uncomp_out)
                toss_zip = True
            else:
                use_file_name = filename
            with open(use_file_name, 'r') as readfile:
                callerName, fileUUID = file_info(use_file_name, program)
                for line in readfile:
                    # Seeing comments in MAF files.
                    if not line.startswith('#'):
                        if first:
                            header_id = line.split('\t')[0]
                            header_names = clean_header_names(line, fields_to_fix, program)
                            header_line = '\t'.join(header_names)
                            outfile.write(header_line.rstrip('\n'))
                            outfile.write('\t')
                            outfile.write('file_gdc_id')
                            if program == "TCGA":
                                outfile.write('\t')
                                outfile.write('caller')
                            else:
                                for field in callers:
                                    outfile.write('\t')
                                    outfile.write(field)
                            outfile.write('\n')
                            first = False
                        if not line.startswith(header_id):
                            outfile.write(line.rstrip('\n'))
                            outfile.write('\t')
                            outfile.write(fileUUID)
                            if program == "TCGA":
                                outfile.write('\t')
                                outfile.write(callerName)
                            else:
                                caller_field = line.split('\t')[124]
                                caller_data = process_callers(caller_field, callers)
                                for caller in callers:
                                    outfile.write('\t')
                                    outfile.write(caller_data[caller])
                            outfile.write('\n')
                if toss_zip:
                    os.remove(use_file_name)

'''
----------------------------------------------------------------------------------------------
Is the table that is replacing the view exactly the same?
'''

def compare_two_tables(old_table, new_table, do_batch):
    sql = compare_two_tables_sql(old_table, new_table)
    return bq_harness_with_result(sql, do_batch)

'''
----------------------------------------------------------------------------------------------
SQL for the compare_two_tables function
'''

def compare_two_tables_sql(old_table, new_table):
    return '''
        (
            SELECT * FROM `{0}`
            EXCEPT DISTINCT
            SELECT * from `{1}`
        )
        UNION ALL
        (
            SELECT * FROM `{1}`
            EXCEPT DISTINCT
            SELECT * from `{0}`
        )
    '''.format(old_table, new_table)

'''
----------------------------------------------------------------------------------------------
Main Control Flow
Note that the actual steps run are configured in the YAML input! This allows you to e.g. skip previously run steps.
'''


def main(args):
    if not confirm_google_vm():
        print('This job needs to run on a Google Cloud Compute Engine to avoid storage egress charges [EXITING]')
        return

    if len(args) != 2:
        print(" ")
        print(" Usage : {} <configuration_yaml>".format(args[0]))
        return

    print('job started')

    #
    # Get the YAML config loaded:
    #

    with open(args[1], mode='r') as yaml_file:
        params, filters, bq_filters, steps, callers, schema_tags = load_config(yaml_file.read())

    if params is None:
        print("Bad YAML load")
        return

    #
    # BQ does not like to be given paths that have "~". So make all local paths absolute:
    #

    home = expanduser("~")
    local_files_dir = "{}/{}".format(home, params['LOCAL_FILES_DIR'])
    one_big_tsv = "{}/{}".format(home, params['ONE_BIG_TSV'])
    manifest_file = "{}/{}".format(home, params['MANIFEST_FILE'])
    local_pull_list = "{}/{}".format(home, params['LOCAL_PULL_LIST'])
    file_traversal_list = "{}/{}".format(home, params['FILE_TRAVERSAL_LIST'])
    hold_schema_dict = "{}/{}".format(home, params['HOLD_SCHEMA_DICT'])
    hold_schema_list = "{}/{}".format(home, params['HOLD_SCHEMA_LIST'])

    # Which table are we building?
    release = params['RELEASE']
    use_schema = params['VER_SCHEMA_FILE_NAME']
    if 'current' in steps:
        print('This workflow will update the schema for the "current" table')
        release = 'current'
        use_schema = params['SCHEMA_FILE_NAME']

    # Create table names
    concat_table = '_'.join([params['PROGRAM'], params['DATA_TYPE'], 'concat'])
    barcode_table = '_'.join([params['PROGRAM'], params['DATA_TYPE'], 'barcode'])
    draft_table = '_'.join([params['PROGRAM'], params['DATA_TYPE'], params['BUILD'], 'gdc', '{}'])
    publication_table = '_'.join([params['DATA_TYPE'], params['BUILD'], 'gdc', '{}'])
    manifest_table = '_'.join([params['PROGRAM'],params['DATA_TYPE'], 'manifest'])

    if 'build_manifest_from_filters' in steps:
        max_files = params['MAX_FILES'] if 'MAX_FILES' in params else None
        manifest_success = get_the_bq_manifest(params['FILE_TABLE'].format(params['RELEASE'].strip('r')),
                                               bq_filters, max_files,
                                               params['WORKING_PROJECT'], params['SCRATCH_DATASET'],
                                               manifest_table, params['WORKING_BUCKET'],
                                               params['BUCKET_MANIFEST_TSV'], manifest_file,
                                               params['BQ_AS_BATCH'])
        if not manifest_success:
            print("Failure generating manifest")
            return

    #
    # Best practice is to clear out the directory where the files are going. Don't want anything left over:
    #

    if 'clear_target_directory' in steps:
        create_clean_target(local_files_dir)

    #
    # We need to create a "pull list" of gs:// URLs to pull from GDC buckets. If you have already
    # created a pull list, just plunk it in 'LOCAL_PULL_LIST' and skip this step. If creating a pull
    # list, you can do it using IndexD calls on a manifest file, OR using BQ as long as you have
    # built the manifest using BQ (that route uses the BQ Manifest table that was created).
    #

    if 'build_pull_list' in steps:

        build_pull_list_with_bq("{}.{}.{}".format(params['WORKING_PROJECT'], params['SCRATCH_DATASET'], manifest_table),
                                params['INDEXD_BQ_TABLE'].format(params['RELEASE']),
                                params['WORKING_PROJECT'], params['SCRATCH_DATASET'],
                                "_".join([params['PROGRAM'], params['DATA_TYPE'], 'pull', 'list']),
                                params['WORKING_BUCKET'],
                                params['BUCKET_PULL_LIST'],
                                local_pull_list, params['BQ_AS_BATCH'])

    #
    # Now hitting GDC cloud buckets, not "downloading". Get the files in the pull list:
    #

    if 'download_from_gdc' in steps:
        with open(local_pull_list, mode='r') as pull_list_file:
            pull_list = pull_list_file.read().splitlines()
        pull_from_buckets(pull_list, local_files_dir)

    #
    # Traverse the tree of downloaded files and create a flat list of all files:
    #

    if 'build_traversal_list' in steps:
        all_files = build_file_list(local_files_dir)
        with open(file_traversal_list, mode='w') as traversal_list:
            for line in all_files:
                traversal_list.write("{}\n".format(line))

    if 'concat_all_files' in steps:
        with open(file_traversal_list, mode='r') as traversal_list_file:
            all_files = traversal_list_file.read().splitlines()
            concat_all_files(all_files, one_big_tsv, params['PROGRAM'], callers, params['FIELDS_TO_FIX'])
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
            print("pull_table_info_from_git failed: {}".format(str(ex)))
            return

    if 'process_git_schemas' in steps:
        print('process_git_schema')
        # Where do we dump the schema git repository?
        schema_file = "{}/{}/{}".format(params['SCHEMA_REPO_LOCAL'], params['RAW_SCHEMA_DIR'], use_schema)
        full_file_prefix = "{}/{}".format(params['PROX_DESC_PREFIX'], draft_table.format(release))
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
        full_file_prefix = "{}/{}".format(params['PROX_DESC_PREFIX'], draft_table.format(release))

        # Write out the details
        success = customize_labels_and_desc(full_file_prefix, tag_map_list)

        if not success:
            print("replace_schema_tags failed")
            return False

    if 'analyze_the_schema' in steps:
        print('analyze_the_schema')
        typing_tups = build_schema(one_big_tsv, params['SCHEMA_SAMPLE_SKIPS'])
        full_file_prefix = "{}/{}".format(params['PROX_DESC_PREFIX'], draft_table.format(release))
        schema_dict_loc = "{}_schema.json".format(full_file_prefix)
        build_combined_schema(None, schema_dict_loc,
                              typing_tups, hold_schema_list, hold_schema_dict)

    bucket_target_blob = '{}/{}-{}-{}.tsv'.format(params['WORKING_BUCKET_DIR'], params['DATE'], params['PROGRAM'],
                                                  params['DATA_TYPE'], params['RELEASE'])

    if 'upload_to_bucket' in steps:
        print('upload_to_bucket')
        upload_to_bucket(params['WORKING_BUCKET'], bucket_target_blob, one_big_tsv)

    #
    # Create the BQ table from the TSV:
    #

    if 'create_bq_from_tsv' in steps:
        print('create_bq_from_tsv')
        bucket_src_url = 'gs://{}/{}'.format(params['WORKING_BUCKET'], bucket_target_blob)
        with open(hold_schema_list, mode='r') as schema_hold_dict:
            typed_schema = json_loads(schema_hold_dict.read())
        csv_to_bq(typed_schema, bucket_src_url, params['SCRATCH_DATASET'], concat_table, params['BQ_AS_BATCH'])

    #
    # Need to merge in aliquot and sample barcodes from other tables:
    #

    if 'collect_barcodes' in steps:
        skel_table = '{}.{}.{}'.format(params['WORKING_PROJECT'],
                                       params['SCRATCH_DATASET'],
                                       concat_table)

        if int(params['RELEASE'].strip('r')) < 25:
            case_table = '{}.{}.{}'.format(params['WORKING_PROJECT'],
                                       params['SCRATCH_DATASET'],
                                       params['CASE_TABLE'].format('r25'))
        else:
            case_table = '{}.{}.{}'.format(params['WORKING_PROJECT'],
                                       params['SCRATCH_DATASET'],
                                       params['CASE_TABLE'].format(params['RELEASE']))

        if params['PROGRAM'] == 'TCGA':
            success = attach_aliquot_ids(skel_table, params['FILE_TABLE'].format(params['RELEASE'].strip('r')),
                                         params['SCRATCH_DATASET'],
                                         '_'.join([barcode_table, 'pre']), params['BQ_AS_BATCH'])
            if not success:
                print("attach_aliquot_ids job failed")
                return

            step_1_table = '{}.{}.{}'.format(params['WORKING_PROJECT'],
                                             params['SCRATCH_DATASET'],
                                             '_'.join([barcode_table, 'pre']))
        else:
            step_1_table = skel_table

        success = attach_barcodes(step_1_table, params['ALIQUOT_TABLE'].format(params['RELEASE'].strip('r')),
                                  params['SCRATCH_DATASET'], barcode_table, params['BQ_AS_BATCH'],
                                  params['PROGRAM'], case_table)
        if not success:
            print("attach_barcodes job failed")
            return

    #
    # Merge the barcode info into the final table we are building:
    #

    if 'create_final_table' in steps:
        skel_table = '{}.{}.{}'.format(params['WORKING_PROJECT'],
                                       params['SCRATCH_DATASET'],
                                       concat_table)
        barcodes_table = '{}.{}.{}'.format(params['WORKING_PROJECT'],
                                           params['SCRATCH_DATASET'],
                                           barcode_table)
        success = final_merge(skel_table, barcodes_table,
                              params['SCRATCH_DATASET'], draft_table.format(release), params['BQ_AS_BATCH'],
                              params['PROGRAM'])
        if not success:
            print("Join job failed")
            return

    #
    # The derived table we generate has no field descriptions. Add them from the github json files:
    #

    if 'update_final_schema' in steps:
        success = update_schema(params['SCRATCH_DATASET'], draft_table.format(release), hold_schema_dict)
        if not success:
            print("Schema update failed")
            return

    #
    # Add the table description:
    #

    if 'add_table_description' in steps:
        print('update_table_description')
        full_file_prefix = "{}/{}".format(params['PROX_DESC_PREFIX'],
                                          draft_table.format(release))
        success = install_labels_and_desc(params['SCRATCH_DATASET'],
                                          draft_table.format(release), full_file_prefix)
        if not success:
            print("update_table_description failed")
            return

    #
    # Create second table
    #

    if 'create_current_table' in steps:
        source_table = '{}.{}.{}'.format(params['WORKING_PROJECT'], params['SCRATCH_DATASET'],
                                         draft_table.format(release))
        current_dest = '{}.{}.{}'.format(params['WORKING_PROJECT'], params['SCRATCH_DATASET'],
                                         draft_table.format('current'))

        success = publish_table(source_table, current_dest)

        if not success:
            print("create current table failed")
            print("remember to rerun schema steps for current table")
            return


    #
    # compare and remove old current table
    #

    # compare the two tables
    if 'compare_remove_old_current' in steps:
        old_current_table = '{}.{}.{}'.format(params['PUBLICATION_PROJECT'], params['PUBLICATION_DATASET'],
                                              publication_table.format('current'))
        previous_ver_table = '{}.{}.{}'.format(params['PUBLICATION_PROJECT'],
                                               "_".join([params['PUBLICATION_DATASET'], 'versioned']),
                                               publication_table.format(params['PREVIOUS_RELEASE']))
        table_temp = '{}.{}.{}'.format(params['WORKING_PROJECT'], params['SCRATCH_DATASET'],
                                       "_".join([params['PROGRAM'],publication_table.format(params['PREVIOUS_RELEASE']),'backup']))

        print('Compare {} to {}'.format(old_current_table, previous_ver_table))

        compare = compare_two_tables(old_current_table, previous_ver_table, params['BQ_AS_BATCH'])

        num_rows = compare.total_rows

        if num_rows == 0:
            print('the tables are the same')
        else:
            print('the tables are NOT the same and differ by {} rows'.format(num_rows))

        if not compare:
            print('compare_tables failed')
            return
        # move old table to a temporary location
        elif compare and num_rows == 0:
            print('Move old table to temp location')
            table_moved = publish_table(old_current_table, table_temp)

            if not table_moved:
                print('Old Table was not moved and will not be deleted')
            # remove old table
            elif table_moved:
                print('Deleting old table: {}'.format(old_current_table))
                delete_table = delete_table_bq_job(params['PUBLICATION_DATASET'], publication_table.format('current'))
                if not delete_table:
                    print('delete table failed')
                    return

    #
    # publish table:
    #

    if 'publish' in steps:

        tables = ['versioned', 'current']

        for table in tables:
            if table == 'versioned':
                print(table)
                source_table = '{}.{}.{}'.format(params['WORKING_PROJECT'], params['SCRATCH_DATASET'],
                                                 draft_table.format(release))
                publication_dest = '{}.{}.{}'.format(params['PUBLICATION_PROJECT'],
                                                     "_".join([params['PUBLICATION_DATASET'], 'versioned']),
                                                     publication_table.format(release))
            elif table == 'current':
                print(table)
                source_table = '{}.{}.{}'.format(params['WORKING_PROJECT'], params['SCRATCH_DATASET'],
                                                 draft_table.format('current'))
                publication_dest = '{}.{}.{}'.format(params['PUBLICATION_PROJECT'],
                                                     params['PUBLICATION_DATASET'],
                                                     publication_table.format('current'))
            success = publish_table(source_table, publication_dest)

        if not success:
            print("publish table failed")
            return


    #
    # Update previous versioned table with archived tag
    #

    if 'update_status_tag' in steps:
        print('Update previous table')

        success = update_status_tag("_".join([params['PUBLICATION_DATASET'], 'versioned']),
                                    publication_table.format(params['PREVIOUS_RELEASE']), 'archived')

        if not success:
            print("update status tag table failed")
            return

    #
    # Clear out working temp tables:
    #

    if 'dump_working_tables' in steps:
        dump_tables = [concat_table,
                       barcode_table,
                       draft_table.format('current'),
                       draft_table.format(release),
                       manifest_table]
        for table in dump_tables:
            delete_table_bq_job(params['SCRATCH_DATASET'], table)
    #
    # Done!
    #

    print('job completed')


if __name__ == "__main__":
    main(sys.argv)
