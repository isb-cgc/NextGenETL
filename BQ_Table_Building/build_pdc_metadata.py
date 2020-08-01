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
import requests
import io
from git import Repo
from json import loads as json_loads
from createSchemaP3 import build_schema

from common_etl.support import get_the_bq_manifest, confirm_google_vm, create_clean_target, \
                               generic_bq_harness, build_file_list, upload_to_bucket, csv_to_bq, \
                               build_pull_list_with_bq, BucketPuller, build_combined_schema, \
                               delete_table_bq_job, install_labels_and_desc, update_schema_with_dict, \
                               generate_table_detail_files, publish_table


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
        return None, None, None

    return yaml_dict['files_and_buckets_and_tables'], yaml_dict['steps']


'''
----------------------------------------------------------------------------------------------
Pull paginated case/sample/aliquot data from PDC and stash in TSV files

'''

def pull_aliquots_from_pdc(endpoint, tsv_cases, tsv_samples, tsv_aliquots):

    #
    # Get paginated case records
    #

    get_paginated_cases_samples_aliquots = \
        '''
        {{ paginatedCasesSamplesAliquots(offset:{offset} limit: {limit}) {{
            total
            casesSamplesAliquots {{
              case_id case_submitter_id external_case_id
              tissue_source_site_code days_to_lost_to_followup
              disease_type index_date lost_to_followup primary_site
              samples {{
                  sample_id sample_submitter_id sample_type sample_type_id gdc_sample_id
                  gdc_project_id biospecimen_anatomic_site composition current_weight days_to_collection
                  days_to_sample_procurement diagnosis_pathologically_confirmed freezing_method
                  initial_weight intermediate_dimension is_ffpe longest_dimension method_of_sample_procurement
                  oct_embedded pathology_report_uuid preservation_method sample_type_id shortest_dimension
                  time_between_clamping_and_freezing time_between_excision_and_freezing tissue_type
                  tumor_code tumor_code_id tumor_descriptor
                  aliquots {{
                    aliquot_id aliquot_submitter_id aliquot_quantity aliquot_volume amount analyte_type
                  }}
              }}
            }}
            pagination {{ count sort from page total pages size }}
          }}
        }}'''

    cases = []
    INCREMENT = 100
    offset = 0
    num_to_download = None
    while True:
        formatted_query = get_paginated_cases_samples_aliquots.format(offset=offset, limit=INCREMENT)
        response = requests.post(endpoint, json={'query': formatted_query})

        if response.ok:
            json_res = response.json()
            case_array = json_res["data"]['paginatedCasesSamplesAliquots']['casesSamplesAliquots']
            cases += case_array
            if num_to_download is None:
                num_to_download = json_res["data"]['paginatedCasesSamplesAliquots']['total']
        else:
            return False

        print("count {} of {}".format(len(cases), num_to_download))
        offset += INCREMENT
        if len(cases) >= num_to_download:
            break

    with open(tsv_cases, "w") as tsv_out:
        tsv_out.write("\t".join(["case_id",
                                 "case_submitter_id",
                                 "days_to_lost_to_followup",
                                 "disease_type",
                                 "external_case_id",
                                 "index_date",
                                 "lost_to_followup",
                                 "primary_site",
                                 "tissue_source_site_code"]) + "\n")
        for case in cases:
            tsv_out.write("\t".join(map(str, [case["case_id"] if case["case_id"] is not None else "",
                                              case["case_submitter_id"] if case["case_submitter_id"] is not None else "",
                                              case["days_to_lost_to_followup"] if case["days_to_lost_to_followup"] is not None else "",
                                              case["disease_type"] if case["disease_type"] is not None else "",
                                              case["external_case_id"] if case["external_case_id"] is not None else "",
                                              case["index_date"] if case["index_date"] is not None else "",
                                              case["lost_to_followup"] if case["lost_to_followup"] is not None else "",
                                              case["primary_site"] if case["primary_site"] is not None else "",
                                              case["tissue_source_site_code"] if case["tissue_source_site_code"] is not None else ""])) + "\n")

    with open(tsv_samples, "w") as tsv_out:
        tsv_out.write("\t".join(["case_id",
                                 "sample_id",
                                 "biospecimen_anatomic_site",
                                 "composition",
                                 "current_weight",
                                 "days_to_collection",
                                 "days_to_sample_procurement",
                                 "diagnosis_pathologically_confirmed",
                                 "freezing_method",
                                 "gdc_project_id",
                                 "gdc_sample_id",
                                 "initial_weight",
                                 "intermediate_dimension",
                                 "is_ffpe",
                                 "longest_dimension",
                                 "method_of_sample_procurement",
                                 "oct_embedded",
                                 "pathology_report_uuid",
                                 "preservation_method",
                                 "sample_submitter_id",
                                 "sample_type",
                                 "sample_type_id",
                                 "shortest_dimension",
                                 "time_between_clamping_and_freezing",
                                 "time_between_excision_and_freezing",
                                 "tissue_type",
                                 "tumor_code",
                                 "tumor_code_id",
                                 "tumor_descriptor"]) + "\n")
        for case in cases:
            case_id = case["case_id"]
            for sample in case["samples"]:
                tsv_out.write('\t'.join(map(str, [case_id if case_id is not None else "",
                                                  sample["sample_id"] if sample["sample_id"] is not None else "",
                                                  sample["biospecimen_anatomic_site"] if sample["biospecimen_anatomic_site"] is not None else "",
                                                  sample["composition"] if sample["composition"] is not None else "",
                                                  sample["current_weight"] if sample["current_weight"] is not None else "",
                                                  sample["days_to_collection"] if sample["days_to_collection"] is not None else "",
                                                  sample["days_to_sample_procurement"] if sample["days_to_sample_procurement"] is not None else "",
                                                  sample["diagnosis_pathologically_confirmed"] if sample["diagnosis_pathologically_confirmed"] is not None else "",
                                                  sample["freezing_method"] if sample["freezing_method"] is not None else "",
                                                  sample["gdc_project_id"] if sample["gdc_project_id"] is not None else "",
                                                  sample["gdc_sample_id"] if sample[ "gdc_sample_id"] is not None else "",
                                                  sample["initial_weight"] if sample["initial_weight"] is not None else "",
                                                  sample["intermediate_dimension"] if sample["intermediate_dimension"] is not None else "",
                                                  sample["is_ffpe"] if sample["is_ffpe"] is not None else "",
                                                  sample["longest_dimension"] if sample["longest_dimension"] is not None else "",
                                                  sample["method_of_sample_procurement"] if sample["method_of_sample_procurement"] is not None else "",
                                                  sample["oct_embedded"] if sample["oct_embedded"] is not None else "",
                                                  sample["pathology_report_uuid"] if sample["pathology_report_uuid"] is not None else "",
                                                  sample["preservation_method"] if sample["preservation_method"] is not None else "",
                                                  sample["sample_submitter_id"] if sample["sample_submitter_id"] is not None else "",
                                                  sample["sample_type"] if sample["sample_type"] is not None else "",
                                                  sample["sample_type_id"] if sample["sample_type_id"] is not None else "",
                                                  sample["shortest_dimension"] if sample["shortest_dimension"] is not None else "",
                                                  sample["time_between_clamping_and_freezing"] if sample["time_between_clamping_and_freezing"] is not None else "",
                                                  sample["time_between_excision_and_freezing"] if sample["time_between_excision_and_freezing"] is not None else "",
                                                  sample["tissue_type"] if sample["tissue_type"] is not None else "",
                                                  sample["tumor_code"] if sample["tumor_code"] is not None else "",
                                                  sample["tumor_code_id"] if sample["tumor_code_id"] is not None else "",
                                                  sample["tumor_descriptor"] if sample["tumor_descriptor"] is not None else ""])) + "\n")

    with open(tsv_aliquots, "w") as tsv_out:
        tsv_out.write("\t".join(["case_id",
                                 "sample_id",
                                 "aliquot_id",
                                 "aliquot_quantity",
                                 "aliquot_submitter_id",
                                 "aliquot_volume",
                                 "amount",
                                 "analyte_type"]) + "\n")
        for case in cases:
            case_id = case["case_id"]
            for sample in case["samples"]:
                sample_id = sample["sample_id"]
                for aliquot in sample["aliquots"]:
                    tsv_out.write("\t".join(map(str, [case_id if case_id is not None else "",
                                                      sample_id if sample_id is not None else "",
                                                      aliquot["aliquot_id"] if aliquot["aliquot_id"] is not None else "",
                                                      aliquot["aliquot_quantity"] if aliquot["aliquot_quantity"] is not None else "",
                                                      aliquot["aliquot_submitter_id"] if aliquot["aliquot_submitter_id"] is not None else "",
                                                      aliquot["aliquot_volume"] if aliquot["aliquot_volume"] is not None else "",
                                                      aliquot["amount"] if aliquot["amount"] is not None else "",
                                                      aliquot["analyte_type"] if aliquot["analyte_type"] is not None else ""])) + "\n")
    return True


'''
----------------------------------------------------------------------------------------------
Pull cases by program from PDC and stash in TSV files

'''

def pull_cases_per_program_from_pdc(endpoint, tsv_file):

    get_cases_by_program = '''{allCases {case_id case_submitter_id project_submitter_id disease_type primary_site}}'''
    cases_by_program = []
    response = requests.post(endpoint, json={'query': get_cases_by_program})
    if response.ok:
        json_res = response.json()
        cases_by_program = json_res["data"]['allCases']
    else:
        return False

    with open(tsv_file, "w") as tsv_out:
        tsv_out.write("\t".join(["case_id",
                                 "case_submitter_id",
                                 "project_submitter_id",
                                 "disease_type",
                                 "primary_site"]) + "\n")

        for case in cases_by_program:
            tsv_out.write("\t".join(map(str, [case["case_id"] if case["case_id"] is not None else "",
                                              case["case_submitter_id"] if case["case_submitter_id"] is not None else "",
                                              case["project_submitter_id"] if case["project_submitter_id"] is not None else "",
                                              case["disease_type"] if case["disease_type"] is not None else "",
                                              case["primary_site"] if case["primary_site"] is not None else ""])) + "\n")
    return True

'''
----------------------------------------------------------------------------------------------
Merge Skeleton With Aliquot Data
Creates the final BQ table by joining the skeleton with the aliquot ID info
'''

def join_with_aliquot_table(cnv_table, aliquot_table, target_dataset, dest_table, do_batch):

    sql = merge_bq_sql(cnv_table, aliquot_table)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)

'''
----------------------------------------------------------------------------------------------
# ### SQL Code For Final Table Generation
# Original author: Sheila Reynolds
'''
def merge_bq_sql(cnv_table, aliquot_table):

    return '''
        WITH
            a1 AS (SELECT DISTINCT GDC_Aliquot
                   FROM `{0}`),
            a2 AS (SELECT b.project_id AS project_short_name,
                          b.case_barcode,
                          b.sample_barcode,
                          b.aliquot_barcode,
                          b.case_gdc_id,
                          b.sample_gdc_id,
                          b.aliquot_gdc_id
                   FROM a1
                   JOIN `{1}` b ON a1.GDC_Aliquot = b.aliquot_gdc_id)
        SELECT
            project_short_name,
            case_barcode,
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
        FROM a2
        JOIN `{0}` b ON a2.aliquot_gdc_id = b.GDC_Aliquot
        '''.format(cnv_table, aliquot_table)

'''
----------------------------------------------------------------------------------------------
Main Control Flow
Note that the actual steps run are configured in the YAML input! This allows you
to e.g. skip previously run steps.
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
        params, steps = load_config(yaml_file.read())


    #
    # BQ does not like to be given paths that have "~". So make all local paths absolute:
    #

    home = expanduser("~")
    local_files_dir = "{}/{}".format(home, params['LOCAL_FILES_DIR'])
    prog_tsv = "{}/{}".format(home, params['PROG_TSV'])
    case_tsv = "{}/{}".format(home, params['CASE_TSV'])
    sample_tsv = "{}/{}".format(home, params['SAMPLE_TSV'])
    aliquot_tsv = "{}/{}".format(home, params['ALIQUOT_TSV'])

    hold_schema_dict_prog = "{}/{}".format(home, params['HOLD_SCHEMA_DICT_PROG'])
    hold_schema_list_prog = "{}/{}".format(home, params['HOLD_SCHEMA_LIST_PROG'])
    hold_schema_dict_case = "{}/{}".format(home, params['HOLD_SCHEMA_DICT_CASE'])
    hold_schema_list_case = "{}/{}".format(home, params['HOLD_SCHEMA_LIST_CASE'])
    hold_schema_dict_sample = "{}/{}".format(home, params['HOLD_SCHEMA_DICT_SAMPLE'])
    hold_schema_list_sample = "{}/{}".format(home, params['HOLD_SCHEMA_LIST_SAMPLE'])
    hold_schema_dict_aliquot = "{}/{}".format(home, params['HOLD_SCHEMA_DICT_ALIQUOT'])
    hold_schema_list_aliquot = "{}/{}".format(home, params['HOLD_SCHEMA_LIST_ALIQUOT'])

    if 'clear_target_directory' in steps:
        print('clear_target_directory')
        create_clean_target(local_files_dir)

    #
    # Use the filter set to build a manifest. Note that if a pull list is
    # provided, these steps can be omitted:
    #

    if 'pull_cases_per_program_from_pdc' in steps:
        endpoint = params["PDC_ENDPOINT"]
        success = pull_cases_per_program_from_pdc(endpoint, prog_tsv)
        if not success:
            print("Failure pulling programs")
            return

    if 'pull_aliquots_from_pdc' in steps:
        endpoint = params["PDC_ENDPOINT"]
        success = pull_aliquots_from_pdc(endpoint, case_tsv, sample_tsv, aliquot_tsv)
        if not success:
            print("Failure pulling programs")
            return

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
        schema_file = "{}/{}/{}".format(params['SCHEMA_REPO_LOCAL'], params['RAW_SCHEMA_DIR'], params['SCHEMA_FILE_NAME'])
        full_file_prefix = "{}/{}".format(params['PROX_DESC_PREFIX'], params['FINAL_TARGET_TABLE'])
        # Write out the details
        success = generate_table_detail_files(schema_file, full_file_prefix)
        if not success:
            print("process_git_schemas failed")
            return

    if 'analyze_the_schema' in steps:
        print('analyze_the_schema')
        typing_tups = build_schema(params["PROG_TSV"], params['SCHEMA_SAMPLE_SKIPS'])
        full_file_prefix = "{}/{}".format(params['PROX_DESC_PREFIX'], params['TARGET_TABLE_PROG'])
        schema_dict_loc = "{}_schema.json".format(full_file_prefix)
        build_combined_schema(None, None,
                              typing_tups, hold_schema_list_prog, hold_schema_dict_prog)
        typing_tups = build_schema(params["PROG_TSV"], params['SCHEMA_SAMPLE_SKIPS'])
        full_file_prefix = "{}/{}".format(params['PROX_DESC_PREFIX'], params['FINAL_TARGET_CASE_TABLE'])
        schema_dict_loc = "{}_schema.json".format(full_file_prefix)
        build_combined_schema(None, None,
                              typing_tups, hold_schema_list_case, hold_schema_dict_case)
        typing_tups = build_schema(params["PROG_TSV"], params['SCHEMA_SAMPLE_SKIPS'])
        full_file_prefix = "{}/{}".format(params['PROX_DESC_PREFIX'], params['TARGET_TABLE_SAMPLE'])
        schema_dict_loc = "{}_schema.json".format(full_file_prefix)
        build_combined_schema(None, None,
                              typing_tups, hold_schema_list_sample, hold_schema_dict_sample)
        typing_tups = build_schema(params["PROG_TSV"], params['SCHEMA_SAMPLE_SKIPS'])
        full_file_prefix = "{}/{}".format(params['PROX_DESC_PREFIX'], params['FINAL_TARGET_TABLE_ALIQUOT'])
        schema_dict_loc = "{}_schema.json".format(full_file_prefix)
        build_combined_schema(None, None,
                              typing_tups, hold_schema_list_aliquot, hold_schema_dict_aliquot)

    bucket_target_program = '{}/{}'.format(params['WORKING_BUCKET_DIR'], params['BUCKET_TSV_PROGRAM'])
    bucket_target_case = '{}/{}'.format(params['WORKING_BUCKET_DIR'], params['BUCKET_TSV_CASE'])
    bucket_target_sample = '{}/{}'.format(params['WORKING_BUCKET_DIR'], params['BUCKET_TSV_SAMPLE'])
    bucket_target_aliquot = '{}/{}'.format(params['WORKING_BUCKET_DIR'], params['BUCKET_TSV_ALIQUOT'])

    if 'upload_to_bucket' in steps:
        print('upload_to_bucket')
        upload_to_bucket(params['WORKING_BUCKET'], bucket_target_program, params["PROG_TSV"])
        upload_to_bucket(params['WORKING_BUCKET'], bucket_target_case, params["CASE_TSV"])
        upload_to_bucket(params['WORKING_BUCKET'], bucket_target_sample, params["SAMPLE_TSV"])
        upload_to_bucket(params['WORKING_BUCKET'], bucket_target_aliquot, params["ALIQUOT_TSV"])

    if 'create_bq_from_tsv' in steps:
        print('create_bq_from_tsv')
        bucket_src_url = 'gs://{}/{}'.format(params['WORKING_BUCKET'], bucket_target_program)
        with open(hold_schema_list_prog, mode='r') as schema_hold_dict:
            typed_schema = json_loads(schema_hold_dict.read())
        csv_to_bq(typed_schema, bucket_src_url, params['TARGET_DATASET'], params['TARGET_TABLE_PROG'], params['BQ_AS_BATCH'])

        bucket_src_url = 'gs://{}/{}'.format(params['WORKING_BUCKET'], bucket_target_case)
        with open(hold_schema_list_case, mode='r') as schema_hold_dict:
            typed_schema = json_loads(schema_hold_dict.read())
        csv_to_bq(typed_schema, bucket_src_url, params['TARGET_DATASET'], params['TARGET_TABLE_CASE'], params['BQ_AS_BATCH'])

        bucket_src_url = 'gs://{}/{}'.format(params['WORKING_BUCKET'], bucket_target_sample)
        with open(hold_schema_list_sample, mode='r') as schema_hold_dict:
            typed_schema = json_loads(schema_hold_dict.read())
        csv_to_bq(typed_schema, bucket_src_url, params['TARGET_DATASET'], params['TARGET_TABLE_SAMPLE'], params['BQ_AS_BATCH'])

        bucket_src_url = 'gs://{}/{}'.format(params['WORKING_BUCKET'], bucket_target_aliquot)
        with open(hold_schema_list_aliquot, mode='r') as schema_hold_dict:
            typed_schema = json_loads(schema_hold_dict.read())
        csv_to_bq(typed_schema, bucket_src_url, params['TARGET_DATASET'], params['TARGET_TABLE_ALIQUOT'], params['BQ_AS_BATCH'])

    if 'join_case_tables' in steps:
        print('join_case_tables')
        full_target_table = '{}.{}.{}'.format(params['WORKING_PROJECT'],
                                              params['TARGET_DATASET'],
                                              params['TARGET_TABLE'])
        success = join_with_aliquot_table(full_target_table, params['ALIQUOT_TABLE'],
                                          params['TARGET_DATASET'], params['FINAL_TARGET_TABLE'], params['BQ_AS_BATCH'])
        if not success:
            print("Join job failed")

    #
    # Update the per-field descriptions:
    #

    if 'update_field_descriptions' in steps:
        print('update_field_descriptions')
        full_file_prefix = "{}/{}".format(params['PROX_DESC_PREFIX'], params['FINAL_TARGET_TABLE'])
        schema_dict_loc = "{}_schema.json".format(full_file_prefix)
        schema_dict = {}
        with open(schema_dict_loc, mode='r') as schema_hold_dict:
            full_schema_list = json_loads(schema_hold_dict.read())
        for entry in full_schema_list:
            schema_dict[entry['name']] = {'description': entry['description']}

        success = update_schema_with_dict(params['TARGET_DATASET'], params['FINAL_TARGET_TABLE'], schema_dict)
        if not success:
            print("update_field_descriptions failed")
            return

    #
    # Add description and labels to the target table:
    #

    if 'update_table_description' in steps:
        print('update_table_description')
        full_file_prefix = "{}/{}".format(params['PROX_DESC_PREFIX'], params['FINAL_TARGET_TABLE'])
        success = install_labels_and_desc(params['TARGET_DATASET'], params['FINAL_TARGET_TABLE'], full_file_prefix)
        if not success:
            print("update_table_description failed")
            return

    #
    # publish table:
    #

    if 'publish' in steps:

        source_table = '{}.{}.{}'.format(params['WORKING_PROJECT'], params['TARGET_DATASET'],
                                         params['FINAL_TARGET_TABLE'])
        publication_dest = '{}.{}.{}'.format(params['PUBLICATION_PROJECT'], params['PUBLICATION_DATASET'],
                                             params['PUBLICATION_TABLE'])

        success = publish_table(source_table, publication_dest)

        if not success:
            print("publish table failed")
            return

    #
    # Clear out working temp tables:
    #

    if 'dump_working_tables' in steps:
        dump_table_tags = ['TARGET_TABLE']
        dump_tables = [params[x] for x in dump_table_tags]
        for table in dump_tables:
            delete_table_bq_job(params['TARGET_DATASET'], table)

    print('job completed')

if __name__ == "__main__":
    main(sys.argv)

