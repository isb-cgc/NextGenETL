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
from os.path import expanduser
import yaml
import io
from git import Repo
from json import loads as json_loads

from common_etl.support import confirm_google_vm, create_clean_target, \
                               generic_bq_harness, \
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
Final Table Generation
'''

def build_final_table(pdc_meta_aliquot_table, pdc_quant_aliquot_table,
                      pdc_meta_cases_table, gdc_case_data_table,
                      target_dataset, dest_table, do_batch):

    sql = build_final_table_sql(pdc_meta_aliquot_table, pdc_quant_aliquot_table, pdc_meta_cases_table, gdc_case_data_table)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)

'''
----------------------------------------------------------------------------------------------
# SQL Code For Final Table Generation
'''
def build_final_table_sql(pdc_meta_aliquot_table, pdc_quant_aliquot_table, pdc_meta_cases_table, gdc_case_data_table):

    return '''
      WITH
        a1 AS (
          SELECT
            a.case_id,
            a.sample_id,
            a.aliquot_id,
            a.aliquot_submitter_id,
            b.pdc_internal_aliquot_id as pdc_internal_aliquot_id_quant,
            b.pdc_external_aliquot_id as pdc_external_id_quant
          FROM `{0}` AS a
          JOIN `{1}` AS b
            ON (a.aliquot_submitter_id = b.pdc_external_aliquot_id)),
        b1 AS (
          SELECT
            c.external_case_id,
            a1.*
          FROM a1
          JOIN `{2}` AS c
            ON (a1.case_id = c.case_id)),
        c1 as (
          SELECT
            # Magic number: cut off the "GDC: "
            SUBSTR(b1.external_case_id, 6, 100) AS gdc_id,
            b1.* from b1
          WHERE b1.external_case_id is not NULL and b1.external_case_id LIKE "GDC: %")
      SELECT
        d.project_name,
        d.case_barcode,
        d.case_gdc_id,
        c1.*
      FROM `{3}` as d
      JOIN c1
        ON (c1.gdc_id = d.case_gdc_id)
      ORDER BY d.project_name, d.case_barcode
        '''.format(pdc_meta_aliquot_table, pdc_quant_aliquot_table, pdc_meta_cases_table, gdc_case_data_table)

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

    if 'build_final_table' in steps:
        print('build_final_table')

        success = build_final_table(params['PDC_META_ALIQUOT_TABLE'], params['PDC_QUANT_ALIQUOT_TABLE'],
                                    params['PDC_META_CASES_TABLE'], params['GDC_CASE_DATA_TABLE'],
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
