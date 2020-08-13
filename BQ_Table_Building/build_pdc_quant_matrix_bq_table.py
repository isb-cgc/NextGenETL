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
import requests

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



def get_quant_matrix_table_one_study(pdc_api_end_point, study_id, study_submitter_id):
    quant_log2_ratio_query = ('{ quantDataMatrix(study_submitter_id: \"'
                              + study_submitter_id + '\" data_type: \"log2_ratio\") }')

    quant_res = requests.post(pdc_api_end_point, json={'query': quant_log2_ratio_query})

    if not quant_res.ok:
        print('Error: PDC API request did not return OK')
        return None

    json_res = quant_res.json()

    if 'errors' in json_res:
        print('No quant matrix for study_submitter_id = ' + study_submitter_id)
        return None

    print('Got quant matrix for study_submitter_id = ' + study_submitter_id)
    quant_matrix = json_res[u'data'][u'quantDataMatrix']

    first_row_data = quant_matrix[0]
    for i in range(1, len(first_row_data)):
        if ":" in first_row_data[i]:
            aliquot_submitter_id = first_row_data[i].split(":")[1]
        else:
            print('no : in here ' + first_row_data[i])
            aliquot_submitter_id = first_row_data[i]
        study_id_and_aliquot_submitter_id = study_id + ':' + aliquot_submitter_id
        quant_matrix[0][i] = study_id_and_aliquot_submitter_id

    print('Converted first row to study_id:aliquot_submitter_id')

    num_rows = len(quant_matrix)
    num_cols = len(quant_matrix[0])
    quant_matrix_table = []
    quant_matrix_table.append(['study_id:aliquot_submitter_id', 'gene', 'log2_ratio'])
    for i in range(1, num_rows):
        for j in range(1, num_cols):
            log2_value = quant_matrix[i][j]
            gene = quant_matrix[i][0]
            study_id_and_aliquot_submitter_id = quant_matrix[0][j]
            quant_matrix_table.append([study_id_and_aliquot_submitter_id, gene, log2_value])

    print('Converted quant matrix into rows of log2ratio values')

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

    if 'get_quant_matrix_table_one_study' in steps:
        print('get_quant_matrix_table_one_study')
        try:
            quant_matrix_table = get_quant_matrix_table_one_study(params['PDC_API_END_POINT'],
                                                                  params['ONE_STUDY_ID'],
                                                                  params['ONE_STUDY_SUBMITTER_ID'])
        except Exception as ex:
            print("get_quant_matrix_table_one_study failed: {}".format(str(ex)))
            return

    print('job completed')

if __name__ == "__main__":
    main(sys.argv)
