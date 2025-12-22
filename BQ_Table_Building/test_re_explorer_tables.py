"""

Copyright 2024-2025, Institute for Systems Biology

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

from google.cloud import bigquery
import random
import sys
import io
import yaml

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
        return None, None

    return yaml_dict['files_and_buckets_and_tables'], yaml_dict['steps']

def run_raw_query(bq_client, query):
  selection_result = bq_client.query(query)
  return selection_result

def run_project_ids(bq_client, project_table):
  retval = []
  short_query = '''
    SELECT * FROM `{}`
    ORDER BY disease_code, description
    '''.format(project_table)
  result = run_raw_query(bq_client, short_query)
  for row in result:
    retval.append(row.get('key'))
  return retval

def pull_n_features(bq_client, dataset, number, category, feature_table):
  retval_id = []
  retval_dict = {}
  short_query = '''
    SELECT
      dataset,
      id,
      alias,
      datatype,
      patient_values
    FROM `{}`
    WHERE dataset = "{}" AND datatype = "{}" LIMIT {}
    '''.format(feature_table, dataset, category, number)
  result = run_raw_query(bq_client, short_query)
  for row in result:
    retval_id.append(row.get('id'))
    retval_dict[row.get('id')] = row.get('patient_values')
  return retval_id, retval_dict

def pull_patient_barcodes(bq_client, dataset, barcode_table):
  retval = []
  short_query = '''
    SELECT barcodes FROM `{}`
    WHERE dataset = "{}"
    '''.format(barcode_table, dataset)
  result = run_raw_query(bq_client, short_query)
  for row in result:
    retval.append(row.get('barcodes'))
  return retval

def pull_numeric_tuples(bq_client, dataset, id, num_tuple_table):
  retval = []
  short_query = '''
    SELECT t.patient, t.value FROM `{}`, UNNEST(tuples) as t
    WHERE dataset = "{}" AND id = {}
    '''.format(num_tuple_table, dataset, id)
  result = run_raw_query(bq_client, short_query)
  for row in result:
    retval.append((row.get('patient'), float(row.get('value')) if row.get('value') is not None else None))
  return retval

def pull_string_tuples(bq_client, dataset, id, string_tuple_table):
  retval = []
  short_query = '''
    SELECT t.patient, t.value FROM `{}`, UNNEST(tuples) as t
    WHERE dataset = "{}" AND id = {}
    '''.format(string_tuple_table, dataset, id)
  result = run_raw_query(bq_client, short_query)
  for row in result:
    retval.append((row.get('patient'), row.get('value') if row.get('value') is not None else None))
  return retval

def build_numeric_tuples_from_lists(vals_list, barcodes_list):
  num_points = len(vals_list)
  tuple_list = []
  for i in range(0, num_points):
    use_val = float(vals_list[i]) if (vals_list[i] != "NA") else None
    tuple_list.append((barcodes_list[i], use_val))
  return tuple_list

def build_string_tuples_from_lists(vals_list, barcodes_list):

  num_points = len(vals_list)
  tuple_list = []
  for i in range(0, num_points):
    use_val = vals_list[i] if (vals_list[i] != "NA") else None
    tuple_list.append((barcodes_list[i], use_val))
  return tuple_list

def dict_from_tuples(tuple_list):
  retval = {}
  for tup in tuple_list:
    retval[tup[0]] = tup[1]
  return retval

def check_tuple_match(bq_client, params, prog_key, num_to_pull, pct_to_check, type_key, barcodes_list, min_samp):
  feature_table = "{}.{}.{}".format(params['WORKING_PROJECT'], params['RE_DATASET'], params['FEATURE_TABLE'])
  numeric_tuples_table = "{}.{}.{}".format(params['WORKING_PROJECT'], params['RE_DATASET'], params['NUMERIC_TUPLE_TABLE'])
  string_tuples_table = "{}.{}.{}".format(params['WORKING_PROJECT'], params['RE_DATASET'], params['STRING_TUPLE_TABLE'])

  feat_list, feat_dict = pull_n_features(bq_client, prog_key, num_to_pull, type_key, feature_table)
  samp_size = int(pct_to_check * len(feat_list))
  if samp_size < min_samp:
    samp_size = min(len(feat_list), min_samp)
  print(len(feat_list), samp_size)
  rando = random.sample(feat_list, samp_size)
  for id in rando:
    feat_string = feat_dict[id]
    vals_list = feat_string.split(":")
    print(vals_list[0:30])
    if type_key == "N":
      tups_from_list = build_numeric_tuples_from_lists(vals_list, barcodes_list)
      tups_from_tables = pull_numeric_tuples(bq_client, prog_key, id, numeric_tuples_table)
    elif (type_key == "C") or (type_key == "B"):
      tups_from_list = build_string_tuples_from_lists(vals_list, barcodes_list)
      tups_from_tables = pull_string_tuples(bq_client, prog_key, id, string_tuples_table)
    else:
      raise Exception()

    if len(tups_from_list) != len(tups_from_tables):
      return False
    tab_dict = dict_from_tuples(tups_from_tables)
    list_dict = dict_from_tuples(tups_from_list)
    if tab_dict != list_dict:
      return False

    return (True)


###Run the analysis:

def main(args):

    if len(args) != 2:
        print(" ")
        print(" Usage : {} <configuration_yaml>".format(args[0]))
        return
    #
    # Get the YAML config loaded:
    #

    with open(args[1], mode='r') as yaml_file:
        params, steps = load_config(yaml_file.read())

    proj_count = params['PROJECT_COUNT']
    pull_count = params['PULL_COUNT']
    test_fraction = params['TEST_FRACTION']
    min_samples = params['MIN_SAMPLES']

    if 'run_tests' in steps:
        print('run tests')
        bq_client = bigquery.Client(params['WORKING_PROJECT'])

        projects_table = "{}.{}.{}".format(params['WORKING_PROJECT'], params['RE_DATASET'], params['PROJECT_TABLE'])
        barcodes_table = "{}.{}.{}".format(params['WORKING_PROJECT'], params['RE_DATASET'], params['BARCODES_TABLE'])

        project_list = run_project_ids(bq_client, projects_table)
        rando = random.sample(project_list, proj_count)

        for prog in rando:
            barcode_result = pull_patient_barcodes(bq_client, prog, barcodes_table)
            barcodes_list = barcode_result[0].split(":")
            for val in ("N", "C", "B"):
                print(prog, val)
                success = check_tuple_match(bq_client, params, prog, pull_count, test_fraction, val, barcodes_list, min_samples)
                if not success:
                    print("Test failed {} {}".format(prog, val))
                    return False
                print("Test success {} {}".format(prog, val))
    return True

if __name__ == "__main__":
    main(sys.argv)
