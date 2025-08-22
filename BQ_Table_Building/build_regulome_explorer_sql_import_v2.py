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

import sys
import csv
from google.cloud import bigquery, storage
from os.path import expanduser
import yaml
import io
import time
from git import Repo
from json import loads as json_loads, dumps as json_dumps

from common_etl.support import generic_bq_harness, bq_harness_with_result, \
                               delete_table_bq_job, clear_table_labels, install_table_metadata, \
                               publish_table

'''
Make sure the VM has BigQuery and Storage Read/Write permissions!
'''
#  Sometimes e.g. \"19_gl000209_random\", not just 1-23 plus X and Y',


CLUSTER_COLS = ["dataset"]

SQL_SCHEMA_MIN = '''[
    {
        "description": "Dataset ID",
        "name": "dataset",
        "type": "STRING",
        "mode": "REQUIRED"
    }
]'''

LOADED_SQL_SCHEMA = '''[
    {
        "description": "Dataset ID",
        "name": "dataset",
        "type": "STRING",
        "mode": "REQUIRED"
    },
    {
        "description": "Feature ID",
        "name": "id",
        "type": "INT64",
        "mode": "REQUIRED"
    },
    {
        "description": "Feature alias",
        "name": "alias",
        "type": "STRING",
        "mode": "REQUIRED"
    },
    {
        "description": "Feature type: one of (B)inary, (N)umeric, or (C)ategorical",
        "name": "datatype",
        "type": "STRING",
        "mode": "REQUIRED"
    },
    {
        "description": "Feature type; one of: RPPA, GEXP, CLIN, GNAB, MIRN, CNVR, METH, SAMP",
        "name": "source",
        "type": "STRING",
        "mode": "REQUIRED"
    },
    {
        "description": "Feature label",
        "name": "label",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "description": "Feature chromosome",
        "name": "chr",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "description": "Start location of feature",
        "name": "start",
        "type": "INT64",
        "mode": "NULLABLE"
    },
    {
        "description": "End location of feature",
        "name": "end",
        "type": "INT64",
        "mode": "NULLABLE"
    },
    {
        "description": "Feature chromosome strand",
        "name": "strand",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "description": "Label description",
        "name": "label_desc",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "description": "Colon-separated patient-ordered list of values, or NA",
        "name": "patient_values",
        "type": "STRING",
        "mode": "REQUIRED"
    },
    {
        "description": "Mean value",
        "name": "patient_values_mean",
        "type": "FLOAT64",
        "mode": "NULLABLE"
    },
    {
        "description": "Interesting score",
        "name": "interesting_score",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "description": "Quantile value",
        "name": "quantile_val",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "description": "Quantile",
        "name": "quantile",
        "type": "STRING",
        "mode": "NULLABLE"
    }
]'''

FINAL_SQL_SCHEMA = '''[
    {
        "description": "Dataset ID",
        "name": "dataset",
        "type": "STRING",
        "mode": "REQUIRED"
    },
    {
        "description": "Feature ID",
        "name": "id",
        "type": "INT64",
        "mode": "REQUIRED"
    },
    {
        "description": "Feature alias",
        "name": "alias",
        "type": "STRING",
        "mode": "REQUIRED"
    },
    {
        "description": "Feature type: one of (B)inary, (N)umeric, or (C)ategorical",
        "name": "datatype",
        "type": "STRING",
        "mode": "REQUIRED"
    },
    {
        "description": "Feature type; one of: RPPA, GEXP, CLIN, GNAB, MIRN, CNVR, METH, SAMP",
        "name": "source",
        "type": "STRING",
        "mode": "REQUIRED"
    },
    {
        "description": "Feature label",
        "name": "label",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "description": "Feature chromosome",
        "name": "chr",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "description": "Start location of feature",
        "name": "start_loc",
        "type": "INT64",
        "mode": "NULLABLE"
    },
    {
        "description": "End location of feature",
        "name": "end_loc",
        "type": "INT64",
        "mode": "NULLABLE"
    },
    {
        "description": "Feature chromosome strand",
        "name": "strand",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "description": "Label description",
        "name": "label_desc",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "description": "Colon-separated patient-ordered list of values, or NA",
        "name": "patient_values",
        "type": "STRING",
        "mode": "REQUIRED"
    },
    {
        "description": "Mean value",
        "name": "patient_values_mean",
        "type": "FLOAT64",
        "mode": "NULLABLE"
    }
]'''

PATIENT_SCHEMA = '''[
    {
        "description": "Patient IDs ordered to match plotting points",
        "name": "barcodes",
        "type": "STRING",
        "mode": "REQUIRED"
    }
]
'''

BARCODE_SCHEMA = '''[
    {
        "description": "Dataset key",
        "name": "dataset",
        "type": "STRING",
        "mode": "REQUIRED"
    },
    {
        "description": "Patient IDs ordered to match patient_values lists in re_features",
        "name": "barcodes",
        "type": "STRING",
        "mode": "REQUIRED"
    }
]
'''

NUMERIC_TUPLE_SCHEMA = '''[
    {
        "description": "Dataset key",
        "name": "dataset",
        "type": "STRING",
        "mode": "REQUIRED"
    },
    {
        "description": "Feature ID",
        "name": "id",
        "type": "INTEGER",
        "mode": "REQUIRED"
    },
    {
        "description": "Feature alias",
        "name": "alias",
        "type": "STRING",
        "mode": "REQUIRED"
    },    
    {
        "description": "Feature type: always (N)umerical in this table",
        "name": "datatype",
        "type": "STRING",
        "mode": "REQUIRED"
    },  
    {
        "description": "List of (patient barcode, numeric value) tuples for the feature",
        "name": "tuples",
        "type": "RECORD",
        "mode": "REPEATED",
        "fields": [
            {
                "description": "Patient barcode",
                "name": "patient",
                "type": "STRING",
                "mode": "REQUIRED"
            },
            {
                "description": "Numeric value",
                "name": "value",
                "type": "NUMERIC",
                "mode": "NULLABLE"
            }
        ]
    }
]
'''

STRING_TUPLE_SCHEMA = '''[
    {
        "description": "Dataset key",
        "name": "dataset",
        "type": "STRING",
        "mode": "REQUIRED"
    },
    {
        "description": "Feature ID",
        "name": "id",
        "type": "INTEGER",
        "mode": "REQUIRED"
    },
    {
        "description": "Feature alias",
        "name": "alias",
        "type": "STRING",
        "mode": "REQUIRED"
    },    
    {
        "description": "Feature type: either (B)inary or (C)ategorical",
        "name": "datatype",
        "type": "STRING",
        "mode": "REQUIRED"
    },  
    {
        "description": "List of (patient barcode, string value) tuples for the feature",
        "name": "tuples",
        "type": "RECORD",
        "mode": "REPEATED",
        "fields": [
            {
                "description": "Patient barcode",
                "name": "patient",
                "type": "STRING",
                "mode": "REQUIRED"
            },
            {
                "description": "String value",
                "name": "value",
                "type": "STRING",
                "mode": "NULLABLE"
            }
        ]
    }
]
'''


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

    return yaml_dict['files_and_buckets_and_tables'], yaml_dict['steps'], yaml_dict['tagging']


'''
----------------------------------------------------------------------------------------------
# Make an empty table. Delete existing if specified
'''

def create_empty_target_with_schema(schema, project_id, dataset_id, targ_table, delete_first, cluster_cols):

    client = bigquery.Client(project_id)

    table_id = "{}.{}.{}".format(project_id, dataset_id, targ_table)

    if delete_first:
        client.delete_table(table_id, not_found_ok=True)

    schema_list = []
    for mydict in schema:
        if mydict['type'].upper() == 'RECORD':
            field_list = []
            for field_dict in mydict['fields']:
                field_list.append(bigquery.SchemaField(field_dict['name'], field_dict['type'].upper(),
                                                        mode=field_dict['mode'], description=field_dict['description']))
            schema_list.append(bigquery.SchemaField(mydict['name'], mydict['type'].upper(),
                                                    mode=mydict['mode'], description=mydict['description'], fields=field_list))
        else:
            schema_list.append(bigquery.SchemaField(mydict['name'], mydict['type'].upper(),
                                                    mode=mydict['mode'], description=mydict['description']))

    table = bigquery.Table(table_id, schema=schema_list)

    if cluster_cols is not None:
        table.clustering_fields = cluster_cols

    table = client.create_table(table)
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )
    return True

'''
----------------------------------------------------------------------------------------------
Combine cores
'''

def glue_features_together(table_names, params, column_lists, common_schema_columns,
                           target_dataset, dest_table, do_batch, project):
    sql = glue_features_sql(table_names, params, column_lists, common_schema_columns)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, False, project=project)

'''
----------------------------------------------------------------------------------------------
Combine cores SQL
'''

def glue_features_sql(table_names, params, column_lists, common_schema_columns):

    #
    # Some of the raw features tables are missing columns: make those null. Also, we
    # need to glue the dataset ID into the first column:
    #

    core_sql = []
    for table in table_names:
        print("table >>>" + table)
        column_order = column_lists[table]
        use_cols = []
        for column in common_schema_columns:
            if column == "dataset":
                use_cols.append('"{}" as {}'.format(table, column))
            elif column not in column_order:
                use_cols.append('null as {}'.format(column))
            else:
                use_cols.append("`{}` ".format(column))
        select_cols = ",".join(use_cols)
        core_sql.append("SELECT {} FROM `{}.{}.raw_{}` \n".format(select_cols, params['WORKING_PROJECT'],
                                                                       params['SCRATCH_DATASET'], table))

    full_sql = " UNION DISTINCT \n".join(core_sql)

    return full_sql

'''
----------------------------------------------------------------------------------------------
Combine cores
'''

def glue_patients_together(study_dict, params, target_dataset, dest_table, do_batch, project):
    sql = glue_patients_sql(study_dict, params)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, False, project=project)

'''
----------------------------------------------------------------------------------------------
Combine cores SQL
'''

def glue_patients_sql(study_dict, params):

    #
    # Patient table is very simple:
    #

    core_sql = []
    for study in study_dict:
        study_name = study_dict[study]
        print(study, study_name)
        table = "{}.{}.{}_patients".format(params['WORKING_PROJECT'], params['SCRATCH_DATASET'], study_name)
        print("table >>>" + table)
        core_sql.append('SELECT "{}" as dataset, barcodes FROM `{}` \n'.format(study_name, table))
    full_sql = " UNION DISTINCT \n".join(core_sql)
    print (full_sql)
    return full_sql


'''
----------------------------------------------------------------------------------------------
Convert : separated patient values to an array of tuples
'''

def value_string_list_to_tuple_array(params, category, target_dataset, dest_table, do_batch, project):
    sql = value_string_list_to_tuple_array_sql(params, category)
    typed_dest_table = dest_table.format(category)
    return generic_bq_harness(sql, target_dataset, typed_dest_table, do_batch, False, project=project)

'''
----------------------------------------------------------------------------------------------
Convert : separated patient values to an array of tuples SQL
'''

def value_string_list_to_tuple_array_sql(params, category):

    #
    # hat tip to https://stackoverflow.com/questions/58241689/how-to-return-an-array-of-structs-from-a-struct-of-arrays-in-standard-sql
    #
    # We want to take a values entry like "12.776:2.641:2.661:NA:12.732:1.081:2.633:11.425:..." and combine it with
    # a patient barcode entry like "TCGA-A1-A0SB-01:TCGA-A1-A0SD-01:TCGA-A1-A0SE-01:TCGA-A1-A0SF-01:TCGA-A1-A0SG-01:...
    # to create an array of tuples for easier data extraction. Note the original format is useful for plotting programs, but
    # not for analysis!
    #

    full_barcode_table = "{}.{}.{}".format(params['WORKING_PROJECT'], params['TARGET_DATASET'], params['BARCODE_TABLE'])
    full_feature_table = "{}.{}.{}".format(params['WORKING_PROJECT'], params['TARGET_DATASET'], params['FINAL_FULL_FEATURE_TABLE'])

    if category == "numeric":
        selector = '"N"'
    elif category == "string":
        selector = '"C", "B"'
    else:
        raise Exception()

    return '''
    WITH a1 AS (SELECT b.dataset, f.id, f.alias, f.datatype, SPLIT(b.barcodes, ":") as patient, SPLIT(f.patient_values, ":") as value FROM
    `{}` AS f JOIN `{}` AS b
    ON b.dataset = f.dataset WHERE f.datatype IN ({})),
    c1 AS (SELECT
      dataset, id, alias, datatype,
      ARRAY(
        SELECT AS STRUCT patient, CAST(IF (value = "NA", NULL, value) AS {}) AS value,
        FROM UNNEST(patient) as patient WITH OFFSET
        LEFT JOIN UNNEST(value) as value WITH OFFSET USING(OFFSET)
      ) AS tuples
      FROM a1)    
    SELECT * from c1
    '''.format(full_feature_table, full_barcode_table, selector, category.upper())

'''
----------------------------------------------------------------------------------------------
Tables are slightly different
'''

def build_custom_schema(params, old_study, new_study, schema_dict, is_first, summary_name):
    custom_schema = []
    column_list = [new_study]
    filename = "{}/mysql/{}/{}_features.sql".format(params['LOCAL_FILES_DIR'], "tcga", old_study)
    summary_filename = "{}/{}".format(params['LOCAL_FILES_DIR'], summary_name)
    print(filename)
    sys.stdout.flush()
    started = False
    ended = False
    with open(filename, "r") as file:
        for line in file:
            if line.find("CREATE TABLE") != -1:
                started = True
            if line.find("PRIMARY KEY") != -1:
                ended = True
            if (not started) or ended:
                continue
            chunks = line.rstrip().replace('`', '').split()
            if chunks[0] in schema_dict:
                custom_schema.append(schema_dict[chunks[0]])
                column_list.append(chunks[0])

    mode = "w" if is_first else "a"
    with open(summary_filename, mode, newline='') as f_output:
        tsv_output = csv.writer(f_output, delimiter='\t')
        tsv_output.writerow(column_list)

    return custom_schema

'''
----------------------------------------------------------------------------------------------
Retrieve column dictionary from file
'''

def read_columns_dict(params, summary_name):
    column_dict = {}
    summary_filename = "{}/{}".format(params['LOCAL_FILES_DIR'], summary_name)
    print(summary_filename)

    with open(summary_filename, 'r', newline='') as f_input:
        tsv_input = csv.reader(f_input, delimiter='\t')
        for column_row in tsv_input:
            column_dict[column_row[0]] = column_row[1:]

    return column_dict

'''
----------------------------------------------------------------------------------------------
# get to bigquery
'''

def tsv_to_bq_write_depo(schema, csv_uri, project_id, dataset_id, targ_table, do_batch, write_depo):

    client = bigquery.Client(project_id)

    dataset_ref = client.dataset(dataset_id)
    job_config = bigquery.LoadJobConfig()
    if do_batch:
        job_config.priority = bigquery.QueryPriority.BATCH

    schema_list = []
    for mydict in schema:
        use_mode = mydict['mode'] if "mode" in mydict else 'NULLABLE'
        schema_list.append(bigquery.SchemaField(mydict['name'], mydict['type'].upper(),
                                                mode=use_mode, description=mydict['description']))

    job_config.schema = schema_list
    job_config.skip_leading_rows = 0
    job_config.source_format = bigquery.SourceFormat.CSV
    if write_depo is not None:
        job_config.write_disposition = write_depo
    # Can make the "CSV" file a TSV file using this:
    job_config.field_delimiter = '\t'

    load_job = client.load_table_from_uri(
        csv_uri,
        dataset_ref.table(targ_table),
        job_config=job_config)  # API request
    print('Starting job {}'.format(load_job.job_id))

    location = 'US'
    job_state = 'NOT_STARTED'
    while job_state != 'DONE':
        load_job = client.get_job(load_job.job_id, location=location)
        print('Job {} is currently in state {}'.format(load_job.job_id, load_job.state))
        sys.stdout.flush()
        job_state = load_job.state
        if job_state != 'DONE':
            time.sleep(5)
    print('Job {} is done'.format(load_job.job_id))
    sys.stdout.flush()

    load_job = client.get_job(load_job.job_id, location=location)
    if load_job.error_result is not None:
        print('Error result!! {}'.format(load_job.error_result))
        for err in load_job.errors:
            print(err)
        return False

    destination_table = client.get_table(dataset_ref.table(targ_table))
    print('Loaded {} rows.'.format(destination_table.num_rows))
    return True

'''
----------------------------------------------------------------------------------------------
Do feature repair
'''

def repair_raw_features(raw_table, target_dataset, dest_table, do_batch, project):
    sql = repair_raw_sql(raw_table)
    print(sql)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, False, project=project)

'''
----------------------------------------------------------------------------------------------
# SQL Code to repair raw data
'''

def repair_raw_sql(source):
    # Known issues in the raw features data:
    # 1) ALL rows have strand = 0. We need to pull that info out of the alias. Suggest, +, -, or null
    #    Seen for MIRN, RPPA, GEXP, and GNAB
    #    Field chr MUST NOT be null; otherwise other values seen in that alias slot.
    # 2) Note that chromosome can be sometimes e.g. "19_gl000209_random", not just 1-23 plus X and Y and m
    # 3) We have 790722 rows where quantile_val = "\N" (which is matched by "\\N" in a where clause) Suggest null.
    # 4) We saw interesting score also = "\N" in gbm_23mayEdist_pw_features.txt, but that dataset
    #    did not make the final cut. Do the test & fix anyway. Suggest null.
    # 5) When chr is null, start and end are "0". Suggest null.
    # 6) When type is categorical, patient_values_mean is 0. Suggest null.
    # 7) For dataset lgg_04oct13 We have chr = "x" (lower case). Suggest normalizing to upper case X
    # 8) Agreement that quantile val, interesting score, quantile (including Q5!) columns will be ditched
    # 9) Change reserved SQL keywords (end, type) for column names
    # 10) Seeing end with "0" when start is a big number. A point mutation! Will set end to NULL (not zero)
    #     even if we have a chromosome as long as start is non-zero
    # Note the "r" string to keep BQ from complaining about invalid escape!
    return r'''
      WITH a1 as (
      SELECT
        dataset,
        id,
        alias,
        `type` as datatype,
        source,
        label,
        CASE
           WHEN chr = "x" THEN "X"
           ELSE chr
        END as chr,
        CASE
           WHEN chr IS NULL THEN NULL
           ELSE start
        END as start_loc,
        CASE
           WHEN chr IS NULL THEN NULL
           WHEN ((chr IS NOT NULL) AND (start != 0) AND (`end` = 0)) THEN NULL
           ELSE `end`
        END as end_loc,
        CASE
           WHEN chr IS NOT NULL THEN SPLIT(alias, ':')[offset(6)]
           ELSE NULL
        END as strand_or_blank,
        label_desc,
        patient_values,
        CASE
           WHEN `type` = "C" THEN NULL
           ELSE patient_values_mean
        END as patient_values_mean
    FROM {})
    SELECT  dataset,
        id,
        alias,
        datatype,
        source,
        label,
        chr,
        start_loc,
        end_loc,
        CASE
           WHEN strand_or_blank IS NULL THEN NULL
           WHEN strand_or_blank = "" THEN NULL
           ELSE strand_or_blank
        END as strand,
        label_desc,
        patient_values,
        patient_values_mean FROM a1
    '''.format(source)


'''
----------------------------------------------------------------------------------------------
Main Control Flow
Note that the actual steps run are configured in the YAML input! This allows you
to e.g. skip previously run steps.
'''
def main(args):

    if len(args) != 2:
        print(" ")
        print(" Usage : {} <configuration_yaml>".format(args[0]))
        return

    print('job started')

    #
    # Get the YAML config loaded:
    #

    with open(args[1], mode='r') as yaml_file:
        params, steps, tagging = load_config(yaml_file.read())

    patient_schema = json_loads(PATIENT_SCHEMA)
    typed_schema = json_loads(LOADED_SQL_SCHEMA)
    schema_dict = {}
    for entry in typed_schema:
        schema_dict[entry["name"]] = entry

    #
    # Keys are filenames, values are new dataset names:
    #
    map_sql = "SELECT * from {}.{}.{}".format(params['WORKING_PROJECT'],
                                              params['ASSOC_SCRATCH_DATASET'],
                                              params['MAPPINGS_TABLE'])
    study_mapping = bq_harness_with_result(map_sql, params['BQ_AS_BATCH'])
    study_dict = {}
    for study in study_mapping:
        study_dict[study.privkey] = study.key
    study_list = list(study_dict.values())

    is_first = True
    for study in study_dict:
        print(study, study_dict[study])
        if 'create_feature_bq_from_tsv' in steps:
            pull_list = ['gs://{}/{}'.format(params['WORKING_BUCKET'], params['FEATURE_SQL'].format(study))]
            bp = BucketPuller(2)
            bp.pull_from_buckets(pull_list, params['LOCAL_FILES_DIR'])

            custom_schema = build_custom_schema(params, study, study_dict[study], schema_dict, is_first, params['SUMMARY_FILE'])
            is_first = False

            bucket_src_url = 'gs://{}/{}'.format(params['WORKING_BUCKET'], params['FEATURE_TSV'].format(study))

            # is study_dict[study] the correct value to use???
            tsv_to_bq_write_depo(custom_schema, bucket_src_url, params['WORKING_PROJECT'],
                                 params['SCRATCH_DATASET'], "raw_{}".format(study_dict[study]),
                                 params['BQ_AS_BATCH'], "WRITE_TRUNCATE")

        if 'create_patient_bq_from_tsv' in steps:
            bucket_src_url = 'gs://{}/{}'.format(params['WORKING_BUCKET'], params['PATIENT_TSV'].format(study))
            table_name = "{}_patients".format(study_dict[study])

            tsv_to_bq_write_depo(patient_schema, bucket_src_url, params['WORKING_PROJECT'],
                                 params['SCRATCH_DATASET'], table_name,
                                 params['BQ_AS_BATCH'], "WRITE_TRUNCATE")

    #
    # Glue all the separate raw feature tables together into one table with a uniform schema,
    # including a (new, public) dataset ID column, which is used to cluster the table.
    #

    if 'build_raw_table' in steps:
        print('build_raw_table')
        typed_schema = json_loads(LOADED_SQL_SCHEMA)
        success = create_empty_target_with_schema(typed_schema, params['WORKING_PROJECT'],
                                                  params['SCRATCH_DATASET'], params['RAW_FULL_FEATURE_TABLE'],
                                                  True, None)
        if not success:
            print("build_raw_table failed")
            return


    if 'glue_features_together' in steps:
        print('glue_features_together')

        column_dict = read_columns_dict(params, params['SUMMARY_FILE'])

        typed_schema = json_loads(LOADED_SQL_SCHEMA)
        common_schema_columns = []
        for entry in typed_schema:
            common_schema_columns.append(entry["name"])

        success = glue_features_together(study_list, params, column_dict, common_schema_columns,
                                         params['SCRATCH_DATASET'], params['RAW_FULL_FEATURE_TABLE'],
                                         params['BQ_AS_BATCH'], params['WORKING_PROJECT'])

        if not success:
            print("glue_features_together failed")
            return

    if 'build_final_cluster_table' in steps:
        print('build_final_cluster_table')
        typed_schema = json_loads(FINAL_SQL_SCHEMA)
        success = create_empty_target_with_schema(typed_schema, params['WORKING_PROJECT'],
                                                  params['TARGET_DATASET'], params['FINAL_FULL_FEATURE_TABLE'],
                                                  True, CLUSTER_COLS)
        if not success:
            print("build_final_cluster_table failed")
            return


    if 'massage_raw_table' in steps:
        print('massage_raw_table')
        raw_table = "{}.{}.{}".format(params['WORKING_PROJECT'], params['SCRATCH_DATASET'],
                                      params['RAW_FULL_FEATURE_TABLE'])
        success = repair_raw_features(raw_table, params['TARGET_DATASET'],
                                      params['FINAL_FULL_FEATURE_TABLE'],
                                      params['BQ_AS_BATCH'], params['WORKING_PROJECT'])

        if not success:
            print("massage_raw_table failed")
            return

    #
    # Create the table that holds patient barcode lists that match the value lists that are in the feature tables:
    #
    if 'glue_barcodes_together' in steps:
        print('glue_barcodes_together')

        typed_schema = json_loads(BARCODE_SCHEMA)
        success = create_empty_target_with_schema(typed_schema, params['WORKING_PROJECT'],
                                                  params['TARGET_DATASET'], params['BARCODE_TABLE'],
                                                  True, None)
        if not success:
            print("glue_barcodes_together create table failed")
            return

        success = glue_patients_together(study_dict, params, params['TARGET_DATASET'], params['BARCODE_TABLE'],
                                         params['BQ_AS_BATCH'], params['WORKING_PROJECT'])
        if not success:
            print("glue_barcodes_together failed")
            return
    #
    # Process the feature table to make a data table that has an array of (patient, value) tuples for each feature
    #

    if 'build_feature_tuple_array_tables' in steps:
        print('build_feature_tuple_array_tables')
        for category in ["numeric", "string"]:
            schema_key = NUMERIC_TUPLE_SCHEMA if category == "numeric" else STRING_TUPLE_SCHEMA
            typed_schema = json_loads(schema_key)
            typed_dest_table = params['FEATURE_TUPLE_ARRAY_TABLE'].format(category)
            success = create_empty_target_with_schema(typed_schema, params['WORKING_PROJECT'],
                                                      params['TARGET_DATASET'], typed_dest_table,
                                                      True, CLUSTER_COLS)
            if not success:
                print("build_feature_tuple_array_tables create table failed")
                return

            success = value_string_list_to_tuple_array(params, category, params['TARGET_DATASET'], params['FEATURE_TUPLE_ARRAY_TABLE'],
                                                       params['BQ_AS_BATCH'], params['WORKING_PROJECT'])
            if not success:
                print("build_feature_tuple_array_tables failed category {}".format(category))
                return

    #
    # Add description and labels to the target table:
    #

    if 'update_table_descriptions' in steps:
        print('update_table_descriptions')

        metadata_dict = {}
        for tag in tagging:
            table_name = tag["table"]
            print("destination table:", tag["target"])

            metadata_dict["description"] = tag["description"].replace("\n", " ")
            metadata_dict["friendlyName"] = tag["friendly"]
            metadata_dict["labels"] = {}
            metadata_dict["labels"]["access"] = tag["access"]
            metadata_dict["labels"]["category"] = tag["data_category"]
            metadata_dict["labels"]["status"] = tag["status"]
            metadata_dict["labels"]["program"] = tag["program"]
            metadata_dict["labels"]["reference_genome_0"] = tag["reference_genome_0"]
            for item in tag["sources"]:
                for key, val in item.items():
                    metadata_dict["labels"][key] = val
            if len(tag["data_types"]) > 1:
                dcount = 0
                for item in tag["data_types"]:
                    metadata_dict["labels"]["data_type_{}".format(dcount)] = item
                    dcount += 1
            else:
                metadata_dict["labels"]["data_type"] = tag["data_types"][0]
            print(json_dumps(metadata_dict))
            print(metadata_dict)

            print("Processing {}".format(table_name))
            success = clear_table_labels(params['TARGET_DATASET'], table_name, project=params['WORKING_PROJECT'])
            if not success:
                print("update_table_descriptions failed to clear labels")
                return

            table_id = "{}.{}.{}".format(params['WORKING_PROJECT'], params['TARGET_DATASET'], table_name)
            install_table_metadata(table_id, metadata_dict, project=params['WORKING_PROJECT'] )

    #
    # Clear out working temp tables:
    #

    if 'dump_working_tables' in steps:
        dump_table_tags = ['SCRATCH_TABLE']
        dump_tables = [params[x] for x in dump_table_tags]
        for table in dump_tables:
            delete_table_bq_job(params['SCRATCH_DATASET'], table)

    print('job completed')

if __name__ == "__main__":
    main(sys.argv)
