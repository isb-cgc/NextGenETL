"""

Copyright 2024, Institute for Systems Biology

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

from common_etl.support import create_clean_target, build_file_list, generic_bq_harness, confirm_google_vm, \
                               upload_to_bucket, csv_to_bq, concat_all_files, delete_table_bq_job, \
                               build_pull_list_with_bq, update_schema, bq_harness_with_result, \
                               update_description, build_combined_schema, get_the_bq_manifest, BucketPuller, \
                               generate_table_detail_files, update_schema_with_dict, install_labels_and_desc, \
                               publish_table, compare_two_tables, update_status_tag

import sys
import csv
from google.cloud import bigquery, storage
from os.path import expanduser
import yaml
import io
import time
from git import Repo
from json import loads as json_loads

from common_etl.support import confirm_google_vm, create_clean_target, \
                               generic_bq_harness, \
                               delete_table_bq_job, install_labels_and_desc, update_schema_with_dict, \
                               generate_table_detail_files, publish_table

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

SQL_SCHEMA = '''[
    {{
        "description": "Dataset ID",
        "name": "dataset",
        "type": "STRING",
        "mode": "REQUIRED"
    }},
    {{
        "description": "Feature ID",
        "name": "id",
        "type": "INT64",
        "mode": "REQUIRED"
    }},
    {{
        "description": "Feature alias",
        "name": "alias",
        "type": "STRING",
        "mode": "REQUIRED"
    }},
    {{
        "description": "Feature type: one of (B)inary, (N)umeric, or (C)ategorical",
        "name": "type",
        "type": "STRING",
        "mode": "REQUIRED"
    }},
    {{
        "description": "Feature type; one of: RPPA, GEXP, CLIN, GNAB, MIRN, CNVR, METH, SAMP",
        "name": "source",
        "type": "STRING",
        "mode": "REQUIRED"
    }},
    {{
        "description": "Feature label",
        "name": "label",
        "type": "STRING",
        "mode": "NULLABLE"
    }},
    {{
        "description": "Feature chromosome",
        "name": "chr",
        "type": "STRING",
        "mode": "NULLABLE"
    }},
    {{
        "description": "Start location of feature",
        "name": "start",
        "type": "INT64",
        "mode": "NULLABLE"
    }},
    {{
        "description": "End location of feature",
        "name": "end",
        "type": "INT64",
        "mode": "NULLABLE"
    }},
    {{
        "description": "Feature chromosome strand",
        "name": "strand",
        "type": "STRING",
        "mode": "NULLABLE"
    }},
    {{
        "description": "Label description",
        "name": "label_desc",
        "type": "STRING",
        "mode": "NULLABLE"
    }},
    {{
        "description": "Colon-separated patient-ordered list of values, or NA",
        "name": "patient_values",
        "type": "STRING",
        "mode": "REQUIRED"
    }},
    {{
        "description": "Mean value",
        "name": "patient_values_mean",
        "type": "FLOAT64",
        "mode": "NULLABLE"
    }},
    {{
        "description": "Interesting score",
        "name": "interesting_score",
        "type": "{0}",
        "mode": "NULLABLE"
    }},
    {{
        "description": "Quantile value",
        "name": "quantile_val",
        "type": "{1}",
        "mode": "NULLABLE"
    }},
    {{
        "description": "Quantile",
        "name": "quantile",
        "type": "STRING",
        "mode": "NULLABLE"
    }}
]'''


# many files needed String for quantile_val due to /N

# gbm_23mayEdist_pw_features.txt needs rerun with String for interesting score due to /N


#DANGER! It appears that strand == 0 in all tables checked!

PATIENT_SCHEMA = '''[
    {
        "description": "Patient IDs ordered to match plotting points",
        "name": "barcodes",
        "type": "STRING",
        "mode": "REQUIRED"
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
        return None, None

    return yaml_dict['files_and_buckets_and_tables'], yaml_dict['steps']


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
                           target_dataset, dest_table, do_batch):
    sql = glue_features_sql(table_names, params, column_lists, common_schema_columns)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, False)

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

def repair_raw_features(raw_table, target_dataset, dest_table, do_batch):
    sql = repair_raw_sql(raw_table)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, False)

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

    return '''
      WITH a1 as (
      SELECT
        dataset,
        id,
        alias,
        type,
        source,
        label,
        CASE
           WHEN chr = "x" THEN "X"
           ELSE chr
        END as chr,
        CASE
           WHEN chr IS NULL THEN NULL
           ELSE start
        END as start,
        CASE
           WHEN chr IS NULL THEN NULL
           ELSE end
        END as end,
        CASE
           WHEN chr IS NOT NULL THEN SPLIT(alias, ':')[offset(6)]
           ELSE NULL
        END as strand_or_blank,
        label_desc,
        patient_values,
        CASE
           WHEN type IS 'C' THEN NULL
           ELSE patient_values_mean
        END as patient_values_mean,
        CASE
           WHEN (interesting_score = "\\N") OR (interesting_score IS NULL) THEN NULL
           ELSE CAST(interesting_score as FLOAT64)
        END as interesting_score,
        CASE
           WHEN (quantile_val = "\\N") OR (quantile_val IS NULL) THEN NULL
           ELSE CAST(quantile_val as FLOAT64)
        END as quantile_val,
        CASE
           WHEN (quantile = "\\N") OR (quantile IS NULL) THEN NULL
           ELSE quantile
        END as quantile
    FROM {})
    SELECT  dataset,
        id,
        alias,
        type,
        source,
        label,
        chr,
        start,
        end,
        CASE
           WHEN strand_or_blank IS NULL THEN NULL
           WHEN strand_or_blank = "" THEN NULL
           ELSE strand_or_blank
        END as strand,
        label_desc,
        patient_values,
        patient_values_mean,
        interesting_score,
        quantile_val,
        quantile FROM a1 order by dataset, id
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
        params, steps = load_config(yaml_file.read())

    patient_schema = json_loads(PATIENT_SCHEMA)
    typed_schema = json_loads(SQL_SCHEMA.format("STRING", "STRING"))
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
        typed_schema = json_loads(SQL_SCHEMA.format("STRING", "STRING"))
        success = create_empty_target_with_schema(typed_schema, params['WORKING_PROJECT'],
                                                  params['SCRATCH_DATASET'], params['RAW_FULL_FEATURE_TABLE'],
                                                  True, None)
        if not success:
            print("build_raw_table failed")
            return


    if 'glue_features_together' in steps:
        print('glue_features_together')

        column_dict = read_columns_dict(params, params['SUMMARY_FILE'])

        typed_schema = json_loads(SQL_SCHEMA.format("STRING", "STRING"))
        common_schema_columns = []
        for entry in typed_schema:
            common_schema_columns.append(entry["name"])

        success = glue_features_together(study_list, params, column_dict, common_schema_columns,
                                         params['SCRATCH_DATASET'], params['RAW_FULL_FEATURE_TABLE'],
                                         params['BQ_AS_BATCH'])

        if not success:
            print("glue_features_together failed")
            return

    if 'build_final_cluster_table' in steps:
        print('build_final_cluster_table')
        typed_schema = json_loads(SQL_SCHEMA.format("FLOAT64", "FLOAT64"))
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
        success = repair_raw_features(raw_table, params['TARGET_DATASET'], params['FINAL_FULL_FEATURE_TABLE'], params['BQ_AS_BATCH'])

        if not success:
            print("massage_raw_table failed")
            return

    if 'update_table_description' in steps:
        print('update_table_description')
        #full_file_prefix = "{}/{}".format(params['PROX_DESC_PREFIX'], params['FINAL_TARGET_TABLE'])
        #success = install_labels_and_desc(params['TARGET_DATASET'], params['FINAL_TARGET_TABLE'], full_file_prefix)
        #if not success:
        #    print("update_table_description failed")
        #    return

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
