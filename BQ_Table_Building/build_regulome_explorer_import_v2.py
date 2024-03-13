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

import sys
from google.cloud import bigquery, storage
from os.path import expanduser
import yaml
import io
import time
from git import Repo
from json import loads as json_loads

from common_etl.support import generic_bq_harness, delete_table_bq_job, install_labels_and_desc

'''
Make sure the VM has BigQuery and Storage Read/Write permissions!
'''

RE_SCHEMA = '''[
    {
        "description": "Feature 1 chromosome",
        "name": "f1chr",
        "type": "STRING",
        "mode": "REQUIRED"
    },
    {
        "description": "Feature 1 chromosome strand; not just + or -",
        "name": "f1strand",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "description": "Feature 2 end coordinate",
        "name": "f2end",
        "type": "INT64",
        "mode": "NULLABLE"
    },
    {
        "description": "Bonferroni factor",
        "name": "bonf_fac",
        "type": "FLOAT64",
        "mode": "REQUIRED"
    },
    {
        "description": "Start location of feature 2",
        "name": "f2start",
        "type": "INT64",
        "mode": "NULLABLE"
    },
    {
        "description": "Link distance",
        "name": "link_distance",
        "type": "INT64",
        "mode": "NULLABLE"
    },
    {
        "description": "Feature 1 ID",
        "name": "feature1id",
        "type": "INT64",
        "mode": "REQUIRED"
    },
    {
        "description": "Feature 1 gene score",
        "name": "f1genescore",
        "type": "FLOAT64",
        "mode": "NULLABLE"
    },
    {
        "description": "Feature 1 type; one of: RPPA, GEXP, CLIN, GNAB, MIRN, CNVR, METH, SAMP",
        "name": "f1source",
        "type": "STRING",
        "mode": "REQUIRED"
    },
    {
        "description": "Feature 2 log P value",
        "name": "logged_pvalue_f1",
        "type": "FLOAT64",
        "mode": "REQUIRED"
    },
    {
        "description": "Feature 2 log P value",
        "name": "logged_pvalue_f2",
        "type": "FLOAT64",
        "mode": "REQUIRED"
    },
    {
        "description": "Feature 1 label description",
        "name": "f1label_desc",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "description": "Timestamp",
        "name": "timestamp",
        "type": "TIMESTAMP",
        "mode": "REQUIRED"
    },
    {
        "description": "ID",
        "name": "id",
        "type": "INT64",
        "mode": "REQUIRED"
    },
    {
        "description": "Feature 1 label",
        "name": "f1label",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "description": "Dataset Identifier",
        "name": "datasetId",
        "type": "STRING",
        "mode": "REQUIRED"
    },
    {
        "description": "Number of non-NA values",
        "name": "num_nonna",
        "type": "INT64",
        "mode": "REQUIRED"
    },
    {
        "description": "Feature 1 number of non-NA values",
        "name": "num_nonna_f1",
        "type": "INT64",
        "mode": "REQUIRED"
    },
    {
        "description": "Feature 2 QT information",
        "name": "f2qtinfo",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "description": "Feature 2 number of non-NA values",
        "name": "num_nonna_f2",
        "type": "INT64",
        "mode": "REQUIRED"
    },
    {
        "description": "Feature 2 chromosome",
        "name": "f2chr",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "description": "Feature 1 end coordinate",
        "name": "f1end",
        "type": "INT64",
        "mode": "NULLABLE"
    },
    {
        "description": "Feature 1 type; one of (B)inary, (N)umeric, or (C)ategorical",
        "name": "f1type",
        "type": "STRING",
        "mode": "REQUIRED"
    },
    {
        "description": "Log P value",
        "name": "logged_pvalue",
        "type": "FLOAT64",
        "mode": "REQUIRED"
    },
    {
        "description": "Correlation",
        "name": "correlation",
        "type": "FLOAT64",
        "mode": "NULLABLE"
    },
    {
        "description": "Log P value Bonferroni correction",
        "name": "logged_pvalue_bonf",
        "type": "FLOAT64",
        "mode": "REQUIRED"
    },
    {
        "description": "Feature 2 label",
        "name": "f2label",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "description": "Rho score",
        "name": "rho_score",
        "type": "FLOAT64",
        "mode": "NULLABLE"
    },
    {
        "description": "Feature 2 label description",
        "name": "f2label_desc",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "description": "Start location of feature 1",
        "name": "f1start",
        "type": "INT64",
        "mode": "NULLABLE"
    },
    {
        "description": "Patient count",
        "name": "patientct",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "description": "Feature 1 QT info",
        "name": "f1qtinfo",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "description": "Feature 2 source; one of: RPPA, GEXP, CLIN, GNAB, MIRN, CNVR, METH, SAMP",
        "name": "f2source",
        "type": "STRING",
        "mode": "REQUIRED"
    },
    {
        "description": "Unique feature 2 string",
        "name": "alias2",
        "type": "STRING",
        "mode": "REQUIRED"
    },
    {
        "description": "Feature 2 gene score",
        "name": "f2genescore",
        "type": "FLOAT64",
        "mode": "NULLABLE"
    },
    {
        "description": "Unique feature 1 string",
        "name": "alias1",
        "type": "STRING",
        "mode": "REQUIRED"
    },
    {
        "description": "Importance",
        "name": "importance",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "description": "Feature 2 type; one of (B)inary, (N)umeric, or (C)ategorical",
        "name": "f2type",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "description": "Chromosome strand for feature 2",
        "name": "f2strand",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "description": "Data set name",
        "name": "dataset",
        "type": "STRING",
        "mode": "REQUIRED"
    },
    {
        "description": "Feature 2 ID",
        "name": "feature2id",
        "type": "INT64",
        "mode": "REQUIRED"
    }
]'''

COLUMN_ORDER = [

    'id',
    'f1source',
    'f1type',
    'f1label',
    'f1chr',
    'f1strand',
    'f1start',
    'f1end',
    'alias1',

    'f2source',
    'f2type',
    'f2label',
    'f2chr',
    'f2strand',
    'f2start',
    'f2end',
    'alias2',

    'logged_pvalue',
    'correlation',
    'logged_pvalue_bonf',
    'bonf_fac',

    'feature1id',
    'f1genescore',
    'f1qtinfo',
    'f1label_desc',
    'logged_pvalue_f1',

    'feature2id',
    'f2genescore',
    'f2qtinfo',
    'f2label_desc',
    'logged_pvalue_f2',

    'timestamp',

    'datasetId',
    'dataset',

    'num_nonna',
    'num_nonna_f1',
    'num_nonna_f2',

    'link_distance',
    'rho_score',
    'patientct',
    'importance'
]

OFFERING_SCHEMA = '''
[
    {
        "description": "Disease Code",
        "name": "disease_code",
        "type": "STRING",
        "mode": "REQUIRED"
    },
    {
        "description": "Description",
        "name": "description",
        "type": "STRING",
        "mode": "REQUIRED"
    },
    {
        "description": "Dataset Key",
        "name": "key",
        "type": "STRING",
        "mode": "REQUIRED"
    }
]
'''

#
# This table lists the datasets we are going to retain from solr, and also maps the
# legacy dataset key to a new key for public display:
#

DATASET_MAPPING_SCHEMA = '''
[
    {
        "description": "Private Dataset Key",
        "name": "privkey",
        "type": "STRING",
        "mode": "REQUIRED"
    },
    {
        "description": "Dataset Key",
        "name": "key",
        "type": "STRING",
        "mode": "REQUIRED"
    }
]
'''

#
# This is the final offering as of 12/7/23
#
# disease code, full description friendly name, internal dataset name, new public BQ value
#

OFFERING = [
    ("ACC", "TCGA Adrenocortical Carcinoma (ACC) Manuscript", "TCGA_ACC", "acc_11aug15"),
    ("BLCA", "TCGA Bladder Cancer (BLCA) Manuscript", "blca_20may13_test_TP", "blca_20may13_manuscript_tumor_only"),
    ("BRCA", "TCGA Breast Invasive Carcinoma (BRCA) Manuscript", "brca_pw_manuscript", "brca_manuscript"),
    ("BRCA", "TCGA TCGA Breast Invasive Carcinoma (BRCA) Feb 2013", "brca_03feb13_seq_tumor_only", "brca_03feb13"),
    ("COADREAD", "TCGA Colorectal Adenocarcinoma (COAD, READ) Manuscript 2012", "coadread_12apr12_tumor_only",
     "coadread_12apr12"),
    ("ESCA_STAD", "TCGA Gastroesophageal Cancer (ESCA) Manuscript", "esca_stad_20160208_edited", "esca_stad_08feb16"),
    ("GBM", "TCGA Glioblastoma Multiforme (GBM) February 2014 Mesenchymal", "gbm_06feb_mesen_pw",
     "gbm_06feb14_mesenchymal"),
    ("GBM", "TCGA Glioblastoma Multiforme (GBM) February 2014 Neural", "gbm_06feb_neura_pw", "gbm_06feb14_neural"),
    (
    "GBM", "TCGA Glioblastoma Multiforme (GBM) February 2014 Classical", "gbm_06feb_class_pw", "gbm_06feb14_classical"),
    (
    "GBM", "TCGA Glioblastoma Multiforme (GBM) February 2014 Proneural", "gbm_06feb_prone_pw", "gbm_06feb14_proneural"),
    ("GBM", "TCGA Glioblastoma Multiforme (GBM) February 2014 All Samples", "gbm_06feb_pw", "gbm_06feb14_all"),
    ("GBM", "TCGA Glioblastoma Multiforme (GBM) Manuscript 2013", "gbm_2013_pub_tumor_only", "gbm_2013_pub"),
    ("HNSC", "TCGA Glioblastoma Multiforme (GBM) Manuscript 2013", "hnsc_03feb13_seq_tumor_only", "hnsc_03feb13"),
    ("KIRC", "TCGA Kidney Renal Clear Cell Carcinoma (KIRC) October 2012", "kirc_01oct12_A_pw", "kirc_01oct12"),
    ("LGG", "TCGA Low Grade Glioma (LGG) October 2013", "lgg_04oct13_seq", "lgg_04oct13"),
    ("LIHC", "TCGA Liver Hepatocellular Carcinoma (LIHC) Manuscript", "lihc_11oct16_TP", "lihc_11oct16"),
    ("LUAD", "TCGA Lung Adenocarcinoma (LUAD) February 2013", "luad_03feb13_seq_tumor_only", "luad_03feb13"),
    ("LUSC", "TCGA Lung Squamous Cell Carcinoma (LUSC) February 2013", "lusc_03feb13_seq_tumor_only", "lusc_03feb13"),
    ("OV", "TCGA Ovarian Serous Cystadenocarcinoma (OV) February 2013", "ov_03feb13_ary_tumor_only", "ov_03feb13"),
    ("PRAD", "TCGA Prostate Adenocarcinoma (PRAD) Manuscript", "prad_17aug15_TP", "prad_17aug15"),
    ("SKCM", "TCGA Skin Cutaneous Melanoma (SKCM) April 2014", "skcm_01apr14_331_all", "skcm_01apr14_all"),
    ("SKCM", "TCGA Skin Cutaneous Melanoma (SKCM) April 2014, Metastatic Tumors Only", "skcm_01apr14_266_TM",
     "skcm_01apr14_metastatic_tumor_only"),
    ("STAD", "TCGA Stomach Adenocarcinoma (STAD) Manuscript", "stad_23jan14_seq_tumor_only", "stad_23jan14"),
    ("THCA", "TCGA Thyroid Carcinoma (THCA) Manuscript", "thca_18oct14_TP", "thca_18oct14"),
    ("UCEC", "TCGA Uterine Corpus Endometrial Carcinoma (UCEC) June 2013", "ucec_28jun13b_seq_tumor_only",
     "ucec_28jun13")
]

REDUCED_COLUMN_ORDER = [
    'feature1id',
    'alias1',
    'feature2id',
    'alias2',
    'link_distance',
    'logged_pvalue_bonf',
    'logged_pvalue',
    'correlation',
    'num_nonna',
    'dataset'
]

CLUSTER_COLS = ["dataset"]

REDUCED_MAP = {
    'alias1': 'feature_1',
    'alias2': 'feature_2',
    'feature1id': 'feature_1_id',
    'feature2id': 'feature_2_id',
    'num_nonna': 'num_samples',
    'link_distance': 'distance'
}

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

    return yaml_dict['files_and_buckets_and_tables'], yaml_dict['steps'], yaml_dict['core_list']


'''
----------------------------------------------------------------------------------------------
Table name for dataset
'''


def build_table_name(dataset):
    return '__'.join(dataset[0:3]).replace('(', '').replace(')', '').replace('-', '_').replace(', ', ' ').replace(',',
                                                                                                                  ' ').replace(
        '+', 'and').replace(' ', '_')


'''
----------------------------------------------------------------------------------------------
Combine cores.
'''


def glue_cores_together(core_tables, target_dataset, dest_table, do_batch):
    sql = glue_cores_sql(core_tables)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)


'''
----------------------------------------------------------------------------------------------
Combine cores SQL
'''


def glue_cores_sql(core_tables):
    core_sql = []
    for core in core_tables:
        core_sql.append("SELECT * FROM `{0}`".format(core))

    full_sql = " UNION DISTINCT ".join(core_sql)
    print(full_sql)

    return full_sql


'''
----------------------------------------------------------------------------------------------
Reduced Core Generation
'''


def build_reduced_core(core_table, project_table, reduced_columns, reduced_map,
                       target_dataset, dest_table, do_batch):
    sql = reduced_core_sql(core_table, project_table, reduced_columns, reduced_map)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)


'''
----------------------------------------------------------------------------------------------
Reduced Core SQL. The join with the project table only retains the projects we are offering and
tosses the rest. Use this opportunity to replace the old dataset key with the new one.
'''


def reduced_core_sql(core_table, project_table, reduced_columns, reduced_map):
    rows = []
    for col in reduced_columns:  # we have dropped the last (dataset) column in the provided argument
        rows.append("a.{} as {}".format(col, reduced_map[col]) if col in reduced_map else "a.{}".format(col))
    rows.append("b.key as dataset")
    selects = ', '.join(rows)

    return '''
      SELECT
      {0}
      FROM `{1}` AS a JOIN `{2}` as b ON a.dataset = b.privkey
    '''.format(selects, core_table, project_table)


'''
----------------------------------------------------------------------------------------------
Fix distance values
'''


def fix_distance_values(orig_bq_table, reorg_typed_schema, target_dataset, dest_table, do_batch):
    sql = repair_distance_sql(orig_bq_table, reorg_typed_schema)
    return generic_bq_harness(sql, target_dataset, dest_table, do_batch, True)


'''
----------------------------------------------------------------------------------------------
# SQL Code For fixing distance values
'''


def repair_distance_sql(orig_bq_table, reorg_typed_schema):
    rows = []
    for dict in reorg_typed_schema:
        col = dict['name']
        if col == 'distance':
            col = "CASE WHEN distance = 500000000 THEN NULL ELSE distance END as distance"
        rows.append(col)
    selects = ', '.join(rows)

    return '''
      SELECT
      {0}
      FROM `{1}`
    '''.format(selects, orig_bq_table)


'''
----------------------------------------------------------------------------------------------
# get to bigquery
'''


def csv_to_bq_write_depo(schema, csv_uri, project_id, dataset_id, targ_table, do_batch, write_depo, hasHeader):
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
    job_config.skip_leading_rows = 1 if hasHeader else 0
    job_config.source_format = bigquery.SourceFormat.CSV
    if write_depo is not None:
        job_config.write_disposition = write_depo
    # Can make the "CSV" file a TSV file using this:
    # job_config.field_delimiter = '\t'

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
# Reorg schema
'''


def reorg_schema(schema, column_order, column_map):
    retval = []
    sch_dict = {}
    for my_dict in schema:
        sch_dict[my_dict["name"]] = my_dict

    for col in column_order:
        dict_for_col = sch_dict[col]
        if dict_for_col['name'] in column_map:
            dict_for_col['name'] = column_map[dict_for_col['name']]
        retval.append(dict_for_col)

    return retval


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
Main Control Flow
Note that the actual steps run are configured in the YAML input! This allows you
to e.g. skip previously run steps.

V2 ROADMAP
* The solr core dumps live in a bucket
1) First, get those into BQ tables, correctly typed
2) We want to merge the cores together
  a) Only the datasets that we care about
  b) Rename the datasets to some public naming scheme
  c) Drop columns not related to associations. Info like chromosome will be in the feature table
  d) Create a single table clustered on dataset name


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
        params, steps, core_list = load_config(yaml_file.read())

    if steps is None:
        steps = []
    #
    # Build a table describing the projects going public:
    #

    if 'build_dataset_mapping' in steps:

        csv_string = ''
        for csv_tuple in OFFERING:
            csv_string += '"' + csv_tuple[2] + '","' + csv_tuple[3] + '"\n'
        print(csv_string)

        storage_client = storage.Client(params['WORKING_PROJECT'])
        mappings_csv_url = 'gs://{}/{}'.format(params['WORKING_BUCKET'], params['MAPPINGS_CSV'])
        bucket = storage_client.get_bucket(params['WORKING_BUCKET'])
        blob = bucket.blob(params['MAPPINGS_CSV'])
        blob.upload_from_string(csv_string)

        typed_schema = json_loads(DATASET_MAPPING_SCHEMA)
        csv_to_bq_write_depo(typed_schema, mappings_csv_url, params['WORKING_PROJECT'],
                             params['SCRATCH_DATASET'], params['MAPPINGS_TABLE'],
                             params['BQ_AS_BATCH'], "WRITE_TRUNCATE", False)

    if 'projects_to_bq' in steps:
        csv_string = ''
        for csv_tuple in OFFERING:
            csv_string += '"' + csv_tuple[0] + '","' + csv_tuple[1] + '","' + csv_tuple[3] + '"\n'
        print(csv_string)

        storage_client = storage.Client(params['WORKING_PROJECT'])
        projects_csv_url = 'gs://{}/{}'.format(params['WORKING_BUCKET'], params['PROJECTS_CSV'])
        bucket = storage_client.get_bucket(params['WORKING_BUCKET'])
        blob = bucket.blob(params['PROJECTS_CSV'])
        blob.upload_from_string(csv_string)

        typed_schema = json_loads(OFFERING_SCHEMA)
        csv_to_bq_write_depo(typed_schema, projects_csv_url, params['WORKING_PROJECT'],
                             params['TARGET_DATASET'], params['PROJECTS_TABLE'],
                             params['BQ_AS_BATCH'], "WRITE_TRUNCATE", False)

    for core in core_list:
        if 'create_bq_from_tsv' in steps:
            bucket_src_url = 'gs://{}/{}'.format(params['WORKING_BUCKET'], params['BUCKET_CSV'].format(core))
            typed_schema = json_loads(RE_SCHEMA)
            csv_to_bq_write_depo(typed_schema, bucket_src_url, params['WORKING_PROJECT'],
                                 params['SCRATCH_DATASET'], params['SCRATCH_TABLE'].format(core),
                                 params['BQ_AS_BATCH'], "WRITE_TRUNCATE", True)

        #
        # We distributed 32 datasets in RE, but the Solr cores hold about 80 datasets. Note also that each dataset is pretty
        # much scattered across all cores. So, prune the cores down to just those datasets we will be distributing. We also
        # are tossing some datasets that were in RE, leaving 25. As we pull in the datasets we want, we also want to
        # convert the dataset ID to a new "public" ID. Finally, we are tossing columns that we do not care about: info
        # about the features was in solr, but that will instead be available in feature set tables.
        #

        if 'prune_to_public' in steps:
            print('prune_to_public')
            core_bq_table = "{}.{}.{}".format(params['WORKING_PROJECT'], params['SCRATCH_DATASET'],
                                              params['SCRATCH_TABLE'].format(core))
            mappings_bq_table = "{}.{}.{}".format(params['WORKING_PROJECT'], params['SCRATCH_DATASET'],
                                                  params['MAPPINGS_TABLE'])
            success = build_reduced_core(core_bq_table, mappings_bq_table, REDUCED_COLUMN_ORDER[0:-1], REDUCED_MAP,
                                         params['SCRATCH_DATASET'], params['REDUCED_CORE_TABLE'].format(core),
                                         params['BQ_AS_BATCH'])
            if not success:
                print("prune_to_public failed")

    #
    # We are going to build a single table, clustered on dataset. Do this now:
    #

    if 'build_cluster_table' in steps:
        print('build_cluster_table')
        typed_schema = json_loads(RE_SCHEMA)
        reorg_typed_schema = reorg_schema(typed_schema, REDUCED_COLUMN_ORDER, REDUCED_MAP)
        success = create_empty_target_with_schema(reorg_typed_schema, params['WORKING_PROJECT'],
                                                  params['SCRATCH_DATASET'], params['RAW_ASSOCIATIONS_TABLE'],
                                                  True, CLUSTER_COLS)
        if not success:
            print("build_cluster_table failed")

    #
    # Glue cores together:
    #

    if 'glue_cores_together' in steps:
        print('glue_cores_together')

        # We need to get the table clustered on dataset first!
        core_bq_tables = []
        for core in core_list:
            core_bq_tables.append("{}.{}.{}".format(params['WORKING_PROJECT'], params['SCRATCH_DATASET'],
                                                    params['REDUCED_CORE_TABLE'].format(core)))
        success = glue_cores_together(core_bq_tables,
                                      params['SCRATCH_DATASET'], params['RAW_ASSOCIATIONS_TABLE'],
                                      params['BQ_AS_BATCH'])
        if not success:
            print("glue_cores_together failed")

    #
    # There were various weird values in the Solr tables:
    #  1) link_distance values of 500000000 need to be converted to null
    #  2) patientct is always null
    #  3) importance is always null
    #  4) f1 and f2 non-na values seem to be nonsense
    # but the only thing left in the reduced public table is that distance = 500000000 needs
    # to be converted to null
    # Note: SELECT alias, (LENGTH(patient_values) - LENGTH(REPLACE(patient_values, 'NA', ''))) / LENGTH('NA') as num_na
    #       FROM `isb-project-zero.regulome_explorer.skcm_01apr14_266_TM` LIMIT 1000

    if 'create_final' in steps:
        print('create_final')

        typed_schema = json_loads(RE_SCHEMA)
        reorg_typed_schema = reorg_schema(typed_schema, REDUCED_COLUMN_ORDER, REDUCED_MAP)
        success = create_empty_target_with_schema(reorg_typed_schema, params['WORKING_PROJECT'],
                                                  params['TARGET_DATASET'], params['FINAL_ASSOCIATIONS_TABLE'],
                                                  True, CLUSTER_COLS)
        if not success:
            print("create_final")

    if 'repair_raw' in steps:
        print('repair_raw')
        raw_bq_table = "{}.{}.{}".format(params['WORKING_PROJECT'], params['SCRATCH_DATASET'],
                                         params['RAW_ASSOCIATIONS_TABLE'])
        typed_schema = json_loads(RE_SCHEMA)
        reorg_typed_schema = reorg_schema(typed_schema, REDUCED_COLUMN_ORDER, REDUCED_MAP)
        success = fix_distance_values(raw_bq_table, reorg_typed_schema,
                                      params['TARGET_DATASET'], params['FINAL_ASSOCIATIONS_TABLE'],
                                      params['BQ_AS_BATCH'])
        if not success:
            print("repair_raw failed")

    #
    # Add description and labels to the target table:
    #

    if 'update_table_description' in steps:
        print('update_table_description')
        # full_file_prefix = "{}/{}".format(params['PROX_DESC_PREFIX'], params['FINAL_TARGET_TABLE'])
        # success = install_labels_and_desc(params['TARGET_DATASET'], params['FINAL_TARGET_TABLE'], full_file_prefix)
        # if not success:
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

