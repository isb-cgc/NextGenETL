"""

Copyright 2019-2020, Institute for Systems Biology

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
import logging
import os
import time
import sys
from os.path import expanduser
from common_etl.support import confirm_google_vm
import yaml
import io
from json import loads as json_loads
from types import SimpleNamespace
import shutil
import zipfile
import gzip

from gdc_file_utils import (format_seconds, update_dir_from_git, query_bq, bq_to_bucket_tsv, bucket_to_local, find_types,
                            create_schema_hold_list, local_to_bucket, update_schema_tags, write_table_schema_with_generic,
                            csv_to_bq, initialize_logging, bq_table_exists)

from open_somatic_mut import create_somatic_mut_table

from RNA_seq import create_rna_seq_table

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


def create_file_list(params, program, datatype, local_location, prefix, file_list, datatype_mappings):

    max_files = params.MAX_FILES if 'MAX_FILES' in params else None
    bucket_location = f"{params.BUCKET}/gdc_{params.RELEASE}"

    file_list_sql = create_file_list_sql(program, datatype_mappings[datatype]['filters'],
                                         params.FILE_TABLE, params.GSC_URL_TABLE, max_files)

    if query_bq(file_list_sql, f"{params.DEV_PROJECT}.{params.DEV_DATASET}.{prefix}_file_list") != 'DONE':
        sys.exit( "Create file list bq table failed" )

    if not bq_to_bucket_tsv(f"{prefix}_file_list", params.DEV_PROJECT, params.DEV_DATASET,
                            bucket_location, params.DO_BATCH, False): # todo double batch?
        sys.exit("bq to bucket failed")

    if not bucket_to_local(bucket_location, file_list, f"{local_location}/{file_list}"):
        sys.exit("bucket to local failed")


def create_file_list_sql(program, filters, file_table, gcs_url_table, max_files):

    formatted_filters = []

    for key, val in filters:
        formatted_filters.append(f"a.{key} = '{val}'")

    joined_filters = " AND ".join(formatted_filters)
    all_filters = f"WHERE {joined_filters}"

    file_limit = "" if max_files is None else f"LIMIT {max_files}"

    return f"""
        SELECT b.file_gdc_url
        FROM  `{file_table}` as a
        JOIN `{gcs_url_table}` as b
        ON a.file_gdc_id = b.file_gdc_id
        WHERE {all_filters} AND a.`access` = "open" AND a.program_name = {program}
        {file_limit}
        """


def concat_all_files(all_files, one_big_tsv):
    # todo description to accurately reflect the function
    """
    Concatenate all Files
    Gather up all files and glue them into one big one. The file name and path often include features
    that we want to add into the table. The provided file_info_func returns a list of elements from
    the file path, and the extra_cols list maps these to extra column names. Note if file is zipped,
    we unzip it, concat it, then toss the unzipped version.
    THIS VERSION OF THE FUNCTION USES THE FIRST LINE OF THE FIRST FILE TO BUILD THE HEADER LINE!
    """
    logger = logging.getLogger('base_script')
    logger.info("building {}".format(one_big_tsv))
    first = True
    header_id = None
    with open(one_big_tsv, 'w') as outfile:
        for filename in all_files:
            toss_zip = False
            if filename.endswith('.zip'):
                dir_name = os.path.dirname(filename)
                logger.info(f"Unzipping {filename}")
                with zipfile.ZipFile(filename, "r") as zip_ref:
                    zip_ref.extractall(dir_name)
                use_file_name = filename[:-4]
                toss_zip = True
            elif filename.endswith('.gz'):
                use_file_name = filename[:-3]
                logger.info(f"Uncompressing {filename}")
                with gzip.open(filename, "rb") as gzip_in:
                    with open(use_file_name, "wb") as uncomp_out:
                        shutil.copyfileobj(gzip_in, uncomp_out)
                toss_zip = True
            else:
                use_file_name = filename

            if os.path.isfile(use_file_name):
                with open(use_file_name, 'r') as readfile:
                    for line in readfile:
                        if not line.startswith('#') or line.startswith(header_id) or first:
                            outfile.write(line.rstrip('\n'))
                            outfile.write('\t')
                            outfile.write('file_name' if first else filename)
                            outfile.write('\n')
                        first = False
            else:
                logger.info(f'{use_file_name} was not found')

            if toss_zip and os.path.isfile(use_file_name):
                os.remove(use_file_name)
    return


def transform_bq_data(datatype, raw_data_table, draft_data_table, aliquot_table, case_table, file_table, gene_table):
    logger = logging.getLogger('base_script')
    intermediate_tables = []

    if datatype == "gene_level_copy_number":  # todo
        print("Creating Gene Level Copy Number draft tables")

    if datatype == "copy_number":  # todo
        print("copy number")

    if datatype == "somatic_mut":
        logger.info("Creating Somatic Mut draft tables")
        som_mut_tables = create_somatic_mut_table(raw_data_table, draft_data_table, aliquot_table, case_table)
        intermediate_tables.extend(som_mut_tables)

    if datatype == "RNA_seq":
        logger.info("Creating RNA seq draft tables")

        rna_seq_table = create_rna_seq_table(raw_data_table, draft_data_table, file_table, aliquot_table, case_table)
        intermediate_tables.extend(rna_seq_table)

    if datatype == "mRNA_seq":  # todo
        print("mRNA_seq")

    return intermediate_tables


def build_bq_tables_steps(params, workflow_run_ver, steps, program_datatype):
    logger = logging.getLogger('base_script')
    program, datatype = program_datatype.split(", ")

    # file variables
    prefix = f"{program}_{datatype}_{params.RELEASE}{workflow_run_ver}"
    home = expanduser("~")
    local_location = f"{home}/{params.LOCAL_DIR}/{program}"
    tables_created_file = f"{home}/{params.LOCAL_DIR}/tables_created_{params.RELEASE}{workflow_run_ver}.txt"

    with open(params.DATATYPE_MAPPINGS, mode='r') as datatype_mappings_file:
        datatype_mappings = json_loads(datatype_mappings_file.read().rstrip())

    file_list = f"{prefix}_file_list.tsv"
    raw_data = f"{prefix}_raw"
    draft_table = f"{prefix}_draft_table" # todo find out where needs the table name and where it should be table id
    field_list = f"{local_location}/{prefix}_field_schema.json"

    if 'create_file_list' in steps:
        logger.info("Running create_file_list Step")
        create_file_list(params, program, datatype, local_location, prefix, file_list, datatype_mappings)

    if 'create_files' in steps:
        logging.info("Running create_files Step")
        concat_all_files(f"{local_location}/{file_list}", raw_data)
        # todo future add header rows if needed (Methylation)
        # todo future break up copy number files into each workflow (maybe)

    if 'analyze_the_schema' in steps:
        logging.info("Running analyze_the_schema Step")
        typing_tups = find_types(raw_data, params.SCHEMA_SAMPLE_SKIPS)

        create_schema_hold_list(typing_tups,
                                f"{home}/schemaRepo/TableFieldUpdates/gdc_{datatype}_desc.json",
                                field_list, True)

    if 'upload_to_bucket' in steps:
        logging.info("Running upload_to_bucket Step")
        local_to_bucket(params.WORKING_BUCKET, raw_data, f"{local_location}/{raw_data}")

    if 'create_bq_from_tsv' in steps:
        logging.info("Running create_bq_from_tsv Step")
        bucket_src_url = f'gs://{params.WORKING_BUCKET}/{raw_data}'
        with open(field_list, mode='r') as schema_list:
            typed_schema = json_loads(schema_list.read())
        csv_to_bq(typed_schema, bucket_src_url, params.DEV_DATASET, raw_data, params.BQ_AS_BATCH, True)

    if 'transform_bq_data' in steps:  # todo currently working on
        logging.info("Running transform_bq_data Step")
        created_tables = transform_bq_data(datatype, raw_data, draft_table, params.ALIQUOT_TABLE, params.CASE_TABLE,
                                           params.FILE_TABLE, params.GENE_NAMES_TABLE)
        with open(tables_created_file, 'w') as outfile:
            for table in created_tables:
                outfile.write(table)

    if 'update_table_schema' in steps:
        # todo
        logging.info("Running update_table_schema Step")

        if bq_table_exists(draft_table):
            updated_schema_tags = update_schema_tags(datatype_mappings, params.RELEASE, params.REL_DATE, program) # todo is this correct?

            write_table_schema_with_generic(
                f"{params.DEV_PROJECT}.{params.DEV_DATASET}.{draft_table}",
                updated_schema_tags,
                f"{home}/schemaRepo/GenericSchemas/{program}_{datatype}.json",
                f"{home}/schemaRepo/TableFieldUpdates/gdc_{program}_{datatype}_desc.json")

    if 'qc_tables' in steps:
        # todo
        # todo separate file for QC?
        logging.info("Running qc_tables Step")

    if 'publish_tables' in steps:
        # todo
        # todo Create a list of tables published in the formatting for readthedocs
        logging.info("Running publish_tables Step")

    if 'clean_up' in steps:
        # todo
        # should this be in the build_bq_tables_steps function?
        # This step will clean up the bigquery intermediate tables and create an archive of the files on the VM after
        logging.info("Running clean_up Step")


def main(args):
    """
    Main Control Flow
    Note that the actual steps run are configured in the YAML input! This allows you
    to e.g. skip previously run steps.
    """

    if not confirm_google_vm():
        print('This job needs to run on a Google Cloud Compute Engine to avoid storage egress charges [EXITING]')
        return

    start_time = time.time()
    print(f"GDC Derived script started at {time.strftime('%x %X', time.localtime())}")

    # Get the YAML config loaded:
    with open(args[1], mode='r') as yaml_file:
        params_dict, steps = load_config(yaml_file.read())
        params = SimpleNamespace(**params_dict)

    end = time.time() - start_time
    print(f"Finished program execution in {format_seconds(end)}!\n")

    # Set the workflow run count from yaml
    workflow_run_ver = f"_{params.WORKFLOW_RUN_VER}" if 'WORKFLOW_RUN_VER' in params_dict else ''

    log_file_time = time.strftime('%Y.%m.%d-%H.%M.%S', time.localtime())
    log_filepath = f"{params.LOGFILE_DIR}/gdc_data_files_{params.RELEASE}{workflow_run_ver}.log"
    logger = initialize_logging(log_filepath)

    logger.info(f"GDC derived data script started at {time.strftime('%x %X', time.localtime())}")

    # Start of Workflow

    if 'update_schema_dir_from_git' in steps:
        logger.info("Running update_schema_dir_from_git Step")
        update_dir_from_git(params.SCHEMA_REPO_LOCAL, params.SCHEMA_REPO_URL, params.SCHEMA_REPO_BRANCH)

    # Derived Data Steps
    for program_datatype in params.PROGRAMS_AND_DATASETS:

        build_bq_tables_steps(params, workflow_run_ver, steps, program_datatype)


if __name__ == "__main__":
    main(sys.argv)
