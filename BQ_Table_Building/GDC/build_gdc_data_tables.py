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
import yaml
import io
from json import loads as json_loads
from types import SimpleNamespace
import shutil
import zipfile
import gzip
from google.cloud import bigquery

from gdc_file_utils import (confirm_google_vm, format_seconds, update_dir_from_git, query_bq, bq_to_bucket_tsv,
                            bucket_to_local, find_types, pull_from_buckets, build_file_list,
                            create_schema_hold_list, local_to_bucket, update_schema_tags,
                            write_table_schema_with_generic, clean_local_file_dir,
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
        print("Could not parse YAML")
        return None, None, None

    return yaml_dict['data_to_gather'], yaml_dict['steps'], yaml_dict['parameters']


def create_file_list(params, program, datatype, local_location, prefix, file_list, datatype_mappings):
    max_files = params.MAX_FILES if 'MAX_FILES' in vars(params) else None
    bucket_location = f"{params.DEV_BUCKET_DIR}/gdc_{params.RELEASE}"

    file_list_sql = create_file_list_sql(program, datatype_mappings[datatype]['filters'],
                                         f"{params.FILE_TABLE}_{params.RELEASE}",
                                         f"{params.GSC_URL_TABLE}_{params.RELEASE}", max_files)

    if query_bq(file_list_sql, f"{params.DEV_PROJECT}.{params.DEV_DATASET}.{prefix}_file_list") != 'DONE':
        sys.exit("Create file list bq table failed")

    bq_to_bucket_tsv(f"{prefix}_file_list", params.DEV_PROJECT, params.DEV_DATASET,
                     params.DEV_BUCKET, f"{bucket_location}/{file_list}", params.BQ_AS_BATCH, False)

    bucket_to_local(params.DEV_BUCKET, f"{bucket_location}/{file_list}",
                    f"{local_location}/{file_list}")


def create_file_list_sql(program, filters, file_table, gcs_url_table, max_files):
    formatted_filters = []

    for key, val in filters.items():
        formatted_filters.append(f"a.{key} = '{val}'")

    joined_filters = " AND ".join(formatted_filters)

    file_limit = "" if max_files is None else f"LIMIT {max_files}"

    return f"""
        SELECT b.file_gdc_url
        FROM  `{file_table}` as a
        JOIN `{gcs_url_table}` as b
        ON a.file_gdc_id = b.file_gdc_id
        WHERE {joined_filters} AND a.`access` = "open" AND a.program_name = '{program}'
        {file_limit}
        """


def concat_all_files(all_files, one_big_tsv, all_files_local_location, headers_to_switch, columns_to_add):
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

    with open(all_files, 'r') as all_files_list:
        files_list = all_files_list.read().splitlines()

    with open(one_big_tsv, 'w') as outfile:
        for filename in files_list:
            toss_zip = False

            if filename.endswith('.zip'):
                logger.info(f"Unzipping {filename}")
                dir_name = os.path.dirname(filename)
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
                        if line.startswith('#'):
                            continue
                        elif first:
                            header = line.rstrip("\n").split("\t")
                            header_id = header[0]
                            for column in header:
                                if headers_to_switch and column in headers_to_switch.keys():
                                    replace_index = header.index(column)
                                    header[replace_index] = headers_to_switch[column]

                            if columns_to_add:
                                header.extend(columns_to_add)

                            header.append("file_name")
                            outfile.write("\t".join(header))
                            outfile.write("\n")
                            first = False
                        elif not line.startswith(header_id):
                            outfile.write(line.rstrip('\n'))
                            outfile.write('\t')
                            outfile.write("\t" * len(columns_to_add))
                            outfile.write(filename.replace(f"{all_files_local_location}/", ''))
                            outfile.write('\n')
            else:
                logger.info(f'{use_file_name} was not found')

            if toss_zip and os.path.isfile(use_file_name):
                os.remove(use_file_name)
    return


def transform_bq_data(datatype, raw_data_table, draft_data_table, aliquot_table, case_table, file_table, gene_table, dev_project, dev_dataset, release):
    logger = logging.getLogger('base_script')
    intermediate_tables = []

    if datatype == "gene_level_copy_number":  # todo
        print("Creating Gene Level Copy Number draft tables")

    if datatype == "copy_number":  # todo
        print("copy number")

    if datatype == "open_somatic_mut":
        logger.info("Creating Somatic Mut draft tables")
        som_mut_tables = create_somatic_mut_table(raw_data_table, draft_data_table, file_table, aliquot_table,
                                                  case_table, dev_project, dev_dataset, release)
        intermediate_tables.extend(som_mut_tables)

    if datatype == "rna_seq":
        logger.info("Creating RNA seq draft tables")

        rna_seq_table = create_rna_seq_table(raw_data_table, draft_data_table, file_table, aliquot_table, case_table, dev_project, dev_dataset)
        intermediate_tables.extend(rna_seq_table)

    if datatype == "mRNA_seq":  # todo
        print("mRNA_seq")

    return intermediate_tables


def build_bq_tables_steps(params, home, local_dir, workflow_run_ver, steps, data_type, program):
    logger = logging.getLogger('base_script')

    with open(f"{home}/{params.SCHEMA_REPO_LOCAL}/{params.PROGRAM_MAPPINGS}", mode='r') as program_mappings_file:
        program_mappings = json_loads(program_mappings_file.read().rstrip())

    # file variables
    prefix = f"{program_mappings[program]['bq_dataset']}_{data_type}_{params.RELEASE}{workflow_run_ver}"
    local_location = f"{local_dir}/{program_mappings[program]['bq_dataset']}"
    raw_files_local_location = f"{local_location}/files{data_type}"
    tables_created_file = f"{home}/{params.LOCAL_DIR}/tables_created_{params.RELEASE}{workflow_run_ver}.txt"

    with open(f"{home}/{params.SCHEMA_REPO_LOCAL}/{params.DATATYPE_MAPPINGS}", mode='r') as datatype_mappings_file:
        datatype_mappings = json_loads(datatype_mappings_file.read().rstrip())

    file_list = f"{prefix}_file_list.tsv"
    file_traversal_list = f"{prefix}_traversal.tsv"
    raw_data = f"{prefix}_raw"
    draft_table = f"{prefix}_draft_table"  # todo find out where needs the table name and where it should be table id
    field_list = f"{local_location}/{prefix}_field_schema.json"

    if 'create_file_list' in steps:
        logger.info("Running create_file_list Step")
        create_file_list(params, program, data_type, local_location, prefix, file_list,
                         datatype_mappings)

    # todo step for downloading files
    if 'transfer_from_gdc' in steps:
        # Bring the files to the local dir from DCF GDC Cloud Buckets
        with open(f"{local_location}/{file_list}", mode='r') as pull_list_file:
            pull_list = pull_list_file.read().splitlines()
        pull_from_buckets(pull_list, raw_files_local_location)

        all_files = build_file_list(raw_files_local_location)
        with open(f"{local_location}/{file_traversal_list}", mode='w') as traversal_list: # todo move to build_file_list
            for line in all_files:
                traversal_list.write(f"{line}\n")

    if 'create_concat_file' in steps:

        logging.info("Creating concat file")
        local_file_dir = f"{local_location}/concat_file"
        local_concat_file = f"{local_file_dir}/{raw_data}.tsv"
        concat_all_files(f"{local_location}/{file_traversal_list}", local_concat_file,
                         raw_files_local_location, datatype_mappings[data_type]['headers_to_switch'],
                         datatype_mappings[data_type]['headers_to_add'])
        # todo future add header rows if needed (Methylation)
        # todo future break up copy number files into each workflow (maybe)

        logging.info("Running analyze_the_schema Step")

        typing_tups = find_types(local_concat_file, params.SCHEMA_SAMPLE_SKIPS)

        create_schema_hold_list(typing_tups,
                                f"{home}/schemaRepo/TableFieldUpdates/gdc_{data_type}_desc.json",
                                field_list, True)

        logging.info("Running upload_to_bucket Step")
        local_to_bucket(params.DEV_BUCKET, f"{params.DEV_BUCKET_DIR}/{params.RELEASE}/{raw_data}.tsv", local_concat_file)

        logging.info("Removing local files")
        clean_local_file_dir(local_file_dir)

    if 'create_bq_from_tsv' in steps:
        logging.info("Running create_bq_from_tsv Step")
        bucket_src_url = f'gs://{params.DEV_BUCKET}/{params.DEV_BUCKET_DIR}/{params.RELEASE}/{raw_data}.tsv'
        with open(field_list, mode='r') as schema_list:
            typed_schema = json_loads(schema_list.read())
        csv_to_bq(typed_schema, bucket_src_url, params.DEV_DATASET, raw_data, params.BQ_AS_BATCH,
                  bigquery.WriteDisposition.WRITE_TRUNCATE)

    if 'transform_bq_data' in steps:
        logging.info("Running transform_bq_data Step")
        created_tables = transform_bq_data(data_type, raw_data, draft_table, params.ALIQUOT_TABLE, params.CASE_TABLE,
                                           params.FILE_TABLE, params.GENE_NAMES_TABLE, params.DEV_PROJECT,
                                           params.DEV_DATASET, params.RELEASE)
        with open(tables_created_file, 'w') as outfile:
            for table in created_tables:
                outfile.write(table)

    if 'update_table_schema' in steps:
        # todo
        logging.info("Running update_table_schema Step")

        if bq_table_exists(f"{params.DEV_PROJECT}.{params.DEV_DATASET}.{draft_table}"):
            updated_schema_tags = update_schema_tags(datatype_mappings, params.PROGRAM_MAPPINGS, params.RELEASE,
                                                     params.REL_DATE, program_mappings[program]['bq_dataset'])  # todo is this correct?

            write_table_schema_with_generic(
                f"{params.DEV_PROJECT}.{params.DEV_DATASET}.{draft_table}",
                updated_schema_tags,
                f"{home}/schemaRepo/GenericSchemas/{program_mappings[program]['bq_dataset']}_{data_type}.json",
                f"{home}/schemaRepo/TableFieldUpdates/gdc_{program_mappings[program]['bq_dataset']}_{data_type}_desc.json")

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
        data_to_gather, steps, params_dict = load_config(yaml_file.read())
        params = SimpleNamespace(**params_dict)

    # Set the workflow run count from yaml
    workflow_run_ver = f"_{params.WORKFLOW_RUN_VER}" if 'WORKFLOW_RUN_VER' in params_dict else ''

    # Make all local paths absolute:
    home = expanduser("~")
    local_dir = f"{home}/{params.LOCAL_DIR}"

    log_filepath = f"{local_dir}/{params.LOGFILE_DIR}/gdc_data_files_{params.RELEASE}{workflow_run_ver}.log"
    logger = initialize_logging(log_filepath)

    logger.info(f"GDC derived data script started at {time.strftime('%x %X', time.localtime())}")

    # Start of Workflow

    if 'update_schema_dir_from_git' in steps:
        logger.info("Running update_schema_dir_from_git Step")
        update_dir_from_git(f"{home}/{params.SCHEMA_REPO_LOCAL}", params.SCHEMA_REPO_URL, params.SCHEMA_REPO_BRANCH)

    # Derived Data Steps
    for data_type in data_to_gather:
        if data_to_gather[data_type]:
            for program in data_to_gather[data_type]:
                build_bq_tables_steps(params, home, local_dir, workflow_run_ver, steps, data_type, program)
        else:
            logger.info(f"{data_type} will not run")

    end = time.time() - start_time
    logger.info(f"Finished program execution in {format_seconds(end)}!\n")


if __name__ == "__main__":
    main(sys.argv)
