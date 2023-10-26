"""
Copyright 2023, Institute for Systems Biology

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""
import sys
import time

from cda_bq_etl.data_helpers import initialize_logging
from cda_bq_etl.utils import load_config, format_seconds
from cda_bq_etl.bq_helpers import create_bq_dataset, list_tables_in_dataset, copy_bq_table, exists_bq_table

PARAMS = dict()
YAML_HEADERS = ('params', 'steps')


def create_datasets():
    for dataset in PARAMS['NEW_DATASETS']:
        create_bq_dataset(PARAMS, project_id=PARAMS['STAGING_PROJECT'], dataset_name=dataset)
        create_bq_dataset(PARAMS, project_id=PARAMS['STAGING_PROJECT'], dataset_name=f"{dataset}_versioned")


def main(args):
    try:
        start_time = time.time()

        global PARAMS
        PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        sys.exit(err)

    log_file_time = time.strftime('%Y.%m.%d-%H.%M.%S', time.localtime())
    log_filepath = f"{PARAMS['LOGFILE_PATH']}.{log_file_time}"
    logger = initialize_logging(log_filepath)

    # create datasets
    if 'create_datasets' in steps:
        logger.info("Creating datasets!")
        create_datasets()

    if 'copy_tables' in steps:
        program_datasets = PARAMS['NEW_DATASETS']
        program_datasets.remove(PARAMS['METADATA_DATASET'])

        metadata_project_dataset_id = f"{PARAMS['PROD_PROJECT']}.{PARAMS['METADATA_DATASET']}"

        metadata_tables = list_tables_in_dataset(metadata_project_dataset_id)

        for table in metadata_tables:
            prod_table_id = f"{metadata_project_dataset_id}.{table}"
            dest_table_id = f"{PARAMS['STAGING_PROJECT']}.{PARAMS['METADATA_DATASET']}.{table}"

            vers_table_name = table.replace("current", "r36")
            vers_dest_table_id = f"{PARAMS['STAGING_PROJECT']}.{PARAMS['METADATA_DATASET']}_versioned.{vers_table_name}"

            copy_bq_table(params=PARAMS, src_table=prod_table_id, dest_table=dest_table_id, replace_table=True)
            copy_bq_table(params=PARAMS, src_table=prod_table_id, dest_table=vers_dest_table_id, replace_table=True)

        for program_dataset in program_datasets:
            for table_type in PARAMS['TABLE_KEYWORDS']:
                program_dataset_id = f"{PARAMS['PROD_PROJECT']}.{program_dataset}"

                filter_words = [table_type, PARAMS['NODE']]

                program_tables = list_tables_in_dataset(program_dataset_id, filter_words)

                for table in program_tables:
                    prod_table_id = f"{program_dataset_id}.{table}"
                    dest_table_id = f"{PARAMS['STAGING_PROJECT']}.{program_dataset}.{table}"

                    vers_table_name = table.replace("current", "r36")

                    for old_word, new_word in PARAMS['WORDS_TO_ALTER'].items():
                        vers_table_name = vers_table_name.replace(old_word, new_word)

                    vers_dest_dataset_id = f"{PARAMS['STAGING_PROJECT']}.{PARAMS['METADATA_DATASET']}_versioned"
                    vers_dest_table_id = f"{vers_dest_dataset_id}.{vers_table_name}"

                    copy_bq_table(PARAMS, src_table=prod_table_id, dest_table=dest_table_id, replace_table=True)
                    copy_bq_table(PARAMS, src_table=prod_table_id, dest_table=vers_dest_table_id, replace_table=True)

    end_time = time.time()
    logger.info(f"Script completed in: {format_seconds(end_time - start_time)}")


if __name__ == "__main__":
    main(sys.argv)
