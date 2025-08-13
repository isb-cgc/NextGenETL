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
import logging
import sys
import time

from cda_bq_etl.data_helpers import initialize_logging
from cda_bq_etl.utils import load_config, input_with_timeout
from cda_bq_etl.bq_helpers.lookup import query_and_retrieve_result
from cda_bq_etl.bq_helpers.create_modify import delete_bq_table, copy_bq_table, update_friendly_name, update_table_labels, \
    update_table_description

PARAMS = dict()
YAML_HEADERS = ('params', 'steps')


def delete_old_tables(project_dataset_id_list: list[str], filter_string: str):
    def make_table_list_query():
        return f"""
            SELECT table_name
            FROM `{project_dataset_id}`.INFORMATION_SCHEMA.TABLES
            WHERE table_name like '%{filter_string}%'
        """

    for project_dataset_id in project_dataset_id_list:
        table_name_result = query_and_retrieve_result(make_table_list_query())

        logger = logging.getLogger('base_script')
        logger.info(f"Deleting the following tables in {project_dataset_id}:")

        table_ids = list()

        for row in table_name_result:
            table_id = f"{project_dataset_id}.{row['table_name']}"
            table_ids.append(table_id)
            logger.info(table_id)

        logger.info(f"Proceed? Y/n (continues automatically in 5 seconds)")

        response = str(input_with_timeout(seconds=5)).lower()

        if response == 'n':
            exit("\nPublish aborted; exiting.")

        for table_id in table_ids:
            delete_bq_table(table_id)

        logger.info(f"Tables deleted.")


def restore_deleted_table(deleted_table_id, new_table_id, snapshot_epoch):
    snapshot_table_id = f"{deleted_table_id}@{snapshot_epoch}"

    copy_bq_table(PARAMS, src_table=snapshot_table_id, dest_table=new_table_id)


def update_friendly_names(friendly_name_dict):
    logger = logging.getLogger("base_script")
    for table_id, friendly_name in friendly_name_dict.items():
        update_friendly_name(PARAMS, table_id, friendly_name)
        logger.info(f"Updated friendly name for {table_id} to {friendly_name}")


def update_labels(column_label_dict: dict[str, str], table_ids: list[str]):
    for table_id in table_ids:
        logger = logging.getLogger("base_script")

        update_table_labels(table_id=table_id, label_dict=column_label_dict)


def update_description(table_ids: list[str], description: str):
    for table_id in table_ids:
        update_table_description(table_id, description)


def main(args):
    try:
        global PARAMS
        PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        sys.exit(err)

    log_file_time = time.strftime('%Y.%m.%d-%H.%M.%S', time.localtime())
    log_filepath = f"{PARAMS['LOGFILE_PATH']}.{log_file_time}"
    logger = initialize_logging(log_filepath)

    logger.info(f"Welcome to the BQ maintenance task script. THIS TOOL IS DANGEROUS. "
                f"It can alter/delete published tables. BE CAREFUL.")

    logger.info(f"It's currently configured to run the following steps:")

    for step in steps:
        logger.info(f"- {step}")

    delay = 5
    logger.info(f"Proceed? Y/n (continues automatically in {delay} seconds)")
    response = str(input_with_timeout(seconds=delay)).lower()

    if response == 'n' or response == 'N':
        exit("Publish aborted; exiting.")

    if 'delete_filtered_tables' in steps:
        project_dataset_id_list = PARAMS['DELETE_TABLES']['project_dataset_id_list']
        filter_string = PARAMS['DELETE_TABLES']['filter_string']

        delete_old_tables(project_dataset_id_list, filter_string)

    if 'restore_deleted_table' in steps:
        deleted_table_id = PARAMS['RESTORE_TABLE_ID']
        new_table_id = ''
        snapshot_epoch = 1697655753782
        restore_deleted_table(deleted_table_id, new_table_id, snapshot_epoch)

    if 'update_friendly_names' in steps:
        friendly_name_dict = PARAMS['FRIENDLY_NAME_DICT']
        update_friendly_names(friendly_name_dict)

    if 'update_column_labels' in steps:
        column_label_dict = PARAMS['UPDATE_LABELS']['labels']
        table_ids = PARAMS['UPDATE_LABELS']['table_ids']

        update_labels(table_ids=table_ids, column_label_dict=column_label_dict)

    if 'update_table_description' in steps:
        table_ids = PARAMS['UPDATE_TABLE_DESCRIPTION']['table_ids']
        description = PARAMS['UPDATE_TABLE_DESCRIPTION']['description']

        update_description(table_ids=table_ids, description=description)


if __name__ == "__main__":
    main(sys.argv)
