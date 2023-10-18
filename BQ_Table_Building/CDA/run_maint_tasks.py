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
from cda_bq_etl.bq_helpers import query_and_retrieve_result, delete_bq_table

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


def main(args):
    try:
        global PARAMS
        PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        sys.exit(err)

    log_file_time = time.strftime('%Y.%m.%d-%H.%M.%S', time.localtime())
    log_filepath = f"{PARAMS['LOGFILE_PATH']}.{log_file_time}"
    logger = initialize_logging(log_filepath)

    if 'delete_filtered_tables' in steps:
        project_dataset_id_list = PARAMS['DELETE_TABLES']['project_dataset_id_list']
        filter_string = PARAMS['DELETE_TABLES']['filter_string']

        delete_old_tables(project_dataset_id_list, filter_string)


if __name__ == "__main__":
    main(sys.argv)
