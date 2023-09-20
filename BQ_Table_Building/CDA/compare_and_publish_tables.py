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

from typing import Union

from google.cloud.bigquery.table import _EmptyRowIterator

from cda_bq_etl.bq_helpers import find_most_recent_published_table_id, exists_bq_table, exists_bq_dataset, \
    copy_bq_table, update_friendly_name, change_status_to_archived, query_and_retrieve_result
from cda_bq_etl.data_helpers import initialize_logging
from cda_bq_etl.utils import input_with_timeout, load_config, format_seconds

PARAMS = dict()
YAML_HEADERS = ('params', 'steps')


def table_has_new_data(previous_table_id: str, current_table_id: str) -> bool:
    """
    Compare newly created table and existing published table. Only publish new table if there's a difference.
    :param previous_table_id: table id for existing published table
    :param current_table_id: table id for new table
    :return:
    """

    def compare_two_tables_sql():
        return f"""
            (
                SELECT * FROM `{previous_table_id}`
                EXCEPT DISTINCT
                SELECT * from `{current_table_id}`
            )
            UNION ALL
            (
                SELECT * FROM `{current_table_id}`
                EXCEPT DISTINCT
                SELECT * from `{previous_table_id}`
            )
        """

    if not previous_table_id:
        return True

    compare_result = query_and_retrieve_result(sql=compare_two_tables_sql())

    if isinstance(compare_result, _EmptyRowIterator):
        # no distinct result rows, tables match
        return False

    if compare_result is None:
        logger = logging.getLogger('base_script')
        logger.info("No result returned for table comparison query. Often means that tables have differing schemas.")
        return True

    for row in compare_result:
        return True if row else False


def compare_tables(source_table_id: str, current_table_id: str, versioned_table_id: str):
    logger = logging.getLogger('base_script')

    previous_versioned_table_id = find_most_recent_published_table_id(PARAMS, versioned_table_id)

    if previous_versioned_table_id is None:
        logger.warning(f"No previous version found for {source_table_id}. Will publish. Investigate if unexpected.")
        return

    logger.info(f"Comparing tables {source_table_id} and {previous_versioned_table_id}.")

    # does source table exist?
    if not exists_bq_table(source_table_id):
        logger.critical("Source table id doesn't exist, cannot publish.")
        sys.exit(-1)

    # does current dataset exist?
    current_dataset = ".".join(current_table_id.split('.')[:-1])

    if not exists_bq_dataset(current_dataset):
        logger.critical(f"Dataset {current_dataset} doesn't exist, cannot publish.")
        sys.exit(-1)

    # does versioned dataset exist?
    versioned_dataset = ".".join(versioned_table_id.split('.')[:-1])

    if not exists_bq_dataset(versioned_dataset):
        logger.critical(f"Dataset {versioned_dataset} doesn't exist, cannot publish.")
        sys.exit(-1)

    logger.info("Previous published and source tables exist, great! Continuing.")

    # display published table_ids
    logger.info("To-be-published table_ids:")
    logger.info(f"current: {current_table_id}")
    logger.info(f"versioned: {versioned_table_id}")

    has_new_data = table_has_new_data(previous_versioned_table_id, source_table_id)

    # is there a previous version to compare with new table?
    # use previous_versioned_table_id
    if has_new_data:
        logger.info(f"New data found--table will be published.")
    elif not has_new_data:
        logger.info(f"Tables are identical (no new data found)--table will not be published.")


def find_record_difference_counts(table_type: str,
                                  source_table_id: str,
                                  versioned_table_id: str,
                                  table_metadata: dict[str, Union[str, list[str]]]):
    def make_record_count_query(table_id):
        return f"""
            SELECT COUNT(*) AS record_count
            FROM `{table_id}`
        """

    def make_added_record_count_query():
        return f"""
            WITH new_rows AS (
                SELECT * 
                FROM `{source_table_id}`
                EXCEPT DISTINCT
                SELECT *
                FROM `{previous_versioned_table_id}`
            ), 
            old_rows AS (
                SELECT * 
                FROM `{previous_versioned_table_id}`
                EXCEPT DISTINCT
                SELECT *
                FROM `{source_table_id}`
            )

            # added aliquots
            SELECT COUNT({primary_key}) AS changed_count, {output_key_string} 
            FROM new_rows
            WHERE {primary_key} NOT IN (
                SELECT {primary_key} 
                FROM old_rows
            )
            GROUP BY {output_key_string}
            ORDER BY {output_key_string}
        """

    def make_removed_record_count_query():
        return f"""
            WITH new_rows AS (
                SELECT * 
                FROM `{source_table_id}`
                EXCEPT DISTINCT
                SELECT *
                FROM `{previous_versioned_table_id}`
            ), 
            old_rows AS (
                SELECT * 
                FROM `{previous_versioned_table_id}`
                EXCEPT DISTINCT
                SELECT *
                FROM `{source_table_id}`
            )

            # added aliquots
            SELECT COUNT({primary_key}) AS changed_count, {output_key_string} 
            FROM old_rows
            WHERE {primary_key} NOT IN (
                SELECT {primary_key} 
                FROM new_rows
            )
            GROUP BY {output_key_string}
            ORDER BY {output_key_string}
        """

    def make_changed_record_count_query():
        return f"""
            WITH new_rows AS (
                SELECT * 
                FROM `{source_table_id}`
                EXCEPT DISTINCT
                SELECT *
                FROM `{previous_versioned_table_id}`
            ), 
            old_rows AS (
                SELECT * 
                FROM `{previous_versioned_table_id}`
                EXCEPT DISTINCT
                SELECT *
                FROM `{source_table_id}`
            ), intersects AS (
                SELECT {primary_key}, {secondary_key} {output_key_string} 
                FROM old_rows
                INTERSECT DISTINCT
                SELECT {primary_key}, {secondary_key} {output_key_string} 
                FROM new_rows
            )

            SELECT COUNT({primary_key}) AS changed_count, {output_key_string}
            FROM intersects
            GROUP BY {output_key_string}
            ORDER BY {output_key_string}
        """

    def compare_records(query: str) -> tuple[int, str]:
        # find added/removed/changed records by project
        result = query_and_retrieve_result(query)

        total_results = 0
        num_columns = len(table_metadata['output_keys']) + 1
        output_string = ""

        if result.total_rows > 0:
            for row in result:
                total_results += row[0]

                # append the count, right justify
                row_str = f"{row[0]:>10}"

                # append the other values (e.g. project id, type) as specified in output keys
                for i in range(1, num_columns):
                    row_str += f"{row[i]:30}"

                output_string += '\n' + row_str

        return total_results, output_string

    logger = logging.getLogger('base_script')

    previous_versioned_table_id = find_most_recent_published_table_id(PARAMS, versioned_table_id)

    if previous_versioned_table_id is None:
        logger.warning(f"No previous table found for {versioned_table_id}; therefore, no changes to report.")
        return

    output_key_string = ",".join(table_metadata['output_keys'])
    primary_key = table_metadata['primary_key']

    if table_metadata['secondary_key'] is not None:
        secondary_key = table_metadata['secondary_key'] + ', '
    else:
        secondary_key = ''

    # get record count from previous versioned table
    previous_version_count_result = query_and_retrieve_result(make_record_count_query(previous_versioned_table_id))

    try:
        previous_version_count = None

        for row in previous_version_count_result:
            previous_version_count = row[0]
            break

        if previous_version_count is None:
            raise TypeError
    except TypeError:
        logger.critical(f"No value returned for previous version row count in {previous_versioned_table_id}.")
        logger.critical("Probably an error in the table id or SQL query.")
        sys.exit(-1)

    new_version_count_result = query_and_retrieve_result(make_record_count_query(source_table_id))

    try:
        new_version_count = None

        for row in new_version_count_result:
            new_version_count = row[0]
            break

        if new_version_count is None:
            raise TypeError
    except TypeError:
        logger.critical(f"No value returned for new version row count in {source_table_id}.")
        logger.critical("Probably an error in the table id or SQL query.")
        sys.exit(-1)

    count_difference = int(new_version_count) - int(previous_version_count)

    logger.info(f"***** {table_type.upper()} *****")
    logger.info(f"Current {table_type} count: {new_version_count}")
    logger.info(f"Previous {table_type} count: {previous_version_count}")
    logger.info(f"Difference: {count_difference}")

    # find added records by project
    added_count, added_str = compare_records(query=make_added_record_count_query())

    logger.info(f"Added {table_type} count: {added_count}")
    if added_str:
        logger.info(added_str)

    # find removed records by project
    removed_count, removed_str = compare_records(query=make_removed_record_count_query())

    logger.info(f"Removed {table_type} count: {removed_count}")
    if removed_str:
        logger.info(removed_str)

    # find changed records by project
    changed_count, changed_str = compare_records(query=make_changed_record_count_query())

    logger.info(f"Changed {table_type} count: {changed_count}")
    if changed_str:
        logger.info(changed_str)


def get_change_details():
    pass


def publish_table(source_table_id: str, current_table_id: str, versioned_table_id: str):
    """
    Publish production BigQuery tables using source_table_id:
        - create current/versioned table ids
        - publish tables
        - update friendly name for versioned table
        - change last version tables' status labels to archived
    :param source_table_id: source (dev) table id
    :param current_table_id: published table id for current
    :param versioned_table_id: published table id for versioned
    """
    logger = logging.getLogger('base_script')

    previous_versioned_table_id = find_most_recent_published_table_id(PARAMS, versioned_table_id)
    logger.info(f"previous_versioned_table_id: {previous_versioned_table_id}")

    if PARAMS['TEST_PUBLISH']:
        logger.error("Cannot run publish table step with TEST_PUBLISH set to true.")
        sys.exit(-1)

    if exists_bq_table(source_table_id):
        if table_has_new_data(previous_versioned_table_id, source_table_id):
            delay = 5

            logger.info(f"""\n\nPublishing the following tables:""")
            logger.info(f"\t - {versioned_table_id}\n\t - {current_table_id}")
            logger.info(f"Proceed? Y/n (continues automatically in {delay} seconds)")

            response = str(input_with_timeout(seconds=delay)).lower()

            if response == 'n':
                exit("\nPublish aborted; exiting.")

            logger.info(f"\nPublishing {versioned_table_id}")
            copy_bq_table(PARAMS, source_table_id, versioned_table_id, replace_table=PARAMS['OVERWRITE_PROD_TABLE'])

            logger.info(f"Publishing {current_table_id}")
            copy_bq_table(PARAMS, source_table_id, current_table_id, replace_table=PARAMS['OVERWRITE_PROD_TABLE'])

            logger.info(f"Updating friendly name for {versioned_table_id}")
            update_friendly_name(PARAMS, table_id=versioned_table_id)

            if previous_versioned_table_id:
                logger.info(f"Archiving {previous_versioned_table_id}")
                change_status_to_archived(previous_versioned_table_id)

        else:
            logger.info(f"{source_table_id} not published, no changes detected")


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

    for table_type in PARAMS['TABLE_TYPES'].keys():
        table_metadata = PARAMS['TABLE_TYPES'][table_type]

        prod_project = PARAMS['PROD_PROJECT']
        prod_dataset = table_metadata['prod_dataset']
        dev_project = PARAMS['DEV_PROJECT']
        dev_dataset = PARAMS['DEV_METADATA_DATASET']
        table_base_name = table_metadata['table_base_name']

        current_table_id = f"{prod_project}.{prod_dataset}.{table_base_name}_current"
        versioned_table_id = f"{prod_project}.{prod_dataset}_versioned.{table_base_name}_{PARAMS['DC_RELEASE']}"
        source_table_id = f"{dev_project}.{dev_dataset}.{table_base_name}_{PARAMS['RELEASE']}"

        if 'compare_tables' in steps:
            # confirm that datasets and table ids exist, and preview whether table will be published
            compare_tables(source_table_id, current_table_id, versioned_table_id)

            # display compare_to_last style output
            find_record_difference_counts(table_type, source_table_id, versioned_table_id, table_metadata)

            # todo
            get_change_details()

        if 'publish_tables' in steps:
            # todo
            publish_table(source_table_id, current_table_id, versioned_table_id)

    end_time = time.time()
    logger.info(f"Script completed in: {format_seconds(end_time - start_time)}")
