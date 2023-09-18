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

from google.cloud.bigquery.table import _EmptyRowIterator

from cda_bq_etl.bq_helpers import find_most_recent_published_table_id, exists_bq_table, exists_bq_dataset, \
    copy_bq_table, update_friendly_name, change_status_to_archived, query_and_retrieve_result
from cda_bq_etl.data_helpers import initialize_logging
from cda_bq_etl.utils import input_with_timeout, load_config, format_seconds

PARAMS = dict()
YAML_HEADERS = ('params', 'steps')


def compare_tables(source_table_id: str, current_table_id: str, versioned_table_id: str):
    logger = logging.getLogger('base_script')

    previous_versioned_table_id = find_most_recent_published_table_id(PARAMS, versioned_table_id)

    if previous_versioned_table_id is None:
        logger.warning(f"No previous version found for {source_table_id}. Will publish. Investigate if unexpected.")
        return

    logger.info(f"Comparing tables {source_table_id} and {previous_versioned_table_id}.")

    # does source table exist?
    if exists_bq_table(source_table_id):
        logger.info("Source table id is valid.")
    else:
        logger.critical("Source table id doesn't exist, cannot publish.")
        sys.exit(-1)

    # does current dataset exist?
    current_dataset = ".".join(current_table_id.split('.')[:-1])
    current_dataset_exists = exists_bq_dataset(current_dataset)

    if current_dataset_exists:
        logger.info(f"Dataset {current_dataset} is valid.")
    else:
        logger.critical(f"Dataset {current_dataset} doesn't exist, cannot publish.")
        sys.exit(-1)

    # does versioned dataset exist?
    versioned_dataset = ".".join(versioned_table_id.split('.')[:-1])
    versioned_dataset_exists = exists_bq_dataset(versioned_dataset)

    if versioned_dataset_exists:
        logger.info(f"Dataset {versioned_dataset} is valid.")
    else:
        logger.critical(f"Dataset {versioned_dataset} doesn't exist, cannot publish.")
        sys.exit(-1)

    # display published table_ids
    logger.info("Published table_ids (to be created--not yet published):")
    logger.info(f"current table_id: {current_table_id}")
    logger.info(f"versioned table_id: {versioned_table_id}")

    has_new_data = table_has_new_data(previous_versioned_table_id, source_table_id)

    # is there a previous version to compare with new table?
    # use previous_versioned_table_id
    if has_new_data:
        logger.info(f"New data found compared to previous published table {previous_versioned_table_id}.")
        logger.info("Table will be published.")
    elif not has_new_data:
        logger.info(f"New table is identical to previous published table {previous_versioned_table_id}.")
        logger.info("Table will not be published.")


def find_changes_to_table(source_table_id: str, versioned_table_id: str):
    logger = logging.getLogger('base_script')

    previous_versioned_table_id = find_most_recent_published_table_id(PARAMS, versioned_table_id)

    if previous_versioned_table_id is None:
        logger.warning(f"No previous table found for {versioned_table_id}; therefore, no changes to report.")
        return

    # gather two sets of data. all the values from these two queries.
    # where primary key exists in previous but not current, the record was removed.
    # where key exists in current but not previous, the record was added.
    # where key exists in both current and previous, the record was changed.
    # for project_id and any secondary display key, store count added, removed, changed
    """
    { 
        'added':
            (project_id, sample_type_name): count
            ...
        'deleted':
            (project_id, sample_type_name): count
            ...
        'changed':
            (project_id, sample_type_name): count
            ...            
    """

    # For added aliquots (really portions)
    """
    WITH new_rows AS (
      SELECT * 
      FROM `isb-project-zero.GDC_metadata.rel35_aliquot2caseIDmap`
      EXCEPT DISTINCT
      SELECT *
      FROM `isb-project-zero.GDC_metadata.rel34_aliquot2caseIDmap`
    ), 
    old_rows AS (
      SELECT * 
      FROM `isb-project-zero.GDC_metadata.rel34_aliquot2caseIDmap`
      EXCEPT DISTINCT
      SELECT *
      FROM `isb-project-zero.GDC_metadata.rel35_aliquot2caseIDmap`
    )
    
    # added aliquots
    SELECT COUNT(portion_gdc_id) AS changed_count, project_id, sample_type_name 
    FROM new_rows
    WHERE portion_gdc_id NOT IN (
        SELECT portion_gdc_id 
        FROM old_rows
    )
    GROUP BY project_id, sample_type_name
    ORDER BY project_id, sample_type_name
    """

    # For removed aliquots (really portions)
    """
    WITH new_rows AS (
      SELECT * 
      FROM `isb-project-zero.GDC_metadata.rel35_aliquot2caseIDmap`
      EXCEPT DISTINCT
      SELECT *
      FROM `isb-project-zero.GDC_metadata.rel34_aliquot2caseIDmap`
    ), 
    old_rows AS (
      SELECT * 
      FROM `isb-project-zero.GDC_metadata.rel34_aliquot2caseIDmap`
      EXCEPT DISTINCT
      SELECT *
      FROM `isb-project-zero.GDC_metadata.rel35_aliquot2caseIDmap`
    )
    
    # removed aliquots
    SELECT COUNT(portion_gdc_id) AS changed_count, project_id, sample_type_name 
    FROM old_rows
    WHERE portion_gdc_id NOT IN (
        SELECT portion_gdc_id 
        FROM new_rows
    )
    GROUP BY project_id, sample_type_name
    ORDER BY project_id, sample_type_name
    """

    # intersects
    """
    WITH new_rows AS (
        SELECT * 
        FROM `isb-project-zero.GDC_metadata.rel35_aliquot2caseIDmap`
        EXCEPT DISTINCT
        SELECT *
        FROM `isb-project-zero.GDC_metadata.rel34_aliquot2caseIDmap`
    ), 
    old_rows AS (
        SELECT * 
        FROM `isb-project-zero.GDC_metadata.rel34_aliquot2caseIDmap`
        EXCEPT DISTINCT
        SELECT *
        FROM `isb-project-zero.GDC_metadata.rel35_aliquot2caseIDmap`
    ), intersects AS (
        SELECT aliquot_gdc_id, portion_gdc_id, project_id, sample_type_name 
        FROM old_rows
        INTERSECT DISTINCT
        SELECT aliquot_gdc_id, portion_gdc_id, project_id, sample_type_name 
        FROM new_rows
    )
    
    # changed aliquots
    SELECT COUNT(aliquot_gdc_id) AS changed_count, project_id, sample_type_name
    FROM intersects
    GROUP BY project_id, sample_type_name
    ORDER BY project_id, sample_type_name
    """


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

    end_time = time.time()
    logger.info(f"Script completed in: {format_seconds(end_time - start_time)}")
