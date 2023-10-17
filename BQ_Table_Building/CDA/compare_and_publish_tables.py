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
import json
import logging
import os
import sys
import time

from typing import Union

from google.cloud.bigquery.table import _EmptyRowIterator

from cda_bq_etl.bq_helpers import find_most_recent_published_table_id, exists_bq_table, exists_bq_dataset, \
    copy_bq_table, update_friendly_name, change_status_to_archived, query_and_retrieve_result
from cda_bq_etl.data_helpers import initialize_logging
from cda_bq_etl.utils import input_with_timeout, load_config, format_seconds, get_filepath

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


def compare_table_columns(left_table_id: str,
                          right_table_id: str,
                          column_list: list[str],
                          primary_key: str,
                          secondary_key: str = None,
                          max_display_rows: int = 5):
    """
    Compare left table to right table on a per-column basis.
    Results returned represent data missing from the right table but found in the left.
    :param left_table_id: left table id
    :param right_table_id: right table id
    :param primary_key: primary key, used to match rows across tables
    :param column_list: list of columns to compare across tables (non-concatenated only, as order is not guaranteed)
    :param secondary_key: optional; secondary key used to map data
    :param max_display_rows: maximum result rows to display in output
    """
    def make_compare_table_column_sql(column_name, table_id_1, table_id_2) -> str:
        secondary_key_sql_string = ''

        if secondary_key is not None:
            secondary_key_sql_string = f"{secondary_key},"

        # outputs values from left table that are not found in right table
        return f"""
            SELECT {secondary_key_sql_string} {primary_key}, {column_name}
            FROM `{table_id_1}`
            EXCEPT DISTINCT 
            SELECT {secondary_key_sql_string} {primary_key}, {column_name}
            FROM `{table_id_2}`
            ORDER BY {primary_key}
        """

    def compare_table_column_left_table(table_id_1, table_id_2):
        column_comparison_query = make_compare_table_column_sql(column, table_id_1, table_id_2)

        result = query_and_retrieve_result(sql=column_comparison_query)

        if not result:
            print(f"\nNo results returned. This can mean that there's a column data type mismatch, "
                  f"or that the column name differs.\n")
        elif result.total_rows > 0:
            print(f"\n{result.total_rows} values in {table_id_1} didn't match value found in {table_id_2}.\n")
            print(f"Example values:\n")

            if secondary_key is not None:
                print(f"{primary_key:40} {secondary_key:40} {column}")
            else:
                print(f"{primary_key:40} {column}")

            count = 0

            for row in result:
                primary_key_value = row.get(primary_key)
                column_value = row.get(column)

                if secondary_key is not None:
                    secondary_key_value = row.get(secondary_key)

                    print(f"{primary_key_value:40} {secondary_key_value:40} {column_value}")
                else:
                    print(f"{primary_key_value:40} {column_value}")

                count += 1
                if count == max_display_rows:
                    print()
                    break
        else:
            print(f"\nNo missing values found in {table_id_2}!")

    for column in column_list:
        print(f"\n* For {column}: *")
        compare_table_column_left_table(left_table_id, right_table_id)
        compare_table_column_left_table(right_table_id, left_table_id)
        print()


def compare_concat_columns(left_table_id: str,
                           right_table_id: str,
                           concat_column_list: list[str],
                           primary_key: str,
                           secondary_key: str = None):
    """
    Compare concatenated column values to ensure matching data, as order is not guaranteed in these column strings.
    :param left_table_id: left table id
    :param right_table_id: right table id
    :param concat_column_list: list of columns containing concatenated strings (associated entities, for example)
    :param primary_key: primary key, used to match rows across tables
    :param secondary_key: optional; secondary key used to map data
    """
    def make_concat_column_query(table_id: str) -> str:
        secondary_key_string = ''

        if secondary_key is not None:
            secondary_key_string = f"{secondary_key},"

        concat_columns_str = ", ".join(concat_column_list)

        return f"""
            SELECT {secondary_key_string} {primary_key}, {concat_columns_str}  
            FROM `{table_id}`
        """

    def make_records_dict(query: str) -> dict[str, dict[str, str]]:
        result = query_and_retrieve_result(sql=query)

        records_dict = dict()

        for record_count, record in enumerate(result):
            primary_key_id = record.get(primary_key)
            records_dict_key = primary_key_id

            if secondary_key is not None:
                records_dict_key += f";{record.get(secondary_key)}"

            record_dict = dict()

            for _column in concat_column_list:
                record_dict[_column] = record.get(_column)

            records_dict[records_dict_key] = record_dict

            if record_count % 100000 == 0 and record_count > 0:
                print(f"{record_count}/{result.total_rows} records added to dict!")

        return records_dict

    left_table_records_dict = make_records_dict(query=make_concat_column_query(left_table_id))
    print("Created dict for left table records!")

    right_table_records_dict = make_records_dict(query=make_concat_column_query(right_table_id))
    print("Created dict for right table records!")

    record_key_set = set(left_table_records_dict.keys())
    record_key_set.update(right_table_records_dict.keys())

    for column in concat_column_list:
        total_record_count = len(record_key_set)
        correct_records_count = 0
        left_table_missing_record_count = 0
        right_table_missing_record_count = 0
        different_lengths_count = 0
        different_values_count = 0
        mismatched_records = list()

        for record_id in record_key_set:
            if record_id not in left_table_records_dict:
                left_table_missing_record_count += 1
                break
            elif record_id not in right_table_records_dict:
                right_table_missing_record_count += 1
                break

            left_column_value = left_table_records_dict[record_id][column]
            right_column_value = right_table_records_dict[record_id][column]

            if left_column_value is None and right_column_value is None:
                correct_records_count += 1
            else:
                if left_column_value is None:
                    left_column_value_list = list()
                else:
                    left_column_value_list = left_column_value.split(';')

                if right_column_value is None:
                    right_column_value_list = list()
                else:
                    right_column_value_list = right_column_value.split(';')

                left_column_value_set = set(left_column_value_list)
                right_column_value_set = set(right_column_value_list)

                if len(left_column_value_list) == len(right_column_value_list) \
                        and len(left_column_value_set ^ right_column_value_set) == 0:
                    correct_records_count += 1
                else:
                    if len(left_column_value_list) != len(right_column_value_list):
                        # if length mismatch, there may be duplicates, so definitely not identical;
                        # set eliminates duplicates, so this is necessary
                        different_lengths_count += 1
                    elif len(left_column_value_set ^ right_column_value_set) > 0:
                        # exclusive or -- values only in exactly one set
                        different_values_count += 1

                    mismatched_records.append({
                        "record_id": record_id,
                        "left_table_value": left_column_value,
                        "right_table_value": right_column_value
                    })

        print(f"For column {column}:")
        print(f"Correct records: {correct_records_count}/{total_record_count}")
        print(f"Missing records from left table: {left_table_missing_record_count}")
        print(f"Missing records from right table: {right_table_missing_record_count}")
        print(f"\nDifferent number of values in record: {different_lengths_count}")
        print(f"Different values in record: {different_values_count}")

        if len(mismatched_records) > 0:
            i = 0

            print("\nExample values:\n")

            for mismatched_record in mismatched_records:
                print(f"{primary_key}: {mismatched_record['record_id']}")
                if len(mismatched_record['left_table_value']) > 0:
                    print(f"left table value(s): {sorted(mismatched_record['left_table_value'].split(';'))}")
                else:
                    print("left table value: None")

                if len(mismatched_record['right_table_value']) > 0:
                    print(f"right table value(s): {sorted(mismatched_record['right_table_value'].split(';'))}\n")
                else:
                    print("right table value: None")

                i += 1

                if i == 5:
                    break


def get_change_details(new_dev_table_id: str, prev_version_table_id: str):
    # compare table columns, both concatenated and non-concatenated
    pass


def get_new_table_names(dataset: str) -> list[str]:
    def make_new_table_names_query():
        return f"""
            SELECT table_name 
            FROM `{PARAMS['DEV_PROJECT']}.{dataset}`.INFORMATION_SCHEMA.TABLES
            WHERE table_name LIKE '%{PARAMS['RELEASE']}'
        """

    table_names = query_and_retrieve_result(make_new_table_names_query())

    return sorted(list(table_names))


def get_current_table_names(table_type) -> list[str]:
    def make_program_tables_query() -> str:
        return f"""
            SELECT table_name 
            FROM `{PARAMS['DEV_PROJECT']}.{program_name}`.INFORMATION_SCHEMA.TABLES
            WHERE table_name LIKE '{table_type}%'
        """
    # get program list from BQEcosystem/MetadataMappings/
    # for each program, look for tables in current list with 'clinical' or 'per_sample_file' prefix
    # add any tables to list object
    logger = logging.getLogger('base_script')

    program_metadata_fp = f"{PARAMS['BQ_REPO']}/{PARAMS['PROGRAM_METADATA_DIR']}"
    program_metadata_fp = get_filepath(program_metadata_fp)

    if not os.path.exists(program_metadata_fp):
        logger.critical("BQEcosystem program metadata path not found")
        sys.exit(-1)
    with open(program_metadata_fp) as field_output:
        program_metadata = json.load(field_output)
        program_names = sorted(list(program_metadata.keys()))

        current_table_names = list()

        suffix = f"_{PARAMS['NODE']}_current"

        for program_name in program_names:
            table_names = query_and_retrieve_result(make_program_tables_query())

            for table_name in list(table_names):
                table_name = table_name.replace(suffix, "")
                program_table_name = f"{table_name}_{program_name}"
                current_table_names.append(program_table_name)

        return sorted(current_table_names)


def find_missing_tables(dataset, table_type):
    """
    Compare published tables to new dev tables. If new table is missing, output a warning.
    :param dataset: development dataset to search for new tables
    :param table_type: table data type, e.g. clinical, per_sample_file
    """
    logger = logging.getLogger('base_script')

    no_release_new_table_names = list()

    current_table_names = get_current_table_names(table_type)
    new_table_names = get_new_table_names(dataset)

    for new_table_name in new_table_names:
        new_table_name = new_table_name.replace(PARAMS['RELEASE'], "")
        no_release_new_table_names.append(new_table_name)

    for current_table_name in current_table_names:
        if current_table_name not in no_release_new_table_names:
            logger.warning(f"Cannot find new dev table for published table {current_table_name}.")


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
        prod_table_name = table_metadata['table_base_name']

        current_table_id = f"{prod_project}.{prod_dataset}.{prod_table_name}_current"
        versioned_table_id = f"{prod_project}.{prod_dataset}_versioned.{prod_table_name}_{PARAMS['RELEASE']}"
        source_table_id = f"{dev_project}.{dev_dataset}.{prod_table_name}_{PARAMS['RELEASE']}"

        if 'compare_tables' in steps:
            # confirm that datasets and table ids exist, and preview whether table will be published
            compare_tables(source_table_id, current_table_id, versioned_table_id)

            # display compare_to_last.sh style output
            find_record_difference_counts(table_type, source_table_id, versioned_table_id, table_metadata)

            get_change_details()

        if 'publish_tables' in steps:
            publish_table(source_table_id, current_table_id, versioned_table_id)

    for table_type, table_type_data in PARAMS['PER_PROGRAM_PROJECT_TABLE_TYPES'].items():
        prod_project = PARAMS['PROD_PROJECT']
        dev_project = PARAMS['DEV_PROJECT']
        dev_dataset = table_type_data['dev_dataset']

        # look for list of last release's published tables to ensure none have disappeared before comparing
        find_missing_tables(dataset=dev_dataset, table_type=table_type)

        # for clinical:
        # get list of tables from clinical dataset for current release
        new_table_names = get_new_table_names(dataset=dev_dataset)
        # find matching previous version table. If none, no comparison
        for table_name in new_table_names:
            # remove release from table name
            table_name_no_rel = table_name.replace(f"_{PARAMS['RELEASE']}", "")

            if table_type == 'clinical' and PARAMS['NODE'] == 'gdc':
                # remove type from table name, leaving the program name (which is also the prod dataset)
                for clinical_fg in PARAMS['CLINICAL_TABLE_KEY'].keys():
                    prod_dataset = table_name_no_rel.replace(f'{clinical_fg}_', "")

                prod_table_name = table_name_no_rel.replace(f"_{prod_dataset}", "")
                prod_table_name = f"{prod_table_name}_{PARAMS['NODE']}"
            else:
                pass
                # todo create prod table names and datasets for per_sample_file in gdc and pdc, clinical in pdc

            current_table_id = f"{prod_project}.{prod_dataset}.{prod_table_name}_current"
            versioned_table_id = f"{prod_project}.{prod_dataset}_versioned.{prod_table_name}_{PARAMS['RELEASE']}"
            source_table_id = f"{dev_project}.{dev_dataset}.{table_name}"

            if 'compare_tables' in steps:
                # confirm that datasets and table ids exist, and preview whether table will be published
                compare_tables(source_table_id, current_table_id, versioned_table_id)

                # display compare_to_last.sh style output
                find_record_difference_counts(table_type, source_table_id, versioned_table_id, table_metadata)

                get_change_details()

            if 'publish_tables' in steps:
                publish_table(source_table_id, current_table_id, versioned_table_id)

            # get column lists for new table and previous table
            # - any new or removed columns? note in output
            # - any column type changes? note in output; won't be able to compare values
            # - for all remaining, matching columns, compare columns, matching on primary key.

    end_time = time.time()
    logger.info(f"Script completed in: {format_seconds(end_time - start_time)}")
