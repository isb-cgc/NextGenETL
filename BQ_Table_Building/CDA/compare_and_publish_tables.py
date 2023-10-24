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
from cda_bq_etl.utils import input_with_timeout, load_config, format_seconds, get_filepath, create_metadata_table_id

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


def compare_tables(table_ids: dict[str, str]) -> bool:
    logger = logging.getLogger('base_script')

    previous_versioned_table_id = find_most_recent_published_table_id(PARAMS, table_ids['versioned'])

    if previous_versioned_table_id is None:
        logger.warning(f"No previous version found for {table_ids['source']}. Will publish. Investigate if unexpected.")
        return False

    logger.info(f"Comparing tables {table_ids['source']} and {previous_versioned_table_id}.")

    # does source table exist?
    if not exists_bq_table(table_ids['source']):
        logger.critical("Source table id doesn't exist, cannot publish.")
        sys.exit(-1)

    # does current dataset exist?
    current_dataset = ".".join(table_ids['current'].split('.')[:-1])

    if not exists_bq_dataset(current_dataset):
        logger.critical(f"Dataset {current_dataset} doesn't exist, cannot publish.")
        sys.exit(-1)

    # does versioned dataset exist?
    versioned_dataset = ".".join(table_ids['versioned'].split('.')[:-1])

    if not exists_bq_dataset(versioned_dataset):
        logger.critical(f"Dataset {versioned_dataset} doesn't exist, cannot publish.")
        sys.exit(-1)

    logger.info("Previous published and source tables exist, great! Continuing.")

    # display published table_ids
    logger.info("To-be-published table_ids:")
    logger.info(f"current: {table_ids['current']}")
    logger.info(f"versioned: {table_ids['versioned']}")

    has_new_data = table_has_new_data(previous_versioned_table_id, table_ids['source'])

    # is there a previous version to compare with new table?
    # use previous_versioned_table_id
    if has_new_data:
        logger.info(f"New data found--table will be published.")
        return True
    elif not has_new_data:
        logger.info(f"Tables are identical (no new data found)--table will not be published.")
        return False


def find_record_difference_counts(table_type: str,
                                  table_ids: dict[str, str],
                                  table_metadata: dict[str, Union[str, list[str]]]):
    def make_record_count_query(table_id):
        return f"""
            SELECT COUNT(*) AS record_count
            FROM `{table_id}`
        """

    def make_added_record_count_query():
        return f"""
            WITH new_rows AS (
                SELECT * {excluded_column_sql_str}
                FROM `{table_ids['source']}`
                EXCEPT DISTINCT
                SELECT * {excluded_column_sql_str}
                FROM `{previous_versioned_table_id}`
            ), 
            old_rows AS (
                SELECT * {excluded_column_sql_str}
                FROM `{previous_versioned_table_id}`
                EXCEPT DISTINCT
                SELECT * {excluded_column_sql_str}
                FROM `{table_ids['source']}`
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
                SELECT * {excluded_column_sql_str}
                FROM `{table_ids['source']}`
                EXCEPT DISTINCT
                SELECT * {excluded_column_sql_str}
                FROM `{previous_versioned_table_id}`
            ), 
            old_rows AS (
                SELECT * {excluded_column_sql_str}
                FROM `{previous_versioned_table_id}`
                EXCEPT DISTINCT
                SELECT * {excluded_column_sql_str}
                FROM `{table_ids['source']}`
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
                SELECT * {excluded_column_sql_str}
                FROM `{table_ids['source']}`
                EXCEPT DISTINCT
                SELECT * {excluded_column_sql_str}
                FROM `{previous_versioned_table_id}`
            ), 
            old_rows AS (
                SELECT * {excluded_column_sql_str}
                FROM `{previous_versioned_table_id}`
                EXCEPT DISTINCT
                SELECT * {excluded_column_sql_str}
                FROM `{table_ids['source']}`
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
            for _row in result:
                total_results += _row[0]

                # append the count, right justify
                row_str = f"{str(_row[0]):>10}  "

                # append the other values (e.g. project id, type) as specified in output keys
                for i in range(1, num_columns):
                    row_str += f"{str(_row[i]):30}"

                output_string += '\n' + row_str

        return total_results, output_string

    columns_excluded_from_compare = table_metadata['columns_excluded_from_compare']

    # added to sql queries if certain columns are excluded
    excluded_column_sql_str = ''
    if columns_excluded_from_compare:
        excluded_columns = ", ".join(columns_excluded_from_compare)
        excluded_column_sql_str = f"EXCEPT ({excluded_columns})"

    logger = logging.getLogger('base_script')

    previous_versioned_table_id = find_most_recent_published_table_id(PARAMS, table_ids['versioned'])

    if previous_versioned_table_id is None:
        logger.warning(f"No previous table found for {table_ids['versioned']}; therefore, no changes to report.")
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

    new_version_count_result = query_and_retrieve_result(make_record_count_query(table_ids['source']))

    try:
        new_version_count = None

        for row in new_version_count_result:
            new_version_count = row[0]
            break

        if new_version_count is None:
            raise TypeError
    except TypeError:
        logger.critical(f"No value returned for new version row count in {table_ids['source']}.")
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


def generate_column_list(table_id_list: list[str], excluded_columns: set[str]) -> list[str]:
    """
    Create a list of column names found in tables, minus any excluded columns.
    :param table_id_list: list of tables from which to retrieve columns
    :param excluded_columns: set of columns to exclude from the list
    :return: a list representing the union of columns from every table in table_id_list, less any excluded_columns
    """
    def make_column_list_query() -> str:
        project_dataset_name = ".".join(table_id.split('.')[0:2])
        table_name = table_id.split('.')[-1]

        return f"""
            SELECT column_name
            FROM `{project_dataset_name}`.INFORMATION_SCHEMA.COLUMNS
            WHERE table_name = '{table_name}'
            """

    column_union_set = set()

    for table_id in table_id_list:
        # retrieve table's column names and create a set
        column_result = query_and_retrieve_result(make_column_list_query())

        column_set = set()

        for row in column_result:
            column_set.add(row[0])

        column_union_set = column_union_set | column_set

    # remove any concatenated columns supplied in yaml config from column_list
    column_union_set = column_union_set - excluded_columns

    return sorted(list(column_union_set))


def compare_table_columns(table_ids: dict[str, str],
                          table_params: dict,
                          max_display_rows: int = 5):
    """
    Compare column in new table and most recently published table, matching values based on primary key
    (and, optionally, secondary key).
    :param table_ids: todo
    :param table_params: todo
    :param max_display_rows: maximum result rows to display in output; default is 5
    """

    # warning suppressed because PyCharm gets confused by the secondary key clause variables
    # noinspection SqlAmbiguousColumn
    def make_compare_table_column_sql(column_name) -> str:
        """
        Make SQL query that compares individual column values
        :param column_name:
        """
        if secondary_key is None:
            secondary_key_with_str = ''
            secondary_key_select_str = ''
            secondary_key_join_str = ''
        else:
            secondary_key_with_str = f"{secondary_key},"
            secondary_key_select_str = f"""
                n.{secondary_key} AS new_{secondary_key},
                o.{secondary_key} AS old_{secondary_key},
            """
            secondary_key_join_str = f"AND n.{secondary_key} = o.{secondary_key}"

        return f"""
            WITH different_in_new AS (
                SELECT {secondary_key_with_str} {primary_key}, {column_name}
                FROM `{table_ids['source']}`
                EXCEPT DISTINCT 
                SELECT {secondary_key_with_str} {primary_key}, {column_name}
                FROM `{table_ids['versioned']}`
                ORDER BY {primary_key}
            ), different_in_old AS (
                SELECT {secondary_key_with_str} {primary_key}, {column_name}
                FROM `{table_ids['versioned']}`
                EXCEPT DISTINCT 
                SELECT {secondary_key_with_str} {primary_key}, {column_name}
                FROM `{table_ids['source']}`
                ORDER BY {primary_key}
            )

            SELECT n.{primary_key} AS new_{primary_key}, 
                o.{primary_key} AS old_{primary_key},
                {secondary_key_select_str}
                n.{column_name} AS new_{column_name},
                o.{column_name} AS old_{column_name}
            FROM different_in_new n 
            FULL JOIN different_in_old o
                ON n.{primary_key} = o.{primary_key}
                    {secondary_key_join_str}
        """

    logger = logging.getLogger('base_script')

    primary_key = table_params['primary_key']
    secondary_key = table_params['secondary_key'] if 'secondary_key' in table_params else None

    if 'concat_columns' in table_params and table_params['concat_columns']:
        concat_column_set = set(table_params['concat_columns'])
    else:
        concat_column_set = set()

    if 'columns_excluded_from_compare' in table_params and table_params['columns_excluded_from_compare']:
        not_compared_column_set = set(table_params['columns_excluded_from_compare'])
    else:
        not_compared_column_set = set()

    excluded_columns = concat_column_set | not_compared_column_set
    excluded_columns.add(primary_key)

    if secondary_key:
        excluded_columns.add(secondary_key)

    old_version_table_id = find_most_recent_published_table_id(PARAMS, table_ids['versioned'])

    if not old_version_table_id:
        logger.info(f"Previous version of table (future versioned table id: {table_ids['versioned']}) not found.")
        return

    column_list = table_params['column_list'] if 'column_list' in table_params else None

    if column_list is None:
        table_id_list = [table_ids['source'], table_ids['versioned']]
        column_list = generate_column_list(table_id_list=table_id_list, excluded_columns=excluded_columns)

    for column in sorted(column_list):
        column_comparison_result = query_and_retrieve_result(sql=make_compare_table_column_sql(column))

        if not column_comparison_result:
            logger.info(f"{column}: Column doesn't exist in one or both tables, or data types don't match.")
            logger.info("")
        elif column_comparison_result.total_rows == 0:
            pass  # no changes found, skipping output here to minimize verbosity
        else:
            logger.info(f"{column}: {column_comparison_result.total_rows} differences found. Examples:")

            new_column_header = f"new {column}"
            old_column_header = f"old {column}"

            # output header row
            if secondary_key is None:
                logger.info(f"{primary_key:40} {old_column_header:40} {new_column_header}")
            else:
                logger.info(f"{primary_key:40} {secondary_key:40} {old_column_header:40} {new_column_header}")

            count = 0

            for row in column_comparison_result:
                new_primary_key_val = row.get(f"new_{primary_key}")
                old_primary_key_val = row.get(f"old_{primary_key}")

                if new_primary_key_val is not None:
                    primary_key_val = str(new_primary_key_val)
                else:
                    primary_key_val = str(old_primary_key_val)

                new_column_val = str(row.get(f"new_{column}"))
                old_column_val = str(row.get(f"old_{column}"))

                if secondary_key is not None:
                    new_second_key_val = row.get(f"new_{secondary_key}")
                    old_second_key_val = row.get(f"old_{secondary_key}")

                    if new_second_key_val is not None:
                        secondary_key_val = str(new_second_key_val)
                    else:
                        secondary_key_val = str(old_second_key_val)

                    logger.info(f"{primary_key_val:40} {secondary_key_val:40} {old_column_val:40} {new_column_val}")
                else:
                    logger.info(f"{primary_key_val:40} {old_column_val:40} {new_column_val}")

                count += 1

                if count == max_display_rows:
                    logger.info("")
                    break


def compare_concat_columns(table_ids: dict[str, str],
                           table_params: dict[str, str]):
    """
    Compare concatenated column values to ensure matching data, as order is not guaranteed in these column strings.
    :param table_ids: todo
    :param table_params:
    """
    def make_concat_column_query(table_id: str) -> str:
        secondary_key_string = ''

        if table_params['secondary_key'] is not None:
            secondary_key_string = f"{table_params['secondary_key']},"

        concat_columns_str = ", ".join(table_params['concat_columns'])

        return f"""
            SELECT {secondary_key_string} {table_params['primary_key']}, {concat_columns_str}  
            FROM `{table_id}`
        """

    def make_records_dict(query: str) -> dict[str, dict[str, str]]:
        result = query_and_retrieve_result(sql=query)

        records_dict = dict()

        for record_count, record in enumerate(result):
            primary_key_id = record.get(table_params['primary_key'])
            records_dict_key = primary_key_id

            if table_params['secondary_key'] is not None:
                records_dict_key += f";{record.get(table_params['secondary_key'])}"

            record_dict = dict()

            for _column in table_params['concat_columns']:
                record_dict[_column] = record.get(_column)

            records_dict[records_dict_key] = record_dict

            if record_count % 100000 == 0 and record_count > 0:
                print(f"{record_count}/{result.total_rows} records added to dict!")

        return records_dict

    logger = logging.getLogger('base_script')

    new_table_records_dict = make_records_dict(query=make_concat_column_query(table_ids['source']))

    old_version_table_id = find_most_recent_published_table_id(PARAMS, table_ids['versioned'])
    old_table_records_dict = make_records_dict(query=make_concat_column_query(old_version_table_id))

    logger.info("Comparing concatenated columns!")

    record_key_set = set(new_table_records_dict.keys())
    record_key_set.update(old_table_records_dict.keys())

    for column in table_params['concat_columns']:
        correct_records_count = 0
        new_table_missing_record_count = 0
        old_table_missing_record_count = 0
        different_lengths_count = 0
        different_values_count = 0
        mismatched_records = list()

        for record_id in record_key_set:
            if record_id not in new_table_records_dict:
                new_table_missing_record_count += 1
                break
            elif record_id not in old_table_records_dict:
                old_table_missing_record_count += 1
                break

            new_column_value = new_table_records_dict[record_id][column]
            old_column_value = old_table_records_dict[record_id][column]

            if new_column_value is None and old_column_value is None:
                correct_records_count += 1
            else:
                if new_column_value is None:
                    new_column_value_list = list()
                else:
                    new_column_value_list = new_column_value.split(';')

                if old_column_value is None:
                    old_column_value_list = list()
                else:
                    old_column_value_list = old_column_value.split(';')

                new_column_value_set = set(new_column_value_list)
                old_column_value_set = set(old_column_value_list)

                if len(new_column_value_list) == len(old_column_value_list) \
                        and len(new_column_value_set ^ old_column_value_set) == 0:
                    correct_records_count += 1
                else:
                    if len(new_column_value_list) != len(old_column_value_list):
                        # if length mismatch, there may be duplicates, so definitely not identical;
                        # set eliminates duplicates, so this is necessary
                        different_lengths_count += 1
                    elif len(new_column_value_set ^ old_column_value_set) > 0:
                        # exclusive or -- values only in exactly one set
                        different_values_count += 1

                    mismatched_records.append({
                        "record_id": record_id,
                        "new_table_value": new_column_value,
                        "old_table_value": old_column_value
                    })

        if new_table_missing_record_count > 0 or old_table_missing_record_count > 0 \
                or different_lengths_count > 0 or different_values_count > 0:
            logger.info(f"{column}:")
            logger.info(f"Missing records in new table: {new_table_missing_record_count}")
            logger.info(f"Missing records in old table: {old_table_missing_record_count}")
            logger.info(f"Different number of values in row: {different_lengths_count}")
            logger.info(f"Different values in row: {different_values_count}")

            if len(mismatched_records) > 0:
                i = 0

                new_column_header = f"new {column}"
                old_column_header = f"old {column}"

                if table_params['secondary_key'] is None:
                    logger.info(f"{table_params['primary_key']:40} {old_column_header: 40} {new_column_header}")
                else:
                    logger.info(f"{table_params['primary_key']:40} {table_params['secondary_key']:40}"
                                f" {old_column_header: 40} {new_column_header}")

                for mismatched_record in mismatched_records:
                    if ';' in mismatched_record['record_id']:
                        id_list = mismatched_record['record_id'].split(";")
                        primary_key_val = id_list[0]
                        secondary_key_val = id_list[1]
                    else:
                        primary_key_val = mismatched_record['record_id']
                        secondary_key_val = None

                    if table_params['secondary_key'] is None:
                        logger.info(f"{primary_key_val:40} {mismatched_record['old_table_value']:40} "
                                    f"{mismatched_record['new_table_value']}")
                    else:
                        logger.info(f"{primary_key_val:40} {secondary_key_val:40} "
                                    f"{mismatched_record['old_table_value']:40} {mismatched_record['new_table_value']}")

                    i += 1

                    if i == 5:
                        break


def get_new_table_names(dataset: str) -> list[str]:
    def make_new_table_names_query():
        return f"""
            SELECT table_name 
            FROM `{PARAMS['DEV_PROJECT']}.{dataset}`.INFORMATION_SCHEMA.TABLES
            WHERE table_name LIKE '%{PARAMS['RELEASE']}%'
        """

    table_names = query_and_retrieve_result(make_new_table_names_query())

    return sorted(list(table_names))


def get_current_table_names(table_type) -> list[str]:
    def make_program_tables_query() -> str:
        return f"""
            SELECT table_name 
            FROM `{PARAMS['PROD_PROJECT']}.{program_name}`.INFORMATION_SCHEMA.TABLES
            WHERE table_name LIKE '%{table_type}%'
        """
    # get program list from BQEcosystem/MetadataMappings/
    # for each program, look for tables in current list with 'clinical' or 'per_sample_file' prefix
    # add any tables to list object
    logger = logging.getLogger('base_script')

    program_metadata_fp = f"{PARAMS['BQ_REPO']}/{PARAMS['PROGRAM_METADATA_DIR']}"
    program_metadata_fp = get_filepath(program_metadata_fp, PARAMS['PROGRAM_METADATA_FILE'])

    if not os.path.exists(program_metadata_fp):
        logger.critical("BQEcosystem program metadata path not found")
        sys.exit(-1)
    with open(program_metadata_fp) as field_output:
        program_metadata = json.load(field_output)
        program_names = sorted(list(program_metadata.keys()))

        current_table_names = list()

        suffix = f"_{PARAMS['NODE']}_current"

        for program_name in program_names:
            table_name_result = query_and_retrieve_result(make_program_tables_query())

            for row in table_name_result:
                table_name = row['table_name']
                print(table_name)
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


def publish_table(table_ids: dict[str, str]):
    """
    Publish production BigQuery tables using source_table_id:
        - create current/versioned table ids
        - publish tables
        - update friendly name for versioned table
        - change last version tables' status labels to archived
    :param table_ids: dict containing source, current and versioned table ids
    """
    logger = logging.getLogger('base_script')

    previous_versioned_table_id = find_most_recent_published_table_id(PARAMS, table_ids['versioned'])
    logger.info(f"previous_versioned_table_id: {previous_versioned_table_id}")

    if PARAMS['TEST_PUBLISH']:
        logger.error("Cannot run publish table step with TEST_PUBLISH set to true.")
        sys.exit(-1)

    if exists_bq_table(table_ids['source']):
        if table_has_new_data(previous_versioned_table_id, table_ids['source']):
            delay = 5

            logger.info(f"""\n\nPublishing the following tables:""")
            logger.info(f"\t - {table_ids['versioned']}\n\t - {table_ids['current']}")
            logger.info(f"Proceed? Y/n (continues automatically in {delay} seconds)")

            response = str(input_with_timeout(seconds=delay)).lower()

            if response == 'n':
                exit("\nPublish aborted; exiting.")

            logger.info(f"\nPublishing {table_ids['versioned']}")
            copy_bq_table(params=PARAMS,
                          src_table=table_ids['source'],
                          dest_table=table_ids['versioned'],
                          replace_table=PARAMS['OVERWRITE_PROD_TABLE'])

            logger.info(f"Publishing {table_ids['current']}")
            copy_bq_table(params=PARAMS,
                          src_table=table_ids['source'],
                          dest_table=table_ids['current'],
                          replace_table=PARAMS['OVERWRITE_PROD_TABLE'])

            logger.info(f"Updating friendly name for {table_ids['versioned']}")
            update_friendly_name(PARAMS, table_id=table_ids['versioned'])

            if previous_versioned_table_id:
                logger.info(f"Archiving {previous_versioned_table_id}")
                change_status_to_archived(previous_versioned_table_id)

        else:
            logger.info(f"{table_ids['source']} not published, no changes detected")


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

    prod_project = PARAMS['PROD_PROJECT']
    dev_project = PARAMS['DEV_PROJECT']

    # COMPARE AND PUBLISH METADATA TABLES
    """
    for table_type, table_params in PARAMS['METADATA_TABLE_TYPES'].items():
        prod_dataset = table_params['prod_dataset']
        prod_table_name = table_params['table_base_name']

        table_ids = {
            'current': f"{prod_project}.{prod_dataset}.{prod_table_name}_current",
            'versioned': f"{prod_project}.{prod_dataset}_versioned.{prod_table_name}_{PARAMS['RELEASE']}",
            'source': create_metadata_table_id(PARAMS, table_params['table_base_name'])
        }

        if 'compare_tables' in steps:
            logger.info(f"Comparing tables for {table_params['table_base_name']}!")
            # confirm that datasets and table ids exist, and preview whether table will be published
            data_to_compare = compare_tables(table_ids)

            if data_to_compare:
                # display compare_to_last.sh style output
                find_record_difference_counts(table_type, table_ids, table_params)
                compare_table_columns(table_ids=table_ids, table_params=table_params)

        if 'publish_tables' in steps:
            logger.info(f"Publishing tables for {table_params['table_base_name']}!")
            publish_table(table_ids)
    """
    # COMPARE AND PUBLISH CLINICAL AND PER SAMPLE FILE TABLES
    for table_type, table_params in PARAMS['PER_PROJECT_TABLE_TYPES'].items():
        # look for list of last release's published tables to ensure none have disappeared before comparing
        find_missing_tables(dataset=table_params['dev_dataset'], table_type=table_type)

        # for clinical:
        # get list of tables from clinical dataset for current release
        new_table_names = get_new_table_names(dataset=table_params['dev_dataset'])
        # find matching previous version table. If none, no comparison
        for table_name in new_table_names:
            # remove release from table name
            table_name_no_rel = table_name.replace(f"{PARAMS['RELEASE']}_", "")

            if table_type == 'clinical' and PARAMS['NODE'] == 'gdc':
                table_name_list = table_name_no_rel.split('_')
                table_type_start_idx = table_name_list.index('clinical')
                program = "_".join(table_name_list[0:table_type_start_idx])
                table_base_name = "_".join(table_name_list[table_type_start_idx:])
                prod_table_name = f"{table_base_name}_{PARAMS['NODE']}"

                table_ids = {
                    'current': f"{prod_project}.{program}.{prod_table_name}_current",
                    'versioned': f"{prod_project}.{program}_versioned.{prod_table_name}_{PARAMS['RELEASE']}",
                    'source': f"{dev_project}.{table_params['dev_dataset']}.{table_name}"
                }

                if 'compare_tables' in steps:
                    logger.info(f"Comparing tables for {table_base_name}!")
                    # confirm that datasets and table ids exist, and preview whether table will be published
                    data_to_compare = compare_tables(table_ids)

                    if data_to_compare:

                        # display compare_to_last.sh style output
                        find_record_difference_counts(table_type, table_ids, table_params)

                        # get key based on field group, e.g. the clinical_diagnosis table uses
                        # diagnosis_id as primary key
                        if table_name_list[-1] == 'clinical':
                            primary_key = 'case_id'
                        else:
                            primary_key = f"{table_name_list[-1]}_id"

                        modified_table_params = {
                            'primary_key': primary_key,
                            'concat_columns': table_params['concat_columns'],
                            'columns_excluded_from_compare': table_params['columns_excluded_from_compare'],
                        }

                        compare_table_columns(table_ids=table_ids, table_params=modified_table_params)

                if 'publish_tables' in steps:
                    logger.info(f"Publishing tables for {table_base_name}!")
                    publish_table(table_ids)
            else:
                pass
                # todo create prod table names and datasets for per_sample_file in gdc and pdc, clinical in pdc

            # get column lists for new table and previous table
            # - any new or removed columns? note in output
            # - any column type changes? note in output; won't be able to compare values
            # - for all remaining, matching columns, compare columns, matching on primary key.

    end_time = time.time()
    logger.info(f"Script completed in: {format_seconds(end_time - start_time)}")


if __name__ == "__main__":
    main(sys.argv)