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

from cda_bq_etl.bq_helpers import (find_most_recent_published_table_id, exists_bq_table, copy_bq_table,
                                   update_friendly_name, change_status_to_archived, query_and_retrieve_result)
from cda_bq_etl.data_helpers import initialize_logging
from cda_bq_etl.utils import input_with_timeout, load_config, format_seconds, get_filepath, create_metadata_table_id

PARAMS = dict()
YAML_HEADERS = ('params', 'steps')
TableParams = dict[str, Union[str, list[str], dict[str, str]]]
TableIDList = list[dict[str, str]]


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
    query_logger = logging.getLogger('query_logger')

    if not previous_table_id:
        return True

    compare_two_tables_query = compare_two_tables_sql()
    query_logger.info(f"Query to find any difference in table data")
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


def list_added_or_removed_rows(select_table_id: str, join_table_id: str, table_params: TableParams):
    def make_added_or_removed_record_query():
        primary_key = table_params['primary_key']
        if 'secondary_key' in table_params and table_params['secondary_key']:
            secondary_key = table_params['secondary_key']
        else:
            secondary_key = None

        select_str = primary_key
        secondary_where_str = ""

        if secondary_key:
            select_str += f", {secondary_key}"
            secondary_where_str += f"AND o.{secondary_key}=n.{secondary_key}"
        if table_params['output_keys']:
            output_keys = ', '.join(table_params['output_keys'])
            select_str += f", {output_keys}"

        return f"""
            SELECT {select_str}
            FROM `{select_table_id}` n
            WHERE NOT EXISTS (
              SELECT 1 
              FROM `{join_table_id}` o 
              WHERE o.{primary_key} = n.{primary_key} 
                {secondary_where_str}
            ) 
        """
    query_logger = logging.getLogger("query_logger")
    logger = logging.getLogger("base_script")

    added_removed_record_query = make_added_or_removed_record_query()
    query_logger.info(added_removed_record_query)
    row_result = query_and_retrieve_result(added_removed_record_query)

    if row_result.total_rows == 0:
        logger.info("None found")
        logger.info("")
        return

    output_str = f"\n{table_params['primary_key']:45}"

    if 'secondary_key' in table_params and table_params['secondary_key'] is not None:
        output_str += f"{table_params['secondary_key']:45}"

    if table_params['output_keys']:
        for output_key in table_params['output_keys']:
            output_str += f"{output_key:45}"

    output_str += "\n\n"

    i = 0

    for row in row_result:
        row_str = f"{row[table_params['primary_key']]:45}"

        if 'secondary_key' in table_params and table_params['secondary_key']:
            if row[table_params['secondary_key']]:
                row_str += f"{row[table_params['secondary_key']]:45}"
            else:
                row_str += f"{'':45}"

        if table_params['output_keys']:
            for output_key in table_params['output_keys']:
                if row[output_key]:
                    row_str += f"{row[output_key]:45}"
                else:
                    row_str += f"{'':45}"

        output_str += f"{row_str}\n"

        i += 1

        if i == PARAMS['MAX_DISPLAY_ROWS']:
            break

    logger.info(f"{output_str}\n")


def get_primary_key(table_type: str, table_ids: dict[str, str], table_params: TableParams):
    logger = logging.getLogger('base_script')

    if table_type == 'clinical' and PARAMS['NODE'] == 'gdc':
        current_table_name = table_ids['current'].split('.')[-1]
        current_table_name = current_table_name.replace("_current", "")
        base_table_name = current_table_name.replace(f"_{PARAMS['NODE']}", "")

        return table_params['primary_key_dict'][base_table_name]
    else:
        logger.critical("Not defined for this node or type")
        sys.exit(-1)


def find_duplicate_keys(table_type: str, table_ids: dict[str, str], table_params: TableParams):
    logger = logging.getLogger('base_script')
    query_logger = logging.getLogger('query_logger')

    if 'keys_for_duplicate_detection' in table_params:
        select_key_str = ", ".join(table_params['keys_for_duplicate_detection'])
        select_str = f"SELECT DISTINCT {select_key_str}"
    else:
        # this fetches primary keys for clinical tables
        select_key_str = get_primary_key(table_type, table_ids, table_params)
        select_str = f"SELECT DISTINCT {select_key_str}"

    distinct_sql_query = f"""
        {select_str}
        FROM `{table_ids['source']}`
    """

    all_count_query = f"""
        SELECT {select_key_str}
        FROM {table_ids['source']}
    """

    query_logger.info(distinct_sql_query)
    distinct_result = query_and_retrieve_result(distinct_sql_query)

    query_logger.info(all_count_query)
    all_result = query_and_retrieve_result(all_count_query)

    if distinct_result.total_rows == all_result.total_rows:
        logger.info("No duplicate records detected!")
        logger.info("")
        return

    duplicate_record_query = f"""
        SELECT {select_key_str}  
        FROM {table_ids['source']}
        GROUP BY {select_key_str}
        HAVING COUNT(*) > 1
    """

    query_logger.info(duplicate_record_query)
    duplicate_record_result = query_and_retrieve_result(duplicate_record_query)

    logger.warning(f"{duplicate_record_result.total_rows} records with duplicated keys detected. Examples:")

    key_list = select_key_str.split(", ")

    i = 0

    # create output header
    output_str = f"\n"

    for key in key_list:
        output_str += f"{key:45}"

    output_str += f"\n\n"

    for row in duplicate_record_result:
        for key in key_list:
            value = str(row[key])
            output_str += f"{value:45}"

        output_str += f"\n"
        i += 1

        if i == PARAMS['MAX_DISPLAY_ROWS']:
            break

    logger.warning(output_str)


def find_record_difference_counts(table_type: str,
                                  table_ids: dict[str, str],
                                  table_metadata: TableParams):
    def make_record_count_query(table_id):
        return f"""
            SELECT COUNT(*) AS record_count
            FROM `{table_id}`
        """

    def make_subquery(table_id_1, table_id_2):
        if not table_metadata['compare_using_primary_only']:
            select_str = f"SELECT * {excluded_column_sql_str} "
        else:
            # this is used for comparison in clinical
            select_str = f"SELECT {primary_key} "

        return f"""
            {select_str}
            FROM `{table_id_1}`
            EXCEPT DISTINCT
            {select_str}
            FROM `{table_id_2}`
        """

    def make_with_clauses(table_id_1, table_id_2):
        return f"""
            WITH new_rows AS (
                {make_subquery(table_id_1, table_id_2)}
            ), old_rows AS (
                {make_subquery(table_id_2, table_id_1)}
            )
        """

    def make_select_clause():
        if output_key_string:
            return f"SELECT COUNT({primary_key}) AS changed_count, {output_key_string}"
        else:
            return f"SELECT COUNT({primary_key}) AS changed_count"

    def make_group_by_clause():
        if output_key_string:
            return f"GROUP BY {output_key_string} " \
                   f"ORDER BY {output_key_string} "
        else:
            return ""

    def make_added_record_count_query():
        return f"""
            {make_with_clauses(table_ids['source'], table_ids['previous_versioned'])}
            {make_select_clause()}
            FROM new_rows
            WHERE {primary_key} NOT IN (
                SELECT {primary_key}
                FROM old_rows
            )
            {make_group_by_clause()}
        """

    def make_removed_record_count_query():
        return f"""
            {make_with_clauses(table_ids['source'], table_ids['previous_versioned'])}
            {make_select_clause()}
            FROM old_rows
            WHERE {primary_key} NOT IN (
                SELECT {primary_key}
                FROM new_rows
            )
            {make_group_by_clause()}
        """

    def make_changed_record_count_query():
        return f"""
            {make_with_clauses(table_ids['source'], table_ids['previous_versioned'])}, 
            intersects AS (
                SELECT {primary_key}, {secondary_key} {output_key_string} 
                FROM old_rows
                INTERSECT DISTINCT
                SELECT {primary_key}, {secondary_key} {output_key_string} 
                FROM new_rows
            )

            {make_select_clause()}
            FROM intersects
            {make_group_by_clause()}
        """

    def get_count_result(previous_or_new: str):
        # version should be previous or current
        if previous_or_new == "previous":
            count_table_id = table_ids['previous_versioned']
        elif previous_or_new == "new":
            count_table_id = table_ids['source']
        else:
            logger.critical(f"invalid argument: {previous_or_new}. Should be 'previous' or 'new'.")
            sys.exit(-1)

        record_count_query = make_record_count_query(count_table_id)
        # get record count from previous versioned table
        query_logger.info(f"{previous_or_new.capitalize()} version record count query: \n{record_count_query}")
        version_count_result = query_and_retrieve_result(record_count_query)

        try:
            version_count_result_str = None

            for count_row in version_count_result:
                version_count_result_str = count_row[0]
                break

            if version_count_result_str is None:
                raise TypeError
            else:
                return version_count_result_str
        except TypeError:
            logger.critical(f"No value returned for {previous_or_new} version row count in {count_table_id}.")
            logger.critical("Probably an error in the table id or SQL query.")
            sys.exit(-1)

    def compare_records(query: str) -> tuple[int, str]:
        # find added/removed/changed records by project
        query_logger.info(query)
        result = query_and_retrieve_result(query)

        if result.total_rows > 0:
            output_string = f"\n{'count':10}"

            for header in table_metadata['output_keys']:
                output_string += f"{header:30}"

            output_string += "\n"

            total_results = 0
            num_columns = len(table_metadata['output_keys']) + 1

            for _row in result:
                total_results += _row[0]

                if result.total_rows > 1:
                    # append the count, right justify
                    row_str = f"{str(_row[0]):>8}  "

                    # append the other values (e.g. project id, type) as specified in output keys
                    for i in range(1, num_columns):
                        row_str += f"{str(_row[i]):30}"

                    output_string += '\n' + row_str

            output_string += "\n"

            return total_results, output_string
        else:
            return 0, ""

    logger = logging.getLogger('base_script')
    query_logger = logging.getLogger("query_logger")

    if table_ids['previous_versioned'] is None:
        logger.warning(f"No previous table found for {table_ids['versioned']}; therefore, no changes to report.")
        return

    excluded_column_sql_str = ''

    # added to sql query if columns are excluded in yaml config
    if table_metadata['columns_excluded_from_compare']:
        excluded_columns = ", ".join(table_metadata['columns_excluded_from_compare'])
        excluded_column_sql_str = f"EXCEPT ({excluded_columns})"

    output_key_string = ''

    # the keys used to filter added/removed/changed record results into types for display in output
    # e.g. project_short_name and data_format for file metadata
    if table_metadata['output_keys']:
        output_key_string = ", ".join(table_metadata['output_keys'])

    primary_key = table_metadata['primary_key']
    secondary_key = ''

    # include secondary key where applicable--in GDC, secondary key is used for aliquot2case map and per sample file
    if 'secondary_key' in table_metadata and table_metadata['secondary_key'] is not None:
        secondary_key = table_metadata['secondary_key'] + ', '

    previous_version_count = get_count_result(previous_or_new="previous")
    new_version_count = get_count_result(previous_or_new="new")
    count_difference = int(new_version_count) - int(previous_version_count)

    if table_type not in ("clinical", "per_sample_file"):
        logger.info(f"***** {table_type.upper()} *****")

    logger.info(f"Current {table_type} count: {new_version_count}")
    logger.info(f"Previous {table_type} count: {previous_version_count}")
    logger.info(f"Row count change since previous version: {count_difference}")
    logger.info("")

    # find added records by project
    query_logger.info("Added record query")
    added_count, added_str = compare_records(query=make_added_record_count_query())
    # find removed records by project
    query_logger.info("Removed record query")
    removed_count, removed_str = compare_records(query=make_removed_record_count_query())

    logger.info(f"Added {table_type} count: {added_count}")

    if table_metadata['data_type'] == 'per_project_or_program':
        logger.info(f"Removed {table_type} count: {removed_count}")
    else:
        # output counts by project or other type, where applicable
        # print added row examples
        if added_str and added_str.strip() != added_count:
            logger.info(added_str)
        else:
            logger.info("")

        logger.info(f"Removed {table_type} count: {removed_count}")
        # output counts by project or other type, where applicable
        # print removed row examples
        if removed_str and removed_str.strip() != removed_count:
            logger.info(removed_str)
        else:
            logger.info("")

        # find changed records by project
        query_logger.info("Changed record query")
        changed_count, changed_str = compare_records(query=make_changed_record_count_query())

        logger.info(f"Changed {table_type} count: {changed_count}")
        # outputs counts by project or other type, where applicable
        if changed_str and changed_str.strip() != changed_count:
            logger.info(changed_str)
        else:
            logger.info("")

    logger.info("")

    return added_count, removed_count


def get_new_table_names(dataset: str) -> list[str]:
    def make_new_table_names_query():
        return f"""
            SELECT table_name 
            FROM `{PARAMS['DEV_PROJECT']}.{dataset}`.INFORMATION_SCHEMA.TABLES
            WHERE table_name LIKE '%{PARAMS['RELEASE']}%'
        """

    table_names = query_and_retrieve_result(make_new_table_names_query())

    table_name_list = list()

    for row in table_names:
        table_name_list.append(row['table_name'])

    return sorted(table_name_list)


def find_missing_tables(dataset: str, table_type: str):
    """
    Compare published tables to new dev tables. If new table is missing, output a warning.
    :param dataset: development dataset to search for new tables
    :param table_type: table data type, e.g. clinical, per_sample_file
    """

    def get_published_table_names() -> list[str]:
        def make_program_tables_query() -> str:
            return f"""
                SELECT table_name 
                FROM `{PARAMS['PROD_PROJECT']}.{program_name}`.INFORMATION_SCHEMA.TABLES
                WHERE table_name LIKE '%{table_type}%'
                    AND table_name LIKE '%{PARAMS['NODE']}%'
            """

        # get program list from BQEcosystem/MetadataMappings/
        # for each program, look for tables in current list with 'clinical' or 'per_sample_file' prefix
        # add any tables to list object
        program_metadata_fp = f"{PARAMS['BQ_REPO']}/{PARAMS['PROGRAM_METADATA_DIR']}"
        program_metadata_fp = get_filepath(program_metadata_fp, PARAMS['PROGRAM_METADATA_FILE'])

        if not os.path.exists(program_metadata_fp):
            logger.critical("BQEcosystem program metadata path not found")
            sys.exit(-1)
        with open(program_metadata_fp) as field_output:
            program_metadata = json.load(field_output)
            program_names = sorted(list(program_metadata.keys()))

            _published_table_names = list()

            suffix = f"_{PARAMS['NODE']}_current"

            for program_name_original in program_names:
                if program_name_original == "BEATAML1.0":
                    program_name = "BEATAML1_0"
                elif program_name_original == "EXCEPTIONAL_RESPONDERS":
                    program_name = "EXC_RESPONDERS"
                else:
                    program_name = program_name_original

                table_name_result = query_and_retrieve_result(make_program_tables_query())

                for row in table_name_result:
                    table_name = row['table_name']
                    table_name = table_name.replace(suffix, "")
                    program_table_name = f"{program_name}_{table_name}"
                    _published_table_names.append(program_table_name)

            return sorted(_published_table_names)

    logger = logging.getLogger('base_script')
    logger.info("Searching for missing tables!")

    new_table_names_no_rel = list()

    published_table_names = get_published_table_names()
    new_table_names = get_new_table_names(dataset)

    if PARAMS['NODE'] and table_type == 'per_sample_file':
        if 'no_url' in new_table_names[0]:
            logger.info("Final tables not yet created for per sample file metadata. "
                        "Please run compare and publish step for this table type after they're created.")
            return False

    for new_table_name in new_table_names:
        new_table_name = new_table_name.replace(f"{PARAMS['RELEASE']}_", "")
        new_table_name = new_table_name.replace(f"_{PARAMS['NODE']}", "")
        new_table_names_no_rel.append(new_table_name)

    for current_table_name in published_table_names:
        if 'hg19' in current_table_name:
            continue

        if current_table_name not in new_table_names_no_rel:
            logger.warning(f"Cannot find new dev table for published table {current_table_name}. "
                           f"Is this due to change from singular to plural?")

    return True


def generate_metadata_table_id_list(table_params: TableParams) -> TableIDList:
    prod_table_name = table_params['table_base_name']

    prod_project_dataset_id = f"{PARAMS['PROD_PROJECT']}.{PARAMS['PROD_METADATA_DATASET']}"

    current_table_id = f"{prod_project_dataset_id}.{prod_table_name}_current"
    versioned_table_id = f"{prod_project_dataset_id}_versioned.{prod_table_name}_{PARAMS['RELEASE']}"
    source_table_id = create_metadata_table_id(PARAMS, table_params['table_base_name'])
    previous_versioned_table_id = find_most_recent_published_table_id(PARAMS, versioned_table_id)

    table_ids = {
        'current': current_table_id,
        'versioned': versioned_table_id,
        'source': source_table_id,
        'previous_versioned': previous_versioned_table_id
    }

    return [table_ids]


def generate_table_id_list(table_type: str, table_params: TableParams) -> TableIDList:
    def parse_gdc_clinical_table_id() -> tuple[str, str]:
        split_table_name_list = table_name.split('_')
        split_table_name_list.remove(PARAMS['RELEASE'])

        # index to split table name from program
        clinical_idx = split_table_name_list.index('clinical')

        dataset_name = "_".join(split_table_name_list[0:clinical_idx])
        base_table_name = "_".join(split_table_name_list[clinical_idx:])
        prod_table_name = f"{base_table_name}_{PARAMS['NODE']}"

        return dataset_name, prod_table_name

    def parse_gdc_per_sample_file_table_id() -> tuple[str, str]:
        base_table_name = PARAMS['TABLE_TYPES']['per_sample_file']['table_base_name']

        table_name_no_rel = table_name.replace(f"{PARAMS['RELEASE']}_", "")
        table_name_no_rel = table_name_no_rel.replace(f"_{PARAMS['NODE']}", "")
        dataset_name = table_name_no_rel.replace(f"_{base_table_name}", "")

        prod_table_name = f"{base_table_name}_{PARAMS['NODE']}"

        return dataset_name, prod_table_name

    logger = logging.getLogger('base_script')
    logger.info("Generating table id list")
    new_table_names = get_new_table_names(dataset=table_params['dev_dataset'])

    table_ids_list = list()

    for table_name in new_table_names:
        if PARAMS['NODE'] == 'gdc':
            if table_type == 'clinical':
                dataset, prod_table = parse_gdc_clinical_table_id()
            elif table_type == 'per_sample_file':
                dataset, prod_table = parse_gdc_per_sample_file_table_id()
            else:
                logger.critical("Not configured for this GDC type")
                sys.exit(-1)
        elif PARAMS['NODE'] == 'pdc':
            logger.critical("Not configured for this PDC type")
            sys.exit(-1)
        else:
            logger.critical("Not configured for this node")
            sys.exit(-1)

        current_table_id = f"{PARAMS['PROD_PROJECT']}.{dataset}.{prod_table}_current"
        versioned_table_id = f"{PARAMS['PROD_PROJECT']}.{dataset}_versioned.{prod_table}_{PARAMS['RELEASE']}"
        source_table_id = f"{PARAMS['DEV_PROJECT']}.{table_params['dev_dataset']}.{table_name}"
        previous_versioned_table_id = find_most_recent_published_table_id(PARAMS, versioned_table_id)

        table_ids = {
            'current': current_table_id,
            'versioned': versioned_table_id,
            'source': source_table_id,
            'previous_versioned': previous_versioned_table_id
        }

        table_ids_list.append(table_ids)

    return table_ids_list


def compare_tables(table_type: str, table_params: TableParams, table_id_list: TableIDList):
    """
    Compare published and newly created dev tables.
    :param table_type: type of table to compare
    :param table_params: metadata dict containing table parameters, such as primary and secondary keys,
                         concatenated columns, columns excluded from comparison
    :param table_id_list: list of dicts of table ids: 'source' (dev table), 'versioned' and 'current'
                          (future published ids) and 'previous_versioned' (most recent published table)
    :return:
    """
    def can_compare_tables() -> bool:
        if not table_ids['previous_versioned']:
            logger.warning(
                f"No previous version found for {table_ids['source']}. Will publish. Investigate if unexpected.")
            logger.warning(f"{table_ids['current']}")
            logger.warning("")
            return False

        # table has changed since last version
        if table_has_new_data(table_ids['previous_versioned'], table_ids['source']):
            logger.info(f"New data found--table will be published.")
            logger.info("")
            return True
        # table has not changed since last version
        else:
            logger.info(f"No changes found--table will not be published.")
            logger.info("")
            return False

    logger = logging.getLogger("base_script")

    for table_ids in table_id_list:
        # table_base_name only defined for metadata tables, so otherwise we'll output the source table
        if table_params['data_type'] == 'metadata':
            logger.info(f"*** Comparing tables for {table_params['table_base_name']}!")
        else:
            logger.info(f"*** Comparing tables for {table_ids['source']}!")

        modified_table_params = dict()

        # confirm that datasets and table ids exist, and preview whether table will be published
        if can_compare_tables():
            if table_type == 'clinical':
                # if clinical table, primary key is not defined by table type--
                # could be a supplementary table, e.g. diagnosis
                for key, value in table_params.items():
                    modified_table_params[key] = value

                if 'primary_key' not in modified_table_params:
                    # primary key is defined in a dict in the yaml config for clinical table type;
                    # this will look up and return the primary key by parsing the 'current' table id.
                    modified_table_params['primary_key'] = get_primary_key(table_type, table_ids, modified_table_params)
            else:
                modified_table_params = table_params

            find_duplicate_keys(table_type=table_type,
                                table_ids=table_ids,
                                table_params=modified_table_params)

            # display compare_to_last.sh style output
            added_count, removed_count = find_record_difference_counts(table_type, table_ids, modified_table_params)

            if added_count > 0:
                # list added rows
                logger.info("Added record examples:")
                list_added_or_removed_rows(table_ids['source'], table_ids['previous_versioned'], modified_table_params)
            if removed_count > 0:
                # list removed rows
                logger.info("Removed record examples:")
                list_added_or_removed_rows(table_ids['previous_versioned'], table_ids['source'], modified_table_params)

            logger.info("Comparing records by column!")
            logger.info("")
            compare_table_columns(table_ids=table_ids,
                                  table_params=modified_table_params,
                                  max_display_rows=PARAMS['MAX_DISPLAY_ROWS'])

            if 'concat_columns' in table_params and table_params['concat_columns']:
                concat_column_str = ", ".join(table_params['concat_columns'])
                logger.info(f"Comparing concatenated columns: {concat_column_str}")
                logger.info("")
                compare_concat_columns(table_ids=table_ids,
                                       table_params=modified_table_params,
                                       max_display_rows=PARAMS['MAX_DISPLAY_ROWS'])


def compare_table_columns(table_ids: dict[str, str], table_params: TableParams, max_display_rows: int = 5):
    """
    Compare column in new table and most recently published table, matching values based on primary key (and,
        optionally, secondary key).
    :param table_ids: dict of table ids: 'source' (dev table), 'versioned' and 'current' (future published ids),
                      and 'previous_versioned' (most recent published table)
    :param table_params: metadata dict containing table parameters, such as primary and secondary keys,
                         concatenated columns, columns excluded from comparison
    :param max_display_rows: maximum number of records to display in log output; defaults to 5
    """

    # warning suppressed because PyCharm gets confused by the secondary key clause variables
    # noinspection SqlAmbiguousColumn
    def make_compare_table_column_sql(column_name) -> str:
        """
        Make SQL query that compares individual column values.
        """
        if secondary_key is None:
            secondary_key_with_str = ''
            secondary_key_select_str = ''
            secondary_key_join_str = ''
            secondary_key_where_str = ''
        else:
            secondary_key_with_str = f"{secondary_key},"
            secondary_key_select_str = f"""
                n.{secondary_key} AS new_{secondary_key},
                o.{secondary_key} AS old_{secondary_key},
            """
            secondary_key_join_str = f"AND n.{secondary_key} = o.{secondary_key}"

            secondary_key_where_str = f"OR (n.{secondary_key} IS NOT NULL AND o.{secondary_key} IS NOT NULL)"

        return f"""
            WITH different_in_new AS (
                SELECT {secondary_key_with_str} {primary_key}, {column_name}
                FROM `{table_ids['source']}`
                EXCEPT DISTINCT 
                SELECT {secondary_key_with_str} {primary_key}, {column_name}
                FROM `{table_ids['previous_versioned']}`
                ORDER BY {primary_key}
            ), different_in_old AS (
                SELECT {secondary_key_with_str} {primary_key}, {column_name}
                FROM `{table_ids['previous_versioned']}`
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
            WHERE (n.{primary_key} IS NOT NULL
                    AND o.{primary_key} IS NOT NULL)
                {secondary_key_where_str}     
        """

    def generate_column_list() -> list[str]:
        """
        Create a list of column names found in tables, minus any excluded columns.
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

            for _row in column_result:
                column_set.add(_row[0])

            column_union_set |= column_set

        # remove any concatenated columns supplied in yaml config from column_list
        column_union_set = column_union_set - excluded_columns

        return sorted(list(column_union_set))

    logger = logging.getLogger('base_script')
    query_logger = logging.getLogger('query_logger')

    primary_key = table_params['primary_key']
    secondary_key = table_params['secondary_key'] if 'secondary_key' in table_params else None

    # retrieve the list of concatenated columns from yaml config and convert to set
    if 'concat_columns' in table_params and table_params['concat_columns']:
        concat_column_set = set(table_params['concat_columns'])
    else:
        concat_column_set = set()

    # retrieve the list of columns excluded from comparison
    if 'columns_excluded_from_compare' in table_params and table_params['columns_excluded_from_compare']:
        not_compared_column_set = set(table_params['columns_excluded_from_compare'])
    else:
        not_compared_column_set = set()

    excluded_columns = concat_column_set | not_compared_column_set
    excluded_columns.add(primary_key)

    if secondary_key:
        excluded_columns.add(secondary_key)

    if not table_ids['previous_versioned']:
        logger.info(f"Previous version of table (future versioned table id: {table_ids['versioned']}) not found.")
        return

    column_list = table_params['column_list'] if 'column_list' in table_params else None

    if column_list is None:
        table_id_list = [table_ids['source'], table_ids['previous_versioned']]
        column_list = generate_column_list()

    for column in sorted(column_list):
        compare_table_column_query = make_compare_table_column_sql(column)

        if table_params['data_type'] == 'metadata':
            table_name = table_params['table_base_name']
        else:
            table_name = table_ids['source']

        query_logger.info(f"SQL to compare values for column: {column}, table: {table_name}\n"
                          f"{compare_table_column_query}")
        column_comparison_result = query_and_retrieve_result(sql=compare_table_column_query)

        if not column_comparison_result:
            logger.info(f"{column}: Column doesn't exist in one or both tables, or data types don't match.")
            logger.info(f"Common reasons: non-trivial field data added to program; field deprecated by GDC.")
            logger.info("")
        elif column_comparison_result.total_rows > 0:
            logger.info(f"{column}: {column_comparison_result.total_rows} differences found. Examples:")

            output_str = ""

            # output header row
            if secondary_key is None:
                output_str += f"\n{primary_key:45}{column}\n\n"
            else:
                output_str += f"\n{primary_key:45}{secondary_key:45}{column}\n\n"

            i = 0

            for row in column_comparison_result:
                new_primary_key_val = row.get(f"new_{primary_key}")
                old_primary_key_val = row.get(f"old_{primary_key}")

                # include both key values if they differ--should only occur if row is added or removed
                if not new_primary_key_val or not old_primary_key_val or new_primary_key_val != old_primary_key_val:
                    primary_key_val = f"{str(old_primary_key_val)} -> {str(new_primary_key_val)}"
                else:
                    primary_key_val = str(old_primary_key_val)

                new_column_val = str(row.get(f"new_{column}"))
                old_column_val = str(row.get(f"old_{column}"))

                column_val = f"{old_column_val} -> {new_column_val}"

                if secondary_key is not None:
                    new_second_key_val = row.get(f"new_{secondary_key}")
                    old_second_key_val = row.get(f"old_{secondary_key}")

                    # include both key values if they differ
                    if not new_second_key_val or not old_second_key_val or new_second_key_val != old_second_key_val:
                        secondary_key_val = f"{str(old_second_key_val)} -> {str(new_second_key_val)}"
                    else:
                        secondary_key_val = str(old_second_key_val)

                    output_str += f"{primary_key_val:45}{secondary_key_val:45}{column_val}\n"
                else:
                    output_str += f"{primary_key_val:45}{column_val}\n"

                i += 1
                if i == max_display_rows:
                    break

            logger.info(f"{output_str}")


def compare_concat_columns(table_ids: dict[str, str], table_params: TableParams, max_display_rows: int = 5):
    """
    Compare concatenated column values to ensure matching data, as order is not guaranteed in these column strings.
    :param table_ids: dict of table ids: 'source' (dev table), 'versioned' and 'current' (future published ids),
                      and 'previous_versioned' (most recent published table)
    :param table_params: metadata dict containing table parameters, such as primary and secondary keys,
                         concatenated columns, columns excluded from comparison
    :param max_display_rows: Maximum number of records to display in log output; defaults to 5
    """

    def make_concat_column_query(table_id: str) -> str:
        secondary_key_string = ''

        if 'secondary_key' in table_params and table_params['secondary_key'] is not None:
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

            if 'secondary_key' in table_params and table_params['secondary_key'] is not None:
                records_dict_key += f";{record.get(table_params['secondary_key'])}"

            record_dict = dict()

            for _column in table_params['concat_columns']:
                record_dict[_column] = record.get(_column)

            records_dict[records_dict_key] = record_dict

            if record_count % 100000 == 0 and record_count > 0:
                print(f"{record_count}/{result.total_rows} records added to dict!")

        return records_dict

    logger = logging.getLogger('base_script')
    query_logger = logging.getLogger('query_logger')

    new_concat_column_query = make_concat_column_query(table_ids['source'])
    query_logger.info(f"SQL to retrieve concat values in current version table: {table_ids['source']} \n"
                      f"{new_concat_column_query}")
    new_table_records_dict = make_records_dict(query=new_concat_column_query)

    previous_concat_column_query = make_concat_column_query(table_ids['previous_versioned'])
    query_logger.info(f"SQL to retrieve concat values in previous version table: {table_ids['previous_versioned']} \n"
                      f"{previous_concat_column_query}")
    old_table_records_dict = make_records_dict(query=previous_concat_column_query)

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
            # record was removed
            if record_id not in new_table_records_dict:
                new_table_missing_record_count += 1
                continue
            # record was added
            elif record_id not in old_table_records_dict:
                old_table_missing_record_count += 1
                continue

            new_column_value = new_table_records_dict[record_id][column]
            old_column_value = old_table_records_dict[record_id][column]

            # value is null in old and new version--matches
            if new_column_value is None and old_column_value is None:
                correct_records_count += 1
            else:
                # explode new and old concatenated strings into lists
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

                # The list length's match, and the values in each set are identical--these records match.
                if len(new_column_value_list) == len(old_column_value_list) \
                        and len(new_column_value_set ^ old_column_value_set) == 0:
                    correct_records_count += 1
                else:
                    if len(new_column_value_set) != len(new_column_value_list):
                        logger.warning(f"Duplicate value detected in new version's concatenated string column. "
                                       f"Column name: {column}, record id: {record_id}")
                        logger.warning(f"Values: {new_column_value}")

                    if len(old_column_value_set) != len(old_column_value_list):
                        logger.warning(f"Duplicate value detected in old version's concatenated string column. "
                                       f"Column name: {column}, record id: {record_id}")
                        logger.warning(f"Values: {old_column_value}")

                    # different number of values in new and old versions
                    if len(new_column_value_list) != len(old_column_value_list):
                        # if length mismatch, there may be duplicates, so definitely not identical;
                        # set eliminates duplicates, so this is necessary
                        different_lengths_count += 1
                    # different values found in new and old version
                    elif len(new_column_value_set ^ old_column_value_set) > 0:
                        # exclusive or -- values only in exactly one set
                        different_values_count += 1

                    if not new_column_value:
                        new_column_value = ""
                    if not old_column_value:
                        old_column_value = ""

                    mismatched_records.append({
                        "record_id": record_id,
                        "new_table_value": new_column_value,
                        "old_table_value": old_column_value
                    })

        if new_table_missing_record_count > 0 or old_table_missing_record_count > 0 \
                or different_lengths_count > 0 or different_values_count > 0:
            logger.info(f"{column}:")
            # logger.info(f"Missing records in old table: {old_table_missing_record_count}")
            # logger.info(f"Missing records in new table: {new_table_missing_record_count}")
            logger.info(f"Rows with differing item counts: {different_lengths_count}")
            logger.info(f"Rows with same count but mismatched records: {different_values_count}")
            logger.info("")

            output_str = ""

            if len(mismatched_records) > 0:
                i = 0

                new_column_header = f"new {column}"
                old_column_header = f"old {column}"

                if table_params['secondary_key'] is None:
                    output_str += f"\n{table_params['primary_key']:45} {old_column_header:45} {new_column_header}\n"
                else:
                    output_str += f"\n{table_params['primary_key']:45} {table_params['secondary_key']:45} " \
                                  f"{old_column_header:45} {new_column_header}\n"

                for mismatched_record in mismatched_records:
                    if ';' in mismatched_record['record_id']:
                        id_list = mismatched_record['record_id'].split(";")
                        primary_key_val = id_list[0]
                        secondary_key_val = id_list[1]
                    else:
                        primary_key_val = mismatched_record['record_id']
                        secondary_key_val = None

                    if table_params['secondary_key'] is None:
                        output_str += f"{primary_key_val:45} {mismatched_record['old_table_value']:45} " \
                                      f"{mismatched_record['new_table_value']}\n"
                    else:
                        output_str += f"{primary_key_val:45} {secondary_key_val:45} " \
                                      f"{mismatched_record['old_table_value']:45} " \
                                      f"{mismatched_record['new_table_value']}\n"

                    i += 1

                    if i == max_display_rows:
                        break
                logger.info(output_str)
                logger.info("")
        else:
            logger.info(f"All concatenated records match for {column}.")
            logger.info("")


def publish_table(table_ids: dict[str, str]):
    """
    Publish production BigQuery tables using source_table_id:
        - create current/versioned table ids
        - publish tables
        - update friendly name for versioned table
        - change last version tables' status labels to archived
    :param table_ids: dict of table ids: 'source' (dev table), 'versioned' and 'current' (future published ids),
                      and 'previous_versioned' (most recent published table)
    """
    logger = logging.getLogger('base_script')

    if exists_bq_table(table_ids['source']):
        if table_has_new_data(table_ids['previous_versioned'], table_ids['source']):
            logger.info(f"Publishing {table_ids['source']}")
            delay = 5

            logger.info(f"Publishing the following tables:")
            logger.info(f"\t- {table_ids['versioned']}")
            logger.info(f"\t- {table_ids['current']}")
            logger.info(f"Proceed? Y/n (continues automatically in {delay} seconds)")

            response = str(input_with_timeout(seconds=delay)).lower()

            if response == 'n':
                exit("Publish aborted; exiting.")

            logger.info(f"Publishing {table_ids['versioned']}")
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

            if table_ids['previous_versioned']:
                logger.info(f"Archiving {table_ids['previous_versioned']}")
                change_status_to_archived(table_ids['previous_versioned'])

        else:
            logger.info(f"{table_ids['source']} not published, no changes detected")
    else:
        logger.error(f"Source table does not exist: {table_ids['source']}")


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
    query_log_filepath = f"{PARAMS['QUERY_LOGFILE_PATH']}.{log_file_time}"

    # todo remove before publishing
    PARAMS['EMIT_QUERY_LOG_TO_CONSOLE'] = False

    query_logger = initialize_logging(query_log_filepath,
                                      name='query_logger',
                                      emit_to_console=PARAMS['EMIT_QUERY_LOG_TO_CONSOLE'])

    for table_type, table_params in PARAMS['TABLE_TYPES'].items():
        if table_params['data_type'] == 'metadata':
            # generates a list of one table id obj, but makes code cleaner to do it this way
            table_id_list = generate_metadata_table_id_list(table_params)
        else:
            # search for missing project tables for the given table type
            can_compare_type = find_missing_tables(dataset=table_params['dev_dataset'], table_type=table_type)

            if not can_compare_type:
                continue

            # generates a list of all the tables of that type--used for clinical and per-project tables
            table_id_list = generate_table_id_list(table_type, table_params)

        if 'compare_tables' in steps:
            compare_tables(table_type, table_params, table_id_list)

        if 'publish_tables' in steps:
            for table_ids in table_id_list:
                publish_table(table_ids)

    end_time = time.time()
    logger.info(f"Script completed in: {format_seconds(end_time - start_time)}")


if __name__ == "__main__":
    main(sys.argv)
