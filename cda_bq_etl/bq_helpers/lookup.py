# Copyright 2023-2025, Institute for Systems Biology

# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
# documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
# rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all copies or substantial portions of the
# Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
# WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
# COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
# OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

"""Look up and/or retrieve data stored in BigQuery."""

import logging
import sys
import time
from typing import Optional

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from cda_bq_etl.custom_typing import BQQueryResult, Params, _EmptyRowIterator
from cda_bq_etl.utils import (create_dev_table_id, create_metadata_table_id)


def exists_bq_dataset(dataset_id: str) -> bool:
    """
    Determine whether the dataset exists in BigQuery project.

    :param dataset_id: dataset id to validate
    :type dataset_id: str
    :return: True if dataset exists, False otherwise
    :rtype: bool
    """
    client = bigquery.Client()

    try:
        client.get_dataset(dataset_id)
        return True
    except NotFound:
        return False


def exists_bq_table(table_id: str) -> bool:
    """
    Determine whether a BigQuery table exists for a given table_id.

    :param table_id: table id in standard SQL format
    :type table_id: str
    :return: True if exists, False otherwise
    :rtype: bool
    """
    client = bigquery.Client()

    try:
        client.get_table(table_id)
    except NotFound:
        return False
    return True


def list_tables_in_dataset(project_dataset_id: str, filter_terms: Optional[str | list[str]] = None) -> list[str]:
    """
    Create a list of table names contained within dataset.

    :param project_dataset_id: search location dataset id
    :type project_dataset_id: str
    :param filter_terms: Optional, pass a string or a list of strings that should match a table name substring
        (e.g. "gdc" would return only tables associated with that node.)
    :type filter_terms: Optional[str | list[str]]
    :return: list of filtered table names
    :rtype: list[str]
    """
    where_clause = ''
    if filter_terms:
        if isinstance(filter_terms, str):
            where_clause = f"WHERE table_name like '%{filter_terms}%' "
        else:
            where_clause = f"WHERE table_name like '%{filter_terms[0]}%' "
            for i in range(1, len(filter_terms)):
                where_clause += f"AND table_name like '%{filter_terms[i]}%' "

    query = f"""
        SELECT table_name
        FROM `{project_dataset_id}`.INFORMATION_SCHEMA.TABLES
        {where_clause}
    """

    tables_result = query_and_retrieve_result(query)

    table_list = list()

    for row in tables_result:
        table_list.append(row[0])

    return table_list


def get_columns_in_table(table_id: str) -> list[str]:
    """
    Retrieve a list of columns found in table.

    :param table_id: table id in standard SQL format
    :type table_id: str
    :return: List of column names
    :rtype: list[str]
    """
    dataset_id = ".".join(table_id.split(".")[0:-1])
    table_name = table_id.split(".")[-1]

    sql = f"""
        SELECT column_name
        FROM `{dataset_id}`.INFORMATION_SCHEMA.COLUMNS
        WHERE table_name = '{table_name}'
    """

    result = query_and_retrieve_result(sql)

    column_list = list()

    for row in result:
        column_list.append(row[0])

    return column_list


def query_and_retrieve_result(sql: str) -> BQQueryResult | None:
    """
    Create and execute a BQ QueryJob; await and return query result.

    :param sql: the query for which to execute and return results
    :type sql: str
    :return: query result, or None if query fails
    :rtype: BQQueryResult | None
    """
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig()
    location = 'US'

    logger = logging.getLogger('base_script.cda_bq_etl.bq_helpers.lookup')

    # Initialize QueryJob
    query_job = client.query(query=sql, location=location, job_config=job_config)

    while query_job.state != 'DONE':
        query_job = client.get_job(job_id=query_job.job_id, location=location)

        if query_job.state != 'DONE':
            time.sleep(3)

    query_job = client.get_job(job_id=query_job.job_id, location=location)

    if query_job.error_result is not None:
        logger.warning(f"Query failed: {query_job.error_result['message']}")
        return None

    return query_job.result()


def query_and_return_row_count(sql: str) -> int | None:
    """
    Create and execute a BQ QueryJob, wait for and return affected row count. Useful for updating table values.

    :param sql: the query for which to execute and return results
    :type sql: str
    :return: number of rows affected, or None if query fails
    :rtype: int | None
    """
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig()
    location = 'US'

    logger = logging.getLogger('base_script.cda_bq_etl.bq_helpers.lookup')

    # Initialize QueryJob
    query_job = client.query(query=sql, location=location, job_config=job_config)

    while query_job.state != 'DONE':
        query_job = client.get_job(job_id=query_job.job_id, location=location)

        if query_job.state != 'DONE':
            time.sleep(3)

    query_job = client.get_job(job_id=query_job.job_id, location=location)

    if query_job.error_result is not None:
        logger.warning(f"Query failed: {query_job.error_result['message']}")
        return None

    return query_job.num_dml_affected_rows


def find_most_recent_published_table_id(params: Params,
                                        versioned_table_id: str,
                                        table_base_name: Optional[str] = None) -> str | None:
    """
    Locate most recent published table id for dataset's previous release, if it exists.

    :param params: params supplied in yaml config
    :type params: Params
    :param versioned_table_id: public versioned table id for current release
    :type versioned_table_id: str
    :param table_base_name: table name stripped of version
    :type table_base_name: Optional[str]
    :return: last published table id, if any; otherwise None
    :rtype: str | None
    """
    if params['NODE'].lower() == 'gdc':
        oldest_etl_release = 300  # r30
        current_gdc_release_number = params['RELEASE'][1:]

        # this shift allows for non-int release versions, e.g. r33.1
        last_gdc_release = float(current_gdc_release_number.replace('p', '.')) * 10 - 1

        # remove release version from versioned_table_id
        table_id_without_release = versioned_table_id.replace(params['RELEASE'], '')

        # iterate through all possible previous releases to find a matching release
        for release in range(int(last_gdc_release), oldest_etl_release - 1, -1):
            # if release is 270, shifts to 27.0--replace p0 gives whole number, e.g. r27
            # if release is 271, shifts to 27.1, giving r27p1, which is legal for BQ table naming
            previous_release = params['RELEASE'][0] + str(float(release) / 10).replace('.', 'p').replace('p0', '')
            prev_release_table_id = f"{table_id_without_release}{previous_release}"
            if exists_bq_table(prev_release_table_id):
                # found last release table, stop iterating
                return prev_release_table_id

        # if there is no previously-published table, return None
        return None
    elif params['NODE'].lower() == 'pdc':
        # note: this function is only used for metadata table types in PDC--for other types,
        # generate_table_id_list() in compare_and_publish_tables.py is used.
        versioned_dataset = versioned_table_id.split(".")[1]
        dataset = versioned_dataset.replace("_versioned", "")

        return get_most_recent_published_table_id_pdc(params=params,
                                                      dataset=dataset,
                                                      table_filter_str=table_base_name,
                                                      is_metadata=True)
    elif params['NODE'].lower() == 'dcf':
        versioned_dataset = versioned_table_id.split(".")[1]
        dataset = versioned_dataset.replace("_versioned", "")

        return get_most_recent_published_table_id_dcf(params=params,
                                                      dataset=dataset,
                                                      table_filter_str=table_base_name)
    else:
        logger = logging.getLogger('base_script.cda_bq_etl.bq_helpers.lookup')
        logger.critical(f"Need to create find_most_recent_published_table_id function for {params['NODE']}.")
        sys.exit(-1)


def find_most_recent_published_refseq_table_id(params: Params, versioned_table_id: str) -> str | None:
    """
    Find table id for most recent published version of UniProt dataset.

    :param params: api_params supplied in YAML config
    :type params: Params
    :param versioned_table_id: (future) published versioned table id for current release
    :type versioned_table_id: str
    :return: previous published versioned table id, if exists; else None
    :rtype: str | None
    """
    # oldest uniprot release used in published dataset
    oldest_year = 2021
    max_month = 12

    split_release = params['UNIPROT_RELEASE'].split('_')
    last_year = int(split_release[0])
    last_month = int(split_release[1])

    while True:
        if last_month > 1 and last_year >= oldest_year:
            last_month -= 1
        elif last_year > oldest_year:
            last_year -= 1
            last_month = max_month
        else:
            return None

        table_id_no_release = versioned_table_id.replace(f"_{params['UNIPROT_RELEASE']}", '')

        if last_month < 10:
            last_month_str = f"0{last_month}"
        else:
            last_month_str = str(last_month)

        prev_release_table_id = f"{table_id_no_release}_{last_year}_{last_month_str}"

        if exists_bq_table(prev_release_table_id):
            return prev_release_table_id


def get_most_recent_published_table_id_pdc(params: Params,
                                           dataset: str,
                                           table_filter_str: str,
                                           is_metadata: bool = False) -> str | None:
    """
    Locate most recent published PDC table id for dataset's previous release, if it exists.

    :param params: params supplied in yaml config
    :type params: Params
    :param dataset: dataset containing versioned tables
    :type dataset: str
    :param table_filter_str: String used to filter table id results
    :type table_filter_str: str
    :param is_metadata: If True, table is metadata type; False for per-project and per-study table types
    :type is_metadata: bool
    :return: last published table id, if any; otherwise None
    :rtype: str | None
    """

    if is_metadata:
        node_table_name_clause = ""
    else:
        node_table_name_clause = f"AND table_name LIKE '%{params['NODE']}%'"

    def make_program_tables_query() -> str:
        return f"""
            SELECT table_name 
            FROM `{params['PROD_PROJECT']}.{dataset}_versioned`.INFORMATION_SCHEMA.TABLES
            WHERE table_name LIKE '%{table_filter_str}%'
                {node_table_name_clause}
            ORDER BY creation_time DESC
            LIMIT 1
        """

    previous_versioned_table_name_result = query_and_retrieve_result(make_program_tables_query())

    if previous_versioned_table_name_result is None:
        return None
    for previous_versioned_table_name in previous_versioned_table_name_result:
        table_name = previous_versioned_table_name[0]
        return f"{params['PROD_PROJECT']}.{dataset}_versioned.{table_name}"


def get_most_recent_published_table_id_dcf(params: Params,
                                           dataset: str,
                                           table_filter_str: str):
    """
    Locate most recent published DCF table.

    :param params: params supplied in yaml config
    :type params: Params
    :param dataset: dataset containing versioned tables
    :type dataset: str
    :param table_filter_str: String used to filter table id results
    :type table_filter_str: str
    """
    def make_program_tables_query() -> str:
        return f"""
            SELECT table_name 
            FROM `{params['PROD_PROJECT']}.{dataset}_versioned`.INFORMATION_SCHEMA.TABLES
            WHERE table_name LIKE '%{table_filter_str}%'
            ORDER BY creation_time DESC
            LIMIT 1
        """

    previous_versioned_table_name_result = query_and_retrieve_result(make_program_tables_query())

    if previous_versioned_table_name_result is None:
        return None
    for previous_versioned_table_name in previous_versioned_table_name_result:
        table_name = previous_versioned_table_name[0]
        return f"{params['PROD_PROJECT']}.{dataset}_versioned.{table_name}"


def get_pdc_per_project_dataset(params: Params, project_short_name: str) -> str:
    """
    Retrieve the program dataset for PDC project.

    :param params: params from YAML config
    :type params: Params
    :param project_short_name: PDC project short name
    :type project_short_name: str
    :return: PDC program dataset to which the project belongs
    :rtype: str
    """
    def make_dataset_query():
        return f"""
            SELECT program_short_name
            FROM {create_metadata_table_id(params, "studies")}
            WHERE project_short_name = '{project_short_name}'
            LIMIT 1
        """

    logger = logging.getLogger('base_script.cda_bq_etl.bq_helpers.lookup')

    dataset_result = query_and_retrieve_result(make_dataset_query())

    if dataset_result is None:
        logger.critical("No dataset found for project " + project_short_name)
        sys.exit(-1)
    for dataset in dataset_result:
        return dataset[0]


def get_pdc_per_study_dataset(params: Params, pdc_study_id: str) -> str:
    """
    Retrieve the program dataset for PDC study.

    :param params: params from YAML config
    :type params: Params
    :param pdc_study_id: PDC study id
    :type pdc_study_id: str
    :return: PDC program dataset to which the study belongs
    :rtype: str
    """
    def make_dataset_query():
        return f"""
            SELECT program_short_name
            FROM {create_metadata_table_id(params, "studies")}
            WHERE pdc_study_id = '{pdc_study_id}'
            LIMIT 1
        """

    logger = logging.getLogger('base_script.cda_bq_etl.bq_helpers.lookup')

    dataset_result = query_and_retrieve_result(make_dataset_query())

    if dataset_result is None:
        logger.critical("No dataset found for study " + pdc_study_id)
        sys.exit(-1)
    for dataset in dataset_result:
        return dataset[0]


def get_pdc_project_metadata(params: Params, project_submitter_id: Optional[str] = None) -> list[dict[str, str]]:
    """
    Get project short name, program short name and project name for given project submitter id.

    :param params: params from YAML config
    :type params: Params
    :param project_submitter_id: Project submitter id for which to retrieve names
    :type project_submitter_id: Optional[str]
    :return: tuple containing (project_short_name, program_short_name, project_name) strings
    :rtype: list[dict[str, str]]
    """

    def make_study_query():
        where_clause = ''
        if project_submitter_id:
            where_clause = f"WHERE project_submitter_id = '{project_submitter_id}'"

        return f"""
            SELECT DISTINCT project_short_name, 
                project_submitter_id, 
                program_labels, 
                project_friendly_name
            FROM {create_metadata_table_id(params, "studies")}
            {where_clause}
        """

    projects_result = query_and_retrieve_result(make_study_query())

    projects_list = list()

    for project in projects_result:
        if not project:
            logger = logging.getLogger('base_script.cda_bq_etl.bq_helpers.lookup')
            logger.critical(f"No project found for {project_submitter_id}")
            sys.exit(-1)

        projects_list.append(dict(project))

    return projects_list


def get_gdc_program_list(params: Params, rename_programs: bool = True) -> list[str]:
    """
    Get GDC program list (used to divide the data into grouped tables).

    :param params: params defined in yaml config
    :type params: Params
    :param rename_programs: Whether to alter the program name; if True, uses the ALTER_PROGRAM_NAMES parameter in
                            the yaml config
    :type rename_programs: bool
    :return: list of programs or projects
    :rtype: list[str]
    """
    logger = logging.getLogger('base_script.cda_bq_etl.bq_helpers.lookup')

    if params['NODE'] == 'gdc':
        def make_program_name_set_query():
            return f"""
                SELECT DISTINCT program_name
                FROM `{create_dev_table_id(params, 'case_project_program')}`
                ORDER BY program_name
            """

        result = query_and_retrieve_result(sql=make_program_name_set_query())
        program_name_set = set()

        for row in result:
            program_name = row[0]

            if rename_programs:
                if program_name in params['ALTER_PROGRAM_NAMES']:
                    program_name = params['ALTER_PROGRAM_NAMES'][program_name]

            program_name_set.add(program_name)

        return list(sorted(program_name_set))
    else:
        logger.critical(f"get_program_list() is not yet defined for {params['NODE']}.")
        sys.exit(-1)


def get_pdc_projects_metadata_list(params: Params) -> list[dict[str, str]]:
    """
    Return current list of PDC projects and associated metadata (pulled from study metadata table in BQEcosystem repo).

    :param params: params defined in yaml config
    :type params: Params
    :return: list of PDC project metadata
    :rtype: list[dict[str, str]]
    """
    def make_all_studies_query() -> str:
        studies_table_id = f"{params['DEV_PROJECT']}.{params['DEV_METADATA_DATASET']}.studies_{params['RELEASE']}"

        return f"""
            SELECT distinct project_short_name, 
            project_friendly_name, 
            project_submitter_id, 
            program_short_name, 
            program_labels
            FROM `{studies_table_id}`
        """

    projects_result = query_and_retrieve_result(make_all_studies_query())

    projects_list = list()

    for project in projects_result:
        projects_list.append(dict(project))

    return projects_list


def find_missing_columns(params: Params, include_trivial_columns: bool = False):
    """
    Get list of columns from CDA table, compare to column order and excluded column lists in yaml config (TABLE_PARAMS),
    output any missing columns in either location.

    :param params: params defined in yaml config
    :type params: Params
    :param include_trivial_columns: Optional; if True, will list columns that are not found in yaml config even if they
                                    have only null values in the dataset
    :type include_trivial_columns: bool
    """

    def make_column_query():
        full_table_name = create_dev_table_id(params, table_name).split('.')[2]

        return f"""
            SELECT column_name
            FROM {params['DEV_PROJECT']}.{params['DEV_RAW_DATASET']}.INFORMATION_SCHEMA.COLUMNS
            WHERE table_name = '{full_table_name}' 
        """

    def make_column_values_query():
        return f"""
            SELECT DISTINCT {column}
            FROM {create_dev_table_id(params, table_name)}
            WHERE {column} IS NOT NULL
        """

    logger = logging.getLogger('base_script.cda_bq_etl.bq_helpers.lookup')
    logger.info("Scanning for missing fields in config yaml!")

    has_missing_columns = False

    for table_name in params['TABLE_PARAMS'].keys():
        # get list of columns from raw CDA tables
        result = query_and_retrieve_result(make_column_query())

        cda_columns_set = set()

        for row in result:
            cda_columns_set.add(row[0])

        excluded_columns_set = set()

        if 'first' not in params['TABLE_PARAMS'][table_name]['column_order']:
            if params['TABLE_PARAMS'][table_name]['excluded_columns'] is not None:
                excluded_columns_set = set(params['TABLE_PARAMS'][table_name]['excluded_columns'])

            # pdc doesn't use the more granular column order params currently
            columns_set = set(params['TABLE_PARAMS'][table_name]['column_order'])
            all_columns_set = columns_set | excluded_columns_set
        else:
            first_columns_set = set()
            middle_columns_set = set()
            last_columns_set = set()

            # columns should either be listed in column order lists or excluded column list in TABLE_PARAMS
            if params['TABLE_PARAMS'][table_name]['column_order']['first'] is not None:
                first_columns_set = set(params['TABLE_PARAMS'][table_name]['column_order']['first'])
            if params['TABLE_PARAMS'][table_name]['column_order']['middle'] is not None:
                middle_columns_set = set(params['TABLE_PARAMS'][table_name]['column_order']['middle'])
            if params['TABLE_PARAMS'][table_name]['column_order']['last'] is not None:
                last_columns_set = set(params['TABLE_PARAMS'][table_name]['column_order']['last'])
            if params['TABLE_PARAMS'][table_name]['excluded_columns'] is not None:
                excluded_columns_set = set(params['TABLE_PARAMS'][table_name]['excluded_columns'])

            # join into one set
            all_columns_set = first_columns_set | middle_columns_set | last_columns_set | excluded_columns_set

        deprecated_columns = all_columns_set - cda_columns_set
        missing_columns = cda_columns_set - all_columns_set

        non_trivial_columns = set()

        for column in missing_columns:
            result = query_and_retrieve_result(make_column_values_query())
            result_list = list(result)

            if len(result_list) > 0:
                non_trivial_columns.add(column)

        trivial_columns = missing_columns - non_trivial_columns

        if len(deprecated_columns) > 0 or len(non_trivial_columns) > 0 \
                or (len(trivial_columns) > 0 and include_trivial_columns):
            logger.info(f"For {table_name}:")

            if len(deprecated_columns) > 0:
                logger.info(f"Columns no longer found in CDA: {deprecated_columns}")
            if len(trivial_columns) > 0 and include_trivial_columns:
                logger.info(f"Trivial (only null) columns missing from TABLE_PARAMS: {trivial_columns}")
            if len(non_trivial_columns) > 0:
                logger.error(f"Non-trivial columns missing from TABLE_PARAMS: {non_trivial_columns}")
                has_missing_columns = True

    if has_missing_columns:
        logger.critical("Missing columns found (see above output). Please take the following steps, then restart:")
        logger.critical(" - add columns to TABLE_PARAMS in yaml config")
        logger.critical(" - confirm column description is provided in BQEcosystem/TableFieldUpdates.")
        sys.exit(-1)
    else:
        logger.info("No missing fields!")

#
# WJRL 12/18/25 Supports existing usages:
#
def table_has_new_data(previous_table_id: str, current_table_id: str) -> bool:
    """
    Compare newly created table and existing published table. Only publish new table if there's a difference.

    :param previous_table_id: table id for existing published table
    :type previous_table_id: str
    :param current_table_id: table id for new table
    :type current_table_id: str
    :return: True if table has new data, False otherwise
    :rtype: bool
    """
    return table_has_new_data_supports_nans(previous_table_id, current_table_id, None)

def table_has_new_data_supports_nans(previous_table_id: str, current_table_id: str, nan_column: Optional[str] = None) -> bool:
    """
    Compare newly created table and existing published table. Only publish new table if there's a difference.

    :param previous_table_id: table id for existing published table
    :type previous_table_id: str
    :param current_table_id: table id for new table
    :type current_table_id: str
    :param nan_column: optional column holding NaNs to cast to string for comparison
    :type nan_column: str
    :return: True if table has new data, False otherwise
    :rtype: bool
    """

    def compare_two_nan_tables_sql(nan_column: str) -> str:
        return f"""
            (
            SELECT * EXCEPT ({nan_column}), CAST({nan_column} AS STRING) AS nan_string FROM `{previous_table_id}`
            EXCEPT DISTINCT
             SELECT * EXCEPT ({nan_column}), CAST({nan_column} AS STRING) AS nan_string from `{current_table_id}`
            )
            UNION ALL
            (
            SELECT * EXCEPT ({nan_column}), CAST({nan_column} AS STRING) AS nan_string FROM `{current_table_id}`
            EXCEPT DISTINCT
            SELECT * EXCEPT ({nan_column}), CAST({nan_column} AS STRING) AS nan_string from `{previous_table_id}`
            ) 
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

    query_logger.info(f"Query to find any difference in table data")
    # WJRL 12/18/25 Since NaN != NaN, you cannot use the raw table to compare if it has a column holding NaNs:
    sql_stmt = compare_two_tables_sql() if nan_column is None else compare_two_nan_tables_sql(nan_column)
    compare_result = query_and_retrieve_result(sql=sql_stmt)

    if isinstance(compare_result, _EmptyRowIterator):
        # no distinct result rows, tables match
        return False

    if compare_result is None:
        logger = logging.getLogger('base_script.cda_bq_etl.bq_helpers.lookup')
        logger.info("No result returned for table comparison query. Often means that tables have differing schemas.")
        return True

    for row in compare_result:
        return True if row else False
