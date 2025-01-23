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
import sys
import time
import os
from typing import Union, Optional, Any

from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.cloud.bigquery import SchemaField, Client, LoadJobConfig, QueryJob
from google.cloud.bigquery.table import RowIterator, _EmptyRowIterator

from cda_bq_etl.utils import get_filename, get_scratch_fp, get_filepath, create_dev_table_id, create_metadata_table_id, \
    input_with_timeout
from cda_bq_etl.gcs_helpers import download_from_bucket, upload_to_bucket
from cda_bq_etl.data_helpers import (recursively_detect_object_structures, get_column_list_tsv,
                                     aggregate_column_data_types_tsv, resolve_type_conflicts, resolve_type_conflict)

Params = dict[str, Union[str, dict, int, bool]]
ColumnTypes = Union[None, str, float, int, bool]
RowDict = dict[str, Union[None, str, float, int, bool]]
JSONList = list[RowDict]
BQQueryResult = Union[None, RowIterator, _EmptyRowIterator]
SchemaFieldFormat = dict[str, list[dict[str, str]]]


def load_create_table_job(params: Params, data_file: str, client: Client, table_id: str, job_config: LoadJobConfig):
    """
    Generate BigQuery LoadJob, which creates a Table and loads it with data.
    :param params: params supplied in yaml config
    :param data_file: file containing case records
    :param client: BigQuery Client object
    :param table_id: BigQuery table identifier
    :param job_config: LoadJobConfig object
    """
    gs_uri = f"gs://{params['WORKING_BUCKET']}/{params['WORKING_BUCKET_DIR']}/{data_file}"

    logger = logging.getLogger('base_script.cda_bq_etl.bq_helpers')

    try:
        load_job = client.load_table_from_uri(source_uris=gs_uri,
                                              destination=table_id,
                                              job_config=job_config)

        logger.info(f' - Inserting into {table_id}... ')
        await_insert_job(params, client, table_id, load_job)

    except TypeError as err:
        logger.critical(err)
        sys.exit(-1)


def await_job(params: Params, client: Client, bq_job: QueryJob) -> bool:
    """
    Monitor the completion of BigQuery Job which doesn't return a result.
    :param params: params from yaml config file
    :param client: BigQuery Client object
    :param bq_job: A QueryJob object, responsible for executing BigQuery function calls
    """
    last_report_time = time.time()
    location = params['LOCATION']
    job_state = "NOT_STARTED"

    logger = logging.getLogger('base_script.cda_bq_etl.bq_helpers')

    while job_state != 'DONE':
        bq_job = client.get_job(bq_job.job_id, location=location)

        if time.time() - last_report_time > 30:
            logger.info(f'\tcurrent job state: {bq_job.state}...\t')
            last_report_time = time.time()

        job_state = bq_job.state

        if job_state != 'DONE':
            time.sleep(2)

    bq_job = client.get_job(bq_job.job_id, location=location)

    if bq_job.error_result is not None:
        err_res = bq_job.error_result
        errs = bq_job.errors
        logger.critical(f"While running BigQuery job: {err_res}\n{errs}")
        sys.exit(-1)

    return True


def await_insert_job(params: Params, client: Client, table_id: str, bq_job: QueryJob):
    """
    Monitor for completion of BigQuery LoadJob or QueryJob that produces some result (generally data insertion).
    :param params: params supplied in yaml config
    :param client: BigQuery Client object
    :param table_id: BigQuery table identifier
    :param bq_job: QueryJob object
    """
    logger = logging.getLogger('base_script.cda_bq_etl.bq_helpers')

    if await_job(params, client, bq_job):
        table = client.get_table(table_id)

        if table.num_rows == 0:
            logger = logging.getLogger('base_script.cda_bq_etl.bq_helpers')
            logger.critical(f"Insert job for {table_id} inserted 0 rows. Exiting.")
            sys.exit(-1)

        logger.info(f" done. {table.num_rows} rows inserted.")
    else:
        # if this happens, it may not work to call await_job--trying not to have duplicate code fragments
        logger.critical(f"await_job didn't return for table_id: {table_id}.")
        sys.exit(-1)


def create_and_upload_schema_for_tsv(params: Params,
                                     tsv_fp: str,
                                     header_row: Optional[int] = None,
                                     skip_rows: int = 0,
                                     schema_fp: Optional[str] = None,
                                     delete_local: bool = True,
                                     sample_interval: int = 1):
    """
    Create and upload schema for a file in tsv format.
    :param params: params supplied in yaml config
    :param tsv_fp: path to tsv data file, parsed to create schema
    :param header_row: optional, integer index of header row within the file
    :param skip_rows: integer representing number of non-data rows at the start of the file, defaults to 0
    :param schema_fp: path to schema location on local vm
    :param delete_local: delete local file after uploading to cloud bucket
    :param sample_interval: how many rows to skip between column type checks
    """
    logger = logging.getLogger('base_script.cda_bq_etl.bq_helpers')

    logger.info(f"Creating schema for {tsv_fp}")

    column_headers = get_column_list_tsv(tsv_fp=tsv_fp, header_row_index=header_row)

    if isinstance(header_row, int) and header_row >= skip_rows:
        logger.critical("Header row not excluded by skip_rows.")
        sys.exit(-1)

    data_types_dict = aggregate_column_data_types_tsv(tsv_fp, column_headers, skip_rows, sample_interval)

    data_type_dict = resolve_type_conflicts(data_types_dict)

    schema_obj = create_schema_object(column_headers, data_type_dict)

    if not schema_fp:
        schema_filename = get_filename(params, file_extension='json', prefix="schema")
        schema_fp = get_scratch_fp(params, schema_filename)

    with open(schema_fp, 'w') as schema_json_file:
        json.dump(schema_obj, schema_json_file, indent=4)

    upload_to_bucket(params, schema_fp, delete_local=delete_local)


def create_and_load_table_from_tsv(params: Params,
                                   tsv_file: str,
                                   table_id: str,
                                   num_header_rows: int,
                                   schema: Optional[list[SchemaField]] = None,
                                   null_marker: Optional[str] = None):
    """
    Create new BigQuery table and populate rows using rows of tsv file.
    :param params: params supplied in yaml config
    :param tsv_file: file containing records in tsv format
    :param schema: list of SchemaField objects; if None, attempt to autodetect schema using BigQuery's native autodetect
    :param table_id: target table id
    :param num_header_rows: number of header rows in file (these are skipped during processing)
    :param null_marker: null_marker character, optional (defaults to empty string for tsv/csv in bigquery)
    """
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig()

    if schema:
        job_config.schema = schema
        job_config.skip_leading_rows = num_header_rows
    else:
        job_config.autodetect = True

    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.field_delimiter = '\t'
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    if null_marker:
        job_config.null_marker = null_marker

    load_create_table_job(params, tsv_file, client, table_id, job_config)


def create_and_upload_schema_for_json(params: Params,
                                      record_list: JSONList,
                                      table_name: str,
                                      include_release: bool = False,
                                      release: str = None,
                                      schema_fp: Optional[str] = None,
                                      delete_local: bool = True):
    """
    Create a schema object by recursively detecting the object structure and data types, storing result,
    and converting that to a Schema dict for BQ ingestion.
    :param params: params supplied in yaml config
    :param record_list: list of records to analyze (used to determine schema)
    :param table_name: table for which the schema is being generated
    :param include_release: if true, includes release in schema file name
    :param release: provide custom release value
    :param schema_fp: path to schema location on local vm
    :param delete_local: delete local file after uploading to cloud bucket
    :return:
    """

    data_types_dict = recursively_detect_object_structures(record_list)

    schema_list = convert_object_structure_dict_to_schema_dict(data_types_dict, list())

    schema_obj = {"fields": schema_list}

    if not schema_fp:
        schema_filename = get_filename(params,
                                       file_extension='json',
                                       prefix="schema",
                                       suffix=table_name,
                                       include_release=include_release,
                                       release=release)

        schema_fp = get_scratch_fp(params, schema_filename)

    with open(schema_fp, 'w') as schema_json_file:
        json.dump(schema_obj, schema_json_file, indent=4)

    upload_to_bucket(params, schema_fp, delete_local=delete_local)


def create_and_load_table_from_jsonl(params: Params,
                                     jsonl_file: str,
                                     table_id: str,
                                     schema: Optional[list[SchemaField]] = None):
    """
    Create new BigQuery table, populated with contents of jsonl file.
    :param params: params supplied in yaml config
    :param jsonl_file: file containing single-line json objects, which represent rows to be loaded into table
    :param table_id: target table id
    :param schema: list of SchemaField objects; if None, attempt to autodetect schema using BigQuery's native autodetect
    """
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig()

    logger = logging.getLogger('base_script.cda_bq_etl.bq_helpers')

    if schema:
        job_config.schema = schema
    else:
        logger.info(f" - No schema supplied for {table_id}, using schema autodetect.")
        job_config.autodetect = True

    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    load_create_table_job(params, jsonl_file, client, table_id, job_config)


def create_table_from_query(params: Params, table_id: str, query: str):
    """
    Create new BigQuery table using result output of BigQuery SQL query.
    :param params: params supplied in yaml config
    :param table_id: target table id
    :param query: data selection query, used to populate a new BigQuery table
    """
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig(destination=table_id)
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    logger = logging.getLogger('base_script.cda_bq_etl.bq_helpers')

    try:
        query_job = client.query(query, job_config=job_config)
        logger.info(f' - Inserting into {table_id}... ')
        await_insert_job(params, client, table_id, query_job)
    except TypeError as err:
        logger.critical(err)
        sys.exit(-1)


def create_view_from_query(view_id: Union[Any, str], view_query: str):
    """
    Create BigQuery view using a SQL query.
    :param view_id: view_id (same structure as a BigQuery table id)
    :param view_query: query from which to construct the view
    """
    client = bigquery.Client()
    view = bigquery.Table(view_id)

    logger = logging.getLogger('base_script.cda_bq_etl.bq_helpers')

    if exists_bq_table(view_id):
        existing_table = client.get_table(view_id)

        if existing_table.table_type == 'VIEW':
            client.delete_table(view_id)
        else:
            logger.critical(f"{view_id} already exists and is type ({view.table_type}). Cannot create view, exiting.")
            sys.exit(-1)

    view.view_query = view_query
    view = client.create_table(view)

    if not exists_bq_table(view_id):
        logger.critical(f"View {view_id} not created, exiting.")
        sys.exit(-1)
    else:
        logger.info(f"Created {view.table_type}: {str(view.reference)}")


def exists_bq_dataset(dataset_id: str) -> bool:
    """
    Determine whether dataset exists in BigQuery project.
    :param dataset_id: dataset id to validate
    :return: True if dataset exists, False otherwise
    """
    client = bigquery.Client()

    try:
        client.get_dataset(dataset_id)
        return True
    except NotFound:
        return False


def exists_bq_table(table_id: str) -> bool:
    """
    Determine whether BigQuery table exists for given table_id.
    :param table_id: table id in standard SQL format
    :return: True if exists, False otherwise
    """
    client = bigquery.Client()

    try:
        client.get_table(table_id)
    except NotFound:
        return False
    return True


def list_tables_in_dataset(project_dataset_id: str, filter_terms: Union[str, list[str]] = None) -> list[str]:
    """
    Create a list of table names contained within dataset.
    :param project_dataset_id: search location dataset id
    :param filter_terms: Optional, pass a string or a list of strings that should match a table name substring
        (e.g. "gdc" would return only tables associated with that node.)
    :return: list of filtered table names
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


def delete_bq_table(table_id: str):
    """
    Permanently delete BigQuery table located by table_id.
    :param table_id: target table id
    """
    logger = logging.getLogger('base_script.cda_bq_etl.bq_helpers')

    client = bigquery.Client()
    client.delete_table(table=table_id, not_found_ok=True)

    if exists_bq_table(table_id):
        logger.error(f"Table {table_id} not deleted.")


def copy_bq_table(params: Params, src_table: str, dest_table: str, replace_table: bool = False):
    """
    Copy an existing BigQuery src_table into location specified by dest_table.
    :param params: param object from yaml config
    :param src_table: ID of table to copy
    :param dest_table: ID of table create
    :param replace_table: Replace existing table, if one exists; defaults to False
    """
    client = bigquery.Client()
    job_config = bigquery.CopyJobConfig()

    logger = logging.getLogger('base_script.cda_bq_etl.bq_helpers')

    if replace_table:
        delete_bq_table(dest_table)

    bq_job = client.copy_table(src_table, dest_table, job_config=job_config)

    if await_job(params, client, bq_job):
        logger.info(f"Successfully copied {src_table} -> ")
        logger.info(f"\t\t\t{dest_table}")


# PyCharm linter gets confused about BQ class typing and the warnings are distracting, so suppressed
# noinspection PyTypeChecker
def create_bq_dataset(params: Params, project_id: str, dataset_name: str):
    logger = logging.getLogger('base_script.cda_bq_etl.bq_helpers')

    dataset_id = f"{project_id}.{dataset_name}"

    if exists_bq_dataset(dataset_id):
        logger.info(f"Dataset {dataset_id} already exists, returning")
        return

    client = bigquery.Client(project=project_id)

    dataset = bigquery.Dataset(dataset_id)
    dataset.location = params['LOCATION']

    dataset = client.create_dataset(dataset)
    logger.info(f"Created dataset {client.project}.{dataset.dataset_id}")


def retrieve_bq_schema_object(params: Params,
                              table_name: Optional[str] = None,
                              release: Optional[str] = None,
                              include_release: bool = True,
                              schema_filename: Optional[str] = None,
                              schema_dir: Optional[str] = None) -> list[SchemaField]:
    """
    Retrieve schema file from GDC bucket and convert into list of SchemaField objects.
    :param params: params supplied in yaml config
    :param table_name: name of table for which schema was created
    :param release: data release number
    :param include_release: Whether to include release in filename
    :param schema_filename: schema file name
    :param schema_dir: schema file directory location
    :return: list of SchemaField objects for BigQuery ingestion
    """
    if not schema_filename:
        schema_filename = get_filename(params=params,
                                       file_extension='json',
                                       prefix="schema",
                                       suffix=table_name,
                                       release=release,
                                       include_release=include_release)

    download_from_bucket(params, filename=schema_filename, dir_path=schema_dir)

    if not schema_dir:
        schema_fp = get_scratch_fp(params, schema_filename)
    else:
        schema_fp = f"{schema_dir}/{schema_filename}"

    with open(schema_fp, "r") as schema_json:
        schema_obj = json.load(schema_json)
        json_schema_obj_list = [field for field in schema_obj["fields"]]
        schema = generate_bq_schema_fields(json_schema_obj_list)

    return schema


def query_and_retrieve_result(sql: str) -> Union[BQQueryResult, None]:
    """
    Create and execute a BQ QueryJob, wait for and return query result.
    Refactored version of bq_harness_with_result, but this doesn't include do_batch, which might be necessary for
    some derived workflows--could be added back by setting do_batch default as None
    :param sql: the query for which to execute and return results
    :return: query result, or None if query fails
    """
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig()
    location = 'US'

    logger = logging.getLogger('base_script.cda_bq_etl.bq_helpers')

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


def query_and_return_row_count(sql: str) -> int:
    """
    Create and execute a BQ QueryJob, wait for and return affected row count. Useful for updating table values.
    :param sql: the query for which to execute and return results
    :return: number of rows affected, or None if query fails
    """
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig()
    location = 'US'

    logger = logging.getLogger('base_script.cda_bq_etl.bq_helpers')

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


# todo should this be in data helpers?
def create_schema_object(column_headers: list[str], data_types_dict: dict[str, str]) -> SchemaFieldFormat:
    """
    Create BigQuery SchemaField object.
    :param column_headers: list of column names
    :param data_types_dict: dictionary of column names and their types
        (should have been run through resolve_type_conflicts() prior to use here)
    :return BQ schema field object list
    """
    schema_field_object_list = list()

    for column_name in column_headers:
        # override typing for ids, even those which are actually in

        schema_field = {
            "name": column_name,
            "type": data_types_dict[column_name],
            "mode": "NULLABLE",
            "description": ''
        }

        schema_field_object_list.append(schema_field)

    return {
        "fields": schema_field_object_list
    }


# todo should this be in data helpers?
def generate_bq_schema_fields(schema_obj_list: JSONList) -> list[SchemaField]:
    """
    Convert list of schema fields into TableSchema object.
    :param schema_obj_list: list of dicts representing BigQuery SchemaField objects
    :returns list of BigQuery SchemaField objects (represents TableSchema object)
    """

    def create_schema_field_obj(_schema_obj: dict[str, str],
                                schema_fields: Optional[list[SchemaField]] = None):
        """
        Output BigQuery SchemaField object.
        :param _schema_obj: dict with schema field values
        :param schema_fields: Optional, child SchemaFields for RECORD type column
        :return: SchemaField object
        """
        if schema_fields:
            return bigquery.schema.SchemaField(name=_schema_obj['name'],
                                               description=_schema_obj['description'],
                                               field_type=_schema_obj['type'],
                                               mode=_schema_obj['mode'],
                                               fields=schema_fields)
        else:
            return bigquery.schema.SchemaField(name=_schema_obj['name'],
                                               description=_schema_obj['description'],
                                               field_type=_schema_obj['type'],
                                               mode=_schema_obj['mode'])

    def generate_bq_schema_field(_schema_obj: Union[dict[str, dict], dict[str, str]],
                                 schema_fields: list[SchemaField]):
        """
        Convert schema field json dict object into SchemaField object.
        :param _schema_obj: direct ancestor of schema_fields
        :param schema_fields: list of SchemaField objects
        """
        if not _schema_obj:
            return
        elif _schema_obj['type'] == 'RECORD':
            child_schema_fields = list()

            if not _schema_obj['fields']:
                logger = logging.getLogger('base_script.cda_bq_etl.bq_helpers')
                logger.critical("Schema object has 'type': 'RECORD' but no 'fields' key.")
                sys.exit(-1)

            for child_obj in _schema_obj['fields']:
                generate_bq_schema_field(child_obj, child_schema_fields)

            schema_field = create_schema_field_obj(_schema_obj, child_schema_fields)
        else:
            schema_field = create_schema_field_obj(_schema_obj)

        schema_fields.append(schema_field)

    schema_fields_obj = list()

    for _schema_obj in schema_obj_list:
        generate_bq_schema_field(_schema_obj, schema_fields_obj)

    return schema_fields_obj


# todo should this be in data helpers?
def convert_object_structure_dict_to_schema_dict(data_schema_dict: Union[RowDict, JSONList, ColumnTypes],
                                                 dataset_format_obj,
                                                 descriptions: Optional[dict[str, str]] = None):
    """
    Parse dict of {<field>: {<data_types>}} representing data object's structure;
    convert into dict representing a TableSchema object.
    :param data_schema_dict: dictionary representing dataset's structure, fields and data types
    :param dataset_format_obj: dataset format obj # todo could use better description
    :param descriptions: (optional) dictionary of field: description string pairs for inclusion in schema definition
    """

    for k, v in data_schema_dict.items():
        if descriptions and k in descriptions:
            description = descriptions[k]
        else:
            description = ""

        if isinstance(v, dict):
            # parent node
            schema_field = {
                "name": k,
                "type": "RECORD",
                "mode": "REPEATED",
                "description": description,
                "fields": list()
            }
            dataset_format_obj.append(schema_field)

            convert_object_structure_dict_to_schema_dict(data_schema_dict[k], schema_field['fields'])
        else:
            # v is a set
            final_type = resolve_type_conflict(k, v)

            if final_type == "ARRAY":
                schema_field = {
                    "name": k,
                    "type": "STRING",
                    "mode": "REPEATED",
                    "description": description
                }
            else:
                # child (leaf) node
                schema_field = {
                    "name": k,
                    "type": final_type,
                    "mode": "NULLABLE",
                    "description": description
                }

            dataset_format_obj.append(schema_field)

    # todo what's the data type for this?
    return dataset_format_obj


def update_friendly_name(params: Params, table_id: str, custom_name: Optional[str] = None) -> Optional[None]:
    """
    Modify BigQuery table's friendly name.
    :param params: API params, supplied via yaml config
    :param table_id: table id in standard SQL format
    if is_gdc and no custom_name is specified, we add REL before the version onto the existing friendly name;
        if custom_name is specified, this behavior is overridden, and the table's friendly name is replaced entirely.
    :param custom_name: specifies a custom friendly name;
           by default, if is_gdc, the following is appended to the friendly name for versioned tables:
           "'REL' + api_params['RELEASE'] + ' VERSIONED'"
           if not is_gdc, the following is appended to the friendly name for versioned tables:
           api_params['RELEASE'] + ' VERSIONED'"
    """
    client = bigquery.Client()

    if not exists_bq_table(table_id):
        return None

    table = client.get_table(table_id)

    if custom_name:
        friendly_name = custom_name
    else:
        if params['NODE'].lower() == 'gdc':
            release = params['RELEASE'].replace('r', '')
            friendly_name = f"{table.friendly_name} REL{release} VERSIONED"
        else:
            friendly_name = f"{table.friendly_name} {params['RELEASE']} VERSIONED"

    table.friendly_name = friendly_name
    client.update_table(table, ["friendly_name"])

    assert table.friendly_name == friendly_name


def change_status_to_archived(archived_table_id: str):
    """
    Change the status label of archived_table_id to 'archived.'
    :param archived_table_id: id for table that is being archived
    """
    try:
        client = bigquery.Client()
        prev_table = client.get_table(archived_table_id)
        prev_table.labels['status'] = 'archived'
        client.update_table(prev_table, ["labels"])
        assert prev_table.labels['status'] == 'archived'
    except NotFound:
        logger = logging.getLogger('base_script.cda_bq_etl.bq_helpers')
        logger.warning("Couldn't find a table to archive. Likely this table's first release; otherwise an error.")


def update_table_labels(table_id: str, label_dict: dict[str, str]) -> None:
    logger = logging.getLogger('base_script.cda_bq_etl.bq_helpers')
    try:
        client = bigquery.Client()
        table_obj = client.get_table(table_id)

        for label, value in label_dict.items():
            table_obj.labels[label] = value

        delay = 5

        logger.info(f"Altering {table_id}. Labels after change: {table_obj.labels}")
        logger.info(f"Proceed? Y/n (continues automatically in {delay} seconds)")

        response = str(input_with_timeout(seconds=delay)).lower()

        if response == 'n' or response == 'N':
            exit("Publish aborted; exiting.")

        client.update_table(table_obj, ["labels"])

        for label, value in label_dict.items():
            assert table_obj.labels[label] == value
    except NotFound:
        logger = logging.getLogger('base_script.cda_bq_etl.bq_helpers')
        logger.warning(f"Couldn't apply table label {label}: {value}. Is this expected?")


def find_most_recent_published_table_id(params: Params, versioned_table_id: str, table_base_name: str = None):
    """
    Function for locating published table id for dataset's previous release, if it exists
    :param table_type:
    :param params: params supplied in yaml config
    :param versioned_table_id: public versioned table id for current release
    :return: last published table id, if any; otherwise None
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

        return get_most_recent_published_table_version_pdc(params=params,
                                                           dataset=dataset,
                                                           table_filter_str=table_base_name,
                                                           is_metadata=True)
    elif params['NODE'].lower() == 'dcf':
        versioned_dataset = versioned_table_id.split(".")[1]
        dataset = versioned_dataset.replace("_versioned", "")

        return get_most_recent_published_table_version_dcf(params=params,
                                                           dataset=dataset,
                                                           table_filter_str=table_base_name)

    else:
        logger = logging.getLogger('base_script.cda_bq_etl.bq_helpers')
        logger.critical(f"Need to create find_most_recent_published_table_id function for {params['NODE']}.")
        sys.exit(-1)


def find_most_recent_published_refseq_table_id(params, versioned_table_id):
    """
    Find table id for most recent published version of UniProt dataset.
    :param params: api_params supplied in YAML config
    :param versioned_table_id: (future) published versioned table id for current release
    :return: previous published versioned table id, if exists; else None
    """
    logger = logging.getLogger('base_script.cda_bq_etl.bq_helpers')

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


def update_table_schema_from_generic(params, table_id, schema_tags=None, friendly_name_suffix=None, metadata_file=None):
    """
    Insert schema tags into generic schema (currently located in BQEcosystem repo).
    :param params: params from YAML config
    :param table_id: table_id where schema metadata should be inserted
    :param schema_tags: schema tags used to populate generic schema metadata
    :param friendly_name_suffix: todo
    :param metadata_file: name of generic table metadata file
    """
    if schema_tags is None:
        schema_tags = dict()

    release = params['RELEASE']

    if params['NODE'].lower() == 'gdc':
        release = release.replace('r', '')
    elif params['NODE'].lower() == 'dcf':
        release = release.replace('dr', '')
    elif params['NODE'].lower() == 'pdc':
        schema_tags['underscore-version'] = release.lower()

    # remove underscore, add decimal to version number
    schema_tags['version'] = ".".join(release.split('_'))
    schema_tags['extracted-month-year'] = params['EXTRACTED_MONTH_YEAR']

    # gdc uses this
    if 'RELEASE_NOTES_URL' in params:
        schema_tags['release-notes-url'] = params['RELEASE_NOTES_URL']

    logger = logging.getLogger('base_script.cda_bq_etl.bq_helpers')

    add_generic_table_metadata(params=params,
                               table_id=table_id,
                               schema_tags=schema_tags,
                               friendly_name_suffix=friendly_name_suffix,
                               metadata_file=metadata_file)
    add_column_descriptions(params=params, table_id=table_id)


def add_generic_table_metadata(params: Params,
                               table_id: str,
                               schema_tags: dict[str, str],
                               friendly_name_suffix: str = None,
                               metadata_file: str = None):
    """
    todo
    :param params: params supplied in yaml config
    :param table_id: table id for which to add the metadata
    :param schema_tags: dictionary of generic schema tag keys and values
    :param friendly_name_suffix: todo
    :param metadata_file: todo
    """
    generic_schema_path = f"{params['BQ_REPO']}/{params['GENERIC_SCHEMA_DIR']}"

    if not metadata_file:
        metadata_fp = get_filepath(f"{generic_schema_path}/{params['GENERIC_TABLE_METADATA_FILE']}")
    else:
        metadata_fp = get_filepath(f"{generic_schema_path}/{metadata_file}")

    with open(metadata_fp) as file_handler:
        table_schema = ''

        for line in file_handler.readlines():
            table_schema += line

        for tag_key, tag_value in schema_tags.items():
            tag = f"{{---tag-{tag_key}---}}"

            table_schema = table_schema.replace(tag, tag_value)

        table_metadata = json.loads(table_schema)

        if friendly_name_suffix:
            table_metadata['friendlyName'] += f" - {friendly_name_suffix}"

        update_table_metadata_pdc(table_id, table_metadata)


def update_table_metadata_pdc(table_id: str, metadata: dict[str, str]):
    """
    Modify an existing BigQuery table's metadata (labels, friendly name, description) using metadata dict argument
    :param table_id: table id in standard SQL format
    :param metadata: metadata containing new field and table attributes
    """
    client = bigquery.Client()
    table = client.get_table(table_id)

    table.labels = metadata['labels']
    table.friendly_name = metadata['friendlyName']
    table.description = metadata['description']

    client.update_table(table, ["labels", "friendly_name", "description"])

    assert table.labels == metadata['labels']
    assert table.friendly_name == metadata['friendlyName']
    assert table.description == metadata['description']


def add_column_descriptions(params: Params, table_id: str):
    """
    Alter an existing table's schema (currently, only column descriptions are mutable without a table rebuild,
    Google's restriction).
    :param params: params supplied in yaml config
    :param table_id: table id in standard SQL format
    """
    logger = logging.getLogger('base_script.cda_bq_etl.bq_helpers')
    logger.info("\t - Adding column descriptions!")

    column_desc_fp = f"{params['BQ_REPO']}/{params['COLUMN_DESCRIPTION_FILEPATH']}"
    column_desc_fp = get_filepath(column_desc_fp)

    if not os.path.exists(column_desc_fp):
        logger.critical("BQEcosystem column description path not found")
        sys.exit(-1)
    with open(column_desc_fp) as column_output:
        descriptions = json.load(column_output)

    update_schema(table_id, descriptions)


def update_schema(table_id: str, new_descriptions: dict[str, str]):
    """
    Modify an existing table's field descriptions.
    :param table_id: table id in standard SQL format
    :param new_descriptions: dict of field names and new description strings
    """
    client = bigquery.Client()
    table = client.get_table(table_id)
    logger = logging.getLogger('base_script.cda_bq_etl.bq_helpers')

    new_schema = []

    for schema_field in table.schema:
        column = schema_field.to_api_repr()

        if column['name'] in new_descriptions.keys():
            name = column['name']
            column['description'] = new_descriptions[name]
        else:
            logger.error(f"Need to define {column['name']} in BQEcosystem field description dictionary.")
        if 'description' in column and column['description'] == '':
            logger.error(f"Still no description for field: {column['name']}")

        mod_column = bigquery.SchemaField.from_api_repr(column)
        new_schema.append(mod_column)

    table.schema = new_schema

    client.update_table(table, ['schema'])


def get_pdc_per_project_dataset(params: Params, project_short_name: str) -> str:
    def make_dataset_query():
        return f"""
            SELECT program_short_name
            FROM {create_metadata_table_id(params, "studies")}
            WHERE project_short_name = '{project_short_name}'
            LIMIT 1
        """
    logger = logging.getLogger('base_script.cda_bq_etl.bq_helpers')

    dataset_result = query_and_retrieve_result(make_dataset_query())

    if dataset_result is None:
        logger.critical("No dataset found for project " + project_short_name)
        sys.exit(-1)
    for dataset in dataset_result:
        return dataset[0]


def get_pdc_per_study_dataset(params: Params, pdc_study_id: str) -> str:
    def make_dataset_query():
        return f"""
            SELECT program_short_name
            FROM {create_metadata_table_id(params, "studies")}
            WHERE pdc_study_id = '{pdc_study_id}'
            LIMIT 1
        """
    logger = logging.getLogger('base_script.cda_bq_etl.bq_helpers')

    dataset_result = query_and_retrieve_result(make_dataset_query())

    if dataset_result is None:
        logger.critical("No dataset found for study " + pdc_study_id)
        sys.exit(-1)
    for dataset in dataset_result:
        return dataset[0]


def get_pdc_projects_metadata(params: Params, project_submitter_id: str = None) -> list[dict[str, str]]:
    """
    Get project short name, program short name and project name for given project submitter id.
    :param params: params from YAML config
    :param project_submitter_id: Project submitter id for which to retrieve names
    :return: tuple containing (project_short_name, program_short_name, project_name) strings
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
            logger = logging.getLogger('base_script.cda_bq_etl.bq_helpers')
            logger.critical(f"No project found for {project_submitter_id}")
            sys.exit(-1)

        projects_list.append(dict(project))

    return projects_list


def get_most_recent_published_table_version_pdc(params: Params,
                                                dataset: str,
                                                table_filter_str: str,
                                                is_metadata: bool = False):
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


def get_most_recent_published_table_version_dcf(params: Params,
                                                dataset: str,
                                                table_filter_str: str):

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


def get_project_level_schema_tags(params: Params, project_submitter_id: str) -> dict[str, str]:
    """
    Get project-level schema tags for populating generic table metadata schema.
    :param params: params from YAML config
    :param project_submitter_id: Project submitter id for which to retrieve schema tags
    :return: Dict of schema tags
    """
    logger = logging.getLogger('base_script.cda_bq_etl.bq_helpers')

    project_name_dict = get_pdc_projects_metadata(params, project_submitter_id)[0]
    program_labels_list = project_name_dict['program_labels'].split("; ")

    if len(program_labels_list) > 2:
        logger.critical("PDC clinical isn't set up to handle >2 program labels yet; support needs to be added.")
        sys.exit(-1)
    elif len(program_labels_list) == 0:
        logger.critical(f"No program label included for {project_submitter_id}, please add to PDCStudy.yaml")
        sys.exit(-1)
    elif len(program_labels_list) == 2:
        return {
            "project-name": project_name_dict['project_short_name'].strip(),
            "mapping-name": "",  # only used by clinical, but including it elsewhere is harmless
            "friendly-project-name-upper": project_name_dict['project_friendly_name'].upper().strip(),
            "program-name-0-lower": program_labels_list[0].lower().strip(),
            "program-name-1-lower": program_labels_list[1].lower().strip()
        }
    else:
        return {
            "project-name": project_name_dict['project_short_name'].strip(),
            "mapping-name": "",  # only used by clinical, but including it elsewhere is harmless
            "friendly-project-name-upper": project_name_dict['project_friendly_name'].upper().strip(),
            "program-name-lower": project_name_dict['program_labels'].lower().strip()
        }


def get_program_schema_tags_gdc(params: Params, program_name: str) -> dict[str, str]:
    metadata_mappings_path = f"{params['BQ_REPO']}/{params['PROGRAM_METADATA_DIR']}"
    program_metadata_fp = get_filepath(f"{metadata_mappings_path}/{params['PROGRAM_METADATA_FILE']}")

    logger = logging.getLogger('base_script.cda_bq_etl.bq_helpers')

    with open(program_metadata_fp, 'r') as fh:
        program_metadata_dict = json.load(fh)

        program_metadata = program_metadata_dict[program_name]

        schema_tags = dict()

        schema_tags['program-name'] = program_metadata['friendly_name']
        schema_tags['friendly-name'] = program_metadata['friendly_name']

        if 'program_label' in program_metadata:
            schema_tags['program-label'] = program_metadata['program_label']
        elif 'program_label_0' in program_metadata and 'program_label_1' in program_metadata:
            schema_tags['program-label-0'] = program_metadata['program_label_0']
            schema_tags['program-label-1'] = program_metadata['program_label_1']
        else:
            logger.critical("Did not find program_label OR program_label_0 and program_label_1 in schema json file.")
            sys.exit(-1)

        return schema_tags


def get_uniprot_schema_tags(params: Params) -> dict[str, str]:
    return {
        "uniprot-version": params['UNIPROT_RELEASE'],
        "uniprot-extracted-month-year": params['UNIPROT_EXTRACTED_MONTH_YEAR']
    }


def get_gene_info_schema_tags(params: Params) -> dict[str, str]:
    return {
        "version": params['RELEASE'],
        "extracted-month-year": params['EXTRACTED_MONTH_YEAR']
    }


def get_program_list(params: Params, rename_programs: bool = True) -> list[str]:
    """
    Get whichever list is used to divide the data into grouped tables; GDC uses program, PDC uses project.
    :param params: params defined in yaml config
    :param rename_programs:
    :return: set of programs or projects
    """
    logger = logging.getLogger('base_script.cda_bq_etl.bq_helpers')

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

    elif params['NODE'] == 'pdc':
        '''
        def make_all_studies_query() -> str:
            return f"""
                SELECT DISTINCT project_short_name
                FROM `{params['DEV_PROJECT']}.{params['DEV_METADATA_DATASET']}.studies_{params['RELEASE']}`
                ORDER BY project_short_name
            """

        projects_result = query_and_retrieve_result(make_all_studies_query())
        project_set = set()

        for row in projects_result:
            project_set.add(row[0])

        return list(sorted(project_set))
        '''
    elif params['NODE'] == 'idc':
        logger.critical("get_project_list() is not yet defined for IDC.")
        sys.exit(-1)
    else:
        logger.critical(f"get_project_list() is not yet defined for {params['NODE']}.")
        sys.exit(-1)


def get_pdc_projects_metadata_list(params: Params) -> list[dict[str, str]]:
    """
    Return current list of PDC projects (pulled from study metadata table in BQEcosystem repo).
    :param params: params defined in yaml config
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
    :param include_trivial_columns: Optional; if True, will list columns that are not found in yaml config even if they
                                    have only null values in the dataset
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

    logger = logging.getLogger('base_script')
    logger.info("Scanning for missing fields in config yaml!")

    has_missing_columns = False

    for table_name in params['TABLE_PARAMS'].keys():
        # get list of columns from raw CDA tables
        result = query_and_retrieve_result(make_column_query())

        cda_columns_set = set()

        for row in result:
            cda_columns_set.add(row[0])

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
            excluded_columns_set = set()

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