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
import time
from typing import Union, Optional, Any

from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField, Client, LoadJobConfig, QueryJob
from google.cloud.bigquery.table import RowIterator, _EmptyRowIterator

from cda_bq_etl.utils import has_fatal_error, get_filename, get_scratch_fp
from cda_bq_etl.gcs_helpers import download_from_bucket, upload_to_bucket
from cda_bq_etl.data_helpers import recursively_detect_object_structures, get_column_list_tsv, \
    aggregate_column_data_types_tsv, resolve_type_conflicts, resolve_type_conflict

Params = dict[str, Union[str, dict, int]]
JSONList = list[dict[str, Union[None, str, float, int, bool]]]
QueryResult = Union[None, RowIterator, _EmptyRowIterator]
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

    try:
        load_job = client.load_table_from_uri(source_uris=gs_uri,
                                              destination=table_id,
                                              job_config=job_config)
        print(f' - Inserting into {table_id}... ', end="")
        await_insert_job(params, client, table_id, load_job)
    except TypeError as err:
        has_fatal_error(err)


def await_insert_job(params: Params, client: Client, table_id: str, bq_job: QueryJob):
    """
    Monitor for completion of BigQuery LoadJob or QueryJob that produces some result (generally data insertion).
    :param params: params supplied in yaml config
    :param client: BigQuery Client object
    :param table_id: BigQuery table identifier
    :param bq_job: QueryJob object
    """
    last_report_time = time.time()
    location = params['LOCATION']
    job_state = "NOT_STARTED"

    while job_state != 'DONE':
        bq_job = client.get_job(bq_job.job_id, location=location)

        if time.time() - last_report_time > 30:
            print(f'\tcurrent job state: {bq_job.state}...\t', end='')
            last_report_time = time.time()

        job_state = bq_job.state

        if job_state != 'DONE':
            time.sleep(2)

    bq_job = client.get_job(bq_job.job_id, location=location)

    if bq_job.error_result is not None:
        has_fatal_error(f'While running BigQuery job: {bq_job.error_result}\n{bq_job.errors}', ValueError)

    table = client.get_table(table_id)

    if table.num_rows == 0:
        has_fatal_error(f"[ERROR] Insert job for {table_id} inserted 0 rows. Exiting.")

    print(f" done. {table.num_rows} rows inserted.")


def create_and_upload_schema_for_tsv(params: Params,
                                     tsv_fp: str,
                                     table_name: Optional[str] = None,
                                     header_list: Optional[list[str]] = None,
                                     header_row: Optional[int] = None,
                                     skip_rows: int = 0,
                                     row_check_interval: int = 1,
                                     release: Optional[str] = None,
                                     schema_fp: Optional[str] = None,
                                     delete_local: bool = True):
    """
    Create and upload schema for a file in tsv format.
    :param params: params supplied in yaml config
    :param table_name: table for which the schema is being generated
    :param tsv_fp: path to tsv data file, parsed to create schema
    :param header_list: optional, list of header strings
    :param header_row: optional, integer index of header row within the file
    :param skip_rows: integer representing number of non-data rows at the start of the file, defaults to 0
    :param row_check_interval: how many rows to sample in order to determine type; defaults to 1 (check every row)
    :param release: string value representing release, in cases where params['RELEASE'] should be overridden
    :param schema_fp: path to schema location on local vm
    :param delete_local: delete local file after uploading to cloud bucket
    """
    print(f"Creating schema for {tsv_fp}")

    # third condition required to account for header row at 0 index

    # if no header list supplied here, headers are generated from header_row.
    column_headers = get_column_list_tsv(header_list, tsv_fp, header_row)

    if isinstance(header_row, int) and header_row >= skip_rows:
        has_fatal_error("Header row not excluded by skip_rows.")

    data_types_dict = aggregate_column_data_types_tsv(tsv_fp, column_headers, skip_rows, row_check_interval)

    data_type_dict = resolve_type_conflicts(data_types_dict)

    schema_obj = create_schema_object(column_headers, data_type_dict)

    if not schema_fp:
        schema_filename = get_filename(params,
                                       file_extension='json',
                                       prefix="schema",
                                       suffix=table_name,
                                       release=release)
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
                                      schema_fp: Optional[str] = None,
                                      delete_local: bool = True):
    """
    Create a schema object by recursively detecting the object structure and data types, storing result,
    and converting that to a Schema dict for BQ ingestion.
    :param params: params supplied in yaml config
    :param record_list: list of records to analyze (used to determine schema)
    :param table_name: table for which the schema is being generated
    :param include_release: if true, includes release in schema file name
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
                                       include_release=include_release)

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

    if schema:
        job_config.schema = schema
    else:
        print(f" - No schema supplied for {table_id}, using schema autodetect.")
        job_config.autodetect = True

    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    load_create_table_job(params, jsonl_file, client, table_id, job_config)


def load_table_from_query(params: Params, table_id: str, query: str):
    """
    Create new BigQuery table using result output of BigQuery SQL query.
    :param params: params supplied in yaml config
    :param table_id: target table id
    :param query: data selection query, used to populate a new BigQuery table
    """
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig(destination=table_id)
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    try:
        query_job = client.query(query, job_config=job_config)
        print(f' - Inserting into {table_id}... ', end="")
        await_insert_job(params, client, table_id, query_job)
    except TypeError as err:
        has_fatal_error(err)


def create_view_from_query(view_id: Union[Any, str], view_query: str):
    """
    Create BigQuery view using a SQL query.
    :param view_id: view_id (same structure as a BigQuery table id)
    :param view_query: query from which to construct the view
    """
    client = bigquery.Client()
    view = bigquery.Table(view_id)

    if exists_bq_table(view_id):
        existing_table = client.get_table(view_id)

        if existing_table.table_type == 'VIEW':
            client.delete_table(view_id)
        else:
            has_fatal_error(f"{view_id} already exists and is type ({view.table_type}). Cannot create view, exiting.")

    view.view_query = view_query
    view = client.create_table(view)

    if not exists_bq_table(view_id):
        has_fatal_error(f"View {view_id} not created, exiting.")
    else:
        print(f"Created {view.table_type}: {str(view.reference)}")


def delete_bq_table(table_id: str):
    """
    Permanently delete BigQuery table located by table_id.
    :param table_id: target table id
    """
    client = bigquery.Client()
    client.delete_table(table=table_id, not_found_ok=True)


def exists_bq_table(table_id: str) -> bool:
    """
    Determine whether bq table exists for given table_id.
    :param table_id: table id in standard SQL format
    :return: True if exists, False otherwise
    """
    client = bigquery.Client()

    try:
        client.get_table(table_id)
    except NotFound:
        return False
    return True


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


def query_and_retrieve_result(sql: str) -> Union[QueryResult, None]:
    """
    Create and execute a BQ QueryJob, wait for and return query result.
    :param sql: the query for which to execute and return results
    :return: query result, or None if query fails
    """
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig()
    location = 'US'

    # Initialize QueryJob
    query_job = client.query(query=sql, location=location, job_config=job_config)

    while query_job.state != 'DONE':
        query_job = client.get_job(job_id=query_job.job_id, location=location)

        if query_job.state != 'DONE':
            time.sleep(3)

    query_job = client.get_job(job_id=query_job.job_id, location=location)

    if query_job.error_result is not None:
        print(f"[ERROR] {query_job.error_result}")
        return None

    return query_job.result()


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
                has_fatal_error("Schema object has 'type': 'RECORD' but no 'fields' key.")

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
def convert_object_structure_dict_to_schema_dict(data_schema_dict: dict[str, dict],
                                                 dataset_format_obj,
                                                 descriptions: dict[str, str] = None):
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