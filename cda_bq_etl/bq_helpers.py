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
import os
from typing import Union, Optional, Any

from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.cloud.bigquery import SchemaField, Client, LoadJobConfig, QueryJob
from google.cloud.bigquery.table import RowIterator, _EmptyRowIterator

from cda_bq_etl.utils import has_fatal_error, get_filename, get_scratch_fp, input_with_timeout, get_filepath, \
    construct_table_name
from cda_bq_etl.gcs_helpers import download_from_bucket, upload_to_bucket
from cda_bq_etl.data_helpers import recursively_detect_object_structures, get_column_list_tsv, \
    aggregate_column_data_types_tsv, resolve_type_conflicts, resolve_type_conflict

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

    try:
        load_job = client.load_table_from_uri(source_uris=gs_uri,
                                              destination=table_id,
                                              job_config=job_config)
        print(f' - Inserting into {table_id}... ', end="")
        await_insert_job(params, client, table_id, load_job)

    except TypeError as err:
        has_fatal_error(err)


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
        err_res = bq_job.error_result
        errs = bq_job.errors
        has_fatal_error(f"While running BigQuery job: {err_res}\n{errs}")

    return True


def await_insert_job(params: Params, client: Client, table_id: str, bq_job: QueryJob):
    """
    Monitor for completion of BigQuery LoadJob or QueryJob that produces some result (generally data insertion).
    :param params: params supplied in yaml config
    :param client: BigQuery Client object
    :param table_id: BigQuery table identifier
    :param bq_job: QueryJob object
    """

    if await_job(params, client, bq_job):
        table = client.get_table(table_id)

        if table.num_rows == 0:
            has_fatal_error(f"[ERROR] Insert job for {table_id} inserted 0 rows. Exiting.")

        print(f" done. {table.num_rows} rows inserted.")
    else:
        # if this happens, it may not work to call await_job--trying not to have duplicate code fragments
        has_fatal_error(f"await_job didn't return for table_id: {table_id}.")


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


def delete_bq_table(table_id: str):
    """
    Permanently delete BigQuery table located by table_id.
    :param table_id: target table id
    """
    client = bigquery.Client()
    client.delete_table(table=table_id, not_found_ok=True)


def copy_bq_table(params, src_table, dest_table, replace_table=False):
    """
    Copy an existing BigQuery src_table into location specified by dest_table.
    :param params: param object from yaml config
    :param src_table: ID of table to copy
    :param dest_table: ID of table create
    :param replace_table: Replace existing table, if one exists; defaults to False
    """
    client = bigquery.Client()
    job_config = bigquery.CopyJobConfig()

    if replace_table:
        delete_bq_table(dest_table)

    bq_job = client.copy_table(src_table, dest_table, job_config=job_config)

    if await_job(params, client, bq_job):
        print("Successfully copied table:")
        print(f"src: {src_table}\n dest: {dest_table}\n")


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


def publish_table(params: Params, source_table_id: str, current_table_id: str, versioned_table_id: str):
    """
    Publish production BigQuery tables using source_table_id:
        - create current/versioned table ids
        - publish tables
        - update friendly name for versioned table
        - change last version tables' status labels to archived
    :param params: params from yaml config
    :param source_table_id: source (dev) table id
    :param current_table_id: published table id for current
    :param versioned_table_id: published table id for versioned
    """
    previous_versioned_table_id = find_most_recent_published_table_id(params, versioned_table_id)
    print(f"previous_versioned_table_id: {previous_versioned_table_id}")

    if params['TEST_PUBLISH']:
        print(f"\nEvaluating publish_tables step for {source_table_id}.\n")

        # does source table exist?
        if exists_bq_table(source_table_id):
            print("Source table id is valid.\n")
        else:
            print("Source table id doesn't exist, cannot publish.")
            exit(1)

        # does current dataset exist?
        current_dataset = ".".join(current_table_id.split('.')[:-1])
        current_dataset_exists = exists_bq_dataset(current_dataset)

        if current_dataset_exists:
            print(f"Dataset {current_dataset} is valid.")
        else:
            print(f"Dataset {current_dataset} doesn't exist, cannot publish.")
            exit(1)

        # does versioned dataset exist?
        versioned_dataset = ".".join(versioned_table_id.split('.')[:-1])
        versioned_dataset_exists = exists_bq_dataset(versioned_dataset)

        if versioned_dataset_exists:
            print(f"Dataset {versioned_dataset} is valid.\n")
        else:
            print(f"Dataset {versioned_dataset} doesn't exist, cannot publish.")
            exit(1)

        # display published table_ids
        print("Published table_ids (to be created--not yet published):")
        print(f"current table_id: {current_table_id}")
        print(f"versioned table_id: {versioned_table_id}\n")

        has_new_data = table_has_new_data(previous_versioned_table_id, source_table_id)

        # is there a previous version to compare with new table?
        # use previous_versioned_table_id
        if previous_versioned_table_id and has_new_data:
            print(f"New data found compared to previous published table {previous_versioned_table_id}.")
            print("Table will be published.\n")
        elif previous_versioned_table_id and not has_new_data:
            print(f"New table is identical to previous published table {previous_versioned_table_id}.")
            print("Table will not be published.\n")
        elif not previous_versioned_table_id:
            print(f"No previous version found for table, will publish.\n")
    else:
        if exists_bq_table(source_table_id):
            if table_has_new_data(previous_versioned_table_id, source_table_id):
                delay = 5

                print(f"""\n\nPublishing the following tables:""")
                print(f"\t - {versioned_table_id}\n\t - {current_table_id}")
                print(f"Proceed? Y/n (continues automatically in {delay} seconds)")

                response = str(input_with_timeout(seconds=delay)).lower()

                if response == 'n':
                    exit("\nPublish aborted; exiting.")

                print(f"\nPublishing {versioned_table_id}")
                copy_bq_table(params, source_table_id, versioned_table_id, replace_table=params['OVERWRITE_PROD_TABLE'])

                print(f"Publishing {current_table_id}")
                copy_bq_table(params, source_table_id, current_table_id, replace_table=params['OVERWRITE_PROD_TABLE'])

                print(f"Updating friendly name for {versioned_table_id}")
                update_friendly_name(params, table_id=versioned_table_id)

                if previous_versioned_table_id:
                    print(f"Archiving {previous_versioned_table_id}")
                    change_status_to_archived(previous_versioned_table_id)
                    print()

            else:
                print(f"{source_table_id} not published, no changes detected")


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

    if not compare_result:
        return True

    for row in compare_result:
        return True if row else False


def update_friendly_name(params: Params, table_id: str, custom_name: Optional[str] = None) -> Optional[None]:
    """
    Modify BigQuery table's friendly name.
    :param params: API params, supplied via yaml config
    :param table_id: table id in standard SQL format
    if is_gdc and no custom_name is specified, , we add REL before the version onto the existing friendly name;
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
        # todo how are we going to handle friendly names with CDA workflow?
        if params['DC_SOURCE'].lower() == 'gdc':
            friendly_name = f"{table.friendly_name} REL{params['RELEASE']} versioned"
        else:
            friendly_name = f"{table.friendly_name} {params['RELEASE']} versioned"

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
        print("Couldn't find a table to archive. Most likely, this is the table's first release. "
              "If not, there's an issue.")


def find_most_recent_published_table_id(params, versioned_table_id):
    """
    Function for locating published table id for dataset's previous release, if it exists
    :param params: params supplied in yaml config
    :param versioned_table_id: public versioned table id for current release
    :return: last published table id, if any; otherwise None
    """
    if params['DC_SOURCE'].lower() == 'gdc':
        oldest_etl_release = 260  # the oldest table release we published
        current_gdc_release_number = params['DC_RELEASE'][1:]

        # this shift allows for non-int release versions, e.g. r33.1
        last_gdc_release = float(current_gdc_release_number.replace('p', '.')) * 10 - 1

        # remove release version from versioned_table_id
        table_id_without_release = versioned_table_id.replace(params['DC_RELEASE'], '')

        # iterate through all possible previous releases to find a matching release
        for release in range(int(last_gdc_release), oldest_etl_release - 1, -1):
            # if release is 270, shifts to 27.0--replace p0 gives whole number, e.g. r27
            # if release is 271, shifts to 27.1, giving r27p1, which is legal for BQ table naming
            previous_release = params['DC_RELEASE'][0] + str(float(release) / 10).replace('.', 'p').replace('p0', '')
            prev_release_table_id = f"{table_id_without_release}{previous_release}"
            if exists_bq_table(prev_release_table_id):
                # found last release table, stop iterating
                return prev_release_table_id

        # if there is no previously-published table, return None
        return None
    elif params['DC_SOURCE'].lower() == 'pdc':
        # Assuming PDC will use 2-digit minor releases--they said they didn't expect this to ever become 3 digits, and
        # making 900 extraneous calls to google seems wasteful.
        max_minor_release_num = 99
        dc_release = params['DC_RELEASE'].replace("V", "")
        split_current_etl_release = dc_release.split("_")
        # set to current release initially, decremented in loop
        last_major_rel_num = int(split_current_etl_release[0])
        last_minor_rel_num = int(split_current_etl_release[1])

        while True:
            if last_minor_rel_num > 0 and last_major_rel_num >= 1:
                last_minor_rel_num -= 1
            elif last_minor_rel_num == 0 and last_major_rel_num > 1:
                # go from version (n).0 to version (n-1).99
                last_major_rel_num -= 1
                last_minor_rel_num = max_minor_release_num
            else:
                return None

            table_id_no_release = versioned_table_id.replace(f"_{params['DC_RELEASE']}", '')
            prev_release_table_id = f"{table_id_no_release}_V{last_major_rel_num}_{last_minor_rel_num}"

            if exists_bq_table(prev_release_table_id):
                # found last release table, stop iterating
                return prev_release_table_id
    else:
        has_fatal_error(f"Need to create find_most_recent_published_table_id function for {params['DC_SOURCE']}.")


def update_table_schema_from_generic(params, table_id, schema_tags=None, metadata_file=None):
    """
    Insert schema tags into generic schema (currently located in BQEcosystem repo).
    :param params: params from YAML config
    :param table_id: table_id where schema metadata should be inserted
    :param schema_tags: schema tags used to populate generic schema metadata
    :param metadata_file: name of generic table metadata file
    """
    if schema_tags is None:
        schema_tags = dict()

    release = params['DC_RELEASE']

    if params['DC_SOURCE'].lower() == 'gdc':
        release = release.replace('r', '')

    # remove underscore, add decimal to version number
    schema_tags['version'] = ".".join(release.split('_'))
    schema_tags['extracted-month-year'] = params['EXTRACTED_MONTH_YEAR']

    # gdc uses this
    if 'RELEASE_NOTES_URL' in params:
        schema_tags['release-notes-url'] = params['RELEASE_NOTES_URL']

    add_generic_table_metadata(params=params,
                               table_id=table_id,
                               schema_tags=schema_tags,
                               metadata_file=metadata_file)
    add_column_descriptions(params=params, table_id=table_id)


def add_generic_table_metadata(params: Params, table_id: str, schema_tags: dict[str, str], metadata_file: str = None):
    """
    todo
    :param params: bq_params supplied in yaml config
    :param table_id: table id for which to add the metadata
    :param schema_tags: dictionary of generic schema tag keys and values
    :param metadata_file:
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
        update_table_metadata(table_id, table_metadata)


def update_table_metadata(table_id: str, metadata: dict[str, str]):
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


def add_column_descriptions(params, table_id):
    """
    Alter an existing table's schema (currently, only column descriptions are mutable without a table rebuild,
    Google's restriction).
    :param params:
    :param table_id:
    :return:
    """
    print("\t - Adding column descriptions!")

    column_desc_fp = f"{params['BQ_REPO']}/{params['COLUMN_DESCRIPTION_FILEPATH']}"
    column_desc_fp = get_filepath(column_desc_fp)

    if not os.path.exists(column_desc_fp):
        has_fatal_error("BQEcosystem column description path not found", FileNotFoundError)
    with open(column_desc_fp) as column_output:
        descriptions = json.load(column_output)

    update_schema(table_id, descriptions)


def update_schema(table_id, new_descriptions):
    """
    Modify an existing table's field descriptions.
    :param table_id: table id in standard SQL format
    :param new_descriptions: dict of field names and new description strings
    """
    client = bigquery.Client()
    table = client.get_table(table_id)

    new_schema = []

    for schema_field in table.schema:
        column = schema_field.to_api_repr()

        if column['name'] in new_descriptions.keys():
            name = column['name']
            column['description'] = new_descriptions[name]
        elif column['description'] == '':
            print(f"Still no description for field: {column['name']}")

        mod_column = bigquery.SchemaField.from_api_repr(column)
        new_schema.append(mod_column)

    table.schema = new_schema

    client.update_table(table, ['schema'])


def get_project_program_names(params, project_submitter_id):
    """
    Get project short name, program short name and project name for given project submitter id.
    :param params: params from YAML config
    :param project_submitter_id: Project submitter id for which to retrieve names
    :return: tuple containing (project_short_name, program_short_name, project_name) strings
    """

    # todo incorrect, fix
    study_table_name = "studies"
    study_table_id = f"{params['DEV_PROJECT']}.{params['META_DATASET']}.{study_table_name}"

    query = f"""
        SELECT project_short_name, program_short_name, project_name, project_friendly_name, program_labels
        FROM {study_table_id}
        WHERE project_submitter_id = '{project_submitter_id}'
        LIMIT 1
    """

    res = query_and_retrieve_result(sql=query)

    for row in res:
        if not row:
            has_fatal_error(f"No result for query: {query}")
        project_short_name = row[0]
        program_short_name = row[1]
        project_name = row[2]
        project_friendly_name = row[3]
        program_labels = row[4]

        project_name_dict = {
            "project_short_name": project_short_name,
            "program_short_name": program_short_name,
            "project_name": project_name,
            "project_friendly_name": project_friendly_name,
            "program_labels": program_labels
        }

        return project_name_dict


def get_project_level_schema_tags(params, project_submitter_id):
    """
    Get project-level schema tags for populating generic table metadata schema.
    :param params: params from YAML config
    :param project_submitter_id: Project submitter id for which to retrieve schema tags
    :return: Dict of schema tags
    """
    project_name_dict = get_project_program_names(params, project_submitter_id)

    program_labels_list = project_name_dict['program_labels'].split("; ")

    if len(program_labels_list) > 2:
        has_fatal_error("PDC clinical isn't set up to handle >2 program labels yet; support needs to be added.")
    elif len(program_labels_list) == 0:
        has_fatal_error(f"No program label included for {project_submitter_id}, please add to PDCStudy.yaml")
    elif len(program_labels_list) == 2:
        return {
            "project-name": project_name_dict['project_name'],
            "mapping-name": "",  # only used by clinical, but including it elsewhere is harmless
            "friendly-project-name-upper": project_name_dict['project_friendly_name'],
            "program-name-0-lower": program_labels_list[0].lower(),
            "program-name-1-lower": program_labels_list[1].lower()
        }
    else:
        return {
            "project-name": project_name_dict['project_name'],
            "mapping-name": "",  # only used by clinical, but including it elsewhere is harmless
            "friendly-project-name-upper": project_name_dict['project_friendly_name'],
            "program-name-lower": project_name_dict['program_labels'].lower()
        }


def get_program_schema_tags_gdc(params, program_name):
    metadata_mappings_path = f"{params['BQ_REPO']}/{params['PROGRAM_METADATA_DIR']}"
    program_metadata_fp = get_filepath(f"{metadata_mappings_path}/{params['PROGRAM_METADATA_FILE']}")

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
            has_fatal_error("Did not find program_label OR program_label_0 and program_label_1 in schema json file.")

        return schema_tags
