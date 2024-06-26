"""
Copyright 2020, Institute for Systems Biology

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
import io
import json
import os
import sys
import time
import re
import datetime
import requests
import yaml
import select
import csv
from distutils import util
import traceback

from google.api_core.exceptions import NotFound, BadRequest
from google.cloud import bigquery, storage, exceptions

from common_etl.support import bq_harness_with_result, compare_two_tables_sql

#   API HELPERS


def get_graphql_api_response(api_params, query, fail_on_error=True):
    """
    Create and submit graphQL API request, returning API response serialized as json object.
    :param api_params: api_params supplied in yaml config
    :param query: GraphQL-formatted query string
    :param fail_on_error: if True, will fail fast--otherwise, tries up to 3 times before failing. False is good for
    longer paginated queries, which often throw random server errors
    :return: json response object
    """
    max_retries = 10

    headers = {'Content-Type': 'application/json'}
    endpoint = api_params['ENDPOINT']

    if not query:
        has_fatal_error("Must specify query for get_graphql_api_response.", SyntaxError)

    req_body = {'query': query}
    api_res = requests.post(endpoint, headers=headers, json=req_body)

    tries = 0

    while not api_res.ok and tries < max_retries:
        if api_res.status_code == 400:
            # don't try again!
            has_fatal_error(
                f"Response status code {api_res.status_code}:\n{api_res.reason}.\nRequest body:\n{req_body}")

        print(f"Response code {api_res.status_code}: {api_res.reason}")
        print(query)
        sleep_time = 3 * tries
        print(f"Retry {tries} of {max_retries}... sleeping for {sleep_time}")
        time.sleep(sleep_time)

        api_res = requests.post(endpoint, headers=headers, json=req_body)

        tries += 1

        if tries > max_retries:
            # give up!
            api_res.raise_for_status()

    json_res = api_res.json()

    if 'errors' in json_res and json_res['errors']:
        if fail_on_error:
            has_fatal_error(f"Errors returned by {endpoint}.\nError json:\n{json_res['errors']}")

    return json_res


def get_rel_prefix(api_params):
    """
    Get API release version (set in yaml config).
    :param api_params: API params, supplied via yaml config
    :return: release version number (with prefix, if included)
    """
    rel_prefix = ''

    if 'REL_PREFIX' in api_params and api_params['REL_PREFIX']:
        rel_prefix += api_params['REL_PREFIX']

    if 'RELEASE' in api_params and api_params['RELEASE']:
        rel_number = api_params['RELEASE']
        rel_prefix += rel_number

    return rel_prefix


#   BIGQUERY TABLE/DATASET OBJECT MODIFIERS


def delete_bq_table(table_id):
    """
    Permanently delete BigQuery table located by table_id.
    :param table_id: table id in standard SQL format
    """
    client = bigquery.Client()
    client.delete_table(table_id, not_found_ok=True)


def delete_bq_dataset(dataset_id):
    """
    Permanently delete BigQuery dataset.
    :param dataset_id: dataset_id for deletion
    """
    client = bigquery.Client()
    client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)


def update_table_metadata(table_id, metadata):
    """
    Modify an existing BigQuery table's metadata (labels, friendly name, description) using metadata dict argument
    :param table_id: table id in standard SQL format
    :param metadata: metadata containing new field and table attributes
    """
    client = bigquery.Client()
    table = get_bq_table_obj(table_id)

    table.labels = metadata['labels']
    table.friendly_name = metadata['friendlyName']
    table.description = metadata['description']
    client.update_table(table, ["labels", "friendly_name", "description"])

    assert table.labels == metadata['labels']
    assert table.friendly_name == metadata['friendlyName']
    assert table.description == metadata['description']


def update_table_labels(table_id, labels_to_remove_list=None, labels_to_add_dict=None):
    """
    Alter table labels for existing BigQuery table (e.g. when changes are necessary for a published table's labels).
    :param table_id: target BigQuery table id
    :param labels_to_remove_list: optional list of label keys to remove
    :param labels_to_add_dict: optional dictionary of label key-value pairs to add to table metadata
    """
    client = bigquery.Client()
    table = get_bq_table_obj(table_id)

    print(f"Processing labels for {table_id}")

    labels = table.labels

    if labels_to_remove_list and isinstance(labels_to_remove_list, list):
        for label in labels_to_remove_list:
            if label in labels:
                del labels[label]
                table.labels[label] = None
        print(f"Deleting label(s)--now: {labels}")
    elif labels_to_remove_list and not isinstance(labels_to_remove_list, list):
        has_fatal_error("labels_to_remove_list not provided in correct format, should be a list.")

    if labels_to_add_dict and isinstance(labels_to_add_dict, dict):
        labels.update(labels_to_add_dict)
        print(f"Adding/Updating label(s)--now: {labels}")
    elif labels_to_add_dict and not isinstance(labels_to_add_dict, dict):
        has_fatal_error("labels_to_add_dict not provided in correct format, should be a dict.")

    table.labels = labels
    client.update_table(table, ["labels"])

    assert table.labels == labels
    print("Labels updated successfully!\n")


def update_friendly_name(api_params, table_id, is_gdc=None, custom_name=None):
    """
    Modify BigQuery table's friendly name.
    :param api_params: API params, supplied via yaml config
    :param table_id: table id in standard SQL format
    :param custom_name: By default, appends "'REL' + api_params['RELEASE'] + ' VERSIONED'"
    :param is_gdc: If this is GDC, we add REL before the version onto the existing friendly name;
        if custom_name is specified, this behavior is overridden, and the table's friendly name is replaced entirely.
    """
    client = bigquery.Client()
    table = get_bq_table_obj(table_id)

    if custom_name:
        new_name = custom_name
    else:
        if is_gdc:
            release_str = ' REL' + api_params['RELEASE']
        else:
            release_str = ' ' + api_params['RELEASE']

        new_name = table.friendly_name + release_str + ' VERSIONED'

    table.friendly_name = new_name
    client.update_table(table, ["friendly_name"])

    assert table.friendly_name == new_name


def update_schema(table_id, new_descriptions):
    """
    Modify an existing table's field descriptions.
    :param table_id: table id in standard SQL format
    :param new_descriptions: dict of field names and new description strings
    """
    client = bigquery.Client()
    table = get_bq_table_obj(table_id)

    new_schema = []

    for schema_field in table.schema:
        field = schema_field.to_api_repr()

        if field['name'] in new_descriptions.keys():
            name = field['name']
            field['description'] = new_descriptions[name]
        elif field['description'] == '':
            print(f"Still no description for field: {field['name']}")

        mod_field = bigquery.SchemaField.from_api_repr(field)
        new_schema.append(mod_field)

    table.schema = new_schema

    client.update_table(table, ['schema'])


def add_generic_table_metadata(bq_params, table_id, schema_tags, metadata_file=None):
    """
    todo
    :param bq_params: bq_params supplied in yaml config
    :param table_id: table id for which to add the metadata
    :param schema_tags: dictionary of generic schema tag keys and values
    :param metadata_file:
    """
    generic_schema_path = f"{bq_params['BQ_REPO']}/{bq_params['GENERIC_SCHEMA_DIR']}"

    if not metadata_file:
        metadata_fp = get_filepath(f"{generic_schema_path}/{bq_params['GENERIC_TABLE_METADATA_FILE']}")
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


def add_column_descriptions(bq_params, table_id):
    """
    Alter an existing table's schema (currently, only field descriptions are mutable without a table rebuild,
    Google's restriction).
    :param bq_params:
    :param table_id:
    :return:
    """
    print("\t - Adding column descriptions!")

    field_desc_fp = f"{bq_params['BQ_REPO']}/{bq_params['FIELD_DESCRIPTION_FILEPATH']}"
    field_desc_fp = get_filepath(field_desc_fp)

    if not os.path.exists(field_desc_fp):
        has_fatal_error("BQEcosystem field description path not found", FileNotFoundError)
    with open(field_desc_fp) as field_output:
        descriptions = json.load(field_output)

    update_schema(table_id, descriptions)


#   BIGQUERY UTILS


def copy_bq_table(bq_params, src_table, dest_table, replace_table=False):
    """
    Copy an existing BigQuery src_table into location specified by dest_table.
    :param bq_params: bq param object from yaml config
    :param src_table: ID of table to copy
    :param dest_table: ID of table create
    :param replace_table: Replace existing table, if one exists; defaults to False
    """
    client = bigquery.Client()

    job_config = bigquery.CopyJobConfig()

    if replace_table:
        delete_bq_table(dest_table)

    bq_job = client.copy_table(src_table, dest_table, job_config=job_config)

    if await_job(bq_params, client, bq_job):
        print("Successfully copied table:")
        print(f"src: {src_table}\n dest: {dest_table}\n")


def exists_bq_table(table_id):
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


def get_bq_table_obj(table_id):
    """

    Get the bq table object referenced by table_id.
    :param table_id: table id in standard SQL format
    :return: BigQuery Table object
    """
    if not exists_bq_table(table_id):
        return None

    client = bigquery.Client()
    return client.get_table(table_id)


def list_bq_tables(dataset_id, release=None):
    """
    Generate list of all tables which exist for given dataset id.
    :param dataset_id: BigQuery dataset_id
    :param release: API release version
    :return: list of bq tables for dataset
    """
    table_list = list()
    client = bigquery.Client()
    tables = client.list_tables(dataset_id)

    for table in tables:
        if not release or release in table.table_id:
            table_list.append(table.table_id)

    return table_list


def change_status_to_archived(archived_table_id):
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
        print("Couldn't find a table to archive. Might be that this is the first table release?")


def publish_new_version_tables(bq_params, previous_table_id, current_table_id):
    """
    todo
    :param bq_params:
    :param previous_table_id:
    :param current_table_id:
    :return:
    """
    if not previous_table_id:
        return True

    compare_result = bq_harness_with_result(sql=compare_two_tables_sql(previous_table_id, current_table_id),
                                            do_batch=bq_params['DO_BATCH'],
                                            verbose=False)
    if not compare_result:
        return True

    for row in compare_result:
        if row:
            return True
        else:
            return False

    return False


def input_with_timeout(seconds):
    input_poll = select.poll()
    input_poll.register(sys.stdin.fileno(), select.POLLIN)

    while True:
        events = input_poll.poll(seconds * 1000)  # milliseconds

        if not events:
            return None

        for fileno, event in events:
            if fileno == sys.stdin.fileno():
                return input()


def test_table_for_version_changes(api_params, bq_params, public_dataset, source_table_id, get_publish_table_ids,
                                   find_most_recent_published_table_id, id_keys="case_id"):

    current_table_id, versioned_table_id = get_publish_table_ids(api_params, bq_params,
                                                                 source_table_id=source_table_id,
                                                                 public_dataset=public_dataset)

    previous_versioned_table_id = find_most_recent_published_table_id(api_params, versioned_table_id)

    publish_new_version = publish_new_version_tables(bq_params, previous_versioned_table_id, source_table_id)

    if exists_bq_table(source_table_id):
        print(f"""
            Current source table: {source_table_id}
            Last published table: {previous_versioned_table_id}
            Publish new version? {publish_new_version}
            """)

        if publish_new_version:
            print(f"""
                Tables to publish:
                - {versioned_table_id}
                - {current_table_id}
                """)

            output_compare_tables_report(api_params, bq_params,
                                         get_publish_table_ids=get_publish_table_ids,
                                         find_most_recent_published_table_id=find_most_recent_published_table_id,
                                         source_table_id=source_table_id,
                                         public_dataset=public_dataset,
                                         id_keys=id_keys)


def publish_table(api_params, bq_params, public_dataset, source_table_id, get_publish_table_ids,
                  find_most_recent_published_table_id, overwrite=False, test_mode=True, id_keys="case_id"):
    """
    Publish production BigQuery tables using source_table_id:
        - create current/versioned table ids
        - publish tables
        - update friendly name for versioned table
        - change last version tables' status labels to archived
    :param id_keys:
    :param api_params: api params from yaml config
    :param bq_params: bq params from yaml config
    :param public_dataset: publish dataset location
    :param source_table_id: source (dev) table id
    :param overwrite: If True, replace existing BigQuery table; defaults to False
    :param get_publish_table_ids: function that returns public table ids based on the source table id
    :param find_most_recent_published_table_id: function that returns previous versioned table id, if any
    :param test_mode: outputs the source table id, versioned and current published table ids,
           last published table id (if any) and whether the dataset would be published if test_mode=False
    """

    current_table_id, versioned_table_id = get_publish_table_ids(api_params, bq_params,
                                                                 source_table_id=source_table_id,
                                                                 public_dataset=public_dataset)

    previous_versioned_table_id = find_most_recent_published_table_id(api_params, versioned_table_id)

    publish_new_version = publish_new_version_tables(bq_params, previous_versioned_table_id, source_table_id)

    if exists_bq_table(source_table_id):
        if publish_new_version:
            delay = 5

            print(f"""\n\nPublishing the following tables:""")
            print(f"\t - {versioned_table_id}\n\t - {current_table_id}")
            print(f"Proceed? Y/n (continues automatically in {delay} seconds)")

            response = input_with_timeout(seconds=delay)

            response = str(response).lower()
            print()

            if response == 'n':
                exit("Publish aborted; exiting.")

            print(f"Publishing {versioned_table_id}")
            copy_bq_table(bq_params, source_table_id, versioned_table_id, overwrite)
    
            print(f"Publishing {current_table_id}")
            copy_bq_table(bq_params, source_table_id, current_table_id, overwrite)
    
            print(f"Updating friendly name for {versioned_table_id}")
            is_gdc = True if api_params['DATA_SOURCE'] == 'gdc' else False
            update_friendly_name(api_params, table_id=versioned_table_id, is_gdc=is_gdc)

            if previous_versioned_table_id:
                print(f"Archiving {previous_versioned_table_id}")
                change_status_to_archived(previous_versioned_table_id)
                print()

        else:
            print(f"{source_table_id} not published, no changes detected")


def await_insert_job(bq_params, client, table_id, bq_job):
    """
    Monitor for completion of BigQuery Load or Query Job that produces some result (generally data insertion).
    :param bq_params: bq params from yaml config file
    :param client: BigQuery api object, allowing for execution of bq lib functions
    :param table_id: table id in standard SQL format
    :param bq_job: A Job object, responsible for executing bq function calls
    """
    last_report_time = time.time()
    location = bq_params['LOCATION']
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
        has_fatal_error(
            f'While running BigQuery job: {bq_job.error_result}\n{bq_job.errors}',
            ValueError)

    table = client.get_table(table_id)

    if table.num_rows == 0:
        has_fatal_error(f"[ERROR] Insert job for {table_id} inserted 0 rows. Exiting.")

    print(f" done. {table.num_rows} rows inserted.")


def await_job(bq_params, client, bq_job):
    """
    Monitor the completion of BigQuery Job which doesn't return a result.
    :param bq_params: bq params from yaml config file
    :param client: BigQuery api object, allowing for execution of bq lib functions
    :param bq_job: A Job object, responsible for executing bq function calls
    """
    location = bq_params['LOCATION']
    job_state = "NOT_STARTED"

    while job_state != 'DONE':
        bq_job = client.get_job(bq_job.job_id, location=location)

        job_state = bq_job.state

        if job_state != 'DONE':
            time.sleep(2)

    bq_job = client.get_job(bq_job.job_id, location=location)

    if bq_job.error_result is not None:
        err_res = bq_job.error_result
        errs = bq_job.errors
        has_fatal_error(f"While running BigQuery job: {err_res}\n{errs}")


def construct_table_name(api_params, prefix, suffix=None, include_release=True, release=None):
    """
    Generate BigQuery-safe table name using supplied parameters.
    :param api_params: API params supplied in yaml config
    :param prefix: table prefix or the base table's root name
    :param suffix: table suffix, optionally supplying another word to append to the prefix
    :param include_release: If False, excludes RELEASE value set in yaml config; defaults to True
    :param release: Optionally supply a custom release (useful for external mapping tables, etc)
    :return: Table name, formatted to be compatible with BigQuery's naming limitations (only: A-Z, a-z, 0-9, _)
    """
    table_name = prefix

    if suffix:
        table_name += '_' + suffix

    if release:
        table_name += '_' + release
    elif include_release:
        table_name += '_' + api_params['RELEASE']

    return re.sub('[^0-9a-zA-Z_]+', '_', table_name)


def construct_table_name_from_list(str_list):
    """
    Construct a table name (str) from list<str>.
    :param str_list: a list<str> of table name segments
    :return: composed table name string
    """
    table_name = "_".join(str_list)

    # replace '.' with '_' so that the name is valid ('.' chars not allowed -- issue with BEATAML1.0, for instance)
    return re.sub('[^0-9a-zA-Z_]+', '_', table_name)


def construct_table_id(project, dataset, table_name):
    """
    Build table_id in {project}.{dataset}.{table} format.
    :param project: BigQuery project id
    :param dataset: BigQuery dataset id
    :param table_name: BigQuery table name
    :return: joined table_id in BigQuery format
    """
    return f'{project}.{dataset}.{table_name}'


def create_and_load_table_from_jsonl(bq_params, jsonl_file, table_id, schema=None):
    """
    Create new BigQuery table, populating rows using contents of jsonl file.
    :param bq_params: bq param obj from yaml config
    :param jsonl_file: file containing case records in jsonl format
    :param schema: list of SchemaFields representing desired BigQuery table schema
    :param table_id: table_id to be created
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

    load_create_table_job(bq_params, jsonl_file, client, table_id, job_config)


def create_and_load_table_from_tsv(bq_params, tsv_file, table_id, num_header_rows, schema=None, null_marker=None):
    """
    Create new BigQuery table, populating rows using contents of tsv file.
    :param bq_params: bq param obj from yaml config
    :param tsv_file: file containing records in tsv format
    :param schema: list of SchemaFields representing desired BigQuery table schema
    :param table_id: table_id to be created
    :param num_header_rows: int value representing number of header rows in file (skipped during processing)
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

    load_create_table_job(bq_params, tsv_file, client, table_id, job_config)


def get_query_results(query):
    """
    Return BigQuery query result (RowIterator) object.
    :param query: query string
    :return: query result object (RowIterator)
    """
    try:
        client = bigquery.Client()
        query_job = client.query(query)
        return query_job.result()
    except BadRequest:
        return None


def load_table_from_query(bq_params, table_id, query):
    """
    Create new BigQuery table using result output of BigQuery SQL query.
    :param bq_params: bq params from yaml config file
    :param table_id: table id in standard SQL format
    :param query: data selection query, used to populate a new BigQuery table
    """
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig(destination=table_id)
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    try:
        query_job = client.query(query, job_config=job_config)
        print(f' - Inserting into {table_id}... ', end="")
        await_insert_job(bq_params, client, table_id, query_job)
    except TypeError as err:
        has_fatal_error(err)


def create_view_from_query(view_id, view_query):
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


def load_bq_schema_from_json(bq_params, filename):
    """
    Open table schema file from BQEcosystem Repo and convert to python dict, in preparation for use in table creation.
    :param bq_params: bq param object from yaml config
    :param filename: name of the schema file
    :return: tuple(<list of schema fields>, <dict containing table metadata>)
    """

    fp = get_filepath(bq_params['BQ_REPO'] + "/" + bq_params['SCHEMA_DIR'], filename)

    if not os.path.exists(fp):
        has_fatal_error("BQEcosystem schema path not found", FileNotFoundError)

    with open(fp, 'r') as schema_file:
        schema_file = json.load(schema_file)

        if 'schema' not in schema_file:
            has_fatal_error("['schema'] not found in schema json file")
        elif 'fields' not in schema_file['schema']:
            has_fatal_error("['schema']['fields'] not found in schema json file")
        elif not schema_file['schema']['fields']:
            has_fatal_error("['schema']['fields'] contains no key:value pairs")
        return schema_file['schema']['fields']


def load_create_table_job(bq_params, data_file, client, table_id, job_config):
    """
    Generate BigQuery LoadJob, for creating and populating table.
    :param bq_params: bq param obj from yaml config
    :param data_file: file containing case records
    :param client: BigQuery Client obj
    :param table_id: table_id to be created
    :param job_config: LoadJobConfig object
    """
    gs_uri = f"gs://{bq_params['WORKING_BUCKET']}/{bq_params['WORKING_BUCKET_DIR']}/{data_file}"

    try:
        load_job = client.load_table_from_uri(source_uris=gs_uri,
                                              destination=table_id,
                                              job_config=job_config)
        print(f' - Inserting into {table_id}... ', end="")
        await_insert_job(bq_params, client, table_id, load_job)
    except TypeError as err:
        has_fatal_error(err)

#   GOOGLE CLOUD STORAGE UTILS

def upload_to_bucket(bq_params, scratch_fp, delete_local=False, verbose=True):

    """
    Upload file to a Google storage bucket (bucket/directory location specified in YAML config).
    :param bq_params: bq param object from yaml config
    :param scratch_fp: name of file to upload to bucket
    :param delete_local: delete scratch file created on VM
    :param verbose: if True, print a confirmation for each file uploaded
    """
    if not os.path.exists(scratch_fp):
        has_fatal_error(f"Invalid filepath: {scratch_fp}", FileNotFoundError)

    try:
        storage_client = storage.Client(project="")

        jsonl_output_file = scratch_fp.split('/')[-1]
        bucket_name = bq_params['WORKING_BUCKET']
        bucket = storage_client.bucket(bucket_name)

        blob_name = f"{bq_params['WORKING_BUCKET_DIR']}/{jsonl_output_file}"
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(scratch_fp)

        if verbose:
            print(f"Successfully uploaded file to {bucket_name}/{blob_name}. ", end="")

        if delete_local:
            os.remove(scratch_fp)
            if verbose:
                print("Local file deleted.")
        else:
            if verbose:
                print(f"Local file not deleted.")

    except exceptions.GoogleCloudError as err:
        has_fatal_error(f"Failed to upload to bucket.\n{err}")
    except FileNotFoundError as err:
        has_fatal_error(f"File not found, failed to access local file.\n{err}")


def download_from_bucket(bq_params, filename, bucket_path=None, dir_path=None, timeout=None):
    """
    Download file from Google storage bucket onto VM.
    :param bq_params: BigQuery params, used to retrieve default bucket directory path
    :param filename: Name of file to download
    :param bucket_path: Optional, override default bucket directory path
    :param dir_path: Optional, location in which to download file; if not specified, defaults to scratch folder defined in bq_params
    :param timeout: Optional, integer value in seconds--how long to wait before file download is considered a failure
    """
    if not dir_path:
        file_path = get_scratch_fp(bq_params, filename)
    else:
        file_path = f"{dir_path}/{filename}"

    if os.path.isfile(file_path):
        os.remove(file_path)

    storage_client = storage.Client(project="")
    if bucket_path:
        blob_name = f"{bucket_path}/{filename}"
    else:
        blob_name = f"{bq_params['WORKING_BUCKET_DIR']}/{filename}"
    bucket = storage_client.bucket(bq_params['WORKING_BUCKET'])
    blob = bucket.blob(blob_name)

    with open(file_path, 'wb') as file_obj:
        blob.download_to_file(file_obj)

    start_time = time.time()

    """
    if timeout:
        while not os.path.isfile(file_path):
            if time.time() - start_time > timeout:
                print(f"ERROR: File download from bucket failed. Source: {blob_name}, Destination: {file_path}")
                exit()
            else:
                print(f"File {filename} not yet downloaded, waiting.")
                time.sleep(2)
    """

    if os.path.isfile(file_path):
        print(f"File successfully downloaded from bucket to {file_path}")


#   I/O - FILESYSTEM HELPERS

def get_filepath(dir_path, filename=None):
    """
    Get file path for location on VM; expands compatibly for local or VM scripts.
    :param dir_path: directory portion of the filepath (starting at user home dir)
    :param filename: name of the file
    :return: full path to file
    """
    join_list = [os.path.expanduser('~'), dir_path]

    if filename:
        join_list.append(filename)

    return '/'.join(join_list)


def get_scratch_fp(params, filename):
    """
    Construct filepath for VM output file.
    :param filename: name of the file
    :param params: bq param object from yaml config
    :return: output filepath for VM
    """
    return get_filepath(params['SCRATCH_DIR'], filename)


def json_datetime_to_str_converter(obj):
    """
    Convert python datetime object to string (necessary for json serialization).
    :param obj: python datetime obj
    :return: datetime obj cast as str type
    """
    if isinstance(obj, datetime.datetime):
        return str(obj)
    if isinstance(obj, datetime.date):
        return str(obj)
    if isinstance(obj, datetime.time):
        return str(obj)


def write_list_to_jsonl(jsonl_fp, json_obj_list, mode='w'):
    """
    Create a jsonl file for uploading data into BigQuery from a list<dict> obj.
    :param jsonl_fp: local VM jsonl filepath
    :param json_obj_list: list of dicts representing json objects
    :param mode: 'a' if appending to a file that's being built iteratively;
                 'w' if file data is written in a single call to the function
                 (in which case any existing data is overwritten)
    """
    with open(jsonl_fp, mode) as file_obj:
        for line in json_obj_list:
            json.dump(obj=line, fp=file_obj, default=json_datetime_to_str_converter)
            file_obj.write('\n')


def write_line_to_jsonl(jsonl_fp, json_line):
    """
    Append dict to jsonl file.
    :param jsonl_fp: local VM jsonl filepath
    """
    with open(jsonl_fp, 'a') as file_obj:
        json.dump(obj=json_line, fp=file_obj, default=json_datetime_to_str_converter)
        file_obj.write('\n')


def write_list_to_jsonl_and_upload(api_params, bq_params, prefix, record_list, local_filepath=None):
    """
    Write joined_record_list to file name specified by prefix and uploads to scratch Google Cloud bucket.
    :param api_params: api_params supplied in yaml config
    :param bq_params: bq_params supplied in yaml config
    :param prefix: string representing base file name (release string is appended to generate filename)
    :param record_list: list of record objects to insert into jsonl file
    :param local_filepath: todo
    """
    if not local_filepath:
        jsonl_filename = get_filename(api_params, file_extension='jsonl', prefix=prefix)
        local_filepath = get_scratch_fp(bq_params, jsonl_filename)

    write_list_to_jsonl(local_filepath, record_list)
    upload_to_bucket(bq_params, local_filepath, delete_local=True)


def write_list_to_tsv(fp, tsv_list):
    """
    todo
    :param fp:
    :param tsv_list:
    :return:
    """
    with open(fp, "w") as tsv_file:
        for row in tsv_list:
            tsv_row = create_tsv_row(row)
            tsv_file.write(tsv_row)

    print(f"{len(tsv_list)} rows written to {fp}!")


def create_tsv_row(row_list, null_marker="None"):
    """
    Convert list of row values into a tab-delimited string.
    :param row_list: list of row values for conversion
    :param null_marker: Value to write to string for nulls
    :return: tab-delimited string representation of row_list
    """
    print_str = ''
    last_idx = len(row_list) - 1

    for i, column in enumerate(row_list):
        if not column:
            column = null_marker

        delimiter = "\t" if i < last_idx else "\n"
        print_str += column + delimiter

    return print_str


def get_filename(api_params, file_extension, prefix, suffix=None, include_release=True, release=None):
    """
    Get filename based on common table-naming (see construct_table_name).
    :param api_params: API params from YAML config
    :param file_extension: File extension, e.g. jsonl or tsv
    :param prefix: file name prefix
    :param suffix: file name suffix
    :param include_release: if True, includes release in file name; defaults to True
    :param release: data release version
    :return: file name
    """
    filename = construct_table_name(api_params, prefix, suffix, include_release, release=release)
    return f"{filename}.{file_extension}"


#   SCHEMA UTILS


def normalize_value(value, is_tsv=False):
    """
    If value is variation of null or boolean value, converts to single form (None, True, False);
    otherwise returns original value.
    :param value: value to convert
    :return: normalized (or original) value
    """

    if value is None:
        return value

    if isinstance(value, str):
        value = value.strip()

        if value in ('NA', 'N/A', 'n/a',
                     'None', '', '--', '-',
                     'NULL', 'Null', 'null',
                     'Not Reported', 'not reported', 'Not reported',
                     'unknown', 'Unknown'):
            if is_tsv:
                return ''
            else:
                return None
        elif value in ('False', 'false', 'FALSE', 'No', 'no', 'NO'):
            return "False"
        elif value in ('True', 'true', 'TRUE', 'Yes', 'yes', 'YES'):
            return "True"

    if is_int_value(value):
        try:
            cast_value = int(float(value))
            return cast_value
        except OverflowError:
            pass
    else:
        return value


def is_int_value(value):
    """
    todo
    :param value:
    :return:
    """
    def is_valid_decimal(val):
        try:
            float(val)
        except ValueError:
            return False
        except TypeError:
            return False
        else:
            return True

    def should_be_string(val):
        val = str(val)
        if val.startswith("0") and len(val) > 1 and ':' not in val and '-' not in val and '.' not in val:
            return True

    if should_be_string(value):
        return False

    if is_valid_decimal(value):
        try:
            if float(value) == int(float(value)):
                return True
        except OverflowError:
            return False

    try:
        int(value)
        return True
    except ValueError:
        return False
    except TypeError:
        return False


def check_value_type(value):
    """
    Check value for corresponding BigQuery type. Evaluates the following BigQuery column data types:
        - datetime formats: DATE, TIME, TIMESTAMP
        - number formats: INT64, FLOAT64, NUMERIC
        - misc formats: STRING, BOOL, ARRAY, RECORD
    :param is_tsv: todo
    :param value: value on which to perform data type analysis
    :return: data type in BigQuery Standard SQL format
    """
    def is_valid_decimal(val):
        try:
            float(val)
        except ValueError:
            return False
        except TypeError:
            return False
        else:
            return True

    if isinstance(value, bool):
        return "BOOL"
    # currently not working for tsv because we don't normalize those files prior to upload yet
    if is_valid_decimal(value):
        # If you don't cast a string to float before casting to int, it will throw a TypeError
        try:
            str_val = str(value)

            if str_val.startswith("0") and len(str_val) > 1 and ':' not in str_val \
                    and '-' not in str_val and '.' not in str_val:
                return "STRING"

            if float(value) == int(float(value)):
                return "INT64"
        except OverflowError:
            # can't cast float infinity to int
            pass
    if isinstance(value, float):
        return "FLOAT64"
    if value != value:  # NaN case
        return "FLOAT64"
    if isinstance(value, list):
        return "ARRAY"
    if isinstance(value, dict):
        return "RECORD"
    if not value:
        return None
    if isinstance(value, datetime.datetime):
        return "TIMESTAMP"
    if isinstance(value, datetime.date):
        return "DATE"
    if isinstance(value, datetime.time):
        return "TIME"

    # A sequence of numbers starting with a 0 represents a string id,
    # but int() check will pass and data loss would occur.
    if isinstance(value, str):
        if value.startswith("0") and len(value) > 1 and ':' not in value and '-' not in value and '.' not in value:
            return "STRING"

    # check to see if value is numeric, float or int;
    # differentiates between these types and datetime or ids, which may be composed of only numbers or symbols
    if '.' in value and ':' not in value and "E+" not in value and "E-" not in value:
        try:
            int(value)
            return "INT64"
        except ValueError:
            try:
                float(value)
                decimal_val = int(value.split('.')[1])

                # if digits right of decimal place are all zero, float can safely be cast as an int
                if not decimal_val:
                    return "INT64"
                return "FLOAT64"
            except ValueError:
                return "STRING"

    # numeric values are numbers with special encoding, like an exponent or sqrt symbol
    elif value.isnumeric() and not value.isdigit() and not value.isdecimal():
        return "NUMERIC"

    # no point in performing regex for this, it's just a string
    if value.count("-") > 2:
        return "STRING"

    """
    BIGQUERY'S CANONICAL DATE/TIME FORMATS:
    (see https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types)
    """

    if value.count("-") == 2 or value.count(":") == 2:
        # Check for BigQuery DATE format: 'YYYY-[M]M-[D]D'
        date_re_str = r"[0-9]{4}-(0[1-9]|1[0-2]|[0-9])-(0[1-9]|[1-2][0-9]|[3][0-1]|[1-9])"
        date_pattern = re.compile(date_re_str)
        if re.fullmatch(date_pattern, value):
            return "DATE"

        # Check for BigQuery TIME format: [H]H:[M]M:[S]S[.DDDDDD]
        time_re_str = r"([0-1][0-9]|[2][0-3]|[0-9]{1}):([0-5][0-9]|[0-9]{1}):([0-5][0-9]|[0-9]{1}])(\.[0-9]{1,6}|)"
        time_pattern = re.compile(time_re_str)
        if re.fullmatch(time_pattern, value):
            return "TIME"

        # Check for BigQuery TIMESTAMP format: YYYY-[M]M-[D]D[( |T)[H]H:[M]M:[S]S[.DDDDDD]][time zone]
        timestamp_re_str = date_re_str + r'( |T)' + time_re_str + r"([ \-:A-Za-z0-9]*)"
        timestamp_pattern = re.compile(timestamp_re_str)
        if re.fullmatch(timestamp_pattern, value):
            return "TIMESTAMP"

        return "STRING"

    try:
        util.strtobool(value)
        return "BOOL"
    except ValueError:
        pass

    # Final check for int and float values.
    # This will catch simple integers or edge case float values (infinity, scientific notation, etc.)
    try:
        int(value)
        return "INT64"
    except ValueError:
        try:
            float(value)
            return "FLOAT64"
        except ValueError:
            return "STRING"


def resolve_type_conflict(field, types_set):
    """
    Resolve BigQuery column data type precedence, where multiple types are detected. Rules for type conversion based on
    BigQuery's implicit conversion behavior.
    See https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_rules#coercion
    :param types_set: Set of BigQuery data types in string format
    :param field: field name
    :return: BigQuery data type with the highest precedence
    """

    datetime_types = {"TIMESTAMP", "DATE", "TIME"}
    number_types = {"INT64", "FLOAT64", "NUMERIC"}

    # remove null type value from set
    none_set = {None}
    types_set = types_set - none_set

    # fix to make even proper INT64 ids into STRING ids
    if "_id" in field:
        return "STRING"

    if len(types_set) == 0:
        # fields with no type values default to string--this would still be safe for skip-row analysis of a data file
        return "STRING"
    if len(types_set) == 1:
        # only one data type for field, return it
        return list(types_set)[0]

    # From here, the field's types_set contains at least two values; select based on BigQuery's implicit conversion
    # rules; when in doubt, declare a string to avoid risk of data loss

    if "ARRAY" in types_set or "RECORD" in types_set:
        # these types cannot be implicitly converted to any other, exit
        print(f"Invalid datatype combination for {field}: {types_set}")
        has_fatal_error("", TypeError)

    if "STRING" in types_set:
        # if it's partly classified as a string, it has to be a string--other data type values are converted
        return "STRING"

    if len(types_set) == 2 and "INT64" in types_set and "BOOL" in types_set:
        # 1 or 0 are labelled bool by type checker; any other ints are labelled as ints.
        # If both 1 and 0 occur, AND there are traditional Boolean values in the column, then it'll be declared a BOOL;
        # otherwise, it should be INT64
        return "INT64"

    has_datetime_type = False

    # are any of the data types datetime types -- {"TIMESTAMP", "DATE", "TIME"}?
    for datetime_type in datetime_types:
        if datetime_type in types_set:
            has_datetime_type = True
            break

    has_number_type = False

    # are any of the data types number types -- {"INT64", "FLOAT64", "NUMERIC"}?
    for number_type in number_types:
        if number_type in types_set:
            has_number_type = True
            break

    # What, data source?! Okay, fine, be a string
    if has_datetime_type and has_number_type:
        # another weird edge case that really shouldn't happen
        return "STRING"

    # Implicitly convert to inclusive datetime format
    if has_datetime_type:
        if "TIME" in types_set:
            # TIME cannot be implicitly converted to DATETIME
            return "STRING"
        # DATE and TIMESTAMP *can* be implicitly converted to DATETIME
        return "DATETIME"

    # Implicitly convert to inclusive number format
    if has_number_type:
        # only number types remain
        # INT64 and NUMERIC can be implicitly converted to FLOAT64
        # INT64 can be implicitly converted to NUMERIC
        if "FLOAT64" in types_set:
            return "FLOAT64"
        elif "NUMERIC" in types_set:
            return "NUMERIC"

    # No BOOL, DATETIME combinations allowed, or whatever other randomness occurs--return STRING
    return "STRING"


def resolve_type_conflicts(types_dict):
    """
    Iteratively resolve data type conflicts for non-nested type dicts (e.g. if there is more than one data type found,
    select the superseding type.)
    :param types_dict: dict containing columns and all detected data types
    :type types_dict: dict {str: set}
    :return dict containing the column name and its BigQuery data type.
    :rtype dict of {str: str}
    """
    type_dict = dict()

    for field, types_set in types_dict.items():
        type_dict[field] = resolve_type_conflict(field, types_set)

    return type_dict


def normalize_flat_json_values(records):
    normalized_json_list = list()

    for record in records:
        normalized_record = dict()
        for key in record.keys():
            value = normalize_value(record[key])
            normalized_record[key] = value
        normalized_json_list.append(normalized_record)

    return normalized_json_list


def recursively_normalize_field_values(json_records, is_single_record=False):
    """
    Recursively explores and normalizes a list of json objects. Useful when there's arbitrary nesting of dicts and
    lists with varying depths.
    :param json_records: list of json objects
    :param is_single_record: If true, json_records contains a single json object,
    otherwise contains a list of json objects
    :return: if is_single_record, returns normalized copy of the json object.
    if multiple records, returns a list of json objects.
    """
    def recursively_normalize_field_value(_obj, _data_set_dict):
        """
        Recursively explore a part of the supplied object. Traverses parent nodes, replicating existing data structures
        and normalizing values when reaching a "leaf" node.
        :param _obj: object in current location of recursion
        :param _data_set_dict: dict of fields and type sets
        """
        for key, value in _obj.items():
            if isinstance(_obj[key], dict):
                if key not in _data_set_dict:
                    # this is a dict, so use dict to nest values
                    _data_set_dict[key] = dict()

                recursively_normalize_field_value(_obj[key], _data_set_dict[key])
            elif isinstance(_obj[key], list) and len(_obj[key]) > 0 and isinstance(_obj[key][0], dict):
                if key not in _data_set_dict:
                    _data_set_dict[key] = list()

                idx = 0
                for _record in _obj[key]:
                    _data_set_dict[key].append(dict())
                    recursively_normalize_field_value(_record, _data_set_dict[key][idx])
                    idx += 1
            elif not isinstance(_obj[key], list) or (isinstance(_obj[key], list) and len(_obj[key]) > 0):
                # create set of Data type values
                if key not in _data_set_dict:
                    _data_set_dict[key] = dict()

                value = normalize_value(value)
                _data_set_dict[key] = value

    if is_single_record:
        record_dict = dict()
        recursively_normalize_field_value(json_records, record_dict)
        return record_dict
    else:
        new_record_jsonl_list = list()

        for record in json_records:
            record_dict = dict()
            recursively_normalize_field_value(record, record_dict)

            new_record_jsonl_list.append(record_dict)

        return new_record_jsonl_list


def recursively_detect_object_structures(nested_obj):
    """
    Traverse a dict or list of objects, analyzing the structure. Order not guaranteed (if anything, it'll be
    backwards)--Not for use with TSV data. Works for arbitrary nesting, even if object structure varies from record to
    record; use for lists, dicts, or any combination therein.
    If nested_obj is a list, function will traverse every record in order to find all possible fields.
    :param nested_obj: object to traverse
    :return data types dict--key is the field name, value is the set of BigQuery column data types returned
    when analyzing data using check_value_type ({<field_name>: {<data_type_set>}})
    """
    # stores the dict of {fields: value types}
    data_types_dict = dict()

    def recursively_detect_object_structure(_obj, _data_types_dict):
        """
        Recursively explore a part of the supplied object. Traverses parent nodes, adding to data_types_dict
        as repeated (RECORD) field objects. Adds child nodes parent's "fields" list.
        :param _obj: object in current location of recursion
        :param _data_types_dict: dict of fields and type sets
        """
        for k, v in _obj.items():
            if isinstance(_obj[k], dict):
                if k not in _data_types_dict:
                    # this is a dict, so use dict to nest values
                    _data_types_dict[k] = dict()

                recursively_detect_object_structure(_obj[k], _data_types_dict[k])
            elif isinstance(_obj[k], list) and len(_obj[k]) > 0 and isinstance(_obj[k][0], dict):
                if k not in _data_types_dict:
                    # this is a dict, so use dict to nest values
                    _data_types_dict[k] = dict()

                for _record in _obj[k]:
                    recursively_detect_object_structure(_record, _data_types_dict[k])
            elif not isinstance(_obj[k], list) or (isinstance(_obj[k], list) and len(_obj[k]) > 0):
                # create set of Data type values
                if k not in _data_types_dict:
                    _data_types_dict[k] = set()

                _obj[k] = normalize_value(_obj[k])
                val_type = check_value_type(_obj[k])

                if val_type:
                    _data_types_dict[k].add(val_type)

    if isinstance(nested_obj, dict):
        recursively_detect_object_structure(nested_obj, data_types_dict)
    elif isinstance(nested_obj, list):
        for record in nested_obj:
            recursively_detect_object_structure(record, data_types_dict)

    return data_types_dict


def convert_object_structure_dict_to_schema_dict(data_types_dict, dataset_format_obj, descriptions=None):
    """
    Parse dict of {<field>: {<data_types>}} representing data object's structure;
    convert into dict representing a TableSchema object.
    :param data_types_dict: dictionary represent dataset's structure, fields and data types
    :param dataset_format_obj: dataset format obj
    :param descriptions: (optional) dictionary of field: description string pairs for inclusion in schema definition
    """

    for k, v in data_types_dict.items():
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

            convert_object_structure_dict_to_schema_dict(data_types_dict[k], schema_field['fields'])
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

    return dataset_format_obj


def create_schema_field_obj(schema_obj, fields=None):
    """
    Output BigQuery SchemaField object.
    :param schema_obj: dict with schema field values
    :param fields: Optional, child SchemaFields for RECORD type column
    :return: SchemaField object
    """
    if fields:
        return bigquery.schema.SchemaField(name=schema_obj['name'],
                                           description=schema_obj['description'],
                                           field_type=schema_obj['type'],
                                           mode=schema_obj['mode'],
                                           fields=fields)
    else:
        return bigquery.schema.SchemaField(name=schema_obj['name'],
                                           description=schema_obj['description'],
                                           field_type=schema_obj['type'],
                                           mode=schema_obj['mode'])


def generate_bq_schema_field(schema_obj, schema_fields):
    """
    Convert schema field json dict object into SchemaField object.
    :param schema_obj: direct ancestor of schema_fields
    :param schema_fields: list of SchemaField objects
    """
    if not schema_obj:
        return
    elif schema_obj['type'] == 'RECORD':
        child_schema_fields = list()

        if not schema_obj['fields']:
            has_fatal_error("Schema object has 'type': 'RECORD' but no 'fields' key.")

        for child_obj in schema_obj['fields']:
            generate_bq_schema_field(child_obj, child_schema_fields)

        schema_field = create_schema_field_obj(schema_obj, child_schema_fields)
    else:
        schema_field = create_schema_field_obj(schema_obj)

    schema_fields.append(schema_field)


def generate_bq_schema_fields(schema_obj_list):
    """
    Convert list of schema fields into TableSchema object.
    :param schema_obj_list: list of dicts representing BigQuery SchemaField objects
    :returns list of BigQuery SchemaField objects (represents TableSchema object)
    """
    schema_fields_obj = list()

    for schema_obj in schema_obj_list:
        generate_bq_schema_field(schema_obj, schema_fields_obj)

    return schema_fields_obj


def retrieve_bq_schema_object(api_params, bq_params, table_name=None, release=None, include_release=True,
                              schema_filename=None, schema_dir=None):
    """
    todo
    :param api_params: api_params supplied in yaml config
    :param bq_params: bq_params supplied in yaml config
    :param table_name:
    :param release:
    :param include_release:
    :return:
    """
    if not schema_filename:
        schema_filename = get_filename(api_params=api_params,
                                       file_extension='json',
                                       prefix="schema",
                                       suffix=table_name,
                                       release=release,
                                       include_release=include_release)

    download_from_bucket(bq_params, filename=schema_filename, dir_path=schema_dir)

    if not schema_dir:
        schema_fp = get_scratch_fp(bq_params, schema_filename)
    else:
        schema_fp = f"{schema_dir}/{schema_filename}"

    with open(schema_fp, "r") as schema_json:
        schema_obj = json.load(schema_json)
        json_schema_obj_list = [field for field in schema_obj["fields"]]
        schema = generate_bq_schema_fields(json_schema_obj_list)

    return schema


def generate_and_upload_schema(api_params, bq_params, table_name, data_types_dict, include_release, release=None,
                               schema_fp=None, delete_local=True):
    """
    todo
    :param api_params: api_params supplied in yaml config
    :param bq_params: bq_params supplied in yaml config
    :param table_name:
    :param data_types_dict:
    :param include_release:
    :param release:
    :return:
    """
    schema_list = convert_object_structure_dict_to_schema_dict(data_types_dict, list())

    schema_obj = {
        "fields": schema_list
    }

    if not schema_fp:
        schema_filename = get_filename(api_params,
                                       file_extension='json',
                                       prefix="schema",
                                       suffix=table_name,
                                       release=release,
                                       include_release=include_release)

        schema_fp = get_scratch_fp(bq_params, schema_filename)

    with open(schema_fp, 'w') as schema_json_file:
        json.dump(schema_obj, schema_json_file, indent=4)

    upload_to_bucket(bq_params, schema_fp, delete_local=delete_local)


def create_and_upload_schema_for_json(api_params, bq_params, record_list, table_name, include_release=False,
                                      schema_fp=None, delete_local=True):
    """
    todo
    :param api_params: api_params supplied in yaml config
    :param bq_params: bq_params supplied in yaml config
    :param record_list:
    :param table_name:
    :param include_release:
    :return:
    """
    data_types_dict = recursively_detect_object_structures(record_list)

    generate_and_upload_schema(api_params, bq_params,
                               table_name=table_name,
                               data_types_dict=data_types_dict,
                               include_release=include_release,
                               schema_fp=schema_fp,
                               delete_local=delete_local)


def get_column_list_tsv(header_list=None, tsv_fp=None, header_row_index=None):
    """
    Return a list of column headers using header_list OR using a header_row index to retrieve column names from tsv_fp.
        NOTE: Specifying both header_list and header_row in parent function triggers a fatal error.
    :param header_list: Optional ordered list of column headers corresponding to columns in dataset tsv file
    :type header_list: list
    :param tsv_fp: Optional string filepath; provided if column names are being obtained directly from tsv header
    :type tsv_fp: str
    :param header_row_index: Optional header row index, if deriving column names from tsv file
    :type header_row_index: int
    :return list of columns with BQ-compatible names
    :rtype list
    """

    if not header_list and not header_row_index and not isinstance(header_row_index, int):
        has_fatal_error("Must supply either the header row index or header list for tsv schema creation.")
    if header_row_index and header_list:
        has_fatal_error("Can't supply both a header row index and header list for tsv schema creation.")

    column_list = list()

    if header_list:
        for column in header_list:
            column = make_string_bq_friendly(column)
            column_list.append(column)
    else:
        with open(tsv_fp, 'r') as tsv_file:
            if header_row_index:
                for index in range(header_row_index):
                    tsv_file.readline()

            column_row = tsv_file.readline()
            columns = column_row.split('\t')

            if len(columns) == 0:
                has_fatal_error("No column name values supplied by header row index")

            for column in columns:
                column = make_string_bq_friendly(column)
                column_list.append(column)

    return column_list


def normalize_header_row(header_row):
    new_header_row = list()

    for value in header_row:
        value = value.lower()
        test_value = value
        suffix_value = 1

        # if column header is a duplicate, append numeric suffix
        while test_value in new_header_row:
            test_value = f"{value}_{str(suffix_value)}"
            suffix_value += 1

        if value != test_value:
            print(f"Changing header value {value} to {test_value} (due to encountering duplicate header).")

        new_header_row.append(test_value)

    return new_header_row


def create_normalized_tsv(raw_tsv_fp, normalized_tsv_fp):
    """
    Opens a raw tsv file, normalizes its data, then writes to new tsv file.
    :param raw_tsv_fp: path to non-normalized data file
    :param normalized_tsv_fp: destination file for normalized data
    """
    with open(normalized_tsv_fp, mode="w", newline="") as normalized_tsv_file:
        tsv_writer = csv.writer(normalized_tsv_file, delimiter="\t")

        with open(raw_tsv_fp, mode="r", newline="") as tsv_file:
            tsv_reader = csv.reader(tsv_file, delimiter="\t")

            raw_row_count = 0

            for row in tsv_reader:
                normalized_record = list()

                if raw_row_count == 0:
                    header_row = normalize_header_row(row)
                    tsv_writer.writerow(header_row)
                    raw_row_count += 1
                    continue

                for value in row:
                    new_value = normalize_value(value, is_tsv=True)
                    normalized_record.append(new_value)

                tsv_writer.writerow(normalized_record)
                raw_row_count += 1
                if raw_row_count % 500000 == 0:
                    print(f"Normalized {raw_row_count} rows.")

            print(f"Normalized {raw_row_count} rows.")

    with open(normalized_tsv_fp, mode="r", newline="") as normalized_tsv_file:
        tsv_reader = csv.reader(normalized_tsv_file, delimiter="\t")
        normalized_row_count = 0

        for row in tsv_reader:
            normalized_row_count += 1

    if normalized_row_count != raw_row_count:
        print(f"ERROR: Row count changed. Original: {raw_row_count}; Normalized: {normalized_row_count}")
        exit()


def aggregate_column_data_types_tsv(tsv_fp, column_headers, skip_rows, sample_interval=1):
    """
    Open tsv file and aggregate data types for each column.
    :param tsv_fp: tsv dataset filepath used to analyze the data types
    :type tsv_fp: str
    :param column_headers: list of ordered column headers
    :type column_headers: list
    :param skip_rows: number of (header) rows to skip before starting analysis
    :type skip_rows: int
    :param sample_interval: sampling interval, used to skip rows in large datasets; defaults to checking every row
        ex.: sample_interval == 10 will sample every 10th row
    :type sample_interval: int
    :return dict of column keys, with value sets representing all data types found for that column
    :rtype dict {str: set}
    """
    data_types_dict = dict()

    for column in column_headers:
        data_types_dict[column] = set()

    with open(tsv_fp, 'r') as tsv_file:
        for i in range(skip_rows):
            tsv_file.readline()

        count = 0

        while True:
            row = tsv_file.readline()

            if not row:
                break

            if count % sample_interval == 0:
                row_list = row.split('\t')

                for idx, value in enumerate(row_list):
                    value = value.strip()
                    # convert non-standard null or boolean value to None, "True" or "False", otherwise return original
                    value = normalize_value(value)
                    value_type = check_value_type(value)
                    data_types_dict[column_headers[idx]].add(value_type)

            count += 1

    return data_types_dict


def create_schema_object(column_headers, data_types_dict):
    """
    Create BigQuery SchemaField object representation.
    :param column_headers: list of column names
    :type column_headers: list
    :param data_types_dict: dictionary of column names and their types
        (should have been run through resolve_type_conflicts() prior to use here)
    :type data_types_dict: dict of {str: str}
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

def find_types(file, sample_interval):
    """
    Finds the field type for each column in the file
    :param file: file name
    :type file: basestring
    :param sample_interval:sampling interval, used to skip rows in large datasets; defaults to checking every row
        example: sample_interval == 10 will sample every 10th row
    :type sample_interval: int
    :return: a tuple with a list of [field, field type]
    :rtype: tuple ([field, field_type])
    """
    column_list = get_column_list_tsv(tsv_fp=file, header_row_index=0)
    field_types = aggregate_column_data_types_tsv(file, column_list,
                                                  sample_interval=sample_interval,
                                                  skip_rows=1)
    final_field_types = resolve_type_conflicts(field_types)
    typing_tups = []
    for column in column_list:
        # Assign columns with no data with type STRING
        if final_field_types[column] is None:
            tup = (column, "STRING")
        else:
            tup = (column, final_field_types[column])
        typing_tups.append(tup)

    return typing_tups

def create_and_upload_schema_for_tsv(api_params, bq_params, tsv_fp, table_name=None, header_list=None, header_row=None,
                                     skip_rows=0, row_check_interval=1, release=None, schema_fp=None,
                                     delete_local=True):
    """
    Create and upload schema for a file in tsv format.
    :param api_params: api_params supplied in yaml config
    :param bq_params: bq_params supplied in yaml config
    :param table_name: name of table belonging to schema
    :param tsv_fp: path to tsv data file, parsed to create schema
    :param header_list: optional, list of header strings
    :param header_row: optional, integer index of header row within the file
    :param skip_rows: integer representing number of non-data rows at the start of the file, defaults to 0
    :param row_check_interval: how many rows to sample in order to determine type; defaults to 1 (check every row)
    :param release: string value representing release, in cases where api_params['RELEASE'] should be overridden
    :param schema_fp: todo
    :param delete_local: todo
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
        schema_filename = get_filename(api_params,
                                       file_extension='json',
                                       prefix="schema",
                                       suffix=table_name,
                                       release=release)
        schema_fp = get_scratch_fp(bq_params, schema_filename)

    with open(schema_fp, 'w') as schema_json_file:
        json.dump(schema_obj, schema_json_file, indent=4)

    upload_to_bucket(bq_params, schema_fp, delete_local=delete_local)


def output_compare_tables_report(api_params, bq_params, get_publish_table_ids,
                                 find_most_recent_published_table_id, source_table_id, public_dataset, id_keys):
    """
    Compare new table with previous version.
    todo
    :param api_params: api_params supplied in yaml config
    :param bq_params: bq_params supplied in yaml config
    :param get_publish_table_ids:
    :param find_most_recent_published_table_id:
    :param source_table_id:
    :param public_dataset:
    :return:
    """
    def make_field_diff_query(is_removed_query):
        """
        Make query for comparing two tables' field sets.
        :param is_removed_query: True if query retrieves removed fields; False if retrieves added fields
        """
        if is_removed_query:
            split_outer_table_id = previous_table_id.split('.')
            dataset_outer = ".".join(split_outer_table_id[:1])
            table_name_outer = split_outer_table_id[2]

            split_inner_table_id = source_table_id.split('.')
            dataset_inner = ".".join(split_inner_table_id[:1])
            table_name_inner = split_inner_table_id[2]
        else:
            split_outer_table_id = source_table_id.split('.')
            dataset_outer = ".".join(split_outer_table_id[:1])
            table_name_outer = split_outer_table_id[2]

            split_inner_table_id = previous_table_id.split('.')
            dataset_inner = ".".join(split_inner_table_id[:1])
            table_name_inner = split_inner_table_id[2]

        return f"""
            SELECT field_path AS field
            FROM `{dataset_outer}`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
            WHERE field_path NOT IN (
                SELECT field_path 
                FROM `{dataset_inner}`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
                WHERE table_name={table_name_inner}
            )
            AND table_name={table_name_outer}
        """

    def make_datatype_diff_query():
        """Make query for comparing two tables' field data types."""
        return f"""
            WITH old_data_types as (
                SELECT field_path, data_type
                FROM `{previous_dataset}`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
                WHERE table_name={previous_table_name}
            ), new_data_types as (
                SELECT field_path, data_type
                FROM `{source_dataset}`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
                WHERE table_name={source_table_name}                
            ),
            distinct_data_types as (
                old_data_types
                UNION ALL 
                new_data_types
                GROUP BY field_path, data_type
            )
            SELECT field_path
            FROM distinct_data_types
            GROUP BY field_path
            HAVING COUNT(field_path) > 1
        """

    def make_removed_ids_query():
        """Make query for finding removed ids in newly released dataset."""
        return f"""
            SELECT {id_keys}
            FROM `{previous_table_id}`
            WHERE CONCAT({id_keys}) NOT IN (
                SELECT CONCAT({id_keys}) 
                FROM `{source_table_id}`
            )    
        """

    def make_added_ids_query():
        """
        Make query for finding added id count for newly released dataset.
        :return: query string for table comparison
        """
        return f"""
            SELECT count({id_keys}) as added_id_count
            FROM `{source_table_id}`
            WHERE CONCAT({id_keys}) NOT IN (
                SELECT CONCAT({id_keys})
                FROM `{previous_table_id}`
            )
        """

    curr_table_id, versioned_table_id = get_publish_table_ids(api_params, bq_params, source_table_id, public_dataset)
    previous_table_id = find_most_recent_published_table_id(api_params, versioned_table_id)

    split_source_table_id = source_table_id.split('.')
    source_dataset = ".".join(split_source_table_id[:1])
    source_table_name = split_source_table_id[2]

    split_previous_table_id = previous_table_id.split('.')
    previous_dataset = ".".join(split_previous_table_id[:1])
    previous_table_name = split_previous_table_id[2]

    print(f"\nTable Comparison Report ***")
    print(f"New table (dev): {source_table_id} \nLast published table: {previous_table_id}")

    # which fields have been removed?
    removed_fields_res = bq_harness_with_result(sql=make_field_diff_query(is_removed_query=True),
                                                do_batch=False, verbose=False)

    print("\nRemoved fields:")
    if removed_fields_res.total_rows == 0:
        print("<none>")
    else:
        for row in removed_fields_res:
            print(row[0])

    # which fields were added?
    added_fields_res = bq_harness_with_result(sql=make_field_diff_query(is_removed_query=False), 
                                              do_batch=False, verbose=False)

    print("\nNew fields:")
    if added_fields_res.total_rows == 0:
        print("<none>")
    else:
        for row in added_fields_res:
            print(row[0])

    # any changes in field data type?
    datatype_diff_res = bq_harness_with_result(sql=make_datatype_diff_query(), do_batch=False, verbose=False)

    print("\nColumns with data type change:")
    if datatype_diff_res.total_rows == 0:
        print("<none>")
    else:
        for row in datatype_diff_res:
            print(row[0])

    print("\nRemoved case ids:")
    removed_case_ids_res = bq_harness_with_result(make_removed_ids_query(id_key), do_batch=False, verbose=False)

    if removed_case_ids_res.total_rows == 0:
        print("<none>")
    else:
        for row in removed_case_ids_res:
            print(row[0])

    print("\nAdded case id count:")
    added_case_ids_res = bq_harness_with_result(make_added_ids_query(id_key), do_batch=False, verbose=False)

    if added_case_ids_res.total_rows == 0:
        print("<none>")
    else:
        for row in added_case_ids_res:
            print(f"{row[0]}: {row[1]} new case ids")

    print("\n*** End Report ***\n\n")


#   MISC UTILS
def make_string_bq_friendly(string):
    """
    todo
    :param string:
    :return:
    """
    string = string.replace('%', 'percent')
    string = re.sub(r'[^A-Za-z0-9_ ]+', ' ', string)
    string = string.strip()
    string = re.sub(r'\s+', '_', string)

    return string


def format_seconds(seconds):
    """
    Round seconds to formatted hour, minute, and/or second output.
    :param seconds: int representing time in seconds
    :return: formatted time string
    """
    if seconds > 3600:
        return time.strftime("%-H hours, %-M minutes, %-S seconds", time.gmtime(seconds))
    if seconds > 60:
        return time.strftime("%-M minutes, %-S seconds", time.gmtime(seconds))

    return time.strftime("%-S seconds", time.gmtime(seconds))


def load_config(args, yaml_dict_keys, validate_config=None):
    """
    Open yaml file and retrieves configuration parameters.
    :param validate_config:
    :param args: args param from python bash cli
    :param yaml_dict_keys: tuple of strings representing a subset of the yaml file's
    top-level dict keys
    :return: tuple of dicts from yaml file (as requested in yaml_dict_keys)
    """

    def open_yaml_and_return_dict(yaml_name):
        """
        todo
        :param yaml_name:
        :return:
        """
        with open(yaml_name, mode='r') as yaml_file:
            yaml_dict = None
            config_stream = io.StringIO(yaml_file.read())

            try:
                yaml_dict = yaml.load(config_stream, Loader=yaml.FullLoader)
            except yaml.YAMLError as ex:
                has_fatal_error(ex, str(yaml.YAMLError))
            if yaml_dict is None:
                has_fatal_error("Bad YAML load, exiting.", ValueError)

            # Dynamically generate a list of dictionaries for the return statement,
            # since tuples are immutable
            return {key: yaml_dict[key] for key in yaml_dict_keys}

    if len(args) < 2 or len(args) > 3:
        has_fatal_error("")
    if len(args) == 2:
        singleton_yaml_dict = open_yaml_and_return_dict(args[1])
        return tuple([singleton_yaml_dict[key] for key in yaml_dict_keys])

    shared_yaml_dict = open_yaml_and_return_dict(args[1])

    data_type_yaml_dict = open_yaml_and_return_dict(args[2])

    merged_yaml_dict = {key: {} for key in yaml_dict_keys}

    for key in yaml_dict_keys:
        if key not in shared_yaml_dict and key not in data_type_yaml_dict:
            has_fatal_error(f"{key} not found in shared or data type-specific yaml config")
        elif not shared_yaml_dict[key] and not data_type_yaml_dict[key]:
            has_fatal_error(f"No values found for {key} in shared or data type-specific yaml config")

        if key in shared_yaml_dict and shared_yaml_dict[key]:
            merged_yaml_dict[key] = shared_yaml_dict[key]

            if key in data_type_yaml_dict and data_type_yaml_dict[key]:
                merged_yaml_dict[key].update(data_type_yaml_dict[key])
        else:
            merged_yaml_dict[key] = data_type_yaml_dict[key]

    if validate_config:
        pass
        # todo create config validation
        # validate_config(tuple(return_dicts))

    return tuple([merged_yaml_dict[key] for key in yaml_dict_keys])


def has_fatal_error(err, exception=None):
    """
    Output error str or list<str>, then exits; optionally throws Exception.
    :param err: error message str or list<str>
    :param exception: Exception type for error (defaults to None)
    """
    err_str_prefix = '[ERROR] '
    err_str = ''

    if isinstance(err, list):
        for item in err:
            err_str += err_str_prefix + str(item) + '\n'
    else:
        err_str = err_str_prefix + err

    print(err_str)

    if exception:
        raise exception

    sys.exit(1)
