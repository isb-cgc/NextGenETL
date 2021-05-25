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
import pprint
import sys
import time
import re
import datetime

import requests
import yaml
from google.api_core.exceptions import NotFound, BadRequest
from google.cloud import bigquery, storage, exceptions


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
    max_retries = 4

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
        print(f"Retry {tries} of {max_retries}...")
        time.sleep(3)

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


def update_friendly_name(api_params, table_id, is_gdc, custom_name=None):
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
        metadata_dir = f"{generic_schema_path}/{bq_params['GENERIC_TABLE_METADATA_FILE']}"
    else:
        metadata_dir = f"{generic_schema_path}/{metadata_file}"
    # adapts path for vm
    metadata_fp = get_filepath(metadata_dir)

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
    Alter an existing table's schema (currently, only field descriptions are mutable
    without a table rebuild, Google's restriction).
    """
    print("\nAdding column descriptions!")

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

    Gets the bq table object referenced by table_id.
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

    Changes the status label of archived_table_id to 'archived.'
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


def publish_table(api_params, bq_params, public_dataset, source_table_id, get_publish_table_ids,
                  find_most_recent_published_table_id, overwrite=False):
    """

    Publish production BigQuery tables using source_table_id:
        - create current/versioned table ids
        - publish tables
        - update friendly name for versioned table
        - change last version tables' status labels to archived
    :param api_params: api params from yaml config
    :param bq_params: bq params from yaml config
    :param public_dataset: publish dataset location
    :param source_table_id: source (dev) table id
    :param overwrite: If True, replace existing BigQuery table; defaults to False
    :param get_publish_table_ids: function that returns public table ids based on the source table id
    :param find_most_recent_published_table_id: function that returns previous versioned table id, if any
    """

    current_table_id, versioned_table_id = get_publish_table_ids(api_params, bq_params,
                                                                 source_table_id=source_table_id,
                                                                 public_dataset=public_dataset)
    previous_versioned_table_id = find_most_recent_published_table_id(api_params, versioned_table_id)

    # TESTING PUBLISH
    if exists_bq_table(source_table_id):
        print(f"""source_table_id = {source_table_id}
                  versioned_table_id = {versioned_table_id}
                  current_table_id = {current_table_id}
                  last_published_table_id = {previous_versioned_table_id}
                  """)

    '''
    if exists_bq_table(source_table_id):
        print(f"Publishing {versioned_table_id}")
        copy_bq_table(bq_params, source_table_id, versioned_table_id, overwrite)

        print(f"Publishing {current_table_id}")
        copy_bq_table(bq_params, source_table_id, current_table_id, overwrite)

        print(f"Updating friendly name for {versioned_table_id}\n")
        is_gdc = True if api_params['DATA_SOURCE'] == 'gdc' else False
        update_friendly_name(api_params, table_id=versioned_table_id, is_gdc=is_gdc)
        change_status_to_archived(previous_versioned_table_id)
    '''


'''
def publish_table(api_params, bq_params, public_dataset, source_table_id, overwrite=False, include_data_source=True):
    """

    Publish production BigQuery tables using source_table_id:
        - create current/versioned table ids
        - publish tables
        - update friendly name for versioned table
        - change last version tables' status labels to archived
    :param api_params: api params from yaml config
    :param bq_params: bq params from yaml config
    :param public_dataset: publish dataset location
    :param source_table_id: source (dev) table id
    :param overwrite: If True, replace existing BigQuery table; defaults to False
    :param include_data_source: whether to include data source in final table names
    """

    def get_publish_table_ids():
        """
        Create current and versioned table ids.
        """
        rel_prefix = get_rel_prefix(api_params)
        split_table_id = source_table_id.split('.')

        # derive data type from table id
        data_type = split_table_id[-1]
        data_type = data_type.replace(rel_prefix, '').strip('_')
        data_type = data_type.replace(public_dataset + '_', '')
        data_type = data_type.replace(api_params['DATA_SOURCE'], '').strip('_')

        print(f"data_type: {data_type}")

        if include_data_source:
            curr_table_name = construct_table_name_from_list([data_type, api_params['DATA_SOURCE'], 'current'])
            vers_table_name = construct_table_name_from_list([data_type, api_params['DATA_SOURCE'], rel_prefix])
        else:
            curr_table_name = construct_table_name_from_list([data_type, 'current'])
            vers_table_name = construct_table_name_from_list([data_type, rel_prefix])

        curr_table_id = f"{bq_params['PROD_PROJECT']}.{public_dataset}.{curr_table_name}"
        vers_table_id = f"{bq_params['PROD_PROJECT']}.{public_dataset}_versioned.{vers_table_name}"

        return curr_table_id, vers_table_id

    def find_last_published_release_table_id(_versioned_table_id):
        if api_params['DATA_SOURCE'] == 'gdc':
            oldest_etl_release = 26  # the oldest table release we published
            last_gdc_release = int(api_params['RELEASE']) - 1
            current_gdc_release = get_rel_prefix(api_params)
            table_id_no_release = _versioned_table_id.replace(current_gdc_release, '')

            for release in range(last_gdc_release, oldest_etl_release - 1, -1):
                prev_release_table_id = f"{table_id_no_release}{api_params['REL_PREFIX']}{release}"
                if exists_bq_table(prev_release_table_id):
                    # found last release table, stop iterating
                    return prev_release_table_id

            return None
        elif api_params['DATA_SOURCE'] == 'pdc':
            # todo assuming PDC will use 2-digit minor releases -- check
            max_minor_release_num = 99
            split_current_etl_release = api_params['RELEASE'][1:].split("_")
            # set to current release initially, decremented in loop
            last_major_rel_num = int(split_current_etl_release[0])
            last_minor_rel_num = int(split_current_etl_release[1])

            while True:
                if last_minor_rel_num > 0 and last_major_rel_num >= 1:
                    last_minor_rel_num = last_minor_rel_num - 1
                elif last_minor_rel_num > 1:
                    last_major_rel_num = last_major_rel_num - 1
                    last_minor_rel_num = max_minor_release_num
                else:
                    return None

                table_id_no_release = _versioned_table_id.replace(f"_{api_params['RELEASE']}", '')
                prev_release_table_id = f"{table_id_no_release}_V{last_major_rel_num}_{last_minor_rel_num}"

                if exists_bq_table(prev_release_table_id):
                    # found last release table, stop iterating
                    return prev_release_table_id

                # stop at oldest mass-published version, 1_17, if no previous table found
                # todo switch back to 17
                if last_major_rel_num == 1 and last_minor_rel_num == 8:
                    return None
        else:
            print(f"Set up release diff check in publish_tables for data source {api_params['DATA_SOURCE']}")
            return None

    def change_status_to_archived(_versioned_table_id):
        """
        Change last version status label to archived.
        """
        try:
            client = bigquery.Client()
            prev_table_id = find_last_published_release_table_id(_versioned_table_id)
            prev_table = client.get_table(prev_table_id)
            prev_table.labels['status'] = 'archived'
            client.update_table(prev_table, ["labels"])
            assert prev_table.labels['status'] == 'archived'
        except NotFound:
            print("Couldn't find a table to archive. Might be that this is the first table release?")

    current_table_id, versioned_table_id = get_publish_table_ids()

    # TESTING PUBLISH
    if exists_bq_table(source_table_id):
        print(f"""
            source_table_id = {source_table_id}
            versioned_table_id = {versioned_table_id}
            current_table_id = {current_table_id}
            last_published_table_id = {find_last_published_release_table_id(versioned_table_id)}
        """)

    # todo turn on actual publishing step
    """
    if exists_bq_table(source_table_id):
        print(f"Publishing {versioned_table_id}")
        copy_bq_table(bq_params, source_table_id, versioned_table_id, overwrite)

        print(f"Publishing {current_table_id}")
        copy_bq_table(bq_params, source_table_id, current_table_id, overwrite)

        print(f"Updating friendly name for {versioned_table_id}\n")
        is_gdc = True if api_params['DATA_SOURCE'] == 'gdc' else False
        update_friendly_name(api_params,
                             table_id=versioned_table_id,
                             is_gdc=is_gdc)

        change_status_to_archived(versioned_table_id)
    """
'''

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

    # replace '.' with '_' so that the name is valid
    # ('.' chars not allowed -- issue with BEATAML1.0, for instance)
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
    job_config.schema = schema
    job_config.skip_leading_rows = num_header_rows
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


def populate_generic_table_schema():
    # todo
    pass


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
        load_job = client.load_table_from_uri(gs_uri, table_id, job_config=job_config)

        print(f' - Inserting into {table_id}... ', end="")
        await_insert_job(bq_params, client, table_id, load_job)
    except TypeError as err:
        has_fatal_error(err)

#   GOOGLE CLOUD STORAGE UTILS


def upload_to_bucket(bq_params, scratch_fp, delete_local=False):
    """

    Upload file to a Google storage bucket (bucket/directory location specified in YAML config).
    :param bq_params: bq param object from yaml config
    :param scratch_fp: name of file to upload to bucket
    :param delete_local: delete scratch file created on VM
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

        print(f"Successfully uploaded file to {bucket_name}/{blob_name}. ", end="")

        if delete_local:
            os.remove(scratch_fp)
            print("Local file deleted.")
        else:
            print(f"Local file not deleted (location: {scratch_fp}).")

    except exceptions.GoogleCloudError as err:
        has_fatal_error(f"Failed to upload to bucket.\n{err}")
    except FileNotFoundError as err:
        has_fatal_error(f"File not found, failed to access local file.\n{err}")


def download_from_bucket(bq_params, filename):
    """

    Download file from Google storage bucket onto VM.
    :param bq_params: BigQuery params
    :param filename: Name of file to download
    """
    storage_client = storage.Client(project="")
    blob_name = f"{bq_params['WORKING_BUCKET_DIR']}/{filename}"
    bucket = storage_client.bucket(bq_params['WORKING_BUCKET'])
    blob = bucket.blob(blob_name)

    scratch_fp = get_scratch_fp(bq_params, filename)
    with open(scratch_fp, 'wb') as file_obj:
        blob.download_to_file(file_obj)


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


def get_scratch_fp(bq_params, filename):
    """

    Construct filepath for VM output file.
    :param filename: name of the file
    :param bq_params: bq param object from yaml config
    :return: output filepath for VM
    """
    return get_filepath(bq_params['SCRATCH_DIR'], filename)


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


def write_list_to_jsonl_and_upload(api_params, bq_params, prefix, record_list):
    """

    Write joined_record_list to file name specified by prefix and uploads to scratch Google Cloud bucket.
    :param api_params: api_params supplied in yaml config
    :param bq_params: bq_params supplied in yaml config
    :param prefix: string representing base file name (release string is appended to generate filename)
    :param record_list: list of record objects to insert into jsonl file
    """
    jsonl_filename = get_filename(api_params,
                                  file_extension='jsonl',
                                  prefix=prefix)
    local_filepath = get_scratch_fp(bq_params, jsonl_filename)
    write_list_to_jsonl(local_filepath, record_list)
    upload_to_bucket(bq_params, local_filepath, delete_local=True)


def write_list_to_tsv(fp, tsv_list):
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


def normalize_value(value):
    """

    If value is variation of null or boolean value, converts to single form (None, True, False);
    otherwise returns original value.
    :param value: value to convert
    :return: normalized (or original) value
    """
    if isinstance(value, str):
        value = value.strip()

    if value in ('NA', 'N/A', 'null', 'None', '', 'NULL', 'Null', 'Not Reported'):
        return None
    elif value in ('False', 'false', 'FALSE', 'No', 'no', 'NO'):
        return "False"
    elif value in ('True', 'true', 'TRUE', 'YES', 'yes', 'Yes'):
        return "True"

    return value


def check_value_type(value):
    """

    Checks value for corresponding BigQuery type. Evaluates the following BigQuery column data types:
        - datetime formats: DATE, TIME, TIMESTAMP
        - number formats: INT64, FLOAT64, NUMERIC
        - misc formats: STRING, BOOL, ARRAY, RECORD
    :param value: value on which to perform data type analysis
    :return: data type in BigQuery Standard SQL format
    """

    if isinstance(value, bool):
        return "BOOL"
    if isinstance(value, int):
        return "INT64"
    if isinstance(value, float):
        return "FLOAT64"
    if isinstance(value, list):
        return "ARRAY"
    if isinstance(value, dict):
        return "RECORD"
    if not value:
        return None

    # check to see if value is numeric, float or int; differentiates between these types and datetime or ids,
    # which may be composed of only numbers or symbols
    if '.' in value and ':' not in value:
        split_value = value.split('.')
        if len(split_value) == 2:
            if split_value[0].isdigit() and split_value[1].isdigit():
                # if in float form, but fraction is .0, .00, etc., then consider it an integer
                if int(split_value[1]) == 0:
                    return "INT64"
                return "FLOAT64"
        return "STRING"

    # numeric values are numbers with special encoding, like an exponent or sqrt symbol
    elif value.isnumeric() and not value.isdigit() and not value.isdecimal():
        return "NUMERIC"
    elif value.isdigit():
        return "INT64"

    # a sequence of numbers starting with a 0 has to be explicitly classified as a, otherwise data loss will occur
    elif value.startswith("0") and ':' not in value and '-' not in value:
        return "STRING"

    """
    BIGQUERY'S CANONICAL DATE/TIME FORMATS:
    (see https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types)
    """

    # Check for BigQuery DATE format: 'YYYY-[M]M-[D]D'
    date_re_str = r"[0-9]{4}-(0[1-9]|1[0-2]|[0-9])-([0-2][0-9]|[3][0-1]|[0-9])"
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

    # This shouldn't really be returned--in case of some missed edge case, however, it's safe to default to string.
    return "STRING"


def resolve_type_conflict(field, types_set):
    """

    Resolve BigQuery column data type precedence, where multiple types are detected. Rules for type conversion based on
    BigQuery's implicit conversion behavior.
    See https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_rules#coercion
    :param types_set: Set of BigQuery data types in string format
    :param field: field name
    :return: BigQuery data type with highest precedence
    """

    datetime_types = {"TIMESTAMP", "DATE", "TIME"}
    number_types = {"INT64", "FLOAT64", "NUMERIC"}

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
    # todo convert to new schema generation
    Iteratively resolve type conflicts in flattened dict (used by GDC clinical).
    :param types_dict: dict of field: types set values
    """
    for field, types_set in types_dict.items():
        types_dict[field] = resolve_type_conflict(field, types_set)


def recursively_detect_object_structures(nested_obj):
    """

    Traverse a dict or list of objects, analyzing the structure. Order not guaranteed (if anything, it'll be
    backwards)--Not for use with TSV data.
    Works for arbitrary nesting, even if object structure varies from record to record; use for lists, dicts,
    or any combination therein.
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
            elif not isinstance(_obj[k], list):
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


def retrieve_bq_schema_object(api_params, bq_params, table_name, release=None, include_release=True):
    """

    todo
    :param api_params: api_params supplied in yaml config
    :param bq_params: bq_params supplied in yaml config
    :param table_name:
    :param release:
    :return:
    """
    schema_filename = get_filename(api_params=api_params,
                                   file_extension='json',
                                   prefix="schema",
                                   suffix=table_name,
                                   release=release,
                                   include_release=include_release)

    download_from_bucket(bq_params, schema_filename)

    with open(get_scratch_fp(bq_params, schema_filename), "r") as schema_json:
        schema_obj = json.load(schema_json)
        json_schema_obj_list = [field for field in schema_obj["fields"]]

    schema = generate_bq_schema_fields(json_schema_obj_list)

    return schema


def generate_and_upload_schema(api_params, bq_params, table_name, data_types_dict, include_release, release=None):
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

    schema_filename = get_filename(api_params,
                                   file_extension='json',
                                   prefix="schema",
                                   suffix=table_name,
                                   release=release,
                                   include_release=include_release)

    schema_fp = get_scratch_fp(bq_params, schema_filename)

    with open(schema_fp, 'w') as schema_json_file:
        json.dump(schema_obj, schema_json_file, indent=4)

    upload_to_bucket(bq_params, schema_fp, delete_local=True)


def create_and_upload_schema_for_json(api_params, bq_params, record_list, table_name, include_release):
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
                               include_release=include_release)


def create_and_upload_schema_for_tsv(api_params, bq_params, table_name, tsv_fp, header_list=None, header_row=None,
                                     skip_rows=0, row_check_interval=1, release=None):
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
    """

    def get_column_list():
        """
        Return a list of column headers using header_list, if provided, OR from the tab-separated file row at the index
        specified by header_row (specifying both header_list and header_row in parent function triggers a fatal error).
        """
        column_list = list()

        if header_list:
            for _column in header_list:
                _column = make_string_bq_friendly(_column)
                column_list.append(_column)
        else:
            with open(tsv_fp, 'r') as _tsv_file:
                if header_row:
                    for index in range(header_row):
                        _tsv_file.readline()

                column_row = _tsv_file.readline()
                _columns = column_row.split('\t')

                if len(_columns) == 0:
                    has_fatal_error("No column name values supplied by header row index")

                for _column in _columns:
                    _column = make_string_bq_friendly(_column)
                    column_list.append(_column)

        return column_list

    def aggregate_column_data_types():
        """
        Open tsv file and aggregate data types for each column.
        """
        with open(tsv_fp, 'r') as tsv_file:
            for i in range(skip_rows):
                tsv_file.readline()

            count = 0

            while True:
                row = tsv_file.readline()

                if not row:
                    break

                if count % row_check_interval == 0:

                    row_list = row.split('\t')

                    for idx, value in enumerate(row_list):
                        value_type = check_value_type(value)
                        data_types_dict[columns[idx]].add(value_type)

                count += 1

    def create_schema_object():
        """
        Create BigQuery SchemaField object representation.
        """
        schema_field_object_list = list()

        for column_name in columns:
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

    print(f"Creating schema for {table_name}")

    # third condition required to account for header row at 0 index

    # if no header list supplied here, headers are generated from header_row.
    columns = get_column_list()

    if not header_list and not header_row and not isinstance(header_row, int):
        has_fatal_error("Must supply either the header row index or header list for tsv schema creation.")
    if header_row and header_list:
        has_fatal_error("Can't supply both a header row index and header list for tsv schema creation.")
    if isinstance(header_row, int) and header_row >= skip_rows:
        has_fatal_error("Header row not excluded by skip_rows.")

    data_types_dict = dict()

    for column in columns:
        data_types_dict[column] = set()

    aggregate_column_data_types()

    resolve_type_conflicts(data_types_dict)

    schema_obj = create_schema_object()

    schema_filename = get_filename(api_params,
                                   file_extension='json',
                                   prefix="schema",
                                   suffix=table_name,
                                   release=release)
    schema_fp = get_scratch_fp(bq_params, schema_filename)

    with open(schema_fp, 'w') as schema_json_file:
        json.dump(schema_obj, schema_json_file, indent=4)

    upload_to_bucket(bq_params, schema_fp, delete_local=True)


#   MISC UTILS

def make_string_bq_friendly(string):
    """

    todo
    :param string:
    :return:
    """
    string = re.sub(r'[^A-Za-z0-9_ ]+', ' ', string)
    string = string.strip()
    string = re.sub(r'\s+', '_', string)

    return string


def format_seconds(seconds):
    """

    Rounds seconds to formatted hour, minute, and/or second output.
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

    Opens yaml file and retrieves configuration parameters.
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

    Outputs error str or list<str>, then exits; optionally throws Exception.
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


def pprinter(print_str):
    """

    Pretty print (with indentation) json or other formatted strings.
    :param print_str: string to format
    """
    pp = pprint.PrettyPrinter(indent=4)

    pp.pprint(print_str)

