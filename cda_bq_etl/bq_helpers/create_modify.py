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

"""Create or modify BigQuery tables."""

import json
import logging
import sys
import time
import os
from typing import Optional, Any, Sequence, Mapping

from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.cloud.bigquery import SchemaField, Client, LoadJobConfig, QueryJob

from cda_bq_etl.bq_helpers.lookup import exists_bq_dataset, exists_bq_table, table_has_new_data, table_has_new_data_supports_nans
from cda_bq_etl.custom_typing import Params
from cda_bq_etl.utils import (get_filepath, input_with_timeout)


def load_create_table_job(params: Params, data_file: str, client: Client, table_id: str, job_config: LoadJobConfig):
    """
    Generate BigQuery LoadJob, which creates a Table and loads it with data.

    :param params: params supplied in yaml config
    :type params: Params
    :param data_file: file containing case records
    :type data_file: str
    :param client: BigQuery Client object
    :type client: Client
    :param table_id: BigQuery table identifier
    :type table_id: str
    :param job_config: LoadJobConfig object
    :type job_config: LoadJobConfig
    """
    gs_uri = f"gs://{params['WORKING_BUCKET']}/{params['WORKING_BUCKET_DIR']}/{data_file}"

    logger = logging.getLogger('base_script.cda_bq_etl.bq_helpers.create_modify')

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
    :type params: Params
    :param client: BigQuery Client object
    :type client: Client
    :param bq_job: A QueryJob object, responsible for executing BigQuery function calls
    :type bq_job: QueryJob
    :return: True if job successfully executes; otherwise throws a critical error and exits
    :rtype: bool
    """
    last_report_time = time.time()
    location = params['LOCATION']
    job_state = "NOT_STARTED"

    logger = logging.getLogger('base_script.cda_bq_etl.bq_helpers.create_modify')

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
    :type params: Params
    :param client: BigQuery Client object
    :type client: Client
    :param table_id: BigQuery table identifier
    :type table_id: str
    :param bq_job: QueryJob object
    :type bq_job: QueryJob
    """
    logger = logging.getLogger('base_script.cda_bq_etl.bq_helpers.create_modify')

    if await_job(params, client, bq_job):
        table = client.get_table(table_id)

        if table.num_rows == 0:
            logger.critical(f"Insert job for {table_id} inserted 0 rows. Exiting.")
            sys.exit(-1)

        logger.info(f" done. {table.num_rows} rows inserted.")
    else:
        # if this happens, it may not work to call await_job--trying not to have duplicate code fragments
        logger.critical(f"await_job didn't return for table_id: {table_id}.")
        sys.exit(-1)


def create_and_load_table_from_tsv(params: Params,
                                   tsv_file: str,
                                   table_id: str,
                                   num_header_rows: int,
                                   schema: Optional[list[SchemaField]] = None,
                                   null_marker: Optional[str] = None):
    """
    Create new BigQuery table and populate rows using rows of tsv file.

    :param params: params supplied in yaml config
    :type params: Params
    :param tsv_file: file containing records in tsv format
    :type tsv_file: str
    :param schema: list of SchemaField objects; if None, attempt to autodetect schema using BigQuery's native autodetect
    :type schema: Optional[list[SchemaField]]
    :param table_id: target table id
    :type table_id: str
    :param num_header_rows: number of header rows in file (these are skipped during processing)
    :type num_header_rows: int
    :param null_marker: null_marker character, optional (defaults to empty string for tsv/csv in bigquery)
    :type null_marker: Optional[str]
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


def create_and_load_table_from_jsonl(params: Params,
                                     jsonl_file: str,
                                     table_id: str,
                                     schema: Optional[list[SchemaField]] = None):
    """
    Create new BigQuery table and populate with jsonl file contents.

    :param params: params supplied in yaml config
    :type params: Params
    :param jsonl_file: file containing single-line json objects, which represent rows to be loaded into table
    :type jsonl_file: str
    :param table_id: target table id
    :type table_id: str
    :param schema: list of SchemaField objects; if None, attempt to autodetect schema using BigQuery's native autodetect
    :type schema: Optional[list[SchemaField]]
    """
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig()

    logger = logging.getLogger('base_script.cda_bq_etl.bq_helpers.create_modify')

    if schema:
        job_config.schema = schema
    else:
        logger.info(f" - No schema supplied for {table_id}, using schema autodetect.")
        job_config.autodetect = True

    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    load_create_table_job(params, jsonl_file, client, table_id, job_config)

def publish_table(params: Params, table_ids: dict[str, str]):
    """
    Publish production BigQuery tables using source_table_id. Update versioned table friendly name.
    Change the last versioned table's 'status' label to 'archived.'

    :param params: params supplied in yaml config
    :type params: Params
    :param table_ids: dict of table ids: 'source' (dev table), 'versioned', 'current' (future published ids),
                      and 'previous_versioned' (most recent published table)
    :type table_ids: dict[str, str]
    """
    return publish_table_with_nan_support(params, table_ids, None)

def publish_table_with_nan_support(params: Params, table_ids: dict[str, str], nan_column: str):
    """
    Publish production BigQuery tables using source_table_id. Update versioned table friendly name.
    Change the last versioned table's 'status' label to 'archived.' This version supports tables that
    may hold NaNs, which need special handling to detect true changes

    :param params: params supplied in yaml config
    :type params: Params
    :param table_ids: dict of table ids: 'source' (dev table), 'versioned', 'current' (future published ids),
                      and 'previous_versioned' (most recent published table)
    :type table_ids: dict[str, str]
    :param nan_column: name of column that might have NaNs. Can be None
    :type nan_column: str

    """
    logger = logging.getLogger('base_script.cda_bq_etl.bq_helpers.create_modify')

    if exists_bq_table(table_ids['source']):
        if table_has_new_data_supports_nans(table_ids['previous_versioned'], table_ids['source'], nan_column):
            logger.info(f"Publishing {table_ids['source']}")
            delay = 5

            logger.info(f"Publishing the following tables:")
            logger.info(f"\t- {table_ids['versioned']}")
            logger.info(f"\t- {table_ids['current']}")
            logger.info(f"Proceed? Y/n (continues automatically in {delay} seconds)")

            response = str(input_with_timeout(seconds=delay)).lower()

            if response == 'n' or response == 'N':
                exit("Publish aborted; exiting.")

            logger.info(f"Publishing {table_ids['versioned']}")
            copy_bq_table(params=params,
                          src_table=table_ids['source'],
                          dest_table=table_ids['versioned'],
                          replace_table=params['OVERWRITE_PROD_TABLE'])

            logger.info(f"Publishing {table_ids['current']}")
            copy_bq_table(params=params,
                          src_table=table_ids['source'],
                          dest_table=table_ids['current'],
                          replace_table=params['OVERWRITE_PROD_TABLE'])

            logger.info(f"Updating friendly name for {table_ids['versioned']}")
            update_friendly_name(params, table_id=table_ids['versioned'])

            if table_ids['previous_versioned']:
                logger.info(f"Archiving {table_ids['previous_versioned']}")
                change_status_to_archived(table_ids['previous_versioned'])

        else:
            logger.info(f"{table_ids['source']} not published, no changes detected")
    else:
        logger.error(f"Source table does not exist: {table_ids['source']}")


def create_table_from_query(params: Params, table_id: str, query: str):
    """
    Create new BigQuery table using result output of BigQuery SQL query.

    :param params: params supplied in yaml config
    :type params: Params
    :param table_id: target table id
    :type table_id: str
    :param query: data selection query, used to populate a new BigQuery table
    :type query: str
    """
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig(destination=table_id)
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    logger = logging.getLogger('base_script.cda_bq_etl.bq_helpers.create_modify')

    try:
        query_job = client.query(query, job_config=job_config)
        logger.info(f' - Inserting into {table_id}... ')
        await_insert_job(params, client, table_id, query_job)
    except TypeError as err:
        logger.critical(err)
        sys.exit(-1)


def create_view_from_query(view_id: str | Any, view_query: str):
    """
    Create BigQuery view using a SQL query.

    :param view_id: view_id (same structure as a BigQuery table id)
    :type view_id: str | Any
    :param view_query: query from which to construct the view
    :type view_query: str
    """
    client = bigquery.Client()
    view = bigquery.Table(view_id)

    logger = logging.getLogger('base_script.cda_bq_etl.bq_helpers.create_modify')

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


def delete_bq_table(table_id: str):
    """
    Permanently delete BigQuery table located by table_id.

    :param table_id: target table id
    :type table_id: str
    """
    logger = logging.getLogger('base_script.cda_bq_etl.bq_helpers.create_modify')

    client = bigquery.Client()
    client.delete_table(table=table_id, not_found_ok=True)

    if exists_bq_table(table_id):
        logger.error(f"Table {table_id} not deleted.")


def copy_bq_table(params: Params, src_table: str, dest_table: str, replace_table: bool = False):
    """
    Copy an existing BigQuery src_table into the location specified by dest_table.

    :param params: param object from yaml config
    :type params: Params
    :param src_table: ID of table to copy
    :type src_table: str
    :param dest_table: ID of table create
    :type dest_table: str
    :param replace_table: Replace existing table, if one exists; defaults to False
    :type replace_table: bool
    """
    client = bigquery.Client()
    job_config = bigquery.CopyJobConfig()

    logger = logging.getLogger('base_script.cda_bq_etl.bq_helpers.create_modify')

    if replace_table:
        delete_bq_table(dest_table)

    bq_job = client.copy_table(src_table, dest_table, job_config=job_config)

    if await_job(params, client, bq_job):
        logger.info(f"Successfully copied {src_table} -> ")
        logger.info(f"\t\t\t{dest_table}")


def create_bq_dataset(params: Params, project_id: str, dataset_name: str):
    """
    Create new BigQuery dataset.

    :param params: param object from yaml config
    :type params: Params
    :param project_id: Parent project of new dataset
    :type project_id: str
    :param dataset_name: Name for new dataset
    :type dataset_name: str
    """
    logger = logging.getLogger('base_script.cda_bq_etl.bq_helpers.create_modify')

    dataset_id = f"{project_id}.{dataset_name}"

    if exists_bq_dataset(dataset_id):
        logger.info(f"Dataset {dataset_id} already exists, returning")
        return

    client = bigquery.Client(project=project_id)

    # bigquery accepts a string input here, so don't worry about the typechecker warning
    # noinspection PyTypeChecker
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = params['LOCATION']

    dataset = client.create_dataset(dataset)
    logger.info(f"Created dataset {client.project}.{dataset.dataset_id}")


def update_friendly_name(params: Params, table_id: str, custom_name: Optional[str] = None):
    """
    Modify BigQuery table's friendly name.

    :param params: API params, supplied via yaml config
    :type params: Params
    :param table_id: table id in standard SQL format
        if is_gdc and no custom_name is specified, we add REL before the version onto the existing friendly name;
        if custom_name is specified, this behavior is overridden, and the table's friendly name is replaced entirely
    :type table_id: str
    :param custom_name: specifies a custom friendly name; by default, if is_gdc,
        append the following to the versioned table friendly name: 'REL ' + api_params['RELEASE'] + ' VERSIONED'
        else append the following to the versioned table friendly name: api_params['RELEASE'] + ' VERSIONED'"
    :type custom_name: Optional[str]
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
        elif params['NODE'].lower() == 'dcf':
            release = params['RELEASE'].replace('dr', '')
            friendly_name = f"{table.friendly_name} REL{release} VERSIONED"
        else:
            friendly_name = f"{table.friendly_name} {params['RELEASE']} VERSIONED"

    table.friendly_name = friendly_name
    client.update_table(table, ["friendly_name"])

    assert table.friendly_name == friendly_name


def update_table_labels(table_id: str, label_dict: dict[str, str]):
    """
    Update metadata labels for table_id.

    :param table_id: table_id for which to update labels
    :type table_id: str
    :param label_dict: dict of labels and their values
    :type label_dict: dict[str, str]
    """
    logger = logging.getLogger('base_script.cda_bq_etl.bq_helpers.create_modify')
    label, value = None, None
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
        if label:
            logger.warning(f"Couldn't apply table label {label}: {value}. Is this expected?")


def update_table_description(table_ids: list[str], description: str):
    """
    Update table description for each table in `table_ids`.

    :param table_ids: list of table ids to update
    :type table_ids: list[str]
    :param description: description to add to each table's metadata--note, the same description is applied for each
                        table in list.
    :type description: str
    """
    logger = logging.getLogger('base_script.cda_bq_etl.bq_helpers.create_modify')

    try:
        for table_id in table_ids:
            client = bigquery.Client()
            table_obj = client.get_table(table_id)
            table_obj.description = description

            delay = 5

            logger.info(f"Altering {table_id}. Description after change: {table_obj.description}")
            logger.info(f"Proceed? Y/n (continues automatically in {delay} seconds)")

            response = str(input_with_timeout(seconds=delay)).lower()

            if response == 'n':
                exit("Publish aborted; exiting.")

            client.update_table(table_obj, ["description"])

            assert table_obj.description == description
    except NotFound:
        logger.critical("Description change failed")


def update_table_schema_from_generic(params: Params,
                                     table_id: str,
                                     schema_tags: Optional[dict] = None,
                                     friendly_name_suffix: Optional[str] = None,
                                     metadata_file: Optional[str] = None,
                                     generate_definitions: bool = False):
    """
    Insert schema tags into generic schema (currently located in BQEcosystem repo).

    :param params: params from YAML config
    :type params: Params
    :param table_id: table_id where schema metadata should be inserted
    :type table_id: str
    :param schema_tags: schema tags used to populate generic schema metadata
    :type schema_tags: Optional[dict]
    :param friendly_name_suffix: string to append to friendly name (e.g. REL XX VERSIONED)
    :type friendly_name_suffix: Optional[str]
    :param metadata_file: name of generic table metadata file
    :type metadata_file: Optional[str]
    :param generate_definitions: if true, generate column definitions by parsing the column name (e.g. this_column_name
                                 definition would be 'this column name')
    """
    def add_generic_table_metadata():
        """Add generic table metadata."""
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

            update_table_metadata(table_metadata)

    def update_table_metadata(metadata: dict[str, str]):
        """Modify an existing BigQuery table's metadata (labels, friendly name, description)."""
        client = bigquery.Client()
        table = client.get_table(table_id)

        table.labels = metadata['labels']
        table.friendly_name = metadata['friendlyName']
        table.description = metadata['description']

        client.update_table(table, ["labels", "friendly_name", "description"])

        assert table.labels == metadata['labels']
        assert table.friendly_name == metadata['friendlyName']
        assert table.description == metadata['description']

    def add_column_descriptions():
        """
        Alter an existing table's schema (currently, only column descriptions are mutable without a table rebuild,
        Google's restriction).
        """
        logger.info("\t - Adding column descriptions!")

        column_desc_fp = f"{params['BQ_REPO']}/{params['COLUMN_DESCRIPTION_FILEPATH']}"
        column_desc_fp = get_filepath(column_desc_fp)

        if not os.path.exists(column_desc_fp):
            logger.critical("BQEcosystem column description path not found")
            sys.exit(-1)
        with open(column_desc_fp) as column_output:
            descriptions = json.load(column_output)

        update_schema_field_descriptions(descriptions)

    def generate_and_add_column_descriptions():
        """
        Alter an existing table's schema (currently, only column descriptions are mutable without a table rebuild,
        Google's restriction).
        """
        logger.info("\t - Adding/generating column descriptions!")

        column_desc_fp = f"{params['BQ_REPO']}/{params['COLUMN_DESCRIPTION_FILEPATH']}"
        column_desc_fp = get_filepath(column_desc_fp)

        if not os.path.exists(column_desc_fp):
            logger.critical("BQEcosystem column description path not found")
            sys.exit(-1)
        with open(column_desc_fp) as column_output:
            descriptions = json.load(column_output)

        client = bigquery.Client()
        table = client.get_table(table_id)

        new_schema = []

        for schema_field in table.schema:
            column = schema_field.to_api_repr()

            if column['name'] in descriptions.keys():
                name = column['name']
                column['description'] = descriptions[name]
            else:
                generated_definition = " ".join(column['name'].split("_"))
                generated_definition = generated_definition.capitalize()
                column['description'] = generated_definition

            mod_column = bigquery.SchemaField.from_api_repr(column)
            new_schema.append(mod_column)

        table.schema = new_schema

        client.update_table(table, ['schema'])

    def update_schema_field_descriptions(new_descriptions: dict[str, str]):
        """Modify an existing table's field descriptions. Recursively adds definitions to nested columns."""
        def update_nested_schema(schema: Optional[Sequence[SchemaField | Mapping[str, Any]]],
                                 new_schema: list[SchemaField]) -> list[SchemaField]:
            """
            Recursively iterate over schema, adding field definitions using BQEcosystem-derived
            json field definitions file.
            """
            for schema_field in schema:
                # convert schema field to dict if necessary
                # (nested columns are automatically converted to dict after first method call)
                field = schema_field.to_api_repr() if isinstance(schema_field, SchemaField) else schema_field

                if field['name'] in new_descriptions:
                    field['description'] = new_descriptions[field['name']]
                if 'description' not in field or not field['description']:
                    logger.error(f"Need to define {field['name']} in BQEcosystem!")
                if field['type'] == "RECORD" and field['fields']:
                    # recursively add nested field descriptions
                    update_nested_schema(field['fields'], list())

                # convert modified dict back into SchemaField object
                modified_field = bigquery.SchemaField.from_api_repr(field)
                new_schema.append(modified_field)

            return new_schema

        client = bigquery.Client()
        table = client.get_table(table_id)
        table.schema = update_nested_schema(table.schema, list())

        client.update_table(table, ['schema'])

    logger = logging.getLogger('base_script.cda_bq_etl.bq_helpers.create_modify')

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
    if params['NODE'].lower() == 'pdc':
        schema_tags['version'] = ".".join(release.split('_'))
    else:
        schema_tags['version'] = release

    schema_tags['extracted-month-year'] = params['EXTRACTED_MONTH_YEAR']

    # gdc uses this
    if 'RELEASE_NOTES_URL' in params:
        schema_tags['release-notes-url'] = params['RELEASE_NOTES_URL']

    logger.info(f"Schema tags: {schema_tags}")

    add_generic_table_metadata()
    if generate_definitions:
        generate_and_add_column_descriptions()
    else:
        add_column_descriptions()


def change_status_to_archived(archived_table_id: str):
    """
    Change the status label of archived_table_id to 'archived.'

    :param archived_table_id: id for table that is being archived
    :type archived_table_id: str
    """
    try:
        client = bigquery.Client()
        prev_table = client.get_table(archived_table_id)
        prev_table.labels['status'] = 'archived'
        client.update_table(prev_table, ["labels"])
        assert prev_table.labels['status'] == 'archived'
    except NotFound:
        logger = logging.getLogger('base_script.cda_bq_etl.bq_helpers.create_modify')
        logger.warning("Couldn't find a table to archive. Likely this table's first release; otherwise an error.")
