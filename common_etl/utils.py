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
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage, exceptions


#    FILESYSTEM HELPERS


def get_filepath(dir_path, filename=None):
    """Get file path for location on VM.

    :param dir_path: directory portion of the filepath (starting at user home dir)
    :param filename: name of the file
    :return: full path to file
    """
    join_list = [os.path.expanduser('~'), dir_path]

    if filename:
        join_list.append(filename)

    return '/'.join(join_list)


def get_scratch_fp(bq_params, filename):
    """Construct filepath for VM output file.

    :param filename: name of the file
    :param bq_params: bq param object from yaml config
    :return: output filepath for VM
    """
    return get_filepath(bq_params['SCRATCH_DIR'], filename)


def json_datetime_to_str_converter(obj):
    """
    todo
    :param obj:
    :return:
    """
    if isinstance(obj, datetime.datetime):
        return str(obj)
    if isinstance(obj, datetime.date):
        return str(obj)
    if isinstance(obj, datetime.time):
        return str(obj)


def write_line_to_jsonl(jsonl_file_obj, line):
    """
    todo
    :param jsonl_file_obj:
    :param line:
    :return:
    """
    jsonl_file_obj.write(json.dumps(obj=line, default=json_datetime_to_str_converter))
    jsonl_file_obj.write('\n')


def write_list_to_jsonl(jsonl_fp, json_obj_list, mode='w'):
    """ Create a jsonl file for uploading data into BQ from a list<dict> obj.

    :param jsonl_fp: filepath of jsonl file to write
    :param json_obj_list: list<dict> object
    :param mode: 'a' if appending to a file that's being built iteratively
                 'w' if file data is written in a single call to the function
                     (in which case any existing data is overwritten)"""
    with open(jsonl_fp, mode) as file_obj:
        for line in json_obj_list:
            print(line)
            json.dump(obj=line, fp=file_obj, default=json_datetime_to_str_converter)
            file_obj.write('\n')


def create_tsv_row(row_list, null_marker="None"):
    """
    todo
    :param row_list:
    :param null_marker:
    :return:
    """
    print_str = ''
    last_idx = len(row_list) - 1

    for i, column in enumerate(row_list):
        if not column:
            column = null_marker

        delimiter = "\t" if i < last_idx else "\n"
        print_str += column + delimiter

    return print_str


#    BIGQUERY API HELPERS

def build_table_name_from_list(str_list):
    """Constructs a table name (str) from list<str>.

    :param str_list: a list<str> of table name segments
    :return: composed table name string
    """
    table_name = "_".join(str_list)

    # replace '.' with '_' so that the name is valid
    # ('.' chars not allowed -- issue with BEATAML1.0, for instance)
    return re.sub('[^0-9a-zA-Z_]+', '_', table_name)


def construct_table_name(api_params, prefix, suffix=None, include_release=True, release=None):
    """
    todo
    :param api_params:
    :param prefix:
    :param suffix:
    :param include_release:
    :param release:
    :return:
    """
    table_name = prefix

    if suffix:
        table_name += '_' + suffix

    if release:
        table_name += '_' + release
    elif include_release:
        table_name += '_' + api_params['RELEASE']

    return re.sub('[^0-9a-zA-Z_]+', '_', table_name)


def build_table_id(project, dataset, table):
    """ Build table_id in {project_id}.{dataset_id}.{table_name} format.

    :param project: project id
    :param dataset: dataset id
    :param table: table name
    :return: table_id
    """
    return '{}.{}.{}'.format(project, dataset, table)


def get_working_table_id(api_params, bq_params, table_name=None):
    """Get table id for development version of the db table.

    :param api_params: todo
    :param bq_params: bq param object from yaml config
    :param table_name: name of the bq table
    :return: table id
    """
    if not table_name:
        table_name = "_".join([get_rel_prefix(api_params), bq_params['MASTER_TABLE']])

    return build_table_id(bq_params["DEV_PROJECT"], bq_params["DEV_DATASET"], table_name)


def get_webapp_table_id(bq_params, table_name):
    """Get table id for webapp db table.

    :param bq_params: bq param object from yaml config
    :param table_name: name of the bq table
    :return: table id
    """
    return build_table_id(bq_params['DEV_PROJECT'], bq_params['APP_DATASET'], table_name)


def exists_bq_table(table_id):
    """Determine whether bq_table exists.

    :param table_id: table id in standard SQL format
    :return: True if exists, False otherwise
    """
    client = bigquery.Client()

    try:
        client.get_table(table_id)
    except NotFound:
        return False
    return True


def list_bq_tables(dataset_id, release=None):
    """
    todo
    :param dataset_id:
    :param release:
    :return:
    """
    table_list = list()
    client = bigquery.Client()
    tables = client.list_tables(dataset_id)

    for table in tables:
        if not release or release in table.table_id:
            table_list.append(table.table_id)

    return table_list


def get_bq_table_obj(table_id):
    """Get the bq table referenced by table_id.

    :param table_id: table id in standard SQL format
    :return: bq Table object
    """
    if not exists_bq_table(table_id):
        return None

    client = bigquery.Client()
    return client.get_table(table_id)


def copy_bq_table(bq_params, src_table, dest_table, replace_table=False):
    """Copy an existing BQ table into a new location.

    :param replace_table:
    :param bq_params: bq param object from yaml config
    :param src_table: Table to copy
    :param dest_table: Table to be created
    """
    client = bigquery.Client()

    job_config = bigquery.CopyJobConfig()

    if replace_table:
        delete_bq_table(dest_table)

    bq_job = client.copy_table(src_table, dest_table, job_config=job_config)

    if await_job(bq_params, client, bq_job):
        print("Successfully copied table:")
        print("src: {}\n dest: {}\n".format(src_table, dest_table))


def delete_bq_table(table_id):
    """Permanently delete BQ table located by table_id.

    :param table_id: table id in standard SQL format
    """
    client = bigquery.Client()
    client.delete_table(table_id, not_found_ok=True)


def delete_bq_dataset(dataset_id):
    """
    todo
    :param dataset_id:
    :return:
    """
    client = bigquery.Client()
    client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)


def load_table_from_query(bq_params, table_id, query):
    """Create a new BQ table from the returned results of querying an existing BQ table.

    :param bq_params: bq params from yaml config file
    :param table_id: table id in standard SQL format
    :param query: query which returns data to populate a new BQ table.
    """
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig(destination=table_id)
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    try:
        query_job = client.query(query, job_config=job_config)
        print(' - Inserting into {0}... '.format(table_id), end="")
        await_insert_job(bq_params, client, table_id, query_job)
    except TypeError as err:
        has_fatal_error(err)


def create_and_load_table(bq_params, jsonl_file, table_id, schema=None):
    """Creates BQ table and inserts case data from jsonl file.

    :param bq_params: bq param obj from yaml config
    :param jsonl_file: file containing case records in jsonl format
    :param schema: list of SchemaFields representing desired BQ table schema
    :param table_id: id of table to create
    """
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig()

    if schema:
        job_config.schema = schema
    else:
        print(" - No schema supplied for {}, using schema autodetect.".format(table_id))
        job_config.autodetect = True

    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    gs_uri = "gs://{}/{}/{}".format(bq_params['WORKING_BUCKET'], bq_params['WORKING_BUCKET_DIR'], jsonl_file)

    try:
        load_job = client.load_table_from_uri(gs_uri, table_id, job_config=job_config)
        print(' - Inserting into {0}... '.format(table_id), end="")
        await_insert_job(bq_params, client, table_id, load_job)
    except TypeError as err:
        has_fatal_error(err)


def create_and_load_table_from_tsv(bq_params, tsv_file, schema, table_id, num_header_rows=1):
    """Creates BQ table and inserts case data from jsonl file.

    :param bq_params: bq param obj from yaml config
    :param tsv_file: file containing case records in tsv format
    :param schema: list of SchemaFields representing desired BQ table schema
    :param table_id: id of table to create
    :param num_header_rows: todo
    """
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig()
    job_config.schema = schema
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.field_delimiter = '\t'
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.skip_leading_rows = num_header_rows
    job_config.null_marker = "None"

    gs_uri = "gs://{}/{}/{}".format(bq_params['WORKING_BUCKET'], bq_params['WORKING_BUCKET_DIR'], tsv_file)

    try:
        load_job = client.load_table_from_uri(gs_uri, table_id, job_config=job_config)

        print(' - Inserting into {0}... '.format(table_id), end="")
        await_insert_job(bq_params, client, table_id, load_job)
    except TypeError as err:
        has_fatal_error(err)


def get_query_results(query):
    """Returns BigQuery query result object.

    :param query: query string
    :return: result object
    """
    client = bigquery.Client()
    query_job = client.query(query)
    return query_job.result()


def await_insert_job(bq_params, client, table_id, bq_job):
    """Monitor the completion of BQ Job which does produce some result
    (usually data insertion).

    :param bq_params: bq params from yaml config file
    :param client: BQ api object, allowing for execution of bq lib functions
    :param table_id: table id in standard SQL format
    :param bq_job: A Job object, responsible for executing bq function calls
    """
    last_report_time = time.time()
    location = bq_params['LOCATION']
    job_state = "NOT_STARTED"

    while job_state != 'DONE':
        bq_job = client.get_job(bq_job.job_id, location=location)

        if time.time() - last_report_time > 30:
            print('\tcurrent job state: {0}...\t'.format(bq_job.state), end='')
            last_report_time = time.time()

        job_state = bq_job.state

        if job_state != 'DONE':
            time.sleep(2)

    bq_job = client.get_job(bq_job.job_id, location=location)

    if bq_job.error_result is not None:
        has_fatal_error(
            'While running BQ job: {}\n{}'.format(bq_job.error_result, bq_job.errors),
            ValueError)

    table = client.get_table(table_id)
    print(" done. {0} rows inserted.".format(table.num_rows))


def await_job(bq_params, client, bq_job):
    """Monitor the completion of BQ Job which doesn't return a result.

    :param bq_params: bq params from yaml config file
    :param client: BQ api object, allowing for execution of bq lib functions
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
        has_fatal_error("While running BQ job: {}\n{}".format(err_res, errs))


def get_graphql_api_response(api_params, query, fail_on_error=True):
    """
    todo
    :param api_params:
    :param query:
    :param fail_on_error:
    :return:
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
            has_fatal_error("Response status code {}:\n{}.\nRequest body:\n{}".
                            format(api_res.status_code, api_res.reason, req_body))

        print("Response code {}: {}".format(api_res.status_code, api_res.reason))
        print("Retry {} of {}...".format(tries, max_retries))
        time.sleep(3)

        api_res = requests.post(endpoint, headers=headers, json=req_body)

        tries += 1

    if tries > max_retries:
        # give up!
        api_res.raise_for_status()

    json_res = api_res.json()

    if 'errors' in json_res and json_res['errors']:
        if fail_on_error:
            has_fatal_error("Errors returned by {}.\nError json:\n{}".format(endpoint, json_res['errors']))

    return json_res


def load_bq_schema_from_json(bq_params, filename):
    # todo could this be reused for GDC?
    """
    Open table schema file and convert to python dict, in order to pass the data to
    BigQuery for table insertion.

    :param bq_params: bq param object from yaml config
    :param filename: name of the schema file
    :return: schema list, table metadata dict
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


def publish_table(api_params, bq_params, public_dataset, source_table_id, overwrite=False):
    """
    todo
    :param api_params:
    :param bq_params:
    :param public_dataset:
    :param source_table_id:
    :param overwrite:
    :return:
    """
    def get_publish_table_ids():
        """
        todo
        :return:
        """
        rel_prefix = get_rel_prefix(api_params)
        data_source = api_params['DATA_SOURCE']

        split_table_id = source_table_id.split('.')
        dataset_type = split_table_id[-1]
        dataset_type.replace(rel_prefix, '').strip('_')

        curr_table_name = build_table_name_from_list([dataset_type, data_source, 'current'])
        curr_table_id = build_table_id(bq_params['PROD_PROJECT'], public_dataset, curr_table_name)
        vers_table_name = build_table_name_from_list([dataset_type, data_source, rel_prefix])
        vers_table_id = build_table_id(bq_params['PROD_PROJECT'], public_dataset + '_versioned', vers_table_name)
        return curr_table_id, vers_table_id

    def change_status_to_archived():
        """
        todo
        :return:
        """
        client = bigquery.Client()
        current_release_tag = get_rel_prefix(api_params)

        stripped_table_id = source_table_id.replace(current_release_tag, "")
        previous_release_tag = get_rel_prefix(api_params, return_last_version=True)
        prev_table_id = stripped_table_id + previous_release_tag

        try:
            prev_table = client.get_table(prev_table_id)
            prev_table.labels['status'] = 'archived'
            client.update_table(prev_table, ["labels"])
            assert prev_table.labels['status'] == 'archived'
        except NotFound:
            print("Couldn't find a table to archive. Might be that this is the first table release?")

    current_table_id, versioned_table_id = get_publish_table_ids()

    if exists_bq_table(source_table_id):
        print("Publishing {}".format(versioned_table_id))
        copy_bq_table(bq_params, source_table_id, versioned_table_id, overwrite)

        print("Publishing {}".format(current_table_id))
        copy_bq_table(bq_params, source_table_id, current_table_id, overwrite)

        print("Updating friendly name for {}\n".format(versioned_table_id))
        is_gdc = True if api_params['DATA_SOURCE'] == 'gdc' else False
        update_friendly_name(api_params, versioned_table_id, is_gdc)

        change_status_to_archived()


def update_table_metadata(table_id, metadata):
    """Modify an existing BQ table with additional metadata.

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
    Alters table labels for existing BQ table (for instance, when changes are needed to a published table's labels).

    :param table_id: target BQ table id
    :param labels_to_remove_list: optional list of label keys to remove
    :param labels_to_add_dict: optional dictionary of label key-value pairs to add to table metadata
    """
    client = bigquery.Client()
    table = get_bq_table_obj(table_id)

    print("Processing labels for {}".format(table_id))

    labels = table.labels

    if labels_to_remove_list and isinstance(labels_to_remove_list, list):
        for label in labels_to_remove_list:
            if label in labels:
                del labels[label]
                table.labels[label] = None
        print("Deleting label(s)--now: {}".format(labels))
    elif labels_to_remove_list and not isinstance(labels_to_remove_list, list):
        has_fatal_error("labels_to_remove_list not provided in correct format, should be a list.")

    if labels_to_add_dict and isinstance(labels_to_add_dict, dict):
        labels.update(labels_to_add_dict)
        print("Adding/Updating label(s)--now: {}".format(labels))
    elif labels_to_add_dict and not isinstance(labels_to_add_dict, dict):
        has_fatal_error("labels_to_add_dict not provided in correct format, should be a dict.")

    table.labels = labels
    client.update_table(table, ["labels"])

    assert table.labels == labels
    print("Labels updated successfully!\n")


def update_friendly_name(api_params, table_id, custom_name=None, is_gdc=True):
    """Modify a table's friendly name metadata.

    :param api_params: api param object from yaml config
    :param table_id: table id in standard SQL format
    :param custom_name: By default, appends "'REL' + api_params['RELEASE'] + ' VERSIONED'"
    :param is_gdc: If this is GDC, we add REL before the version
    onto the existing friendly name. If custom_name is specified, this behavior is
    overridden, and the table's friendly name is replaced entirely.
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
    """Modify an existing table's field descriptions.

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
            print("Still no description for field: {0}".format(field['name']))

        mod_field = bigquery.SchemaField.from_api_repr(field)
        new_schema.append(mod_field)

    table.schema = new_schema

    client.update_table(table, ['schema'])


#    GOOGLE CLOUD STORAGE HELPERS


def upload_to_bucket(bq_params, scratch_fp, delete_local=False):
    """Uploads file to a google storage bucket (location specified in yaml config).

    :param bq_params: bq param object from yaml config
    :param scratch_fp: name of file to upload to bucket
    :param delete_local: delete scratch file created on VM
    """
    if not os.path.exists(scratch_fp):
        has_fatal_error("Invalid filepath: {}".format(scratch_fp), FileNotFoundError)

    try:
        storage_client = storage.Client(project="")

        jsonl_output_file = scratch_fp.split('/')[-1]
        bucket_name = bq_params['WORKING_BUCKET']
        bucket = storage_client.bucket(bucket_name)

        blob_name = "{}/{}".format(bq_params['WORKING_BUCKET_DIR'], jsonl_output_file)
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(scratch_fp)

        print("Successfully uploaded file to {}/{}. ".format(bucket_name, blob_name), end="")

        if delete_local:
            os.remove(scratch_fp)
            print("Local file deleted.\n")
        else:
            print("Local file not deleted (location: {}).\n".format(scratch_fp))

    except exceptions.GoogleCloudError as err:
        has_fatal_error("Failed to upload to bucket.\n{}".format(err))
    except FileNotFoundError as err:
        has_fatal_error("File not found, failed to access local file.\n{}".format(err))


# not currently used, but leaving it so I don't have to re-write it down the road
def download_from_bucket(bq_params, filename):
    """
    todo
    :param bq_params:
    :param filename:
    :return:
    """
    storage_client = storage.Client(project="")
    blob_name = "{}/{}".format(bq_params['WORKING_BUCKET_DIR'], filename)
    bucket = storage_client.bucket(bq_params['WORKING_BUCKET'])
    blob = bucket.blob(blob_name)

    scratch_fp = get_scratch_fp(bq_params, filename)
    with open(scratch_fp, 'wb') as file_obj:
        blob.download_to_file(file_obj)


#    ANALYZE DATA


def normalize_value(value):
    """
    todo
    :param value:
    :return:
    """
    if value in ('NA', 'N/A', 'null', 'None', ''):
        return None
    if value in ('False', 'false', 'FALSE'):
        return False
    if value in ('True', 'true', 'TRUE'):
        return True
    else:
        return value


def check_value_type(value):
    """Checks value for type (possibilities are string, float and integers).

    :param value: value to type check
    :return: type in BQ column format
    """
    # if has leading zero, then should be considered a string, even if only
    # composed of digits

    value = normalize_value(value)

    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, int):
        return "INTEGER"
    if isinstance(value, float):
        return "FLOAT"
    if isinstance(value, list):
        return "ARRAY"
    if isinstance(value, list):
        return "RECORD"
    if not value:
        return None
    # check to see if value is numeric, float or int

    if '.' in value and ':' not in value:
        split_value = value.split('.')
        if len(split_value) == 2:
            if split_value[0].isdigit() and split_value[1].isdigit():
                # if in float form, but fraction is .0, .00, etc., then consider it an integer
                if int(split_value[1]) == 0:
                    return "INTEGER"
                return "FLOAT"
        return "STRING"
    elif value.isnumeric() and not value.isdigit() and not value.isdecimal():
        return "NUMERIC"
    elif value.isdigit():
        return "INTEGER"
    elif value.startswith("0") and ':' not in value and '-' not in value:
        return "STRING"
    # BQ CANONICAL DATE/TIME FORMATS: (see https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types)
    # BQ date format: 'YYYY-[M]M-[D]D'
    date_re_str = r"[0-9]{4}-(0[1-9]|1[0-2]|[0-9])-([0-2][0-9]|[3][0-1]|[0-9])"
    date_pattern = re.compile(date_re_str)
    if re.fullmatch(date_pattern, value):
        return "DATE"

    # TIME: [H]H:[M]M:[S]S[.DDDDDD]
    time_re_str = r"([0-1][0-9]|[2][0-3]|[0-9]{1}):([0-5][0-9]|[0-9]{1}):([0-5][0-9]|[0-9]{1}])(\.[0-9]{1,6}|)"
    time_pattern = re.compile(time_re_str)

    if re.fullmatch(time_pattern, value):
        return "TIME"

    # TIMESTAMP: YYYY-[M]M-[D]D[( |T)[H]H:[M]M:[S]S[.DDDDDD]][time zone]
    timestamp_re_str = date_re_str + r'( |T)' + time_re_str + r"([ \-:A-Za-z0-9]*)"
    timestamp_pattern = re.compile(timestamp_re_str)
    if re.fullmatch(timestamp_pattern, value):
        return "TIMESTAMP"

    return "STRING"


def test_check_value_type():
    """
    todo
    :return:
    """
    # todo relocate test

    value_type_dict = {
        "000": "STRING",
        "0.0": "INTEGER",
        "0.001": "FLOAT",
        "100": "INTEGER",
        "Hi": "STRING",
        "0.1.1": "STRING",
        "1.1.1": "STRING",
        "111-222": "STRING",
        "2000-12-31": "DATE",
        "2000-1-1": "DATE",
        "2000-01-01": "DATE",
        "9:03:22.0001": "TIME",
        "09:03:22": "TIME",
        "9:3:22": "TIME",
        "2019-05-01T13:44:50.898263-05:00": "TIMESTAMP",
        "2019-05-01 13:44:50.898263-05:00": "TIMESTAMP",
        "2019-05-01T13:44:50.898263": "TIMESTAMP",
        "2019-05-01 13:44:50.898263": "TIMESTAMP",
        "2019-5-1T13:44:50.898263": "TIMESTAMP",
        "True": "BOOLEAN",
        "False": "BOOLEAN"
    }

    for value, expected_type in value_type_dict.items():
        actual_type = check_value_type(value)

        assert expected_type == actual_type, \
            "Type mismatch for {}: expected {}, actual {}".format(value, expected_type, actual_type)

    print("Types checked successfully!")


def resolve_type_conflicts(types_dict):
    """
    todo
    :param types_dict:
    :return:
    """
    for field, types_set in types_dict.items():
        if len(types_set) == 1:
            for col_type in types_set:
                types_dict[field] = col_type
        if len(types_set) == 0:
            types_dict[field] = "STRING"
        elif "STRING" in types_set:
            types_dict[field] = "STRING"
        elif "FLOAT" in types_set:
            types_dict[field] = "FLOAT"


def infer_data_types(flattened_json):
    """Infer data type of fields based on values contained in dataset.

    :param flattened_json: file containing dict of {field name: set of field values}
    :return: dict of field names and inferred type (None if no data in value set)
    """
    data_types = dict()

    for column in flattened_json:
        data_types[column] = None

        for value in flattened_json[column]:
            if data_types[column] == 'STRING':
                break

            # adding this change because organoid submitter_ids look like
            # ints, but they should be str for uniformity
            if column[-2:] == 'id':
                data_types[column] = 'STRING'
                break

            val_type = check_value_type(str(value))

            if not val_type:
                continue
            if val_type in ('FLOAT', 'STRING') or (val_type in ('INTEGER', 'BOOLEAN') and not data_types[column]):
                data_types[column] = val_type

    return data_types


#       MISC UTILITIES


def get_rel_prefix(api_params, return_last_version=False, version=None):
    """Get current release number/date (set in yaml config).
    todo
    :param api_params:
    :param version:
    :param return_last_version:
    :return: release abbreviation
    """
    rel_prefix = ''

    if 'REL_PREFIX' in api_params and api_params['REL_PREFIX']:
        rel_prefix += api_params['REL_PREFIX']

    if version:
        rel_prefix += version
        return rel_prefix

    if 'RELEASE' in api_params and api_params['RELEASE']:
        rel_number = api_params['RELEASE']

        if return_last_version:
            if api_params['DATA_SOURCE'] == 'gdc':
                rel_number -= 1
            elif api_params['DATA_SOURCE'] == 'pdc':
                rel_number = api_params['PREV_RELEASE']

        rel_prefix += rel_number

    return rel_prefix


def format_seconds(seconds):
    """
    todo
    :param seconds:
    :return:
    """
    if seconds > 3600:
        return time.strftime("%-H hours, %-M minutes, %-S seconds", time.gmtime(seconds))
    if seconds > 60:
        return time.strftime("%-M minutes, %-S seconds", time.gmtime(seconds))

    return time.strftime("%-S seconds", time.gmtime(seconds))


def load_config(args, yaml_dict_keys, validate_config=None):
    """Opens yaml file and retrieves configuration parameters.

    :param validate_config:
    :param args: args param from python bash cli
    :param yaml_dict_keys: tuple of strings representing a subset of the yaml file's
    top-level dict keys
    :return: tuple of dicts from yaml file (as requested in yaml_dict_keys)
    """
    def open_yaml_and_return_dict(yaml_name):
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
            return {key:yaml_dict[key] for key in yaml_dict_keys}

    if len(args) < 2 or len(args) > 3:
        has_fatal_error("")
    if len(args) == 2:
        singleton_yaml_dict = open_yaml_and_return_dict(args[1])
        return tuple([singleton_yaml_dict[key] for key in yaml_dict_keys])

    shared_yaml_dict = open_yaml_and_return_dict(args[1])

    data_type_yaml_dict = open_yaml_and_return_dict(args[2])

    merged_yaml_dict = {key:{} for key in yaml_dict_keys}

    for key in yaml_dict_keys:
        if key not in shared_yaml_dict and key not in data_type_yaml_dict:
            has_fatal_error("{} not found in shared or data type-specific yaml config".format(key))
        elif not shared_yaml_dict[key] and not data_type_yaml_dict[key]:
            has_fatal_error("No values found for {} in shared or data type-specific yaml config".format(key))

        if key in shared_yaml_dict and shared_yaml_dict[key]:
            merged_yaml_dict[key] = shared_yaml_dict[key]

            if key in data_type_yaml_dict and data_type_yaml_dict[key]:
                merged_yaml_dict[key].update(data_type_yaml_dict[key])
        else:
            merged_yaml_dict[key] = data_type_yaml_dict[key]

    if validate_config:
        pass
        # todo
        # validate_config(tuple(return_dicts))

    return tuple([merged_yaml_dict[key] for key in yaml_dict_keys])


def has_fatal_error(err, exception=None):
    """Error handling function--outputs error str or list<str>;
    optionally throws Exception as well.

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
    pp = pprint.PrettyPrinter(indent=4)

    pp.pprint(print_str)
