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

import requests
import yaml
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage, exceptions


def has_fatal_error(err, exception=None):
    """
    Error handling function, formats error strings or a list of strings, and optionally
    shares exception info.
    :param err: error message string
    :param exception: Exception object relating to the fatal error, defaults to none
    """
    err_str = '[ERROR] '
    error_output = ''
    if isinstance(err, list):
        for item in err:
            error_output += err_str + str(item) + '\n'
    else:
        error_output = err_str + err

    print(error_output)

    if exception:
        raise exception
    sys.exit(1)


##################################################################################
#
#       DATA ANALYSIS FUNCTIONS
#
##################################################################################


def check_value_type(value):
    """
    Checks value for type (possibilities are string, float and integers)
    :param value: value to type check
    :return: type in BQ column format
    """
    # if has leading zero, then should be considered a string, even if only
    # composed of digits
    val_is_none = value in ('NA', 'null', 'None') or not value
    val_is_bool = value in ('True', 'False', True, False)
    val_is_decimal = value.startswith('0.')
    val_is_id = value.startswith('0') and not val_is_decimal and len(value) > 1

    try:
        float(value)
        val_is_num = True
        # Changing this because google won't accept loss of precision in the
        # data insert job
        # (won't cast 1.0 as 1)
        val_is_float = not value.isdigit()
        # If this is used, a field with only trivial floats will be cast as
        # Integer. However, BQ errors due to loss
        # of precision.
        # val_is_float = True if int(float(value)) != float(value) else False
    except ValueError:
        val_is_num = False
        val_is_float = False

    if val_is_none:
        return None
    if val_is_id:
        return 'STRING'
    if val_is_decimal or val_is_float:
        return 'FLOAT'
    if val_is_num:
        return 'INTEGER'
    if val_is_bool:
        return 'BOOLEAN'

    return 'STRING'


def infer_data_types(flattened_json):
    """
    Infer data type of fields based on values contained in dataset.
    :param flattened_json: file containing dict of {field name: set of field values}
    :return: dict of field names and inferred type (None if no data in value set).
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
            if val_type in ('FLOAT', 'STRING') or (
                    val_type in ('INTEGER', 'BOOLEAN') and not data_types[column]):
                data_types[column] = val_type

    return data_types


def collect_values(fields, field, parent, field_grp_prefix):
    """
    Recursively inserts sets of values for a given field into return dict (
    used to infer field data type)
    :param fields: A dict of key:value pairs -- field_name : set(
    field_values)
    :param field: field name
    :param parent: dict containing field and it's values
    :param field_grp_prefix: string representation of current location in field hierarchy
    :return: field_dict containing field names and a set of its values.
    """
    # If the value of parent_dict[key] is a list at this level, and a dict at the next
    # (or a dict at this level, as seen in second conditional statement),
    # iterate over each list element's dictionary entries. (Sometimes lists are composed
    # of strings rather than dicts, and those are later converted to strings.)
    field_name = field_grp_prefix + field
    new_prefix = field_name + '.'

    if isinstance(parent[field], list) \
            and len(parent[field]) > 0 and isinstance(parent[field][0], dict):
        for dict_item in parent[field]:
            for dict_key in dict_item:
                fields = collect_values(fields, dict_key, dict_item, new_prefix)
    elif isinstance(parent[field], dict):
        for dict_key in parent[field]:
            fields = collect_values(fields, dict_key, parent[field], new_prefix)
    else:
        if field_name not in fields:
            fields[field_name] = set()

        # This type of list can be converted to a comma-separated value string
        if isinstance(parent[field], list):
            value = ", ".join(parent[field])
        else:
            value = parent[field]

        fields[field_name].add(value)

    return fields


##################################################################################
#
#       BQ SCHEMA CREATION FUNCTIONS
#
##################################################################################


def create_mapping_dict(endpoint):
    """
    Creates a dict containing field mappings for given endpoint.
    Note: only differentiates the GDC API's 'long' type (called 'integer' in GDC data
    dictionary) and 'float' type (called 'number' in GDC data dictionary). All others
    typed as string.
    :param endpoint: API endpoint for which to retrieve mapping
    :return: dict of field maps. Each entry contains field name, type, and description
    """
    field_mapping_dict = {}

    # retrieve mappings json object
    res = requests.get(endpoint + '/_mapping')
    field_mappings = res.json()['_mapping']

    for field in field_mappings:
        # convert data types from GDC format to formats used in BQ
        if field_mappings[field]['type'] == 'long':
            field_type = 'INTEGER'
        elif field_mappings[field]['type'] == 'float':
            field_type = 'FLOAT'
        else:
            field_type = 'STRING'

        # create json object of field mapping data
        field_mapping_dict[field] = {
            'name': field.split('.')[-1],
            'type': field_type,
            'description': field_mappings[field]['description']
        }

    return field_mapping_dict


def generate_bq_schema(schema_dict, record_type, expand_fields_list):
    """
    Generates BigQuery SchemaField list for insertion of case records.
    :param schema_dict: dict of schema fields
    :param record_type: type of field/field group
    :param expand_fields_list: list of field groups included in API request
    :return: list of SchemaFields for case record insertion
    """
    # add fields to a list in order to generate a dict representing nested fields
    field_group_names = [record_type]
    nested_depth = 0

    for field_group in expand_fields_list.split(','):
        nested_field_name = record_type + '.' + field_group
        nested_depth = max(nested_depth, len(nested_field_name.split('.')))
        field_group_names.append(nested_field_name)

    record_lists_dict = {field_grp_name: [] for field_grp_name in field_group_names}
    # add field to correct field grouping list based on full field name
    for field in schema_dict:
        # record_lists_dict key is equal to the parent field components of
        # full field name
        record_lists_dict[get_field_group(field)].append(schema_dict[field])

    temp_schema_field_dict = {}

    while nested_depth >= 1:
        for field_group_name in record_lists_dict:
            split_group_name = field_group_name.split('.')

            # building from max depth inward, to avoid iterating through
            # entire schema object in order to append
            # child field groupings. Therefore, skip any field groupings at a
            # shallower depth.
            if len(split_group_name) != nested_depth:
                continue

            schema_field_sublist = []

            for record in record_lists_dict[field_group_name]:
                schema_field_sublist.append(
                    bigquery.SchemaField(name=record['name'],
                                         field_type=record['type'],
                                         mode='NULLABLE',
                                         description=record['description'],
                                         fields=()))

            # parent_name = '.'.join(split_group_name[:-1])
            parent_name = get_field_group(field_group_name)

            if field_group_name in temp_schema_field_dict:
                schema_field_sublist += temp_schema_field_dict[field_group_name]

            if parent_name:
                if parent_name not in temp_schema_field_dict:
                    temp_schema_field_dict[parent_name] = list()

                temp_schema_field_dict[parent_name].append(
                    bigquery.SchemaField(name=get_field_name(field_group_name),
                                         field_type='RECORD',
                                         mode='REPEATED',
                                         description='',
                                         fields=tuple(schema_field_sublist)))
            else:
                if nested_depth > 1:
                    has_fatal_error("Empty parent_name at level {}"
                                    .format(nested_depth), ValueError)
                return schema_field_sublist

        nested_depth -= 1
    return None


def create_schema_dict(api_params, bq_params, is_webapp=False):
    """
    Creates schema dict using master table's bigquery.table.Table.schema attribute
    :param api_params: api params from yaml config file
    :param bq_params: bq params from yaml config file
    :param is_webapp: is script currently running the 'create_webapp_tables' step?
    :return: flattened schema dict in format:
        {full field name: {name: 'name', type: 'field_type', description: 'description'}}
    """
    table_id = get_working_table_id(bq_params)

    client = bigquery.Client()
    table_obj = client.get_table(table_id)

    return get_schema_from_master_table(api_params,
                                        dict(),
                                        get_base_grp(api_params),
                                        table_obj.schema,
                                        is_webapp)


##################################################################################
#
#       GOOGLE CLOUD HELPERS
#
##################################################################################


def get_query_results(query):
    """
    Returns result of BigQuery query.
    :param query: query string
    :return: result object
    """
    client = bigquery.Client()

    query_job = client.query(query)
    return query_job.result()


def load_table_from_query(bq_params, table_id, query):
    """
    Create a new BQ table from the returned results of querying an existing BQ db.
    :param bq_params: bq params from yaml config file
    :param table_id: table id in standard SQL format
    :param query: query which returns data to populate a new BQ table.
    """
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig(destination=table_id)
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    try:
        query_job = client.query(query, job_config=job_config)
        print(' - Inserting into {}... '.format(table_id), end="")
        await_insert_job(bq_params, client, table_id, query_job)
    except TypeError as err:
        has_fatal_error(err)


def create_and_load_table(bq_params, jsonl_file, schema, table_id):
    """
    Creates BQ table and inserts case data from jsonl file.
    :param bq_params: bq params from yaml config file
    :param jsonl_file: file containing case records in jsonl format
    :param schema: list of SchemaFields representing desired BQ table schema
    :param table_id: id of table to create
    """

    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig()
    job_config.schema = schema
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    gs_uri = get_working_gs_uri(bq_params, jsonl_file)

    try:
        load_job = client.load_table_from_uri(gs_uri, table_id, job_config=job_config)

        print(' - Inserting into {}... '.format(table_id), end="")
        await_insert_job(bq_params, client, table_id, load_job)
    except TypeError as err:
        has_fatal_error(err)


def await_job(bq_params, client, bq_job):
    """
    Monitor the completion of BQ Job which doesn't return a result
    :param bq_params: bq params from yaml config file
    :param client: BQ api object, allowing for execution of bq lib functions.
    :param bq_job: A Job object, responsible for executing bq function calls.
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


def await_insert_job(bq_params, client, table_id, bq_job):
    """
    Monitor the completion of BQ Job which does produce some result
    (table insertion usually)
    :param bq_params: bq params from yaml config file
    :param client: BQ api object, allowing for execution of bq lib functions.
    :param table_id: table id in standard SQL format
    :param bq_job: A Job object, responsible for executing bq function calls.
    """
    last_report_time = time.time()

    location = bq_params['LOCATION']
    job_state = "NOT_STARTED"

    while job_state != 'DONE':
        bq_job = client.get_job(bq_job.job_id, location=location)

        if time.time() - last_report_time > 15:
            print('\t- job is currently in state {}'.format(bq_job.state))
            last_report_time = time.time()

        job_state = bq_job.state

        if job_state != 'DONE':
            time.sleep(2)

    bq_job = client.get_job(bq_job.job_id, location=location)

    if bq_job.error_result is not None:
        has_fatal_error('While running BQ job: {}\n{}'
                        .format(bq_job.error_result, bq_job.errors), ValueError)

    table = client.get_table(table_id)

    print(" done. {} rows inserted.".format(table.num_rows))


def get_program_list(bq_params):
    """
    Get a list of the programs participating in submitting data to GDC's research program
    :param bq_params: bq params from yaml config file
    :return: list of programs participating in GDC data sharing
    """
    programs_query = ("""
        SELECT DISTINCT(proj) 
        FROM (
            SELECT SPLIT(
                (SELECT project_id
                 FROM UNNEST(project)), '-')[OFFSET(0)] AS proj
            FROM `{}`)
        ORDER BY proj
    """).format(
        get_working_table_id(bq_params))

    return {prog.proj for prog in get_query_results(programs_query)}


def get_cases_by_program(bq_params, program):
    """
    Get a dict obj containing all the cases associated with a given program.
    :param bq_params: bq params from yaml config file
    :param program: the program from which the cases originate.:
    :return:
    """
    cases = []

    sample_table_id = get_biospecimen_table_id(bq_params, program)

    query = ("""
        SELECT * 
        FROM `{}` 
        WHERE case_id IN (
            SELECT DISTINCT(case_gdc_id) 
            FROM `{}`
            WHERE proj = '{}')
    """).format(
        get_working_table_id(bq_params),
        sample_table_id,
        program)

    for case_row in get_query_results(query):
        cases.append(dict(case_row.items()))

    return cases


def exists_bq_table(table_id):
    """
    Determine whether bq_table exists or now.
    :param table_id: table id in standard SQL format
    :return: True if exists, False otherwise
    """
    client = bigquery.Client()

    try:
        client.get_table(table_id)
    except NotFound:
        return False
    return True


def get_bq_table(table_id):
    """
    Get the bq table referenced by table_id
    :param table_id: table id in standard SQL format
    :return: bq Table object
    """
    if not exists_bq_table(table_id):
        return None

    client = bigquery.Client()
    return client.get_table(table_id)


def update_bq_table(table_id, metadata):
    """
    Modify an existing BQ table with additional metadata.
    :param table_id: table id in standard SQL format
    :param metadata: metadata containing new field and table attributes
    """
    client = bigquery.Client()
    table = get_bq_table(table_id)

    table.labels = metadata['labels']
    table.friendly_name = metadata['friendlyName']
    table.description = metadata['description']

    client.update_table(table, ["labels", "friendly_name", "description"])

    assert table.labels == metadata['labels']
    assert table.friendly_name == metadata['friendlyName']
    assert table.description == metadata['description']


def modify_friendly_name(bq_params, table_id):
    """
    Modify a table's friendly name metadata.
    :param bq_params: bq params from yaml config file
    :param table_id: table id in standard SQL format
    """
    client = bigquery.Client()
    table = get_bq_table(table_id)

    friendly_name = table.friendly_name
    friendly_name += ' REL' + bq_params['GDC_RELEASE'] + ' VERSIONED'

    table.friendly_name = friendly_name

    client.update_table(table, ["friendly_name"])


def modify_friendly_name_custom(table_id, new_name):
    """
    Modify a table's friendly name metadata.
    :param table_id: table id in standard SQL format
    :param new_name: string containing a new table friendly name value
    """
    client = bigquery.Client()
    table = get_bq_table(table_id)

    table.friendly_name = new_name

    print(table.friendly_name)

    client.update_table(table, ["friendly_name"])


def delete_bq_table(table_id):
    """
    Permanently delete BQ table located by table_id.
    :param table_id: table id in standard SQL format
    """
    client = bigquery.Client()
    client.delete_table(table_id, not_found_ok=True)

    print("deleted table: {}".format(table_id))


def copy_bq_table(bq_params, src_table, dest_table):
    """
    Copy an existing BQ table into a new location.
    :param bq_params: bq params from yaml config file
    :param src_table: Table to copy
    :param dest_table: Table to be created
    """
    client = bigquery.Client()

    bq_job = client.copy_table(src_table, dest_table)

    if await_job(bq_params, client, bq_job):
        print("Successfully copied table:")
        print("src:  {}\n dest: {}\n".format(src_table, dest_table))


def update_table_schema(table_id, new_descriptions):
    """
    Modify an existing table's field descriptions
    :param table_id: table id in standard SQL format
    :param new_descriptions: dict of field names and new description strings
    """
    client = bigquery.Client()
    table = get_bq_table(table_id)

    new_schema = []

    for schema_field in table.schema:
        field = schema_field.to_api_repr()

        if field['name'] in new_descriptions.keys():
            name = field['name']
            field['description'] = new_descriptions[name]
        elif field['description'] == '':
            print("Still no description for field: " + field['name'])

        mod_field = bigquery.SchemaField.from_api_repr(field)
        new_schema.append(mod_field)

    table.schema = new_schema

    client.update_table(table, ['schema'])


def get_schema_from_master_table(api_params, schema, field_grp, fields=None,
                                 is_webapp=False):
    """
    Recursively build schema using master table's bigquery.table.Table.schema attribute
    :param api_params: api params from yaml config file
    :param schema: dict of flattened schema entries
    :param field_grp: current field group name
    :param fields: schema field entries for field_group
    :param is_webapp: is script currently running the 'create_webapp_tables' step?
    :return: flattened schema dict {full field name:
        {name: 'name', type: 'field_type', description: 'description'}}
    """
    if field_grp not in api_params['FIELD_CONFIG'].keys():
        return schema

    for field in fields:
        field_dict = field.to_api_repr()
        schema_key = get_field_key(field_grp, field_dict['name'])

        if 'fields' in field_dict:
            schema = get_schema_from_master_table(api_params,
                                                  schema,
                                                  schema_key,
                                                  field.fields,
                                                  is_webapp)

            for required_column in get_required_columns(api_params, field_grp):
                schema[required_column]['mode'] = 'REQUIRED'
        else:
            field_dict['name'] = get_bq_name(api_params,
                                             schema_key,
                                             is_webapp=is_webapp)

            schema[schema_key] = field_dict

    return schema


def modify_fields_for_app(api_params, schema, column_order_dict, columns):
    """
    Alter field naming conventions so that they're compatible with those in the web app.
    :param api_params: api params from yaml config file
    :param schema: dict containing schema records
    :param column_order_dict:
    :param columns: dict containing table column keys
    :return:
    """
    renamed_fields = dict(api_params['RENAMED_FIELDS'])

    excluded_field_grps = get_app_excluded_field_grps(api_params)
    excluded_fields = get_excluded_fields(api_params, column_order_dict.keys(),
                                          is_webapp=True)

    for field_grp in column_order_dict.keys():
        # rename case_id no matter which field_grp it's in
        for renamed_field in renamed_fields.keys():
            group_order_dict = column_order_dict[field_grp]

            if renamed_field in group_order_dict:
                new_field = renamed_fields[renamed_field]
                group_order_dict[new_field] = group_order_dict[renamed_field]
                group_order_dict.pop(renamed_field)

            if field_grp in columns and renamed_field in columns[field_grp]:
                columns[field_grp].add(renamed_fields[renamed_field])
                columns[field_grp].remove(renamed_field)

    # field is fully associated name
    for field in schema.copy().keys():
        base_field_grp = ".".join(field.split('.')[:-1])
        base_grp_order = column_order_dict[base_field_grp]
        field_name = field.split('.')[-1]

        # substitute base field name for prefixed
        schema[field]['name'] = field_name

        # exclude any field groups or fields explicitly excluded in yaml
        if field in excluded_fields or base_field_grp in excluded_field_grps:
            schema.pop(field)
        # field exists in renamed_fields, change its name
        elif field in renamed_fields:
            new_field = renamed_fields[field]

            schema[field]['name'] = new_field.split('.')[-1]
            schema[new_field] = schema[field]
            schema.pop(field)

            # change the field name in the column order dict
            if base_field_grp in column_order_dict and field in base_grp_order:
                base_grp_order[new_field] = base_grp_order[field]
                base_grp_order.pop(field)

        if field in excluded_fields and base_field_grp in column_order_dict:
            # remove excluded field from column order lists
            if field in base_grp_order:
                base_grp_order.pop(field)


def from_schema_file_to_obj(bq_params, filename):
    fp = '/'.join([bq_params['BQ_REPO'], bq_params['SCHEMA_DIR'], filename])

    with open(fp, 'r') as schema_file:
        return json.load(schema_file)


def to_bq_schema_obj(schema_field_dict):
    """
    Convert schema entry dict to SchemaField object.
    :param schema_field_dict: dict containing schema field keys
    (name, field_type, mode, fields, description)
    :return: bigquery.SchemaField object
    """
    return bigquery.SchemaField.from_api_repr(schema_field_dict)


def upload_to_bucket(bq_params, filename):
    """
    Uploads file to a google storage bucket (location specified in yaml config)
    :param bq_params: bq params from yaml config file
    :param filename: name of file to upload to bucket
    """
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bq_params['WORKING_BUCKET'])
        blob = bucket.blob("/".join([bq_params['WORKING_BUCKET_DIR'], filename]))
        blob.upload_from_filename(get_scratch_path(bq_params, filename))
    except exceptions.GoogleCloudError as err:
        has_fatal_error("Failed to upload to bucket.\n{}".format(err))


##################################################################################
#
#       YAML CONFIG GETTERS
#
##################################################################################


def get_fields_to_idx_last(api_params):
    """
    Get list of fields to always include at the end of merged tables, via the yaml config.
    :param api_params: api params from yaml config file
    :return: fields to include at the end of the table
    """
    if 'FG_CONFIG' not in api_params or \
            'fields_to_idx_last' not in api_params['FG_CONFIG']:
        has_fatal_error("Missing FG_CONFIG in YAML", KeyError)

    return api_params['FG_CONFIG']['fields_to_idx_last']


def get_column_order_list(api_params, field_grp):
    """
    Returns table's column order list (from yaml config file)
    :param api_params: api params from yaml config file
    :param field_grp: table for which to retrieve column order
    :return: table's column order list
    """
    if field_grp not in api_params['FIELD_CONFIG']:
        has_fatal_error("'{}' not found in FIELD_CONFIG in yaml config".format(field_grp))

    if 'column_order' not in api_params['FIELD_CONFIG'][field_grp]:
        has_fatal_error(
            "No column order provided for {} in yaml config.".format(field_grp))

    field_order_list = api_params['FIELD_CONFIG'][field_grp]['column_order']

    # return full field key, in order, for given field_grp
    return [get_field_key(field_grp, field) for field in field_order_list]


def get_required_columns(api_params, table):
    """
    Get list of required columns.
    :param api_params: api params from yaml config file
    :param table: name of table for which to retrieve required columns.
    :return: list of required columns (currently, only returns table's primary id)
    """
    if table not in api_params['FIELD_CONFIG']:
        return None
    if 'id_key' not in api_params['FIELD_CONFIG'][table]:
        return None

    table_id_field = api_params['FIELD_CONFIG'][table]['id_key']
    table_id_name = get_field_key(table, table_id_field)
    return [table_id_name]


def get_master_table_name(bq_params):
    """
    Get master table name from yaml config.
    :param bq_params: bq params from yaml config file
    :return: master table name
    """
    return "_".join([get_gdc_rel(bq_params), bq_params['MASTER_TABLE']])


def get_renamed_fields(api_params):
    """
    Get renamed fields dict from yaml config.
    :param api_params: api params from yaml config file
    :return: renamed fields dict
    """
    if 'RENAMED_FIELDS' not in api_params:
        has_fatal_error("RENAMED_FIELDS not found in API_PARAMS")
    if not api_params['RENAMED_FIELDS']:
        return None

    return api_params['RENAMED_FIELDS']


def get_new_field_name(api_params, field):
    """
    Gets the new field name for an existing field. Used to rename fields
    for web app integration.
    :param api_params: api params from yaml config file
    :param field:
    :return:
    """
    renamed_field_dict = get_renamed_fields(api_params)
    if not renamed_field_dict or field not in renamed_field_dict:
        return None

    return renamed_field_dict[field]


def get_grp_id_field(api_params, field_grp_key, is_webapp=False):
    """
    Retrieves the id key used to uniquely identify a table record.
    :param api_params: api params from yaml config file
    :param field_grp_key: Table for which to determine the id key.
    :param is_webapp: is script currently running the 'create_webapp_tables' step?
    :return: String representing table key.
    """
    if field_grp_key not in api_params['FIELD_CONFIG']:
        return None

    if 'id_key' not in api_params['FIELD_CONFIG'][field_grp_key]:
        has_fatal_error(
            "table_id_key not found in API_PARAMS for {}".format(field_grp_key))

    table_id_name = api_params['FIELD_CONFIG'][field_grp_key]['id_key']

    if is_webapp:
        table_id_key = ".".join([field_grp_key, table_id_name])
        new_table_id_key = get_new_field_name(api_params, table_id_key)

        if new_table_id_key:
            table_id_name = get_field_name(new_table_id_key)

    return table_id_name


def get_field_grp_id_key(api_params, field_grp_key, is_webapp=False):
    """
    Retrieves the id key used to uniquely identify a table record.
    :param api_params: api params from yaml config file
    :param field_grp_key: Table for which to determine the id key.
    :param is_webapp: is script currently running the 'create_webapp_tables' step?
    :return: String representing table key.
    """
    if field_grp_key not in api_params['FIELD_CONFIG']:
        return None

    if 'id_key' not in api_params['FIELD_CONFIG'][field_grp_key]:
        has_fatal_error(
            "table_id_key not found in API_PARAMS for {}".format(field_grp_key))

    field_grp_id_name = api_params['FIELD_CONFIG'][field_grp_key]['id_key']

    field_grp_id_key = '.'.join([field_grp_key, field_grp_id_name])

    if is_webapp:
        new_id_key = get_new_field_name(api_params, field_grp_id_key)

        if new_id_key:
            field_grp_id_key = new_id_key

    return field_grp_id_key


def get_table_suffixes(api_params):
    """
    Get abbreviations for included field groups
    :param api_params: api params from yaml config file
    :return: dict of {table name: abbreviation}
    """
    suffixes = dict()

    for table, metadata in api_params['FIELD_CONFIG'].items():
        suffixes[table] = metadata['table_suffix'] if metadata['table_suffix'] else ''

    return suffixes


def get_prefix(api_params, field_grp):
    """
    Get abbreviations for included field groups
    :param api_params: api params from yaml config file
    :param field_grp: field group (a set of fields in the case data)
    :return: dict of {table name: abbreviation}
    """
    if 'FIELD_CONFIG' not in api_params or not api_params['FIELD_CONFIG']:
        has_fatal_error('FIELD_CONFIG not in api_params, or is empty', KeyError)
    if field_grp not in api_params['FIELD_CONFIG']:
        has_fatal_error('{} not found in not in FIELD_CONFIG'.format(field_grp), KeyError)
    if 'prefix' not in api_params['FIELD_CONFIG'][field_grp]:
        has_fatal_error("prefix not found in FIELD_CONFIG for {}".format(field_grp),
                        KeyError)

    prefix = api_params['FIELD_CONFIG'][field_grp]['prefix']

    return prefix


def get_excluded_for_grp(api_params, field_grp, is_webapp=False):
    """
    Get excluded fields for given field group (pulled from yaml config file)
    :param api_params: api params from yaml config file
    :param field_grp: field group (a set of fields in the case data)
    :param is_webapp: is script currently running the 'create_webapp_tables' step?
    :return: list of excluded fields
    """
    excluded_columns = set()

    if 'FIELD_CONFIG' not in api_params:
        has_fatal_error("FIELD_CONFIG not set in YAML.".format(field_grp), KeyError)
    if field_grp not in api_params['FIELD_CONFIG']:
        has_fatal_error("{} not set in YAML.".format(field_grp), KeyError)
    if not api_params['FIELD_CONFIG'][field_grp]:
        has_fatal_error("api_params['FIELD_CONFIG']['{}'] not found".format(field_grp),
                        KeyError)

    excluded_key = 'webapp_excluded_fields' if is_webapp else 'excluded_fields'

    if excluded_key not in api_params['FIELD_CONFIG'][field_grp]:
        has_fatal_error("{}'s excluded_fields not found.".format(field_grp))

    for field in api_params['FIELD_CONFIG'][field_grp][excluded_key]:
        excluded_columns.add(get_bq_name(api_params, field, field_grp, is_webapp))

    return excluded_columns


def get_excluded_fields(api_params, field_grps, is_webapp=False):
    """
    Get a list of fields to exclude from the final product, via the yaml config file
    :param api_params: api params from yaml config file
    :param field_grps: field groups (sets of fields in the case data)
    :param is_webapp: is script currently running the 'create_webapp_tables' step?
    :return: set of fields to exclude
    """
    if 'FIELD_CONFIG' not in api_params or not api_params['FIELD_CONFIG']:
        has_fatal_error('FIELD_CONFIG not in api_params, or is empty', KeyError)

    exclude_fields = set()
    excluded_key = 'webapp_excluded_fields' if is_webapp else 'excluded_fields'

    for field_grp in field_grps:
        if field_grp not in api_params['FIELD_CONFIG']:
            has_fatal_error('{} not found in not in FIELD_CONFIG'.format(field_grp),
                            KeyError)
        if not api_params['FIELD_CONFIG'][field_grp]:
            continue

        field_grp_params = api_params['FIELD_CONFIG'][field_grp]

        if excluded_key not in field_grp_params:
            has_fatal_error("One of the excluded field_grp params missing from YAML.",
                            KeyError)
        if not field_grp_params[excluded_key]:
            continue

        for field in field_grp_params[excluded_key]:
            exclude_fields.add(get_field_key(field_grp, field))

    return exclude_fields


def get_app_excluded_field_grps(api_params):
    """
    Get a list of fields to exclude from the final product, via the yaml config file
    :param api_params: api params from yaml config file
    :return: set of fields to exclude
    """
    if 'FG_CONFIG' not in api_params or not api_params['FG_CONFIG']:
        has_fatal_error('FG_CONFIG not in api_params, or is empty', KeyError)
    if 'app_excluded_field_grps' not in api_params['FG_CONFIG']:
        has_fatal_error('app_excluded_field_grps not found in not in FG_CONFIG', KeyError)

    return api_params['FG_CONFIG']['app_excluded_field_grps']


def get_gdc_rel(bq_params):
    """
    Get current GDC release, as configured in yaml
    :param bq_params: bq params from yaml config file
    :return: GDC release abbreviation (usually rXX or relXX)
    """
    return bq_params['REL_PREFIX'] + bq_params['GDC_RELEASE']


def get_working_table_id(bq_params, table_name=None):
    """
    Get table id for development version of the db table
    :param bq_params: bq params from yaml config file
    :param table_name: name of the bq table
    :return: table id
    """
    if not table_name:
        table_name = get_master_table_name(bq_params)

    return ".".join([bq_params["DEV_PROJECT"], bq_params["DEV_DATASET"], table_name])


def get_webapp_table_id(bq_params, table_name):
    """
    Get table id for webapp db table
    :param bq_params: bq params from yaml config file
    :param table_name: name of the bq table
    :return: table id
    """
    return ".".join([bq_params['DEV_PROJECT'], bq_params['APP_DATASET'], table_name])


def get_base_grp(api_params):
    """
    Get the first-level field group, of which all other field groups are descendents
    :param api_params: api params from yaml config file
    :return: base field group name
    """
    if 'FG_CONFIG' not in api_params:
        has_fatal_error("FG_CONFIG not set (in api_params) in YAML.", KeyError)
    if 'base_field_grp' not in api_params['FG_CONFIG'] \
            or not api_params['FG_CONFIG']['base_field_grp']:
        has_fatal_error("base_field_grp not set (in api_params['FG_CONFIG']) in YAML.",
                        KeyError)

    return api_params['FG_CONFIG']['base_field_grp']


def get_expand_groups(api_params):
    """
    Get expand field groups from yaml config
    :param api_params: api params from yaml config file
    :return: list of expand field groups.
    """
    if 'EXPAND_FIELD_GROUPS' not in api_params:
        has_fatal_error('EXPAND_FIELD_GROUPS not in api_params (check yaml config file)')

    return ",".join(list(api_params['EXPAND_FIELD_GROUPS']))


def build_table_name(str_list):
    """
    Constructs a string table name from a list of string values.
    :param str_list: a list of string table name components
    :return: constructed table name
    """
    table_name = "_".join(str_list)
    return table_name.replace('.', '_')


##################################################################################
#
#       FIELD, COLUMN, TABLE GETTERS
#
##################################################################################


def get_biospecimen_table_id(bq_params, program):
    """
    Builds and retrives a table ID for the biospecimen stub tables.
    :param bq_params: bq params from yaml config file
    :param program: the program from which the cases originate.:
    :return: table id
    """
    bio_table_name = build_table_name([get_gdc_rel(bq_params),
                                       str(program),
                                       bq_params['BIOSPECIMEN_SUFFIX']])

    bio_table_id = get_webapp_table_id(bq_params, bio_table_name)

    return bio_table_id


def get_field_key(field_group, field):
    """
    get full field name for field
    :param field_group: field group to which the field belongs
    :param field: field name
    :return: full field name string
    """
    return '.'.join([field_group, field])


def get_field_name(field_or_column_name):
    """
    Get short field name from full field or bq column name.
    :param field_or_column_name: full field or bq column name
    :return: short field name
    """
    if '.' in field_or_column_name:
        return field_or_column_name.split('.')[-1]
    if '__' in field_or_column_name:
        return field_or_column_name.split('__')[-1]
    return field_or_column_name


def get_count_field(field_group):
    """
    Get count field associated with the provided field group
    :param field_group: field group for which to retreive a count field
    :return: count field key
    """
    return field_group + '.count'


def get_field_depth(full_field_name):
    """
    Gets nested depth for given field.
    :param full_field_name: full field name
    :return: nested depth (int value)
    """
    return len(full_field_name.split('.'))


def get_sorted_field_grp_depths(record_counts, reverse=False):
    """
    Returns a sorted dict of field groups: depths
    :param record_counts: dict containing field groups and associated record counts
    :param reverse: if True, sort in DESC order, otherwise sort in ASC order
    :return: tuples composed of field group names and record counts
    """
    table_depths = {table: get_field_depth(table) for table in record_counts}
    table_depth_tuples = sorted(table_depths.items(),
                                key=lambda item: item[1], reverse=reverse)

    return table_depth_tuples


def get_bq_name(api_params, field, field_grp=None, is_webapp=False):
    """
    Get column name (in bq format) from full field name.
    :param api_params: api params from yaml config file
    :param field: if not table_path, full field name; else short field name
    :param field_grp: field group containing field
    :param is_webapp: is script currently running the 'create_webapp_tables' step?
    :return: bq column name for given field name
    """
    base_field_grp = get_base_grp(api_params)

    # if field_grp provided as argument, create an appended key
    # if field is name (not key, only one degree deep), create field_key using base
    # field_grp
    # else, field is already a key
    if field_grp:
        field_key = get_field_key(field_grp, field)
    elif len(field.split('.')) == 1:  # not a full field key, just the name
        field_key = get_field_key(base_field_grp, field)
    else:
        field_key = field

    field_grp = get_field_group(field_key)
    field_name = get_field_name(field_key)

    prefix = get_prefix(api_params, field_grp)

    # if there's no prefix supplied for the field group, the field is member of the
    # base field_grp, or the function is being called by wep app portion of the script:
    # leave field name alone. Else, append prefix specified in yaml's FIELD_CONFIG
    if is_webapp or field_grp == base_field_grp or not prefix:
        return field_name
    return "__".join([prefix, field_name])


def get_field_group(field_name):
    """
    Gets ancestor field group. (Might not be the parent table,
    as ancestor could be flattened.)
    :param field_name: field name for which to retrieve ancestor field group
    :return: ancestor field group
    """
    split_field_name = field_name.split('.')

    return ".".join(split_field_name[:-1])


def get_tables(api_params, record_counts):
    """
    Get one-to-many tables for program.
    :param api_params: api params from yaml config file
    :param record_counts: dict max field group record counts for program
    :return: set of table names
    """
    table_keys = set()

    for table in record_counts:
        if record_counts[table] > 1:
            table_keys.add(table)

    table_keys.add(api_params['FG_CONFIG']['base_field_grp'])

    return table_keys


def get_record_cnt_dict(record_cnts):
    """
    Get a dictionary of field groups and their max record counts.
    :param record_cnts: max record count for field group
    :return: dict of record counts
    """
    return {field_grp: dict() for field_grp in record_cnts
            if record_cnts[field_grp] > 1}


def get_parent_field_grp(tables, field_name):
    """
    Get field's parent table name.
    :param tables: list of table names for program
    :param field_name: full field name for which to retrieve parent table
    :return: parent table name
    """
    base_table = field_name.split('.')[0]

    if not base_table or base_table not in tables:
        has_fatal_error("'{}' has no parent table: {}".format(field_name, tables))

    parent_table = get_field_group(field_name)

    while parent_table and parent_table not in tables:
        parent_table = get_field_group(parent_table)

    if not parent_table:
        has_fatal_error("No parent found for {}".format(field_name))

    return parent_table


def get_project_name(table_id):
    """
    Get the BQ project name for a given table id
    :param table_id: table id in standard SQL format
    :return: BQ project to which the table belongs
    """
    split_table = table_id.split('.')

    if len(split_table) != 3:
        has_fatal_error("Incorrect naming for table_id: {}".format(table_id))

    return split_table[0]


##################################################################################
#
#       FILESYSTEM HELPERS
#
##################################################################################


def get_scratch_dir(bq_params):
    """
    Construct filepath for VM output file
    :param bq_params: bq params from yaml config file
    :return: output filepath for VM
    """
    return '/'.join([os.path.expanduser('~'), bq_params['SCRATCH_DIR']])


def get_scratch_path(bq_params, filename):
    """
    Construct filepath for VM output file
    :param bq_params: bq params from yaml config file
    :return: output filepath for VM
    """
    return '/'.join([os.path.expanduser('~'), bq_params['SCRATCH_DIR'], filename])


def get_dir_files(dir_path):
    """
    Get all the file names in a directory as a list of as strings
    :param dir_path: path to desired directory
    :return: list of filenames
    """
    f_path = '/'.join([os.path.expanduser('~'), dir_path])
    return [f for f in os.listdir(f_path) if os.path.isfile(os.path.join(f_path, f))]


def get_filepath(dir_path, filename):
    """
    Get file path for location on VM
    :param dir_path: directory portion of the filepath (starting at user home dir)
    :param filename: name of the file
    :return: full path to file
    """
    return '/'.join([os.path.expanduser('~'), dir_path, filename])


def get_metadata_path(bq_params):
    """
    Gets the path to the directory where table and field metadata json files are
    found on the VM.
    :param bq_params: bq params from yaml config file
    :return: metadata file path
    """
    return "/".join([bq_params['BQ_REPO'], bq_params['TABLE_METADATA_DIR'],
                     get_gdc_rel(bq_params), ''])


def convert_json_to_table_name(bq_params, json_file):
    """
    Convert json file from BQEcosystem repo. Naming matches table ID of corresponding
    dev BQ clinical tables.
    :param bq_params: bq params from yaml config file
    :param json_file: json file from BQEcosystem repo, storing table metadata;
    json file naming matches table ID of production clinical tables
    :return: name of corresponding BQ table in dev project
    """
    # handles naming for *webapp* tables
    # json file name 'isb-cgc-bq.HCMI.clinical_follow_ups_gdc_r24.json'
    # def table name 'r24_HCMI_clinical_follow_ups'
    split_json_name = json_file.split('.')
    program_name = split_json_name[1]
    split_table_name = split_json_name[2].split('_')
    partial_table_name = '_'.join(split_table_name[0:-2])
    return '_'.join([get_gdc_rel(bq_params), program_name, partial_table_name])


def convert_json_to_prod_table_id(bq_params, json_file):
    """
    Convert json file from BQEcosystem repo into component dataset and table names.
    Naming matches table ID of corresponding production BQ clinical tables.
    :param bq_params: bq params from yaml config file
    :param json_file: json file from BQEcosystem repo, storing table metadata
    :return: names of datasets and tables for production current and versioned
    repositories.
    """
    split_json = json_file.split('.')

    curr_dataset = split_json[1]
    versioned_dataset = "_".join([curr_dataset, bq_params['VERSIONED_SUFFIX']])

    src_table = "_".join(split_json[2].split('_')[:-2])
    table_name = "_".join(split_json[2].split('_')[:-1])
    curr_table = "_".join([table_name, bq_params['CURRENT_SUFFIX']])
    versioned_table = "_".join([table_name, get_gdc_rel(bq_params)])

    return curr_dataset, versioned_dataset, src_table, curr_table, versioned_table


def load_config(yaml_file, yaml_dict_keys):
    """
    Opens yaml file and retrieves configuration parameters.
    :param yaml_file: yaml config file name
    :param yaml_dict_keys: tuple of strings representing a subset of the yaml file's
    top-level dict keys.
    :return: tuple of dicts from yaml file (as requested in yaml_dict_keys)
    """
    yaml_dict = None

    config_stream = io.StringIO(yaml_file.read())

    try:
        yaml_dict = yaml.load(config_stream, Loader=yaml.FullLoader)
    except yaml.YAMLError as ex:
        has_fatal_error(ex, yaml.YAMLError)
    if yaml_dict is None:
        has_fatal_error("Bad YAML load, exiting.", ValueError)

    # Dynamically generate a list of dictionaries for the return statement,
    # since tuples are immutable
    return_dicts = [yaml_dict[key] for key in yaml_dict_keys]

    return tuple(return_dicts)


def write_obj_list_to_jsonl(bq_params, fp, obj_list):
    cnt = 0

    with open(fp, 'w') as file_obj:
        for obj in obj_list:
            obj_str = convert_dict_to_string(obj)
            json.dump(obj=obj_str, fp=file_obj)
            file_obj.write('\n')
            cnt += 1

        print("Successfully output {} records to {}".format(cnt, fp))


def get_working_gs_uri(bq_params, filename):
    return "/".join(['gs:/',
                     bq_params['WORKING_BUCKET'],
                     bq_params['WORKING_BUCKET_DIR'],
                     filename]
                    )


##################################################################################
#
#       MISC UTILITIES
#
##################################################################################


def convert_dict_to_string(obj):
    """
    Converts dict/list of primitives or strings to a comma-separated string.
    Used to write data to file
    :param obj: object to converts
    :return: modified object
    """
    if isinstance(obj, list):
        if not isinstance(obj[0], dict):
            str_list = ', '.join(obj)
            obj = str_list
        else:
            for idx, value in enumerate(obj.copy()):
                obj[idx] = convert_dict_to_string(value)
    elif isinstance(obj, dict):
        for key in obj:
            obj[key] = convert_dict_to_string(obj[key])
    return obj
