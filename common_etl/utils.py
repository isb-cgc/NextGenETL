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
import sys
import os
import time
import requests
import yaml
from google.cloud import bigquery, storage, exceptions
from google.api_core.exceptions import NotFound


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








#########################################
#
#       DATA ANALYSIS FUNCTIONS
#
#########################################


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

            # adding this change because organoid sumbitter_ids look like
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


def collect_values(fields, field, parent, fg_prefix):
    """
    Recursively inserts sets of values for a given field into return dict (
    used to infer field data type)
    :param fields: A dict of key:value pairs -- field_name : set(
    field_values)
    :param field: field name
    :param parent: dict containing field and it's values
    :param fg_prefix: string representation of current location in field hierarchy
    :return: field_dict containing field names and a set of its values.
    """
    # If the value of parent_dict[key] is a list at this level, and a dict at the next
    # (or a dict at this level, as seen in second conditional statement),
    # iterate over each list element's dictionary entries. (Sometimes lists are composed
    # of strings rather than dicts, and those are later converted to strings.)
    field_name = fg_prefix + field
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


#########################################
#
#       BQ SCHEMA CREATION FUNCTIONS
#
#########################################


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

    record_lists_dict = {fg_name: [] for fg_name in field_group_names}
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
    :param is_webapp:
    :param api_params: api params from yaml config file
    :param bq_params: bq params from yaml config file
    :return: flattened schema dict in format:
        {full field name: {name: 'name', type: 'field_type', description: 'description'}}
    """
    table_id = get_working_table_id(bq_params)

    client = bigquery.Client()
    table_obj = client.get_table(table_id)

    return get_schema_from_master_table(api_params,
                                        dict(),
                                        get_base_fg(api_params),
                                        table_obj.schema,
                                        is_webapp)


#########################################
#
#       GOOGLE CLOUD HELPERS
#
#########################################


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
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig(destination=table_id)
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    try:
        query_job = client.query(query, job_config=job_config)
        await_insert_job(bq_params, client, table_id, query_job)
    except TypeError as err:
        has_fatal_error(err)


def create_and_load_table(bq_params, jsonl_rows_file, schema, table_name,
                          is_webapp=False):
    """
    Creates BQ table and inserts case data from jsonl file.
    :param is_webapp:
    :param bq_params: bq params from yaml config file
    :param jsonl_rows_file: file containing case records in jsonl format
    :param schema: list of SchemaFields representing desired BQ table schema
    :param table_name: name of table to create
    """

    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig()
    job_config.schema = schema
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    gs_uri = "/".join(['gs:/',
                       bq_params['WORKING_BUCKET'],
                       bq_params['WORKING_BUCKET_DIR'],
                       jsonl_rows_file])

    if is_webapp:
        table_id = get_webapp_table_id(bq_params, table_name)
    else:
        table_id = get_working_table_id(bq_params, table_name)

    try:
        load_job = client.load_table_from_uri(gs_uri, table_id, job_config=job_config)
        await_insert_job(bq_params, client, table_id, load_job)
    except TypeError as err:
        has_fatal_error(err)


def await_job(bq_params, client, bq_job):
    location = bq_params['LOCATION']

    bq_job = client.get_job(bq_job.job_id, location=location)

    bq_job.done(timeout=3)

    if bq_job.error_result is not None:
        has_fatal_error('[ERROR] While running BQ job: {}\n{}'.format(
            bq_job.error_result, bq_job.errors))
    return True


def await_insert_job(bq_params, client, table_id, load_job):
    print(' - Starting insert for {}... '.format(table_id), end="")

    last_report_time = time.time()

    location = bq_params['LOCATION']
    job_state = "NOT_STARTED"

    while job_state != 'DONE':
        load_job = client.get_job(load_job.job_id, location=location)

        if time.time() - last_report_time > 15:
            print('\t- job is currently in state {}'.format(load_job.state))
            last_report_time = time.time()

        job_state = load_job.state

        if job_state != 'DONE':
            time.sleep(2)

    load_job = client.get_job(load_job.job_id, location=location)

    if load_job.error_result is not None:
        has_fatal_error('While running BQ job: {}\n{}'
                        .format(load_job.error_result, load_job.errors), ValueError)

    table = client.get_table(table_id)

    print(" done. {} rows inserted.".format(table.num_rows))


def get_program_list(bq_params):
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
    cases = []

    sample_table_id = get_webapp_table_id(
        bq_params, build_table_name([program, bq_params['BIOSPECIMEN_SUFFIX']]))

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
    client = bigquery.Client()

    try:
        client.get_table(table_id)
    except NotFound:
        return False
    return True


def get_bq_table(table_id):
    if not exists_bq_table(table_id):
        return None

    client = bigquery.Client()
    return client.get_table(table_id)


def update_bq_table(table_id, metadata):
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
    client = bigquery.Client()
    table = get_bq_table(table_id)

    friendly_name = table.friendly_name
    friendly_name += ' REL' + bq_params['GDC_RELEASE'] + ' VERSIONED'

    table.friendly_name = friendly_name

    client.update_table(table, ["friendly_name"])


def modify_friendly_name_custom(table_id, new_name):
    client = bigquery.Client()
    table = get_bq_table(table_id)

    table.friendly_name = new_name

    print(table.friendly_name)

    client.update_table(table, ["friendly_name"])


def delete_bq_table(table):
    client = bigquery.Client()
    res = client.delete_table(table, not_found_ok=True)

    if res:
        has_fatal_error(res)
    else:
        print("deleted table: {}".format(table))


def copy_bq_table(bq_params, src_table, dest_table):
    client = bigquery.Client()

    bq_job = client.copy_table(src_table, dest_table)

    if await_job(bq_params, client, bq_job):
        print("Successfully copied table:")
        print("src:  {}\n dest: {}\n".format(src_table, dest_table))


def update_table_schema(table_id, new_descriptions):
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


def get_schema_from_master_table(api_params, schema, fg, fields=None, is_webapp=False):
    """
    Recursively build schema using master table's bigquery.table.Table.schema attribute
    :param is_webapp:
    :param api_params: api params from yaml config file
    :param schema: dict of flattened schema entries
    :param fg: current field group name
    :param fields: schema field entries for field_group
    :return: flattened schema dict {full field name:
        {name: 'name', type: 'field_type', description: 'description'}}
    """
    if fg not in api_params['FIELD_CONFIG'].keys():
        return schema

    for field in fields:
        field_dict = field.to_api_repr()
        schema_key = get_field_key(fg, field_dict['name'])

        if 'fields' in field_dict:
            schema = get_schema_from_master_table(api_params,
                                                  schema,
                                                  schema_key,
                                                  field.fields,
                                                  is_webapp)

            for required_column in get_required_columns(api_params, fg):
                schema[required_column]['mode'] = 'REQUIRED'
        else:
            field_dict['name'] = get_bq_name(api_params,
                                             schema_key,
                                             is_webapp=is_webapp)

            schema[schema_key] = field_dict

    return schema


def rename_fields_for_app(column_orders, api_params):
    for old_name, new_name in api_params['RENAMED_FIELDS'].items():
        fg = get_field_group(old_name)

        if fg in column_orders and old_name in column_orders[fg]:
            idx = column_orders[fg][old_name]
            column_orders[fg][new_name] = idx
            column_orders[fg].pop(old_name)


def modify_fields_for_app(schema, column_order_dict, columns, api_params):
    renamed_fields = dict(api_params['RENAMED_FIELDS'])

    fgs = column_order_dict.keys()

    excluded_fgs = get_app_excluded_fgs(api_params)
    excluded_fields = get_excluded_fields(fgs, api_params, is_webapp=True)

    for fg in fgs:
        # rename case_id no matter which fg it's in
        for renamed_field in renamed_fields.keys():
            if renamed_field in column_order_dict[fg]:
                new_field = renamed_fields[renamed_field]
                column_order_dict[fg][new_field] = column_order_dict[fg][renamed_field]
                column_order_dict[fg].pop(renamed_field)
            if fg in columns and renamed_field in columns[fg]:
                columns[fg].add(renamed_fields[renamed_field])
                columns[fg].remove(renamed_field)

    # field is fully associated name
    for field in {k for k in schema.keys()}:
        base_fg = ".".join(field.split('.')[:-1])
        field_name = field.split('.')[-1]

        # substitute base field name for prefixed
        schema[field]['name'] = field_name

        # exclude any field groups or fields explicitly excluded in yaml
        if field in excluded_fields or base_fg in excluded_fgs:
            schema.pop(field)
        # field exists in renamed_fields, change its name
        elif field in renamed_fields:
            new_field = renamed_fields[field]

            schema[field]['name'] = new_field.split('.')[-1]
            schema[new_field] = schema[field]
            schema.pop(field)

            # change the field name in the column order dict
            if base_fg in column_order_dict and field in column_order_dict[base_fg]:
                column_order_dict[base_fg][new_field] = column_order_dict[base_fg][field]
                column_order_dict[base_fg].pop(field)

        if field in excluded_fields and base_fg in column_order_dict:
            # remove excluded field from column order lists
            if field in column_order_dict[base_fg]:
                column_order_dict[base_fg].pop(field)


def to_bq_schema_obj(schema_field_dict):
    """
    Convert schema entry dict to SchemaField object.
    :param schema_field_dict: dict containing schema field keys
    (name, field_type, mode, fields, description)
    :return: bigquery.SchemaField object
    """
    return bigquery.SchemaField.from_api_repr(schema_field_dict)


def upload_to_bucket(bq_params, file_name):
    """
    Uploads file to a google storage bucket (location specified in yaml config)
    :param bq_params: bq params from yaml config file
    :param file_name: name of file to upload to bucket
    """
    filepath = get_scratch_dir(bq_params)
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bq_params['WORKING_BUCKET'])
        blob = bucket.blob("/".join([bq_params['WORKING_BUCKET_DIR'], file_name]))
        blob.upload_from_filename("/".join([filepath, file_name]))
    except exceptions.GoogleCloudError as err:
        has_fatal_error("Failed to upload to bucket.\n{}".format(err))


#########################################
#
#       YAML CONFIG GETTERS
#
#########################################


def get_required_columns(api_params, table):
    """
    Get list of required columns.
    :param api_params: api params from yaml config file
    :param table: name of table for which to retrieve required columns.
    :return: list of required columns (currently, only returns table's primary id)
    """
    if table not in api_params['FIELD_CONFIG']:
        return None
    elif 'id_key' not in api_params['FIELD_CONFIG'][table]:
        return None

    table_id_field = api_params['FIELD_CONFIG'][table]['id_key']
    table_id_name = get_field_key(table, table_id_field)
    return [table_id_name]


def get_master_table_name(bq_params):
    """
    # todo
    :param bq_params:
    :return:
    """
    return "_".join([get_gdc_rel(bq_params), bq_params['MASTER_TABLE']])


def get_renamed_fields(api_params):
    if 'RENAMED_FIELDS' not in api_params:
        has_fatal_error("RENAMED_FIELDS not found in API_PARAMS")
    if not api_params['RENAMED_FIELDS']:
        return None

    return api_params['RENAMED_FIELDS']


def get_new_field_name(api_params, field):
    renamed_field_dict = get_renamed_fields(api_params)
    if not renamed_field_dict or field not in renamed_field_dict:
        return None

    return renamed_field_dict[field]


def get_fg_id_name(api_params, fg_key, is_webapp=False):
    """
    Retrieves the id key used to uniquely identify a table record.
    :param is_webapp:
    :param api_params:
    :param fg_key: Table for which to determine the id key.
    :return: String representing table key.
    """
    if fg_key not in api_params['FIELD_CONFIG']:
        return None

    if 'id_key' not in api_params['FIELD_CONFIG'][fg_key]:
        has_fatal_error("table_id_key not found in API_PARAMS for {}".format(fg_key))

    table_id_name = api_params['FIELD_CONFIG'][fg_key]['id_key']

    if is_webapp:
        table_id_key = ".".join([fg_key, table_id_name])
        new_table_id_key = get_new_field_name(api_params, table_id_key)

        if new_table_id_key:
            table_id_name = get_field_name(new_table_id_key)

    return table_id_name


def get_fg_id_key(api_params, fg_key, is_webapp=False):
    """
    Retrieves the id key used to uniquely identify a table record.
    :param is_webapp:
    :param api_params:
    :param fg_key: Table for which to determine the id key.
    :return: String representing table key.
    """
    if fg_key not in api_params['FIELD_CONFIG']:
        return None

    if 'id_key' not in api_params['FIELD_CONFIG'][fg_key]:
        has_fatal_error("table_id_key not found in API_PARAMS for {}".format(fg_key))

    fg_id_name = api_params['FIELD_CONFIG'][fg_key]['id_key']

    fg_id_key = '.'.join([fg_key, fg_id_name])

    if is_webapp:
        new_id_key = get_new_field_name(api_params, fg_id_key)

        if new_id_key:
            fg_id_key = new_id_key

    return fg_id_key


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


def get_prefix(api_params, fg):
    """
    Get abbreviations for included field groups
    :param api_params: api params from yaml config file
    :return: dict of {table name: abbreviation}
    """
    if 'FIELD_CONFIG' not in api_params or not api_params['FIELD_CONFIG']:
        has_fatal_error('FIELD_CONFIG not in api_params, or is empty', KeyError)
    if fg not in api_params['FIELD_CONFIG']:
        has_fatal_error('{} not found in not in FIELD_CONFIG'.format(fg), KeyError)
    if 'prefix' not in api_params['FIELD_CONFIG'][fg]:
        has_fatal_error("prefix not found in FIELD_CONFIG for {}".format(fg),KeyError)

    prefix = api_params['FIELD_CONFIG'][fg]['prefix']

    return prefix


def get_excluded_fields(fgs, api_params, is_webapp=False):
    exclude_fields = set()

    for fg in fgs:
        fg_metadata = api_params['FIELD_CONFIG'][fg]

        if ('excluded_fields' not in fg_metadata
                or (is_webapp and 'webapp_excluded_fields' not in fg_metadata)):
            has_fatal_error("One of the excluded fg params missing from YAML.", KeyError)

        if is_webapp:
            if fg_metadata['webapp_excluded_fields']:
                for w_field in fg_metadata['webapp_excluded_fields']:
                    # add webapp-specific excluded fields
                    exclude_fields.add('.'.join([fg, w_field]))
        else:
            if fg_metadata['excluded_fields']:
                for field in fg_metadata['excluded_fields']:
                    # add generic excluded fields
                    exclude_fields.add('.'.join([fg, field]))

    return exclude_fields


def get_app_excluded_fgs(api_params):
    if 'FG_CONFIG' not in api_params or not api_params['FG_CONFIG']:
        has_fatal_error('FG_CONFIG not in api_params, or is empty', KeyError)
    if 'app_excluded_fgs' not in api_params['FG_CONFIG']:
        has_fatal_error('app_excluded_fgs not found in not in FG_CONFIG', KeyError)

    return api_params['FG_CONFIG']['app_excluded_fgs']


def get_gdc_rel(bq_params):
    return bq_params['REL_PREFIX'] + bq_params['GDC_RELEASE']


def get_working_table_id(bq_params, table_name=None):
    if not table_name:
        table_name = get_master_table_name(bq_params)

    return ".".join([bq_params["DEV_PROJECT"], bq_params["DEV_DATASET"], table_name])


def get_webapp_table_id(bq_params, table_name):
    return ".".join([bq_params['DEV_PROJECT'], bq_params['APP_DATASET'], table_name])


def get_base_fg(api_params):
    if 'FG_CONFIG' not in api_params:
        has_fatal_error("FG_CONFIG not set (in api_params) in YAML.", KeyError)
    if 'base_fg' not in api_params['FG_CONFIG'] or not api_params['FG_CONFIG']['base_fg']:
        has_fatal_error("base_fg not set (in api_params['FG_CONFIG']) in YAML.", KeyError)

    return api_params['FG_CONFIG']['base_fg']


def get_expand_groups(api_params):
    """
    Get expand field groups from yaml config
    :return: list of expand field groups.
    """
    if 'EXPAND_FIELD_GROUPS' not in api_params:
        has_fatal_error('EXPAND_FIELD_GROUPS not in api_params (check yaml config file)')

    return ",".join(list(api_params['EXPAND_FIELD_GROUPS']))


########
def build_table_name(arr):
    table_name = "_".join(arr)
    return table_name.replace('.', '_')


#########################################
#
#       FIELD, COLUMN, TABLE GETTERS
#
#########################################


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
    # todo
    :param field_group:
    :return:
    """
    return field_group + '.count'


def get_case_id_field(field_group):
    """
    # todo
    :param field_group:
    :return:
    """
    return field_group + '.case_id'


def get_field_depth(full_field_name):
    """
    Gets nested depth for given field.
    :param full_field_name: full field name
    :return: nested depth (int value)
    """
    return len(full_field_name.split('.'))


def get_sorted_fg_depths(record_counts, reverse=False):
    """
    # todo
    :param record_counts:
    :param reverse:
    :return:
    """
    table_depths = {table: get_field_depth(table) for table in record_counts}
    table_depth_tuples = sorted(table_depths.items(),
                                key=lambda item: item[1], reverse=reverse)

    return table_depth_tuples


def get_bq_name(api_params, field, table_path=None, is_webapp=False):
    """
    Get column name (in bq format) from full field name.
    :param api_params: api params from yaml config file
    :param field: if not table_path, full field name; else short field name
    :param table_path: field group containing field
    :param is_webapp:
    :return: bq column name for given field name
    """
    base_fg = get_base_fg(api_params)

    if table_path:
        field_key = ".".join([table_path, field])
    else:
        split_name = field.split('.')

        if len(split_name) == 1:
            field_key = ".".join([base_fg, field])
        else:
            field_key = field

    '''
    split_name = field_key.split('.')

    if split_name[0] != get_base_fg(api_params):
        split_name.insert(0, get_base_fg(api_params))
        field_key = '.'.join(split_name)
    '''

    fg = get_field_group(field_key)
    field_name = get_field_name(field_key)

    if not is_webapp and fg != base_fg:
        prefix = get_prefix(api_params, fg)

        if prefix:
            return "__".join([prefix, field_name])

    # prefix is blank, like in the instance of api_params['FG_CONFIG']['base_fg']
    return field_name


def get_field_group(field_name):
    """
    Gets ancestor field group. (Might not be the parent table,
    as ancestor could be flattened.)
    :param field_name: field name for which to retrieve ancestor field group
    :return: ancestor field group
    """
    split_field_name = field_name.split('.')

    return ".".join(split_field_name[:-1])


def get_tables(record_counts, api_params):
    """
    Get one-to-many tables for program.
    :param api_params:
    :param record_counts: dict max field group record counts for program
    :return: set of table names
    """
    table_keys = set()

    for table in record_counts:
        if record_counts[table] > 1:
            table_keys.add(table)

    table_keys.add(api_params['FG_CONFIG']['base_fg'])

    return table_keys


def get_parent_table(tables, field_name):
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


def is_valid_fg(api_params, fg_name):
    return fg_name in api_params['FIELD_CONFIG'].keys()


#########################################
#
#       FILESYSTEM HELPERS
#
#########################################


def get_scratch_dir(bq_params):
    """
    Construct filepath for VM output file
    :return: output filepath for VM
    """
    return '/'.join([os.path.expanduser('~'), bq_params['SCRATCH_DIR']])


def get_dir_files(dir_path):
    f_path = '/'.join([os.path.expanduser('~'), dir_path])
    return [f for f in os.listdir(f_path) if os.path.isfile(os.path.join(f_path, f))]


def get_filepath(dir_path, filename):
    return '/'.join([os.path.expanduser('~'), dir_path, filename])


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


#########################################
#
#       MISC UTILITIES
#
#########################################


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
