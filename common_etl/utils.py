"""
Copyright 2020, Institute for Systems Biology

Permission is hereby granted, free of charge, to any person obtaining a copy of this
software and associated documentation files (the "Software"), to deal in the Software
without restriction, including without limitation the rights to use, copy, modify,
merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies
or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING
BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""
import io
import yaml
import pprint
import json
import requests
import time
from google.cloud import bigquery


def load_config(yaml_file, yaml_dict_keys):
    """
    Opens yaml file and retrieves configuration parameters.
    :param yaml_file: yaml config file name
    :param yaml_dict_keys: tuple of strings representing a subset of the yaml file's top-level dictionary keys.
    file
    :return: tuple of dicts from yaml file (as requested in yaml_dict_keys)
    """
    yaml_dict = None

    config_stream = io.StringIO(yaml_file.read())

    try:
        yaml_dict = yaml.load(config_stream, Loader=yaml.FullLoader)
    except yaml.YAMLError as ex:
        print(ex)
        exit(1)

    if yaml_dict is None:
        print("Bad YAML load, exiting.")
        exit(1)

    # Dynamically generate a list of dictionaries for the return statement, since tuples are immutable
    return_dicts = [yaml_dict[key] for key in yaml_dict_keys]

    return tuple(return_dicts)


def check_value_type(value):
    """
    Checks value for type (possibilities are string, float and integers)
    :param value: value to type check
    :return: type in BQ's format
    """
    # if has leading zero, then should be considered a string, even if only composed of digits
    val_is_none = value == '' or value == 'NA' or value == 'null' or value is None or value == 'None'
    val_is_decimal = value.startswith('0.')
    val_is_id = value.startswith('0') and not val_is_decimal and len(value) > 1

    try:
        float(value)
        val_is_num = True
    except ValueError:
        val_is_num = False
        val_is_float = False

    if val_is_num:
        val_is_float = True if int(float(value)) != float(value) else False

    if val_is_none:
        return None
    elif val_is_id:
        return 'STRING'
    elif val_is_decimal or val_is_float:
        return 'FLOAT64'
    elif val_is_num:
        return 'INT64'
    else:
        return 'STRING'


def infer_data_types(flattened_json):
    """
    Infer data type of fields based on values contained in dataset.
    :param flattened_json: file containing dict of field names (key) and sets of field values (value)
    :return: dict of field names and inferred type (None if no data in value set).
    """
    data_types = dict()
    for column in flattened_json:
        data_types[column] = None

        for value in flattened_json[column]:
            if data_types[column] == 'STRING':
                break

            val_type = check_value_type(str(value))

            if not val_type:
                continue
            elif val_type == 'FLOAT64' or val_type == 'STRING':
                data_types[column] = val_type
            elif val_type == 'INT64':
                if not data_types[column]:
                    data_types[column] = 'INT64'
            else:
                print("[ERROR] NO TYPE SET FOR val {}, type {}".format(value, val_type))

    return data_types


def infer_data_type(data_types_dict, key, value):
    """
    Infer data type of fields based on values contained in dataset.
    :param value:
    :param key:
    :param data_types_dict:
    :return: dict of field names and inferred type (None if no data in value set).
    """
    if data_types_dict[key] != 'STRING':
        val_type = check_value_type(str(value))

        if val_type == 'FLOAT64' or val_type == 'STRING':
            data_types_dict[key] = val_type
        elif val_type == 'INT64':
            if not data_types_dict[key]:
                data_types_dict[key] = 'INT64'

    return data_types_dict


def create_nested_schema_obj(name, field_list):
    """
    Create a repeated record for BQ schema.
    :param name: field's short name (without parent hierarchy)
    :param field_list: parent list containing field referenced by name
    :return: BQ schema repeated record
    """
    return {
        "name": name,
        "type": "RECORD",
        "mode": "REPEATED",
        "fields": field_list
    }


def collect_field_values(field_dict, key, parent_dict, prefix):
    """
    Recursively inserts sets of values for a given field into return dict (used to infer field data type)
    :param field_dict: A dict of key:value pairs -- field_name : set(field_values)
    :param key: field name
    :param parent_dict: dict containing field and it's values
    :param prefix: string representation of current location in field hierarchy
    :return: field_dict
    """
    # If the value of parent_dict[key] is a list at this level, and a dict at the next (or a dict at this level,
    # as seen in second conditional statement), iterate over each list element's dictionary entries.
    # (Sometimes lists are composed of strings rather than dicts, and those are later converted to strings.)
    if isinstance(parent_dict[key], list) and isinstance(parent_dict[key][0], dict):
        for dict_item in parent_dict[key]:
            for dict_key in dict_item:
                field_dict = collect_field_values(field_dict, dict_key, dict_item, prefix + key + ".")
    elif isinstance(parent_dict[key], dict):
        for dict_key in parent_dict[key]:
            field_dict = collect_field_values(field_dict, dict_key, parent_dict[key], prefix + key + ".")
    else:
        field_name = prefix + key

        if field_name not in field_dict:
            field_dict[field_name] = set()

        # This type of list can be converted to a comma-separated value string
        if isinstance(parent_dict[key], list):
            value = ", ".join(parent_dict[key])
        else:
            value = parent_dict[key]

        field_dict[field_name].add(value)

    return field_dict


def create_mapping_dict(endpoint):
    """
    Creates a dict containing field mappings for given endpoint.
    Note: only differentiates the GDC API's 'long' type (called 'integer' in GDC data dictionary) and
    'float' type (called 'number' in GDC data dictionary). All others typed as string.
    :param endpoint: API endpoint for which to retrieve mapping.
    :return: dict of field mappings. Each entry object contains field name, type, and description
    """
    field_mapping_dict = {}

    # retrieve mappings json object
    res = requests.get(endpoint + '/_mapping')
    field_mappings = res.json()['_mapping']

    for field in field_mappings:
        # convert data types from GDC format to formats used in BQ
        if field_mappings[field]['type'] == 'long':
            field_type = 'INT64'
        elif field_mappings[field]['type'] == 'float':
            field_type = 'FLOAT64'
        else:
            field_type = 'STRING'

        # create json object of field mapping data
        field_mapping_dict[field] = {
            'name': field.split('.')[-1],
            'type': field_type,
            'description': field_mappings[field]['description']
        }

    return field_mapping_dict


def generate_bq_schema_json(schema_dict, record_type, expand_fields_list, output_fp):

    # create a dict of lists using expand field group names
    field_groups = expand_fields_list.split(',')

    # add field group names to a list, in order to generate a dict
    field_group_names = [record_type]

    for field_group in field_groups:
        field_group_names.append(record_type + '.' + field_group)

    record_lists_dict = {fg:[] for fg in field_group_names}

    # add field to correct field grouping list based on full field name
    for field in schema_dict:
        split_field_name = field.split('.')

        # record_lists_dict key is equal to the parent field components of full field name
        list_key = '.'.join(split_field_name[:-1])
        record_lists_dict[list_key].append(schema_dict[field])

    # calculate max field depth in order to nest expand field groups in bq schema
    max_field_depth = 0

    for list_key in record_lists_dict:
        curr_depth = len(list_key.split('.'))
        if max_field_depth < curr_depth:
            max_field_depth = curr_depth

    curr_level = max_field_depth

    # insert nested field groupings into appropriate parent list
    while curr_level > 1:
        field_group_names = list(record_lists_dict.keys())

        for field_group_name in field_group_names:
            split_group_name = field_group_name.split('.')

            # building from max depth inward, to avoid iterating through entire schema object in order to append
            # child field groupings. Therefore, skip any field groupings at a shallower depth.
            if len(split_group_name) == curr_level:
                parent_name = '.'.join(split_group_name[:-1])
                field_name = split_group_name[-1]

                # pop in order to avoid adding the fields twice
                field_group_list = record_lists_dict.pop(field_group_name)
                record_lists_dict[parent_name].append(create_nested_schema_obj(field_name, field_group_list))

        curr_level -= 1

    schema_base_list = record_lists_dict[record_type]

    with open(output_fp, 'w') as schema_file:
        json.dump(schema_base_list, schema_file)
        print("BQ schema file creation is complete--file output at {}.".format(output_fp))


def convert_json_schema_to_python_schema(json_schema, schema):
    for field in json_schema:
        if 'mode' in field and field['mode'] == 'REPEATED':
            schema.append(
                bigquery.SchemaField(
                    field['name'],
                    'RECORD',
                    mode="REPEATED",
                    fields=convert_json_schema_to_python_schema(field['fields'], list())
                )
            )
        else:
            schema.append(
                bigquery.SchemaField(
                    field['name'],
                    field['type'],
                    mode='NULLABLE'
                )
            )

    return schema


def create_table_from_json_schema(params):
    with open(params['BQ_SCHEMA_FILEPATH']) as schema_file:
        json_obj = json.load(schema_file)
    schema = convert_json_schema_to_python_schema(json_obj, list())

    job_config = bigquery.LoadJobConfig()
    if params['DO_BATCH']:
        job_config.priority = bigquery.QueryPriority.BATCH
    job_config.schema = schema
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    client = bigquery.Client()
    gs_uri = 'gs://' + params['WORKING_BUCKET'] + "/" + params['WORKING_BUCKET_DIR'] + '/' + params['DATA_OUTPUT_FILE']
    table_id = params['WORKING_PROJECT'] + '.' + params['TARGET_DATASET'] + '.' + params['TARGET_TABLE']
    load_job = client.load_table_from_uri(gs_uri, table_id, job_config=job_config)
    print('Starting job {}'.format(load_job.job_id))

    location = 'US'
    job_state = "NOT_STARTED"

    while job_state != 'DONE':
        load_job = client.get_job(load_job.job_id, location=location)

        print('Job {} is currently in state {}'.format(load_job.job_id, load_job.state))

        job_state = load_job.state

        if job_state != 'DONE':
            time.sleep(5)

    print('Job {} is done'.format(load_job.job_id))

    load_job = client.get_job(load_job.job_id, location=location)
    if load_job.error_result is not None:
        print('[ERROR] While running BQ job: {}'.format(load_job.error_result))
        for err in load_job.errors:
            print(err)
        return False

    destination_table = client.get_table(table_id)
    print('Loaded {} rows.'.format(destination_table.num_rows))
    return True


def pprint_json(json_obj):
    """
    Pretty prints json objects.
    :param json_obj: json object to pprint
    """
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(json_obj)
