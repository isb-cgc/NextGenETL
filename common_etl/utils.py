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
import requests
import time
from google.cloud import bigquery


def has_fatal_error(e, exception=None):
    """
    Error handling function, formats error strings or a list of strings, and optionally shares exception info.
    :param e: error message string
    :param exception: Exception object relating to the fatal error, defaults to none
    """
    err_ = '[ERROR] '
    error_output = ''
    if isinstance(e, list):
        for item in e:
            error_output += err_ + str(item) + '\n'
    else:
        error_output = err_ + e

    print(error_output)

    if exception:
        raise exception
    else:
        exit(1)


def load_config(yaml_file, yaml_dict_keys):
    """
    Opens yaml file and retrieves configuration parameters.
    :param yaml_file: yaml config file name
    :param yaml_dict_keys: tuple of strings representing a subset of the yaml file's top-level dictionary keys.
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

    # Dynamically generate a list of dictionaries for the return statement, since tuples are immutable
    return_dicts = [yaml_dict[key] for key in yaml_dict_keys]

    return tuple(return_dicts)


def check_value_type(value):
    """
    Checks value for type (possibilities are string, float and integers)
    :param value: value to type check
    :return: type in BQ column format
    """
    # if has leading zero, then should be considered a string, even if only composed of digits
    val_is_none = value == '' or value == 'NA' or value == 'null' or value is None or value == 'None'
    val_is_bool = isinstance(value, bool)
    val_is_decimal = value.startswith('0.')
    val_is_id = value.startswith('0') and not val_is_decimal and len(value) > 1

    try:
        float(value)
        val_is_num = True
        # Changing this because google won't accept loss of precision in the data insert job
        # (won't cast 1.0 as 1)
        val_is_float = False if value.isdigit() else True
        # If this is used, a field with only trivial floats will be cast as Integer. However, BQ errors due to loss
        # of precision.
        # val_is_float = True if int(float(value)) != float(value) else False
    except ValueError:
        val_is_num = False
        val_is_float = False

    if val_is_none:
        return None
    elif val_is_id:
        return 'STRING'
    elif val_is_decimal or val_is_float:
        return 'FLOAT'
    elif val_is_num:
        return 'INTEGER'
    elif val_is_bool:
        return 'BOOLEAN'

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

            # adding this change because organoid sumbitter_ids look like ints, but they should be str for uniformity
            if column[-2:] == 'id':
                data_types[column] = 'STRING'
                break

            val_type = check_value_type(str(value))

            if not val_type:
                continue
            elif val_type == 'FLOAT' or val_type == 'STRING':
                data_types[column] = val_type
            elif val_type == 'INTEGER' and not data_types[column]:
                data_types[column] = val_type

    return data_types


def collect_field_values(field_dict, key, parent_dict, prefix):
    """
    Recursively inserts sets of values for a given field into return dict (used to infer field data type)
    :param field_dict: A dict of key:value pairs -- field_name : set(field_values)
    :param key: field name
    :param parent_dict: dict containing field and it's values
    :param prefix: string representation of current location in field hierarchy
    :return: field_dict containing field names and a set of its values.
    """
    # If the value of parent_dict[key] is a list at this level, and a dict at the next (or a dict at this level,
    # as seen in second conditional statement), iterate over each list element's dictionary entries.
    # (Sometimes lists are composed of strings rather than dicts, and those are later converted to strings.)
    if isinstance(parent_dict[key], list) and len(parent_dict[key]) > 0 and isinstance(parent_dict[key][0], dict):
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


def arrays_to_str_list(obj):
    """
    Converts array/list of primitives or strings to a comma-separated string
    :param obj: object to converts
    :return: modified object
    """
    if isinstance(obj, list):
        if not isinstance(obj[0], dict):
            str_list = ', '.join(obj)
            obj = str_list
        else:
            for i in range(len(obj)):
                obj[i] = arrays_to_str_list(obj[i])
    elif isinstance(obj, dict):
        for key in obj:
            obj[key] = arrays_to_str_list(obj[key])
    return obj


def generate_bq_schema(schema_dict, record_type, expand_fields_list):
    """

    :param schema_dict:
    :param record_type:
    :param expand_fields_list:
    :return:
    """
    # add field group names to a list, in order to generate a dict representing nested fields
    field_group_names = [record_type]
    nested_depth = 0

    for field_group in expand_fields_list.split(','):
        nested_field_name = record_type + '.' + field_group
        nested_depth = max(nested_depth, len(nested_field_name.split('.')))
        field_group_names.append(nested_field_name)

    record_lists_dict = {fg_name:[] for fg_name in field_group_names}
    # add field to correct field grouping list based on full field name
    for field in schema_dict:
        # record_lists_dict key is equal to the parent field components of full field name
        json_obj_key = '.'.join(field.split('.')[:-1])
        record_lists_dict[json_obj_key].append(schema_dict[field])

    temp_schema_field_dict = {}

    while nested_depth >= 1:
        for field_group_name in record_lists_dict:
            split_group_name = field_group_name.split('.')

            # building from max depth inward, to avoid iterating through entire schema object in order to append
            # child field groupings. Therefore, skip any field groupings at a shallower depth.
            if len(split_group_name) != nested_depth:
                continue

            schema_field_sublist = []

            for record in record_lists_dict[field_group_name]:
                schema_field_sublist.append(
                    bigquery.SchemaField(record['name'], record['type'], 'NULLABLE', record['description'], ())
                )

            parent_name = '.'.join(split_group_name[:-1])
            field_name = split_group_name[-1]

            if field_group_name in temp_schema_field_dict:
                schema_field_sublist += temp_schema_field_dict[field_group_name]

            if parent_name:
                if parent_name not in temp_schema_field_dict:
                    temp_schema_field_dict[parent_name] = list()

                temp_schema_field_dict[parent_name].append(
                    bigquery.SchemaField(field_name, 'RECORD', 'REPEATED', '', tuple(schema_field_sublist))
                )
            else:
                if nested_depth > 1:
                    has_fatal_error("Empty parent_name at level {}".format(nested_depth), ValueError)
                return schema_field_sublist

        nested_depth -= 1
    return None


def get_program_from_bq(case_barcode):
    client = bigquery.Client()

    program_name_query = """
        SELECT program_name
        FROM `isb-project-zero.GDC_metadata.rel22_caseData`
        WHERE case_barcode = '{}'
        """.format(case_barcode)

    query_job = client.query(program_name_query)

    results = query_job.result()

    for row in results:
        program_name = row.get('program_name')
        return program_name


def get_programs_from_bq():
    results = get_query_results(
        """
        SELECT case_barcode, program_name
        FROM `isb-project-zero.GDC_metadata.rel22_caseData`
        """
    )

    program_submitter_dict = {}

    for row in results:
        program_name = row.get('program_name')
        submitter_id = row.get('case_barcode')
        program_submitter_dict[submitter_id] = program_name

    return program_submitter_dict


def get_cases_by_program(program_name):
    cases = []
    nested_key_set = set()
    results = get_query_results(
        """
        SELECT * 
        FROM `isb-project-zero.GDC_Clinical_Data.rel22_clinical_data`
        WHERE submitter_id 
        IN (SELECT case_barcode
            FROM `isb-project-zero.GDC_metadata.rel22_caseData`
            WHERE program_name = '{}')
        """.format(program_name)
    )

    non_null_fieldset = set()
    fieldset = set()

    for case_row in results:
        case_dict = dict(case_row.items())

        for key in case_dict.copy():
            fieldset.add(key)
            # note fields with values
            if case_dict[key]:
                non_null_fieldset.add(key)

            # note nested fields with a reason to be nested
            if isinstance(case_dict[key], list):
                # print(case_dict)
                if len(case_dict[key]) > 1:
                    nested_key_set.add(key)

        cases.append(case_dict)

    null_parent_fields = fieldset - non_null_fieldset

    return cases, nested_key_set, null_parent_fields


def get_case_from_bq(case_id):
    results = get_query_results(
        """
        SELECT *
        FROM `isb-project-zero.GDC_metadata.rel22_caseData`
        WHERE case_id = '{}'
        """.format(case_id)
    )

    for row in results:
        print(row)


def get_query_results(query):
    client = bigquery.Client()

    query_job = client.query(query)
    return query_job.result()


def create_and_load_table(bq_params, data_file_name, schema):
    """

    :param bq_params:
    :param data_file_name:
    :param schema:
    :return:
    """
    job_config = bigquery.LoadJobConfig()

    if bq_params['BQ_AS_BATCH']:
        job_config.priority = bigquery.QueryPriority.BATCH
    job_config.schema = schema
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    client = bigquery.Client()
    gs_uri = 'gs://' + bq_params['WORKING_BUCKET'] + "/" + bq_params['WORKING_BUCKET_DIR'] + '/' + data_file_name
    table_id = bq_params['WORKING_PROJECT'] + '.' + bq_params['TARGET_DATASET'] + '.' + bq_params['TARGET_TABLE']
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
        err_list = ['While running BQ job: {}'.format(load_job.error_result)]
        for e in load_job.errors:
            err_list.append(e)
        has_fatal_error(err_list, ValueError)

    destination_table = client.get_table(table_id)
    print('Loaded {} rows.'.format(destination_table.num_rows))
    return True


def pprint_json(json_obj):
    """
    Pretty prints json objects.
    :param json_obj: json object to pprint
    """
    pp = pprint.PrettyPrinter(indent=2)
    pp.pprint(json_obj)
