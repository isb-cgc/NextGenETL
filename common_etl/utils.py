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
import yaml
import pprint
import requests
import time
from google.cloud import bigquery, storage


def has_fatal_error(e, exception=None):
    """
    Error handling function, formats error strings or a list of strings,
    and optionally shares exception info.
    :param e: error message string
    :param exception: Exception object relating to the fatal error, defaults
    to none
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
    :param yaml_dict_keys: tuple of strings representing a subset of the yaml
    file's top-level dictionary keys.
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


def check_value_type(value):
    """
    Checks value for type (possibilities are string, float and integers)
    :param value: value to type check
    :return: type in BQ column format
    """
    # if has leading zero, then should be considered a string, even if only
    # composed of digits
    val_is_none = value == '' or value == 'NA' or value == 'null' or value is \
                  None or value == 'None'
    val_is_bool = value == 'True' or value == 'False'
    val_is_decimal = value.startswith('0.')
    val_is_id = value.startswith('0') and not val_is_decimal and len(value) > 1

    try:
        float(value)
        val_is_num = True
        # Changing this because google won't accept loss of precision in the
        # data insert job
        # (won't cast 1.0 as 1)
        val_is_float = False if value.isdigit() else True
        # If this is used, a field with only trivial floats will be cast as
        # Integer. However, BQ errors due to loss
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
    :param flattened_json: file containing dict of field names (key) and sets
    of field values (value)
    :return: dict of field names and inferred type (None if no data in value
    set).
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
            elif val_type == 'FLOAT' or val_type == 'STRING':
                data_types[column] = val_type
            elif (val_type == 'INTEGER' or val_type == 'BOOLEAN') and not \
                    data_types[column]:
                data_types[column] = val_type

    return data_types


def collect_field_values(field_dict, key, parent_dict, prefix):
    """
    Recursively inserts sets of values for a given field into return dict (
    used to infer field data type)
    :param field_dict: A dict of key:value pairs -- field_name : set(
    field_values)
    :param key: field name
    :param parent_dict: dict containing field and it's values
    :param prefix: string representation of current location in field hierarchy
    :return: field_dict containing field names and a set of its values.
    """
    # If the value of parent_dict[key] is a list at this level, and a dict at
    # the next (or a dict at this level,
    # as seen in second conditional statement), iterate over each list
    # element's dictionary entries.
    # (Sometimes lists are composed of strings rather than dicts, and those
    # are later converted to strings.)
    if isinstance(parent_dict[key], list) and len(
            parent_dict[key]) > 0 and isinstance(parent_dict[key][0], dict):
        for dict_item in parent_dict[key]:
            for dict_key in dict_item:
                field_dict = collect_field_values(field_dict, dict_key,
                                                  dict_item, prefix + key + ".")
    elif isinstance(parent_dict[key], dict):
        for dict_key in parent_dict[key]:
            field_dict = collect_field_values(field_dict, dict_key,
                                              parent_dict[key],
                                              prefix + key + ".")
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
    Note: only differentiates the GDC API's 'long' type (called 'integer' in
    GDC data dictionary) and
    'float' type (called 'number' in GDC data dictionary). All others typed
    as string.
    :param endpoint: API endpoint for which to retrieve mapping.
    :return: dict of field mappings. Each entry object contains field name,
    type, and description
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


def convert_dict_to_string(obj):
    """
    Converts dict/list of primitives or strings to a comma-separated string
    :param obj: object to converts
    :return: modified object
    """
    if isinstance(obj, list):
        if not isinstance(obj[0], dict):
            str_list = ', '.join(obj)
            obj = str_list
        else:
            for i in range(len(obj)):
                obj[i] = convert_dict_to_string(obj[i])
    elif isinstance(obj, dict):
        for key in obj:
            obj[key] = convert_dict_to_string(obj[key])
    return obj


def generate_bq_schema(schema_dict, record_type, expand_fields_list):
    """

    :param schema_dict:
    :param record_type:
    :param expand_fields_list:
    :return:
    """
    # add field group names to a list, in order to generate a dict
    # representing nested fields
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
        json_obj_key = '.'.join(field.split('.')[:-1])
        record_lists_dict[json_obj_key].append(schema_dict[field])

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
                    bigquery.SchemaField(record['name'], record['type'],
                                         'NULLABLE', record['description'], ())
                )

            parent_name = '.'.join(split_group_name[:-1])
            field_name = split_group_name[-1]

            if field_group_name in temp_schema_field_dict:
                schema_field_sublist += temp_schema_field_dict[field_group_name]

            if parent_name:
                if parent_name not in temp_schema_field_dict:
                    temp_schema_field_dict[parent_name] = list()

                temp_schema_field_dict[parent_name].append(
                    bigquery.SchemaField(field_name, 'RECORD', 'REPEATED', '',
                                         tuple(schema_field_sublist))
                )
            else:
                if nested_depth > 1:
                    has_fatal_error(
                        "Empty parent_name at level {}".format(nested_depth),
                        ValueError)
                return schema_field_sublist

        nested_depth -= 1
    return None


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


def get_query_results(query):
    client = bigquery.Client()

    query_job = client.query(query)
    return query_job.result()


def create_and_load_table(bq_params, jsonl_rows_file, schema, table_name):
    job_config = bigquery.LoadJobConfig()
    job_config.schema = schema
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    if bq_params['BQ_AS_BATCH']:
        job_config.priority = bigquery.QueryPriority.BATCH

    client = bigquery.Client()
    gs_uri = 'gs://' + bq_params['WORKING_BUCKET'] + "/" + \
             bq_params['WORKING_BUCKET_DIR'] + '/' + jsonl_rows_file

    table_id = bq_params['WORKING_PROJECT'] + '.' + \
               bq_params['TARGET_DATASET'] + '.' + table_name

    try:
        load_job = client.load_table_from_uri(
            gs_uri, table_id, job_config=job_config)

        print('\tStarting insert for {}, job ID: {}'.
              format(table_name, load_job.job_id))

        last_report_time = time.time()

        location = 'US'
        job_state = "NOT_STARTED"

        while job_state != 'DONE':
            load_job = client.get_job(load_job.job_id, location=location)

            if time.time() - last_report_time > 15:
                print('\t- job is currently in state {}'.format(load_job.state))
                last_report_time = time.time()

            job_state = load_job.state

            if job_state != 'DONE':
                time.sleep(3)

        load_job = client.get_job(load_job.job_id, location=location)

        if load_job.error_result is not None:
            has_fatal_error('While running BQ job: {}\n{}'.
                            format(load_job.error_result, load_job.errors),
                            ValueError)

        destination_table = client.get_table(table_id)

        print('\tDone! {} rows inserted.\n'.
              format(destination_table.num_rows))
    except Exception as err:
        print(schema)
        has_fatal_error(err)


def pprint_json(json_obj):
    """
    Pretty prints json objects.
    :param json_obj: json object to pprint
    """
    pp = pprint.PrettyPrinter(indent=1)
    pp.pprint(json_obj)


def ordered_print(flattened_case_dict, order_dict):
    def make_tabs(indent_):
        tab_list = indent_ * ['\t']
        return "".join(tab_list)

    tables_string = '{\n'
    indent = 1

    for table in sorted(flattened_case_dict.keys()):
        tables_string += "{}'{}': [\n".format(make_tabs(indent), table)

        split_prefix = table.split(".")
        if len(split_prefix) == 1:
            prefix = ''
        else:
            prefix = '__'.join(split_prefix[1:])
            prefix += '__'

        for entry in flattened_case_dict[table]:
            entry_string = "{}{{\n".format(make_tabs(indent + 1))
            field_order_dict = dict()

            for key in entry.copy():
                col_order_lookup_key = prefix + key

                try:
                    field_order_dict[key] = order_dict[col_order_lookup_key]
                except KeyError:
                    print("ORDERED PRINT -- {} not in column order dict".format(
                        col_order_lookup_key))
                    for k, v in sorted(order_dict.items(),
                                       key=lambda item: item[0]):
                        print("{}: {}".format(k, v))
                    field_order_dict[key] = 0

            for field_key, order in sorted(field_order_dict.items(),
                                           key=lambda item: item[1]):
                entry_string += "{}{}: {},\n".format(make_tabs(indent + 2),
                                                     field_key,
                                                     entry[field_key])
            entry_string = entry_string.rstrip('\n')
            entry_string = entry_string.rstrip(',')

            entry_string += '{}}}\n'.format(make_tabs(indent + 1))
            tables_string += entry_string
        tables_string = tables_string.rstrip('\n')
        tables_string = tables_string.rstrip(',')
        tables_string += '\n'
        tables_string += "{}],\n".format(make_tabs(indent))
    tables_string = tables_string.rstrip('\n')
    tables_string = tables_string.rstrip(',')
    tables_string += "\n}"

    print(tables_string)


def get_cases_by_program(bq_params, program_name):
    cases = []

    dataset_path = bq_params["WORKING_PROJECT"] + '.' + \
                   bq_params["TARGET_DATASET"]

    main_table_id = dataset_path + '.' + \
                    bq_params["GDC_RELEASE"] + '_clinical_data'

    programs_table_id = bq_params['WORKING_PROJECT'] + '.' + \
                        bq_params['METADATA_DATASET'] + '.' + \
                        bq_params['GDC_RELEASE'] + '_caseData'

    results = get_query_results(
        """
        SELECT * 
        FROM `{}`
        WHERE case_id 
        IN (SELECT case_gdc_id
            FROM `{}`
            WHERE program_name = '{}')
        """.format(main_table_id, programs_table_id, program_name)
    )

    for case_row in results:
        cases.append(dict(case_row.items()))
    if cases:
        print("{} cases retrieved.".format(len(cases)))
    else:
        print("No case records found for program {}, skipping."
              .format(program_name))
    return cases


def get_full_field_name(fg, field):
    return fg + '.' + field


def get_field_name(column):
    if '.' in column:
        return column.split('.')[-1]
    elif '__' in column:
        return column.split('__')[-1]
    elif not column:
        return None
    else:
        return column


def get_field_depth(field):
    return len(field.split('.'))


def get_abbr_dict(api_params):
    table_abbr_dict = dict()

    for table_key, table_metadata in api_params['TABLE_METADATA'].items():
        table_abbr_dict[table_key] = table_metadata['prefix']

    return table_abbr_dict


def get_bq_name(api_params, table_path, column):
    table_abbr_dict = get_abbr_dict(api_params)

    if not table_path:
        split_column = column.split('.')
    else:
        full_name = table_path + '.' + column
        split_column = full_name.split('.')

    if len(split_column) == 1 or \
            (len(split_column) == 2 and split_column[0] == 'cases'):
        return split_column[-1]

    if split_column[0] != 'cases':
        split_column.insert(0, 'cases')

    column = split_column[-1]
    split_column = split_column[:-1]
    table_key = ".".join(split_column)

    if table_key not in table_abbr_dict:
        return column

    prefix = table_abbr_dict[table_key]
    return prefix + '__' + column


def get_parent_field_group(table_key):
    split_key = table_key.split('.')

    return ".".join(split_key[:-1])


def get_tables(record_counts):
    table_keys = set()

    for table in record_counts:
        if record_counts[table] > 1:
            table_keys.add(table)

    table_keys.add('cases')

    return table_keys


def get_table_id(bq_params, table_name):
    """
    Get the full table_id (Including project and dataset) for a given table.
    :param bq_params:
    :param table_name: Desired table name (can be created using get_table_id
    :return: String of the form bq_project_name.bq_dataset_name.bq_table_name.
    """
    return bq_params["WORKING_PROJECT"] + '.' + \
           bq_params["TARGET_DATASET"] + '.' + table_name


def convert_bq_table_id_to_fg(table_id):
    short_table_name = "_".join(table_id.split('_')[3:])

    table_name = 'cases'

    if short_table_name:
        table_name += '.' + '.'.join(short_table_name.split('__'))

    return table_name


def get_max_count(record_count_list):
    max_count = 0
    max_count_id = None
    for record_count_entry in record_count_list:
        if record_count_entry['record_count'] > max_count:
            max_count_id = record_count_entry
            max_count = record_count_entry['record_count']

    return max_count, max_count_id


def in_bq_format(name):
    if '__' in name:
        return True
    else:
        return False


def get_parent_table(table_keys, field_group):
    base_table = field_group.split('.')[0]

    if not base_table or base_table not in table_keys:
        has_fatal_error(
            "'{}' has no parent table in tables list: {}".format(field_group,
                                                                 table_keys))

    parent_table_key = get_parent_field_group(field_group)

    while parent_table_key and parent_table_key not in table_keys:
        parent_table_key = get_parent_field_group(parent_table_key)

    if not parent_table_key:
        has_fatal_error("No parent found for {}".format(field_group))

    return parent_table_key


def new_column_type_lookup(table_id):
    client = bigquery.Client()
    table = client.get_table(table_id)

    print(table)
    schema_fields = table.schema

    for schema_field in schema_fields:
        # todo delete print
        field = schema_field.name
        field_type = schema_field.field_type

        print("schema_field.name: {}".format(schema_field.name))
        print("schema_field.field_type: {}".format(schema_field.field_type))

        if field_type == 'RECORD':
            # todo delete print
            print("\tschema_field.fields: {}".format(schema_field.fields))




def upload_to_bucket(bq_params, fp, file_name):
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bq_params['WORKING_BUCKET'])
        blob = bucket.blob(bq_params['WORKING_BUCKET_DIR'] + '/' + file_name)

        blob.upload_from_filename(fp + '/' + file_name)
    except Exception as err:
        has_fatal_error("Failed to upload to bucket.\n{}".format(err))


def get_dataset_table_list(bq_params):
    client = bigquery.Client()
    dataset = client.get_dataset(bq_params['WORKING_PROJECT'] + '.' +
                                 bq_params['TARGET_DATASET'])
    results = client.list_tables(dataset)

    table_id_prefix = bq_params["GDC_RELEASE"] + '_clin_'

    table_id_list = []

    for table in results:
        table_id_name = table.table_id
        if table_id_name and table_id_prefix in table_id_name:
            table_id_list.append(table_id_name)

    table_id_list.sort()

    return table_id_list


def make_SchemaField(schema_dict, schema_key, required_columns):
    return bigquery.SchemaField(
        name=schema_dict[schema_key]['name'],
        field_type=schema_dict[schema_key]['type'],
        mode='REQUIRED' if schema_key in required_columns else 'NULLABLE',
        description=schema_dict[schema_key]['description'],
        fields=())


def download_from_bucket(src_file, dest_file, bq_params):
    client = storage.Client()

    with open(dest_file) as file_obj:
        bucket_path = ('gs://' + bq_params['WORKING_BUCKET'] + "/" +
                       bq_params['WORKING_BUCKET_DIR'] + '/')
        path_to_file = bucket_path + '/' + bq_params['GDC_RELEASE'] + src_file

        client.download_blob_to_file(path_to_file, file_obj)
