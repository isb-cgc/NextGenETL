from common_etl.utils import create_mapping_dict, get_query_results, has_fatal_error, load_config, create_and_load_table
from google.cloud import bigquery, storage
from google.api_core import exceptions
import sys
import math
import pprint
import json
import os

YAML_HEADERS = 'params'
PARENT_ID_OFFSET = 1
CASE_ID_OFFSET = 2
COUNT_COLUMN_OFFSET = 5
REFERENCE_COLUMNS_OFFSET = 50

##
#  Functions for retrieving programs and cases
##
def get_programs_list(params):
    programs_table_id = params['WORKING_PROJECT'] + '.' + params['PROGRAM_ID_TABLE']

    programs = set()
    results = get_query_results(
        """
        SELECT distinct(program_name)
        FROM `{}`
        """.format(programs_table_id)
    )

    for result in results:
        programs.add(result.program_name)

    return programs


def get_cases_by_program(program_name, params):
    print("Retrieving cases... ", end='')

    cases = []

    dataset_path = params["WORKING_PROJECT"] + '.' + params["TARGET_DATASET"]
    main_table_id = dataset_path + '.' + params["GDC_RELEASE"] + '_clinical_data'
    programs_table_id = params['WORKING_PROJECT'] + '.' + params['PROGRAM_ID_TABLE']

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
    print("DONE. {} cases retrieved.".format(len(cases)))
    return cases


def get_row_count(table_id):
    results = get_query_results(
        """
        SELECT count(*)
        FROM `{}`
        """.format(table_id)
    )

    for result in results:
        return result.values()[0]


def print_key_sorted_dict(dict_to_print):
    for key, value in sorted(dict_to_print.items(), key=lambda item: item[0]):

        # if not isinstance(value, dict) and not isinstance(value, list):
        # print("{}: {}".format(key, value))
        print("{}".format(key))

        """
        else:
            print("{}:".format(key))
            if isinstance(value, dict):
                for v_key, v_value in sorted(value.items(), key=lambda item: item[0]):
                    print("{}: {}".format(v_key, v_value))
            else:
                value.sort()
                for v in value:
                    print(v)
        """

##
#  Functions for creating the BQ table schema dictionary
##
def retrieve_program_case_structure(program_name, cases, params):
    def build_case_structure(tables_, case_, record_counts_, parent_path):
        """
        Recursive function for retrieve_program_data, finds nested fields
        """
        if not case_:
            return tables_, record_counts_

        if parent_path not in tables_:
            tables_[parent_path] = set()
        if parent_path not in record_counts_:
            record_counts_[parent_path] = 1

        for field_key in case_:
            if not case_[field_key]:
                continue
            # Hits for cases
            elif isinstance(case_[field_key], list):
                new_path = parent_path + '.' + field_key
                if new_path not in record_counts_:
                    record_counts_[new_path] = 1

                # find needed one-to-many tables
                record_counts_[new_path] = max(record_counts_[new_path], len(case_[field_key]))

                for entry in case_[field_key]:
                    tables_, record_counts_ = build_case_structure(tables_, entry, record_counts_, new_path)
            elif isinstance(case_[field_key], dict):

                tables_, record_counts_ = build_case_structure(tables_, case_[field_key], record_counts_, parent_path)
            else:
                table_columns[parent_path].add(field_key)

        return tables_, record_counts_

    table_columns = {}
    record_counts = {}

    for case in cases:
        table_columns, record_counts = build_case_structure(table_columns, case, record_counts, parent_path='cases')

    table_columns = flatten_tables(table_columns, record_counts, params)

    if not table_columns:
        has_fatal_error("[ERROR] no case structure returned for program {}".format(program_name))

    # print("... DONE.")
    print("Record counts for each field group: {}".format(record_counts))

    return table_columns, record_counts


def remove_unwanted_fields(record, table_name, params):
    if isinstance(record, dict):
        excluded_fields = get_excluded_fields(table_name, params, fatal=True, flattened=True)
        for field in record.copy():
            if field in excluded_fields or not record[field]:
                record.pop(field)
    elif isinstance(record, set):
        excluded_fields = get_excluded_fields(table_name, params, fatal=True)
        for field in record.copy():
            if field in excluded_fields:
                record.remove(field)
    else:
        has_fatal_error("Wrong type of data structure for remove_unwanted_fields")

    return record


def flatten_tables(tables, record_counts, params):
    """
    Used by retrieve_program_case_structure
    """
    # record_counts uses fg naming convention
    field_group_counts = dict.fromkeys(record_counts.keys(), 0)

    # sort field group keys by depth
    for fg_key in field_group_counts:
        field_group_counts[fg_key] = len(fg_key.split("."))

    for field_group, depth in sorted(field_group_counts.items(), key=lambda item: item[1], reverse=True):

        tables[field_group] = remove_unwanted_fields(tables[field_group], field_group, params)

        # this is cases, already flattened
        if depth == 1:
            break
        # this fg represents a one-to-many table grouping
        if record_counts[field_group] > 1:
            continue

        split_field_group = field_group.split('.')

        for field in tables[field_group]:
            # check field naming on doubly-nested fields

            prefix = ''
            parent_key = None

            for i in range(len(split_field_group) - 1, 0, -1):
                parent_key = '.'.join(split_field_group[:i])

                if parent_key not in tables:
                    prefix += split_field_group[i] + '__'

            if not parent_key:
                has_fatal_error("Cases should be the default parent key for any column without another table.")
            else:
                tables[parent_key].add(get_bq_name(field_group + '.' + field))

        tables.pop(field_group)

    if len(tables.keys()) - 1 != sum(val > 1 for val in record_counts.values()):
        has_fatal_error("Flattened tables dictionary has incorrect number of keys.")
    return tables


def lookup_column_types(params):
    def split_datatype_array(col_dict, col_string, name_prefix):
        columns = col_string[13:-2].split(', ')

        for column in columns:
            column_type = column.split(' ')

            column_name = name_prefix + column_type[0]
            col_dict[column_name] = column_type[1].strip(',')

        return col_dict

    def generate_base_query(field_groups_):
        exclude_column_query_str = ''
        for fg_ in field_groups_:
            exclude_column_query_str += "AND column_name != '{}' ".format(fg_)

        query = """
        SELECT column_name, data_type FROM `{}.{}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = '{}_clinical_data' 
        """.format(params["WORKING_PROJECT"], params["TARGET_DATASET"], params["GDC_RELEASE"])

        return query + exclude_column_query_str

    def generate_field_group_query(field_group_):
        return """
        SELECT column_name, data_type FROM `{}.{}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = '{}_clinical_data' and column_name = '{}'
        """.format(params["WORKING_PROJECT"], params["TARGET_DATASET"], params["GDC_RELEASE"], field_group_)

    field_groups = []
    child_field_groups = {}

    for fg in params['EXPAND_FIELD_GROUPS'].split(','):
        if len(fg.split(".")) == 1:
            field_groups.append(fg)
        elif len(fg.split(".")) == 2:
            parent_fg = fg.split(".")[0]
            child_fg = fg.split(".")[1]
            if parent_fg not in child_field_groups:
                child_field_groups[parent_fg] = set()
            child_field_groups[parent_fg].add(child_fg)

    column_type_dict = dict()

    # todo there's more to optimize here in terms of automation
    base_query = generate_base_query(field_groups)
    follow_ups_query = generate_field_group_query("follow_ups")
    exposures_query = generate_field_group_query("exposures")
    demographic_query = generate_field_group_query("demographic")
    diagnoses_query = generate_field_group_query("diagnoses")
    family_histories_query = generate_field_group_query("family_histories")

    results = get_query_results(base_query)

    for result in results:
        vals = result.values()
        column_type_dict[vals[0]] = vals[1]

    single_nested_query_dict = {
        "family_histories": family_histories_query,
        "demographic": demographic_query,
        "exposures": exposures_query
    }

    for key in single_nested_query_dict.keys():
        results = get_query_results(single_nested_query_dict[key])

        for result in results:
            vals = result.values()
            column_type_dict = split_datatype_array(column_type_dict, vals[1], key + '__')

    results = get_query_results(follow_ups_query)

    for result in results:
        vals = result.values()
        split_vals = vals[1].split('molecular_tests ')

        column_type_dict = split_datatype_array(column_type_dict, split_vals[0] + ' ', 'follow_ups__')

        column_type_dict = split_datatype_array(column_type_dict, split_vals[1][:-2], 'follow_ups__molecular_tests__')

    results = get_query_results(diagnoses_query)

    diagnoses = None
    treatments = None
    annotations = None

    # create field list string
    for result in results:
        vals = result.values()
        split_vals = vals[1].split('treatments ')
        diagnoses = split_vals[0]
        treatments = split_vals[1]

        split_diagnoses = diagnoses.split('annotations ')
        if len(split_diagnoses) > 1:
            diagnoses = split_diagnoses[0]
            annotations = split_diagnoses[1][:-2]
            treatments = treatments[:-2]
        else:
            split_treatments = treatments.split('annotations ')
            treatments = split_treatments[0][:-2]
            annotations = split_treatments[1][:-2]

        diagnoses = diagnoses[:-2] + '>>'

    # parse field list strings
    column_type_dict = split_datatype_array(column_type_dict, diagnoses, 'diagnoses__')
    column_type_dict = split_datatype_array(column_type_dict, treatments, 'diagnoses__treatments__')
    column_type_dict = split_datatype_array(column_type_dict, annotations, 'diagnoses__annotations__')

    return column_type_dict


def create_schema_dict(params):
    column_type_dict = lookup_column_types(params)
    field_mapping_dict = create_mapping_dict(params['ENDPOINT'])

    schema_dict = {}

    for key in column_type_dict:
        field_map_name = "cases." + ".".join(key.split('__'))

        try:
            description = field_mapping_dict[field_map_name]['description']
        except KeyError:
            # cases.id not returned by mapping endpoint. In such cases, substitute an empty description string.
            description = ""

        field_type = column_type_dict[key]

        # this is the format for bq schema json object entries
        schema_dict[key] = {
            "name": key,
            "type": field_type,
            "description": description
        }

    return schema_dict


def get_field_name(column):
    if '.' in column:
        return column.split('.')[-1]
    elif '__' in column:
        return column.split('__')[-1]
    elif not column:
        return None
    else:
        return column


def get_fg_name(column):
    if not column:
        return None

    split_col = column.split('__')

    if not split_col:
        return None
    elif len(split_col) == 1:
        return 'cases.' + column
    else:
        return 'cases.' + '.'.join(split_col)


def get_bq_name(column):
    if not column or '.' not in column:
        return ''

    split_name = column.split('.')

    if split_name[0] == 'cases':
        if len(split_name) == 1:
            return None
        split_name = split_name[1:]

    return '__'.join(split_name)


def generate_long_name(params, program_name, table):
    file_name_parts = [params['GDC_RELEASE'], 'clin', program_name]

    # if one-to-many table, append suffix
    file_name_parts.append(get_bq_name(table)) if get_bq_name(table) else None

    return '_'.join(file_name_parts)


def get_jsonl_filename(params, program_name, table):
    return generate_long_name(params, program_name, table) + '.jsonl'


def get_temp_filepath(params, program_name, table):
    return params['TEMP_PATH'] + '/' + get_jsonl_filename(params, program_name, table)


def get_gs_filepath(params, program_name, table):
    gs_uri = 'gs://' + params['WORKING_BUCKET'] + "/" + params['WORKING_BUCKET_DIR'] + '/'
    return gs_uri + get_jsonl_filename(params, program_name, table)


def get_table_id(params, program_name, table):
    return generate_long_name(params, program_name, table)


def upload_to_bucket(params, file):
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(params['WORKING_BUCKET'])
        blob = bucket.blob(params['WORKING_BUCKET_DIR'] + '/' + file)
        blob.upload_from_filename(params["TEMP_PATH"] + '/' + file)
    except Exception as err:
        has_fatal_error("Failed to upload to bucket.\n{}".format(err))


def get_parent_table(table_key):
    if not table_key:
        return None

    split_key = table_key.split('.')
    parent_table = ".".join(split_key[:-1])
    return parent_table


def get_table_id_key(table_key, params):
    if not params["FIELD_GROUP_METADATA"]:
        has_fatal_error("params['FIELD_GROUP_METADATA'] not found")
    if 'table_id_key' not in params["FIELD_GROUP_METADATA"][table_key]:
        has_fatal_error("table_id_key not found in params['FIELD_GROUP_METADATA']['{}']".format(table_key))
    return params["FIELD_GROUP_METADATA"][table_key]['table_id_key']


def get_excluded_fields(table_key, params, fatal=False, flattened=False):
    if not params["FIELD_GROUP_METADATA"]:
        has_fatal_error("params['FIELD_GROUP_METADATA'] not found")

    if 'excluded_fields' not in params["FIELD_GROUP_METADATA"][table_key]:
        if fatal:
            has_fatal_error("excluded_fields not found in params['FIELD_GROUP_METADATA']['{}']".format(
                table_key))
        else:
            return None

    base_column_names = params["FIELD_GROUP_METADATA"][table_key]['excluded_fields']

    if flattened:
        return set(get_bq_name(table_key + '.' + column) for column in base_column_names)
    else:
        return base_column_names


def get_record_count_id_key(table_key, params, fatal=False):
    if not params["FIELD_GROUP_METADATA"]:
        has_fatal_error("params['FIELD_GROUP_METADATA'] not found")

    if 'record_count_id_key' not in params["FIELD_GROUP_METADATA"][table_key]:
        if fatal:
            has_fatal_error("record_count_id_key not found in params['FIELD_GROUP_METADATA']['{}']".format(
                table_key))
        else:
            return None
    return params["FIELD_GROUP_METADATA"][table_key]['record_count_id_key']


##
#  Functions for ordering the BQ table schema and creating BQ tables
##
def get_tables(record_counts):
    table_keys = set()

    for table in record_counts:
        if record_counts[table] > 1:
            table_keys.add(table)

    table_keys.add('cases')

    return table_keys


def import_column_order(path):
    column_dict = dict()
    count = 0

    with open(path, 'r') as file:
        columns = file.readlines()

        for column in columns:
            column_dict[column.strip()] = count
            if column.endswith('id'):
                # arbitrarily big, leaving space for reference key ordering
                count += REFERENCE_COLUMNS_OFFSET
            else:
                count += 1

    return column_dict


def generate_table_ids(params, program_name, record_counts):
    table_keys = get_tables(record_counts)

    table_ids = dict()
    program_name = "_".join(program_name.split('.'))
    base_table_name = [params["GDC_RELEASE"], 'clin', program_name]

    for table in table_keys:
        # eliminate '.' char from program name if found (which would otherwise create illegal table_id)
        split_table_path = table.split(".")
        table_name = "_".join(base_table_name)

        if len(split_table_path) > 1:
            table_suffix = "__".join(split_table_path[1:])
            table_name = table_name + '_' + table_suffix

        if not table_name:
            has_fatal_error("generate_table_name returns empty result.")

        table_id = params["WORKING_PROJECT"] + '.' + params["TARGET_DATASET"] + '.' + table_name

        table_ids[table] = table_id

    return table_ids


def add_reference_columns(table_columns, schema_dict, params, column_order_dict):
    def generate_id_schema_entry(column_name, parent_table_key_):
        if parent_table_key_ in table_columns.keys():
            parent_field_name = get_field_name(parent_table_key_)
            ancestor_table = '*_{}'.format(parent_field_name)
        else:
            ancestor_table = 'main'

        description = "Reference to the {} field of the {} record to which this record belongs. " \
                      "Parent record found in the program's {} table.".format(column_name,
                                                                              column_name[:-3],
                                                                              ancestor_table)

        return {"name": column_name, "type": 'STRING', "description": description}

    def generate_record_count_schema_entry(record_count_id_key_, parent_table_key_):
        description = "Total count of records associated with this case, located in {} table".format(parent_table_key_)
        return {"name": record_count_id_key_, "type": 'INTEGER', "description": description}

    for table_key in table_columns.keys():
        table_depth = len(table_key.split('.'))
        id_column_position = column_order_dict[get_table_id_key(table_key, params)]

        if table_depth > 2:
            # add reference to parent field group id (even if flattened)
            parent_table_key = get_parent_table(table_key)
            parent_id_key = get_table_id_key(parent_table_key, params)

            parent_id_column_name = get_bq_name(table_key) + '__' + parent_id_key
            schema_dict[parent_id_column_name] = generate_id_schema_entry(parent_id_key, parent_table_key)
            column_order_dict[parent_id_column_name] = id_column_position + PARENT_ID_OFFSET

        if table_depth > 1:
            # add case id to one-to-many table
            case_id_key = get_bq_name(table_key) + '__case_id'
            schema_dict[case_id_key] = generate_id_schema_entry('case_id', 'main')
            column_order_dict[case_id_key] = id_column_position + CASE_ID_OFFSET

            # Find actual parent table (may not be direct ancestor, which might've been flattened)
            parent_table_key = get_parent_table(table_key)

            while parent_table_key and parent_table_key not in table_columns:
                parent_table_key = get_parent_table(parent_table_key)

            if not parent_table_key:
                has_fatal_error("No parent table found for: {}".format(table_key))

            # add one-to-many count column to parent table
            count_id_key = get_bq_name(table_key) + '__count'
            schema_dict[count_id_key] = generate_record_count_schema_entry(count_id_key, parent_table_key)
            table_columns[parent_table_key].add(count_id_key)

            # dictate table ordering for count fields
            parent_id_column_position = column_order_dict[get_table_id_key(parent_table_key, params)]
            column_order_dict[count_id_key] = parent_id_column_position + COUNT_COLUMN_OFFSET

    return schema_dict, column_order_dict


def create_schemas(table_columns, params, schema_dict, column_order_dict):
    table_schema_fields = dict()

    # modify schema dict, add reference columns for this program
    schema_dict, column_order_dict = add_reference_columns(table_columns, schema_dict, params, column_order_dict)
    print_key_sorted_dict(schema_dict)

    for table_key in table_columns:
        table_order_dict = dict()
        schema_field_keys = []

        for column in table_columns[table_key]:
            bq_column_name = get_bq_name(table_key + '.' + column)

            if not bq_column_name or bq_column_name not in column_order_dict:
                has_fatal_error('{} not in COLUMN_ORDER_DICT!'.format(bq_column_name))
            table_order_dict[bq_column_name] = column_order_dict[bq_column_name]

            table_id_key = get_table_id_key(table_key, params)
            count_column_idx = table_order_dict[table_id_key] + COUNT_COLUMN_OFFSET

            count_keys = []
            count_key_idx = count_column_idx

            for key, value in table_order_dict.items():
                if value == count_column_idx:
                    count_keys.append(key)

            for count_key in count_keys:
                table_order_dict[count_key] = count_key_idx
                count_key_idx += 1

        for column, value in sorted(table_order_dict.items(), key=lambda x: x[1]):
            schema_field_keys.append(column)

        schema_list = []

        for schema_key in schema_field_keys:
            if schema_key in params["REQUIRED_COLUMNS"]:
                mode = 'REQUIRED'
            else:
                mode = "NULLABLE"

            schema_list.append(bigquery.SchemaField(name=schema_key,
                                                    field_type=schema_dict[schema_key]['type'],
                                                    mode=mode,
                                                    description=schema_dict[schema_key]['description'],
                                                    fields=()
                                                    ))

        table_schema_fields[table_key] = schema_list

    return table_schema_fields


##
#  Functions for inserting case entries into BQ tables
##
def create_table_mapping(tables_dict):
    # string manipulation for bigquery result which looks like an object but doesn't seem to have methods.
    # Parsing this so we can avoid explicitly selecting all the table's columns (which would otherwise be required due
    # to naming collisions.
    table_mapping_dict = dict()

    for table in tables_dict:
        prefix = "__".join(table.split('.')) + "__"
        prefix = prefix[7:]

        for column in tables_dict[table]:
            table_mapping_dict[prefix + column] = table

    return table_mapping_dict


def flatten_case(case, prefix, flattened_case_dict, params, table_keys, case_id=None, parent_id=None, parent_id_key=None):
    if isinstance(case, list):
        entry_list = []

        for entry in case:
            entry_dict = dict()

            for key in entry:
                if isinstance(entry[key], list):
                    if case_id != parent_id:
                        entry_dict['case_id'] = case_id
                        entry_dict[get_bq_name(prefix) + '__' + parent_id_key] = parent_id
                    else:
                        entry_dict['case_id'] = case_id

                    # note -- If you're here because you've added a new doubly-nested field group,
                    # this is where you'll want to capture the parent field group's id.
                    id_key = get_table_id_key(prefix, params)

                    if id_key:
                        new_parent_id_key = id_key
                        new_parent_id = entry[new_parent_id_key]
                    else:
                        new_parent_id = parent_id
                        new_parent_id_key = parent_id_key

                    flattened_case_dict = flatten_case(entry[key], prefix + '.' + key, flattened_case_dict, params,
                                                       table_keys, case_id, new_parent_id, new_parent_id_key)
                else:

                    col_name = get_bq_name(prefix + '.' + key)
                    entry_dict[col_name] = entry[key]

            entry_dict = remove_unwanted_fields(entry_dict, prefix, params)
            entry_list.append(entry_dict)
        if prefix in flattened_case_dict:
            flattened_case_dict[prefix] = flattened_case_dict[prefix] + entry_list
        else:
            if entry_list:
                flattened_case_dict[prefix] = entry_list
    else:
        entry_list = []
        entry_dict = dict()
        if prefix not in flattened_case_dict:
            flattened_case_dict[prefix] = []

        parent_id = case_id = case['case_id']
        parent_id_key = 'case_id'

        for key in case:
            if isinstance(case[key], list):
                flattened_case_dict = flatten_case(case[key], prefix + '.' + key, flattened_case_dict, params, table_keys,
                                                   case_id, parent_id, parent_id_key)
            else:
                col_name = get_bq_name(prefix + '.' + key)
                entry_dict[col_name] = case[key]
        if entry_dict:
            entry_dict = remove_unwanted_fields(entry_dict, prefix, params)
            entry_list.append(entry_dict)
            flattened_case_dict[prefix] = entry_list
    return flattened_case_dict


def merge_single_entry_field_groups(flattened_case_dict, table_keys, params):
    for field_group_key, field_group in flattened_case_dict.copy().items():
        if field_group_key in table_keys:
            if field_group_key != 'cases':
                record_count = len(field_group)
                parent_table_key = get_parent_table(field_group_key)
                if parent_table_key not in table_keys:
                    parent_table_key = get_parent_table(parent_table_key)
                if parent_table_key not in table_keys:
                    has_fatal_error("no parent {}, keys: {}".format(field_group_key, table_keys))

                record_count_key = get_record_count_id_key(field_group_key, params)
                flattened_case_dict[parent_table_key][0][record_count_key] = record_count
            continue
        else:
            if len(flattened_case_dict[field_group_key]) > 1:
                has_fatal_error("{} in flattened_dict has > 1 record, but not a table.".format(field_group_key))

            field_group = flattened_case_dict.pop(field_group_key)[0]

            parent_table_key = field_group_key

            while parent_table_key and parent_table_key not in table_keys:
                parent_table_key = get_parent_table(parent_table_key)

            if not parent_table_key:
                has_fatal_error("Couldn't find any parent table in tables list for {}".format(field_group_key))

            if 'case_id' in field_group:
                field_group.pop('case_id')

            if len(flattened_case_dict[parent_table_key]) > 1:
                has_fatal_error("parent has multiple records.")
            for key in field_group.keys():
                if not field_group[key]:
                    continue
                flattened_case_dict[parent_table_key][0][key] = field_group[key]
    return flattened_case_dict


def create_and_load_tables(program_name, cases, params, table_schemas):
    print("Inserting case records... ")
    table_keys = table_schemas.keys()

    for table in table_keys:
        fp = get_temp_filepath(params, program_name, table)
        if os.path.exists(fp):
            os.remove(fp)

    for case in cases:
        flattened_case_dict = flatten_case(case, 'cases', dict(), params, table_keys, case['case_id'], case['case_id'])
        flattened_case_dict = merge_single_entry_field_groups(flattened_case_dict, table_keys, params)

        for table in flattened_case_dict.keys():
            if table not in table_keys:
                has_fatal_error("Table {} not found in table keys".format(table))

            jsonl_fp = get_temp_filepath(params, program_name, table)

            with open(jsonl_fp, 'a') as jsonl_file:
                for row in flattened_case_dict[table]:
                    json.dump(obj=row, fp=jsonl_file)
                    jsonl_file.write('\n')

    for table in table_schemas:
        jsonl_file = get_jsonl_filename(params, program_name, table)

        upload_to_bucket(params, jsonl_file)

        table_id = get_table_id(params, program_name, table)

        create_and_load_table(params, jsonl_file, table_schemas[table], table_id)


def check_data_integrity(params, cases, record_counts, table_columns):
    frequency_dict = {}

    tables = get_tables(record_counts)

    if len(tables) < 2:
        return

    for case in cases:
        count_dict = dict()

        hierarchy = {
            'follow_ups': {
                'molecular_tests'
            },
            'exposures': None,
            'family_histories': None,
            'demographic': None,
            'diagnoses': {
                'treatments',
                'annotations'
            }
        }

        for key in hierarchy:
            if key in case and case[key] and isinstance(case[key], list):
                cnt = len(case[key])

                if key in count_dict:
                    count_dict[key] += cnt
                else:
                    count_dict[key] = cnt

                for entry in case[key]:
                    if hierarchy[key]:
                        for c_key in hierarchy[key]:
                            if c_key in entry and entry[c_key] and isinstance(c_key, list):
                                cnt = len(entry[c_key])

                                if c_key in count_dict:
                                    count_dict[c_key] += cnt
                                else:
                                    count_dict[c_key] = cnt

        for field in count_dict:
            frequency_key = count_dict[field]
            if field not in frequency_dict:
                frequency_dict[field] = dict()
            if frequency_key in frequency_dict[field]:
                frequency_dict[field][frequency_key] += 1
            else:
                frequency_dict[field][frequency_key] = 1

    if frequency_dict:
        print("Frequency of records per case for one-to-many tables:\n")
        for fg_key in frequency_dict.copy():
            if not frequency_dict[fg_key]:
                frequency_dict.pop(fg_key)
            if len(frequency_dict[fg_key].keys()) == 1 and 1 in frequency_dict[fg_key].keys():
                frequency_dict.pop(fg_key)
            else:
                print("{}".format(fg_key))
                for value in frequency_dict[fg_key]:
                    print("\t{}: {}".format(value, frequency_dict[fg_key][value]))


##
#  Functions for creating documentation
##
def generate_documentation(params, program_name, documentation_dict, record_counts):
    print("Inserting documentation", end='')
    # print("{} \n".format(program_name))
    # print("{}".format(documentation_dict))
    # print("{}".format(record_counts))

    """
    documentation_dict = {
        'tables_overview': {
            table1: fields,
            table2: fields
            ...
        },
        'table_schemas': {
            table_key: {
                'table_id': full table name in BQ,
                'table_schema': [
                    {
                        'type': column_type,
                        'name': name,
                        'column_description': description
                    }
                ]
            }
        }
    }
    """
    with open(params['DOCS_OUTPUT_FILE'], 'a') as doc_file:
        doc_file.write("{} \n".format(program_name))
        doc_file.write("{}".format(documentation_dict))
        doc_file.write("{}".format(record_counts))

    print("... DONE.")


def main(args):
    '''
    fg_name_types: (cases.diagnoses.annotations): tables_dict, record_counts keys, insert_lists
    bq_name_types: (diagnoses__annotations__case_id): schema_dict, column_order_dict keys, flattened_case_dict
    '''

    """
    if len(args) != 3:
        has_fatal_error('Usage : {} <configuration_yaml> <column_order_txt>".format(args[0])', ValueError)

    with open(args[1], mode='r') as yaml_file:
        try:
            params = load_config(yaml_file, YAML_HEADERS)
        except ValueError as e:
            has_fatal_error(str(e), ValueError)

    # programs_table_id = params['WORKING_PROJECT'] + '.' + params['PROGRAM_ID_TABLE']
    """

    params = {
        "INSERT_BATCH_SIZE": 1000,
        'ENDPOINT': 'https://api.gdc.cancer.gov/cases',
        "DOCS_OUTPUT_FILE": 'docs/documentation.txt',
        "EXPAND_FIELD_GROUPS": 'demographic,diagnoses,diagnoses.treatments,diagnoses.annotations,exposures,'
                               'family_histories,follow_ups,follow_ups.molecular_tests',
        "GDC_RELEASE": 'rel23',
        "WORKING_PROJECT": 'isb-project-zero',
        "TARGET_DATASET": 'GDC_Clinical_Data',
        "PROGRAM_ID_TABLE": 'GDC_metadata.rel23_caseData',
        "FIELD_GROUP_METADATA": {
            'cases.diagnoses': {
                'excluded_fields': ["submitter_id"],
                'table_id_key': 'diagnosis_id'
            },
            'cases.demographic': {
                'excluded_fields': ["submitter_id"],
                'table_id_key': 'demographic_id'
            },
            'cases.exposures': {
                'excluded_fields': ["submitter_id"],
                'table_id_key': 'exposure_id'
            },
            'cases.family_histories': {
                'excluded_fields': ["submitter_id"],
                'table_id_key': 'family_history_id'
            },
            'cases.follow_ups': {
                'excluded_fields': ["submitter_id"],
                'table_id_key': 'follow_up_id'
            },
            'cases.follow_ups.molecular_tests': {
                'excluded_fields': ["submitter_id"],
                'table_id_key': 'molecular_test_id'
            },
            'cases.diagnoses.treatments': {
                'excluded_fields': ["submitter_id"],
                'table_id_key': 'treatment_id'
            },
            'cases.diagnoses.annotations': {
                'excluded_fields': ["submitter_id", "case_submitter_id", "entity_submitter_id"],
                'table_id_key': 'annotation_id'
            },
            'cases': {
                'excluded_fields': [
                    "aliquot_ids", "analyte_ids", "case_autocomplete", "diagnosis_ids", "id", "portion_ids",
                    "sample_ids", "slide_ids", "submitter_aliquot_ids", "submitter_analyte_ids",
                    "submitter_diagnosis_ids", "submitter_portion_ids", "submitter_sample_ids", "submitter_slide_ids"
                ],
                'table_id_key': 'case_id'
            }
        },
        "REQUIRED_COLUMNS": {
            'case_id',
            'diagnoses__diagnosis_id',
            'diagnoses__treatments__treatment_id',
            'follow_ups__follow_up_id',
            'follow_ups__molecular_tests__molecular_test_id'
        },
        "BQ_AS_BATCH": False,
        'WORKING_BUCKET': 'next-gen-etl-scratch',
        'WORKING_BUCKET_DIR': 'law',
        "TEMP_PATH": 'temp'
    }

    # program_names = get_programs_list(params)
    program_names = ['VAREPOP']

    column_order_dict = import_column_order(args[2])

    with open(params['DOCS_OUTPUT_FILE'], 'w') as doc_file:
        doc_file.write("New BQ Documentation")

    schema_dict = create_schema_dict(params)

    for program_name in program_names:
        print("\n*** Running script for {} ***".format(program_name))
        cases = get_cases_by_program(program_name, params)

        if not cases:
            print("No case records found for {}, skipping.".format(program_name))
            continue

        table_columns, record_counts = retrieve_program_case_structure(program_name, cases, params)

        table_schemas = create_schemas(table_columns, params, schema_dict, column_order_dict)

        # documentation_dict, table_names_dict = create_bq_tables(
        #   program_name, params, table_columns, record_counts, schema_dict)

        # create_and_load_tables(program_name, cases, params, table_schemas)

        # generate_documentation(params, program_name, documentation_dict, record_counts)

        # check_data_integrity(params, cases, record_counts, table_columns)


if __name__ == '__main__':
    main(sys.argv)
