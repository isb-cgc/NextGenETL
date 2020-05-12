from common_etl.utils import *
from google.cloud import bigquery, storage
import sys
import json
import os
import time

YAML_HEADERS = 'params'
API_PARAMS = None
BQ_PARAMS = None


def get_programs_list():
    programs_table_id = BQ_PARAMS['WORKING_PROJECT'] + '.' + BQ_PARAMS['PROGRAM_ID_TABLE']

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


def get_dataset_table_list():
    client = bigquery.Client()
    dataset = client.get_dataset(BQ_PARAMS['WORKING_PROJECT'] + '.' + BQ_PARAMS['TARGET_DATASET'])
    results = client.list_tables(dataset)

    table_id_prefix = BQ_PARAMS["GDC_RELEASE"] + '_clin_'

    table_id_list = []

    for table in results:
        table_id_name = table.table_id
        if table_id_name and table_id_prefix in table_id_name:
            table_id_list.append(table_id_name)

    table_id_list.sort()

    return table_id_list


def retrieve_program_case_structure(program_name, cases):
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

    table_columns = flatten_tables(table_columns, record_counts)

    if not table_columns:
        has_fatal_error("[ERROR] no case structure returned for program {}".format(program_name))

    # print("... DONE.")
    print("Record counts for each field group: {}".format(record_counts))

    return table_columns, record_counts


def remove_unwanted_fields(record, table_name):
    if isinstance(record, dict):
        excluded_fields = get_excluded_fields(table_name, fatal=True, flattened=True)
        for field in record.copy():
            if field in excluded_fields or not record[field]:
                record.pop(field)
    elif isinstance(record, set):
        excluded_fields = get_excluded_fields(table_name, fatal=True)
        for field in record.copy():
            if field in excluded_fields:
                record.remove(field)
    else:
        has_fatal_error("Wrong type of data structure for remove_unwanted_fields")

    return record


def flatten_tables(tables, record_counts):
    """
    Used by retrieve_program_case_structure
    """
    # record_counts uses fg naming convention
    field_group_counts = dict.fromkeys(record_counts.keys(), 0)

    # sort field group keys by depth
    for fg_key in field_group_counts:
        field_group_counts[fg_key] = len(fg_key.split("."))

    for field_group, depth in sorted(field_group_counts.items(), key=lambda item: item[1], reverse=True):

        tables[field_group] = remove_unwanted_fields(tables[field_group], field_group)

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


def lookup_column_types():
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
        """.format(BQ_PARAMS["WORKING_PROJECT"], BQ_PARAMS["TARGET_DATASET"], BQ_PARAMS["GDC_RELEASE"])

        return query + exclude_column_query_str

    def generate_field_group_query(field_group_):
        return """
        SELECT column_name, data_type FROM `{}.{}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = '{}_clinical_data' and column_name = '{}'
        """.format(BQ_PARAMS["WORKING_PROJECT"], BQ_PARAMS["TARGET_DATASET"],BQ_PARAMS["GDC_RELEASE"], field_group_)

    field_groups = []
    child_field_groups = {}

    for fg in API_PARAMS['EXPAND_FIELD_GROUPS'].split(','):
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


def build_column_order_dict():
    column_order_dict = dict()
    field_groups = API_PARAMS['FIELD_GROUP_ORDER']
    max_reference_cols = len(field_groups)

    idx = 0

    for fg in field_groups:
        try:
            column_order_list = API_PARAMS['FIELD_GROUP_METADATA'][fg]['column_order']
            id_column = API_PARAMS['FIELD_GROUP_METADATA'][fg]['table_id_key']
            for column in column_order_list:
                bq_column = get_bq_name(fg + '.' + column.strip())

                if not bq_column:
                    has_fatal_error("Null value in field group {}'s column_order list".format(fg))

                column_order_dict[bq_column] = idx

                if id_column == column:
                    # this creates space for reference columns (parent id or one-to-many record count columns)
                    # leaves a gap for submitter_id
                    idx += max_reference_cols * 2
                else:
                    idx += 1
        except KeyError:
            has_fatal_error("{} found in API_PARAMS['FIELD_GROUP_ORDER'] "
                            "but not in API_PARAMS['FIELD_GROUP_METADATA']".format(fg))

    column_order_dict['state'] = idx
    column_order_dict['created_datetime'] = idx + 1
    column_order_dict['updated_datetime'] = idx + 2

    return column_order_dict


def create_schema_dict():
    column_type_dict = lookup_column_types()
    field_mapping_dict = create_mapping_dict(API_PARAMS['ENDPOINT'])

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


def get_count_column_position(table_key, column_order_dict):
    table_id_key = get_table_id_key(table_key)
    bq_table_id_column_name = get_bq_name(table_key + '.' + table_id_key)
    id_column_position = column_order_dict[bq_table_id_column_name]

    count_columns_position = id_column_position + len(API_PARAMS['FIELD_GROUP_ORDER'])

    return count_columns_position


def generate_long_name(program_name, table):
    # remove invalid char from program name
    if '.' in program_name:
        program_name = '_'.join(program_name.split('.'))

    file_name_parts = [BQ_PARAMS['GDC_RELEASE'], 'clin', program_name]

    # if one-to-many table, append suffix
    file_name_parts.append(get_bq_name(table)) if get_bq_name(table) else None

    return '_'.join(file_name_parts)


def get_jsonl_filename(program_name, table):
    return generate_long_name(program_name, table) + '.jsonl'


def get_temp_filepath(program_name, table):
    return API_PARAMS['TEMP_PATH'] + '/' + get_jsonl_filename(program_name, table)


def get_table_id(program_name, table):
    return generate_long_name(program_name, table)


def upload_to_bucket(file):
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(BQ_PARAMS['WORKING_BUCKET'])
        blob = bucket.blob(BQ_PARAMS['WORKING_BUCKET_DIR'] + '/' + file)
        blob.upload_from_filename(API_PARAMS["TEMP_PATH"] + '/' + file)
    except Exception as err:
        has_fatal_error("Failed to upload to bucket.\n{}".format(err))


def get_required_columns(table_key):
    required_columns = list()

    table_id_key = get_table_id_key(table_key)

    required_columns.append(get_bq_name(table_key + '.' + table_id_key))

    return required_columns


def get_table_id_key(table_key):
    if not API_PARAMS['FIELD_GROUP_METADATA']:
        has_fatal_error("params['FIELD_GROUP_METADATA'] not found")
    if 'table_id_key' not in API_PARAMS['FIELD_GROUP_METADATA'][table_key]:
        has_fatal_error("table_id_key not found in API_PARAMS['FIELD_GROUP_METADATA']['{}']".format(table_key))
    return API_PARAMS['FIELD_GROUP_METADATA'][table_key]['table_id_key']


def get_excluded_fields(table_key, fatal=False, flattened=False):
    if not API_PARAMS['FIELD_GROUP_METADATA']:
        has_fatal_error("params['FIELD_GROUP_METADATA'] not found")

    if 'excluded_fields' not in API_PARAMS['FIELD_GROUP_METADATA'][table_key]:
        if fatal:
            has_fatal_error("excluded_fields not found in API_PARAMS['FIELD_GROUP_METADATA']['{}']".format(
                table_key))
        else:
            return None

    base_column_names = API_PARAMS['FIELD_GROUP_METADATA'][table_key]['excluded_fields']

    if flattened:
        return set(get_bq_name(table_key + '.' + column) for column in base_column_names)
    else:
        return base_column_names


def get_id_column_position(table_key, column_order_dict):
    table_id_key = get_table_id_key(table_key)
    id_column = get_bq_name(table_key + '.' + table_id_key)
    return column_order_dict[id_column]


def generate_table_ids(program_name, record_counts):
    table_keys = get_tables(record_counts)

    table_ids = dict()
    program_name = "_".join(program_name.split('.'))
    base_table_name = [BQ_PARAMS["GDC_RELEASE"], 'clin', program_name]

    for table in table_keys:
        # eliminate '.' char from program name if found (which would otherwise create illegal table_id)
        split_table_path = table.split(".")
        table_name = "_".join(base_table_name)

        if len(split_table_path) > 1:
            table_suffix = "__".join(split_table_path[1:])
            table_name = table_name + '_' + table_suffix

        if not table_name:
            has_fatal_error("generate_table_name returns empty result.")

        table_id = BQ_PARAMS["WORKING_PROJECT"] + '.' + BQ_PARAMS["TARGET_DATASET"] + '.' + table_name

        table_ids[table] = table_id

    return table_ids


def add_reference_columns(table_columns, schema_dict, column_order_dict):
    def generate_id_schema_entry(column_name, parent_table_key_):
        parent_field_name = get_field_name(parent_table_key_)

        if parent_table_key_ in table_columns.keys():
            ancestor_table = '*_{}'.format(parent_field_name)
        else:
            ancestor_table = 'main'

        if '__' in column_name:
            ancestor_column_name = parent_field_name + '__' + get_field_name(column_name)
        else:
            ancestor_column_name = column_name

        description = "Reference to the parent_id ({}) of the record to which this record belongs. " \
                      "Parent record found in the program's {} table.".format(ancestor_column_name,
                                                                              ancestor_table)

        return {"name": ancestor_column_name, "type": 'STRING', "description": description}

    def generate_record_count_schema_entry(record_count_id_key_, parent_table_key_):
        description = "Total count of records associated with this case, located in {} table".format(parent_table_key_)
        return {"name": record_count_id_key_, "type": 'INTEGER', "description": description}

    for table_key in table_columns.keys():
        table_depth = len(table_key.split('.'))

        id_column_position = get_id_column_position(table_key, column_order_dict)
        reference_col_position = id_column_position + 1

        if table_depth == 1:
            # base table references inserted while processing child tables, so skip
            continue
        elif table_depth > 2:
            # if the > 2 cond. is removed (and the case_id insertion below) tables will only reference direct ancestor
            # tables with depth > 2 have case_id reference and parent_id reference
            parent_fg = get_parent_field_group(table_key)
            parent_id_key = get_table_id_key(parent_fg)
            parent_id_column = get_bq_name(table_key + '.' + parent_id_key)

            # add parent_id to one-to-many table
            schema_dict[parent_id_column] = generate_id_schema_entry(parent_id_column, parent_fg)
            table_columns[table_key].add(parent_id_column)
            column_order_dict[parent_id_column] = reference_col_position

            reference_col_position += 1

        case_id_key = 'case_id'
        case_id_column = get_bq_name(table_key + '.' + case_id_key)

        # add case_id to one-to-many table
        schema_dict[case_id_column] = generate_id_schema_entry(case_id_key, 'main')
        table_columns[table_key].add(case_id_key)
        column_order_dict[case_id_column] = reference_col_position

        reference_col_position += 1

        parent_table_key = get_parent_table(table_columns.keys(), table_key)
        parent_id_column_position = get_id_column_position(parent_table_key, column_order_dict)
        count_columns_position = parent_id_column_position + len(API_PARAMS['FIELD_GROUP_ORDER'])
        count_id_key = get_bq_name(table_key + '.count')

        # add one-to-many record count column to parent table
        schema_dict[count_id_key] = generate_record_count_schema_entry(count_id_key, parent_table_key)
        table_columns[parent_table_key].add(count_id_key)
        column_order_dict[count_id_key] = count_columns_position

    return schema_dict, table_columns, column_order_dict


def create_schemas(table_columns, schema_dict, column_order_dict):
    table_schema_fields = dict()

    # modify schema dict, add reference columns for this program
    schema_dict, table_columns, column_order_dict = add_reference_columns(table_columns, schema_dict,
                                                                          column_order_dict)
    for table_key in table_columns:
        table_order_dict = dict()

        for column in table_columns[table_key]:
            count_column_position = get_count_column_position(table_key, column_order_dict)
            # don't rename if this is a parent_id column
            if '__' in column:
                column_name = column
            else:
                column_name = get_bq_name(table_key + '.' + column)

            if not column_name or column_name not in column_order_dict:
                has_fatal_error("'{}' not in column_order_dict!".format(column_name))

            table_order_dict[column_name] = column_order_dict[column_name]

            count_columns = []

            for key, value in table_order_dict.items():
                if value == count_column_position:
                    count_columns.append(key)

            # index in alpha order
            count_columns.sort()

            for count_column in count_columns:
                table_order_dict[count_column] = count_column_position
                count_column_position += 1

        required_columns = get_required_columns(table_key)
        schema_list = []

        for schema_key, val in sorted(table_order_dict.items(), key=lambda item: item[1]):
            schema_list.append(
                bigquery.SchemaField(
                    name=schema_dict[schema_key]['name'],
                    field_type=schema_dict[schema_key]['type'],
                    mode='REQUIRED' if schema_key in required_columns else 'NULLABLE',
                    description=schema_dict[schema_key]['description'],
                    fields=()
                )
            )

        table_schema_fields[table_key] = schema_list

    return table_schema_fields


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


def flatten_case(case, prefix, flattened_case_dict, table_keys, case_id=None, parent_id=None,
                 parent_id_key=None):
    if isinstance(case, list):
        entry_list = []

        for entry in case:
            entry_dict = dict()

            if case_id != parent_id:
                entry_dict['case_id'] = case_id
                entry_dict[parent_id_key] = parent_id
            else:
                entry_dict['case_id'] = case_id

            entry_id_key = get_table_id_key(prefix)

            for key in entry:
                if isinstance(entry[key], list):
                    # note -- If you're here because you've added a new doubly-nested field group,
                    # this is where you'll want to capture the parent field group's id.
                    new_parent_id_key = get_bq_name(prefix + '.' + entry_id_key)
                    new_parent_id = entry[entry_id_key]

                    flattened_case_dict = flatten_case(entry[key], prefix + '.' + key, flattened_case_dict,
                                                       table_keys, case_id, new_parent_id, new_parent_id_key)
                else:
                    col_name = get_bq_name(prefix + '.' + key)

                    entry_dict[col_name] = entry[key]

            entry_dict = remove_unwanted_fields(entry_dict, prefix)
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

        parent_id = case['case_id']
        case_id = case['case_id']
        parent_id_key = 'case_id'

        for key in case:
            if isinstance(case[key], list):
                flattened_case_dict = flatten_case(case[key], prefix + '.' + key, flattened_case_dict,
                                                   table_keys, case_id, parent_id, parent_id_key)
            else:
                col_name = get_bq_name(prefix + '.' + key)
                entry_dict[col_name] = case[key]

        if entry_dict:
            entry_dict = remove_unwanted_fields(entry_dict, prefix)
            entry_list.append(entry_dict)
            flattened_case_dict[prefix] = entry_list
    return flattened_case_dict


def merge_single_entry_field_groups(flattened_case_dict, table_keys):
    field_group_counts = dict.fromkeys(flattened_case_dict.keys(), 0)

    # sort field group keys by depth
    for fg_key in field_group_counts:
        field_group_counts[fg_key] = len(fg_key.split("."))

    for field_group_key, fg_depth in sorted(field_group_counts.items(), key=lambda item: item[1], reverse=True):
        field_group = flattened_case_dict[field_group_key].copy()
        # skip merge for cases
        if field_group_key == 'cases':
            continue

        parent_table_key = get_parent_table(table_keys, field_group_key)
        parent_id_key = get_table_id_key(parent_table_key)
        bq_parent_id_column = get_bq_name(parent_table_key + '.' + parent_id_key)

        if field_group_key in table_keys:
            record_count_dict = dict()

            cnt = 0
            for entry in flattened_case_dict[parent_table_key].copy():
                entry_id = entry[bq_parent_id_column]
                record_count_dict[entry_id] = dict()
                record_count_dict[entry_id]['entry_idx'] = cnt
                record_count_dict[entry_id]['record_count'] = 0
                cnt += 1

            for record in field_group:
                if bq_parent_id_column not in record:
                    print("no parent_id_key {} in record.".format(parent_id_key))
                    continue

                parent_id = record[bq_parent_id_column]

                record_count_dict[parent_id]['record_count'] += 1

            for parent_id in record_count_dict:
                entry_idx = record_count_dict[parent_id]['entry_idx']
                record_count_key = get_bq_name(field_group_key + '.count')
                record_count = record_count_dict[parent_id]['record_count']

                flattened_case_dict[parent_table_key][entry_idx][record_count_key] = record_count
        else:
            field_group = flattened_case_dict.pop(field_group_key)

            if len(field_group) > 1:
                has_fatal_error("length of record > 1, but this is supposed to be a flattened field group.")

            field_group = field_group[0]

            if 'case_id' in field_group:
                field_group.pop('case_id')

            # include keys with values
            for key in field_group.keys():
                if field_group[key]:
                    flattened_case_dict[parent_table_key][0][key] = field_group[key]

    return flattened_case_dict


def create_and_load_tables(program_name, cases, table_schemas):
    table_ids = set()
    print("Inserting case records... ")
    table_keys = table_schemas.keys()

    for table in table_keys:
        fp = get_temp_filepath(program_name, table)
        if os.path.exists(fp):
            os.remove(fp)

    for case in cases:
        flattened_case_dict = flatten_case(case, 'cases', dict(), table_keys, case['case_id'],
                                           case['case_id'])
        flattened_case_dict = merge_single_entry_field_groups(flattened_case_dict, table_keys)

        for table in flattened_case_dict.keys():
            if table not in table_keys:
                has_fatal_error("Table {} not found in table keys".format(table))

            jsonl_fp = get_temp_filepath(program_name, table)

            with open(jsonl_fp, 'a') as jsonl_file:
                for row in flattened_case_dict[table]:
                    json.dump(obj=row, fp=jsonl_file)
                    jsonl_file.write('\n')

    for table in table_schemas:
        jsonl_file = get_jsonl_filename(program_name, table)

        upload_to_bucket(jsonl_file)

        table_id = get_table_id(program_name, table)

        create_and_load_table(BQ_PARAMS, jsonl_file, table_schemas[table], table_id)

        table_ids.add(table_id)

    return table_ids


def test_table_output():

    table_ids = get_dataset_table_list()

    program_names = get_programs_list()

    program_names.remove('CCLE')

    program_table_lists = dict()

    for program_name in program_names:
        print("\nFor program {}:".format(program_name))

        main_table_id = get_table_id(program_name, 'cases')
        program_table_lists[main_table_id] = []

        for table in table_ids:
            if main_table_id in table and main_table_id != table:
                program_table_lists[main_table_id].append(table)

        if not program_table_lists[main_table_id]:
            print("... no one-to-many tables")
            continue

        table_fg_list = ['cases']

        for table in program_table_lists[main_table_id]:
            table_fg_list.append(convert_bq_table_id_to_fg(table))

        program_table_query_max_counts = dict()

        for table in program_table_lists[main_table_id]:
            table_fg = convert_bq_table_id_to_fg(table)
            table_id_key = get_table_id_key(table_fg)
            table_field = get_field_name(table_fg)

            parent_table_fg = get_parent_table(table_fg_list, table_fg)
            parent_id_key = get_table_id_key(parent_table_fg)

            full_parent_id_key = get_bq_name(parent_table_fg + '.' + parent_id_key)

            record_count_list = get_record_count_list(table,
                                                      table_fg, full_parent_id_key)

            max_count, max_count_id = get_max_count(record_count_list)

            parent_fg = get_parent_field_group(table_fg)
            parent_fg_id_key = get_table_id_key(parent_fg)
            parent_fg_field = get_field_name(parent_fg)

            mt_case_id, mt_child_id, mt_max_count = get_main_table_count(
                program_name, table_id_key, table_field, parent_fg_id_key, parent_fg_field)

            if max_count != mt_max_count:
                has_fatal_error("NOT A MATCH for {}. {} != {}".format(table_fg, max_count, mt_max_count))

            program_table_query_max_counts[table_fg] = max_count

        cases = get_cases_by_program(BQ_PARAMS, program_name)

        table_columns, record_counts = retrieve_program_case_structure(program_name, cases)

        cases_tally_max_counts = dict()

        for key in record_counts:
            count = record_counts[key]

            if count > 1:
                cases_tally_max_counts[key] = count

        for key in cases_tally_max_counts:
            if key not in program_table_query_max_counts:
                has_fatal_error("No match found for {} in program_table_query_max_counts: {}".format(
                    key, program_table_query_max_counts))
            elif cases_tally_max_counts[key] != program_table_query_max_counts[key]:
                has_fatal_error("NOT A MATCH for {}. {} != {}".format(
                    key, cases_tally_max_counts[key], program_table_query_max_counts[key]))
        print("Counts all match! Moving on.")


def get_main_table_count(program_name, table_id_key, field_name,
                         parent_table_id_key=None, parent_field_name=None):

    table_path = BQ_PARAMS['WORKING_PROJECT'] + '.' + BQ_PARAMS['TARGET_DATASET'] + '.' \
                 + BQ_PARAMS['GDC_RELEASE'] + '_clinical_data'
    program_table_path = BQ_PARAMS['WORKING_PROJECT'] + '.' + BQ_PARAMS['PROGRAM_ID_TABLE']
    if not parent_table_id_key or not parent_field_name or parent_table_id_key == 'case_id':
        query = """
            SELECT case_id, count(p.{}) as cnt
            FROM `{}`,
            UNNEST({}) as p
            WHERE case_id in (
            SELECT case_gdc_id 
            FROM `{}` 
            WHERE program_name = '{}'
            )
            GROUP BY case_id
            ORDER BY cnt DESC
            LIMIT 1
        """.format(table_id_key,
                   table_path,
                   field_name,
                   program_table_path,
                   program_name)

        results = get_query_results(query)

        for result in results:
            res = result.values()
            return res[0], None, res[1]

    else:
        query = """
            SELECT case_id, p.{}, count(pc.{}) as cnt
            FROM `{}`,
            UNNEST({}) as p,
            UNNEST(p.{}) as pc
            WHERE case_id in (
            SELECT case_gdc_id 
            FROM `{}` 
            WHERE program_name = '{}'
            )
            GROUP BY {}, case_id
            ORDER BY cnt DESC
            LIMIT 1
        """.format(parent_table_id_key,
                   table_id_key,
                   table_path,
                   parent_field_name,
                   field_name,
                   program_table_path,
                   program_name,
                   parent_table_id_key)

        results = get_query_results(query)

        for result in results:
            res = result.values()
            return res[0], res[1], res[2]


def get_record_count_list(table, table_fg_key, parent_table_id_key):
    dataset_path = BQ_PARAMS["WORKING_PROJECT"] + '.' + BQ_PARAMS["TARGET_DATASET"]
    table_id_key = get_table_id_key(table_fg_key)

    table_id_column = get_bq_name(table_fg_key + '.' + table_id_key)

    results = get_query_results(
        """
        SELECT distinct({}), count({}) as record_count 
        FROM `{}.{}` 
        GROUP BY {}
        """.format(parent_table_id_key, table_id_column, dataset_path, table, parent_table_id_key)
    )

    record_count_list = []

    for result in results:
        result_tuple = result.values()

        record_count = result_tuple[1]
        count_label = 'record_count'

        record_count_list.append({
            parent_table_id_key: parent_table_id_key,
            'table': table,
            count_label: record_count
        })

    return record_count_list


##
#  Functions for creating documentation
##
def generate_documentation(program_name, documentation_dict, record_counts):
    print("Inserting documentation... ")
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

    docs_filename = API_PARAMS['DOCS_OUTPUT_FILE']
    with open(docs_filename, 'a') as doc_file:
        doc_file.write("{} \n".format(program_name))
        doc_file.write("{}".format(documentation_dict))
        doc_file.write("{}".format(record_counts))

    print("... DONE.")

    upload_to_bucket(docs_filename)


def main(args):
    start = time.time()
    # fg_name_types: (cases.diagnoses.annotations): tables_dict, record_counts keys, insert_lists
    # bq_name_types: (diagnoses__annotations__case_id): schema_dict, column_order_dict keys, flattened_case_dict

    """
    if len(args) != 3:
        has_fatal_error('Usage : {} <configuration_yaml> <column_order_txt>".format(args[0])', ValueError)

    with open(args[1], mode='r') as yaml_file:
        try:
            params = load_config(yaml_file, YAML_HEADERS)
        except ValueError as e:
            has_fatal_error(str(e), ValueError)

    # programs_table_id = BQ_PARAMS['WORKING_PROJECT'] + '.' + BQ_PARAMS['PROGRAM_ID_TABLE']
    """
    
    global API_PARAMS

    API_PARAMS = {
        'ENDPOINT': 'https://api.gdc.cancer.gov/cases',
        "DOCS_OUTPUT_FILE": 'docs/documentation.txt',
        "EXPAND_FIELD_GROUPS": 'demographic,diagnoses,diagnoses.treatments,diagnoses.annotations,exposures,'
                               'family_histories,follow_ups,follow_ups.molecular_tests',
        "FIELD_GROUP_METADATA": {
            'cases': {
                'table_id_key': 'case_id',
                'excluded_fields': ["aliquot_ids", "analyte_ids", "case_autocomplete", "diagnosis_ids", "id",
                                    "portion_ids", "sample_ids", "slide_ids", "submitter_aliquot_ids",
                                    "submitter_analyte_ids", "submitter_diagnosis_ids", "submitter_portion_ids",
                                    "submitter_sample_ids", "submitter_slide_ids"],
                'column_order': ['submitter_id', 'case_id', 'primary_site', 'disease_type', 'index_date',
                                 'days_to_index', 'consent_type', 'days_to_consent', 'lost_to_followup',
                                 'days_to_lost_to_followup', 'state', 'created_datetime', 'updated_datetime']
            },
            'cases.demographic': {
                'table_id_key': 'demographic_id',
                'excluded_fields': ["submitter_id"],
                'column_order': ['demographic_id', 'gender', 'race', 'ethnicity', 'country_of_residence_at_enrollment',
                                 'vital_status', 'premature_at_birth', 'weeks_gestation_at_birth', 'days_to_birth',
                                 'year_of_birth', 'age_is_obfuscated', 'age_at_index', 'year_of_death', 'days_to_death',
                                 'cause_of_death', 'cause_of_death_source', 'occupation_duration_years', 'state',
                                 'created_datetime', 'updated_datetime']
            },
            'cases.diagnoses': {
                'table_id_key': 'diagnosis_id',
                'excluded_fields': ["submitter_id"],
                'column_order': ['diagnosis_id', 'ajcc_clinical_n', 'masaoka_stage', 'greatest_tumor_dimension',
                                 'percent_tumor_invasion', 'mitosis_karyorrhexis_index', 'ajcc_clinical_m',
                                 'anaplasia_present', 'primary_diagnosis', 'primary_gleason_grade',
                                 'days_to_last_known_disease_status', 'gross_tumor_weight', 'year_of_diagnosis',
                                 'best_overall_response', 'international_prognostic_index',
                                 'perineural_invasion_present', 'margins_involved_site',
                                 'peripancreatic_lymph_nodes_tested', 'weiss_assessment_score',
                                 'inpc_histologic_group', 'micropapillary_features', 'transglottic_extension',
                                 'figo_stage', 'days_to_diagnosis', 'progression_or_recurrence', 'ajcc_pathologic_m',
                                 'inrg_stage', 'days_to_recurrence', 'inss_stage', 'metastasis_at_diagnosis',
                                 'ovarian_specimen_status', 'cog_rhabdomyosarcoma_risk_group',
                                 'gastric_esophageal_junction_involvement', 'site_of_resection_or_biopsy',
                                 'ajcc_staging_system_edition', 'icd_10_code', 'laterality', 'gleason_grade_group',
                                 'age_at_diagnosis', 'peritoneal_fluid_cytological_status', 'ajcc_clinical_t',
                                 'days_to_last_follow_up', 'anaplasia_present_type', 'enneking_msts_tumor_site',
                                 'breslow_thickness', 'lymph_nodes_tested', 'goblet_cells_columnar_mucosa_present',
                                 'metastasis_at_diagnosis_site', 'supratentorial_localization', 'ajcc_pathologic_stage',
                                 'non_nodal_tumor_deposits', 'esophageal_columnar_metaplasia_present', 'tumor_grade',
                                 'lymph_nodes_positive', 'tumor_largest_dimension_diameter',
                                 'last_known_disease_status', 'non_nodal_regional_disease', 'pregnant_at_diagnosis',
                                 'irs_group', 'ann_arbor_extranodal_involvement', 'days_to_best_overall_response',
                                 'papillary_renal_cell_type', 'burkitt_lymphoma_clinical_variant', 'residual_disease',
                                 'medulloblastoma_molecular_classification', 'tumor_regression_grade',
                                 'enneking_msts_grade', 'vascular_invasion_present', 'child_pugh_classification',
                                 'first_symptom_prior_to_diagnosis', 'enneking_msts_stage', 'irs_stage',
                                 'esophageal_columnar_dysplasia_degree', 'ajcc_clinical_stage', 'ishak_fibrosis_score',
                                 'secondary_gleason_grade', 'synchronous_malignancy', 'gleason_patterns_percent',
                                 'lymph_node_involved_site', 'tumor_depth', 'morphology', 'gleason_grade_tertiary',
                                 'ajcc_pathologic_t', 'igcccg_stage', 'inpc_grade',
                                 'largest_extrapelvic_peritoneal_focus', 'figo_staging_edition_year',
                                 'lymphatic_invasion_present', 'vascular_invasion_type',
                                 'wilms_tumor_histologic_subtype', 'tumor_confined_to_organ_of_origin',
                                 'ovarian_surface_involvement', 'cog_liver_stage', 'classification_of_tumor',
                                 'margin_distance', 'mitotic_count', 'cog_renal_stage', 'enneking_msts_metastasis',
                                 'ann_arbor_clinical_stage', 'ann_arbor_pathologic_stage',
                                 'circumferential_resection_margin', 'ann_arbor_b_symptoms', 'tumor_stage', 'iss_stage',
                                 'tumor_focality', 'prior_treatment', 'peripancreatic_lymph_nodes_positive',
                                 'ajcc_pathologic_n', 'method_of_diagnosis', 'cog_neuroblastoma_risk_group',
                                 'tissue_or_organ_of_origin', 'prior_malignancy', 'state', 'created_datetime',
                                 'updated_datetime']
            },
            'cases.diagnoses.annotations': {
                'table_id_key': 'annotation_id',
                'excluded_fields': ["submitter_id", "case_submitter_id", "entity_submitter_id"],
                'column_order': ['annotation_id', 'entity_id', 'creator', 'entity_type', 'category', 'classification',
                                 'notes', 'status', 'state', 'created_datetime', 'updated_datetime',
                                 'legacy_created_datetime', 'legacy_updated_datetime']
            },
            'cases.diagnoses.treatments': {
                'table_id_key': 'treatment_id',
                'excluded_fields': ["submitter_id"],
                'column_order': ['treatment_id', 'days_to_treatment_start', 'number_of_cycles', 'treatment_outcome',
                                 'reason_treatment_ended', 'chemo_concurrent_to_radiation', 'treatment_arm',
                                 'treatment_type', 'treatment_effect', 'treatment_anatomic_site',
                                 'treatment_or_therapy', 'treatment_effect_indicator', 'treatment_dose_units',
                                 'treatment_dose', 'therapeutic_agents', 'initial_disease_status',
                                 'days_to_treatment_end', 'treatment_frequency', 'regimen_or_line_of_therapy',
                                 'treatment_intent_type', 'state', 'created_datetime', 'updated_datetime']
            },
            'cases.exposures': {
                'table_id_key': 'exposure_id',
                'excluded_fields': ["submitter_id"],
                'column_order': ['exposure_id', 'height', 'weight', 'bmi', 'age_at_onset', 'tobacco_use_per_day',
                                 'type_of_tobacco_used', 'smoking_frequency', 'marijuana_use_per_week',
                                 'tobacco_smoking_status', 'tobacco_smoking_onset_year', 'tobacco_smoking_quit_year',
                                 'years_smoked', 'pack_years_smoked', 'cigarettes_per_day',
                                 'time_between_waking_and_first_smoke', 'secondhand_smoke_as_child', 'exposure_type',
                                 'exposure_duration', 'asbestos_exposure', 'coal_dust_exposure',
                                 'environmental_tobacco_smoke_exposure', 'radon_exposure',
                                 'respirable_crystalline_silica_exposure', 'type_of_smoke_exposure', 'alcohol_history',
                                 'alcohol_intensity', 'alcohol_drinks_per_day', 'alcohol_days_per_week', 'state',
                                 'created_datetime', 'updated_datetime']
            },
            'cases.family_histories': {
                'table_id_key': 'family_history_id',
                'excluded_fields': ["submitter_id"],
                'column_order': ['family_history_id', 'relatives_with_cancer_history_count',
                                 'relative_with_cancer_history', 'relationship_primary_diagnosis', 'relationship_type',
                                 'relationship_age_at_diagnosis', 'relationship_gender', 'state', 'created_datetime',
                                 'updated_datetime']
            },
            'cases.follow_ups': {
                'table_id_key': 'follow_up_id',
                'excluded_fields': ["submitter_id"],
                'column_order': ['follow_up_id', 'days_to_follow_up', 'days_to_progression_free', 'height', 'weight',
                                 'bmi', 'progression_or_recurrence_type', 'evidence_of_recurrence_type',
                                 'days_to_progression', 'comorbidity', 'days_to_comorbidity', 'hysterectomy_type',
                                 'menopause_status', 'hormonal_contraceptive_use', 'dlco_ref_predictive_percent',
                                 'fev1_fvc_pre_bronch_percent', 'fev1_ref_pre_bronch_percent',
                                 'diabetes_treatment_type', 'hiv_viral_load', 'aids_risk_factors',
                                 'barretts_esophagus_goblet_cells_present', 'recist_targeted_regions_sum',
                                 'karnofsky_performance_status', 'disease_response', 'body_surface_area',
                                 'fev1_ref_post_bronch_percent', 'viral_hepatitis_serologies', 'adverse_event_grade',
                                 'comorbidity_method_of_diagnosis', 'risk_factor_treatment', 'scan_tracer_used',
                                 'hysterectomy_margins_involved', 'pregnancy_outcome', 'cdc_hiv_risk_factors',
                                 'reflux_treatment_type', 'fev1_fvc_post_bronch_percent', 'hpv_positive_type',
                                 'ecog_performance_status', 'cd4_count', 'progression_or_recurrence',
                                 'progression_or_recurrence_anatomic_site', 'recist_targeted_regions_number',
                                 'pancreatitis_onset_year', 'risk_factor', 'haart_treatment_indicator', 'adverse_event',
                                 'imaging_type', 'imaging_result', 'days_to_imaging',
                                 'hepatitis_sustained_virological_response', 'immunosuppressive_treatment_type',
                                 'days_to_recurrence', 'cause_of_response', 'nadir_cd4_count', 'days_to_adverse_event',
                                 'state', 'created_datetime', 'updated_datetime']
            },
            'cases.follow_ups.molecular_tests': {
                'table_id_key': 'molecular_test_id',
                'excluded_fields': ["submitter_id"],
                'column_order': ['molecular_test_id', 'biospecimen_type', 'variant_type', 'variant_origin',
                                 'laboratory_test', 'specialized_molecular_test', 'test_analyte_type', 'test_result',
                                 'transcript', 'test_units', 'pathogenicity', 'aa_change',
                                 'blood_test_normal_range_upper', 'loci_count', 'antigen', 'exon', 'second_exon',
                                 'loci_abnormal_count', 'zygosity', 'test_value', 'clonality', 'molecular_consequence',
                                 'molecular_analysis_method', 'gene_symbol', 'second_gene_symbol', 'chromosome',
                                 'locus', 'copy_number', 'mismatch_repair_mutation', 'blood_test_normal_range_lower',
                                 'ploidy', 'cell_count', 'histone_family', 'histone_variant', 'intron', 'cytoband',
                                 'state', 'created_datetime', 'updated_datetime']
            },
        },
        "FIELD_GROUP_ORDER": [
            'cases',
            'cases.demographic',
            'cases.diagnoses',
            'cases.diagnoses.treatments',
            'cases.diagnoses.annotations',
            'cases.exposures',
            'cases.family_histories',
            'cases.follow_ups',
            'cases.follow_ups.molecular_tests'
        ],
        "TEMP_PATH": 'temp',
        "TEST_MODE": False
    }
    
    global BQ_PARAMS
    
    BQ_PARAMS = {
        "GDC_RELEASE": 'rel23',
        "WORKING_PROJECT": 'isb-project-zero',
        "TARGET_DATASET": 'GDC_Clinical_Data',
        "PROGRAM_ID_TABLE": 'GDC_metadata.rel23_caseData',
        "BQ_AS_BATCH": False,
        'WORKING_BUCKET': 'next-gen-etl-scratch',
        'WORKING_BUCKET_DIR': 'law'
    }

    if API_PARAMS['TEST_MODE']:
        test_table_output()
        exit()

    program_names = get_programs_list()
    # program_names = ['VAREPOP']

    column_order_dict = build_column_order_dict()

    with open(API_PARAMS['DOCS_OUTPUT_FILE'], 'w') as doc_file:
        doc_file.write("New BQ Documentation")

    schema_dict = create_schema_dict()

    for program_name in program_names:
        print("\n*** Running script for program {} ***".format(program_name))
        cases = get_cases_by_program(BQ_PARAMS, program_name)

        if not cases:
            print("No case records found for {}, skipping.".format(program_name))
            continue

        table_columns, record_counts = retrieve_program_case_structure(program_name, cases)

        table_schemas = create_schemas(table_columns, schema_dict, column_order_dict.copy())

        table_ids = create_and_load_tables(program_name, cases, table_schemas)

        # generate_documentation(program_name, documentation_dict, record_counts)

    end = time.time()

    total = end - start

    print("Program completed in {} seconds".format(total))


if __name__ == '__main__':
    main(sys.argv)
