import math
from common_etl.utils import *
import sys
import json
import os
import time

API_PARAMS = None
BQ_PARAMS = None
YAML_HEADERS = ('api_params', 'bq_params', 'steps')


##
# Getter functions, employed for readability/consistency
##
def generate_long_name(program_name, table):
    # remove invalid char from program name
    if '.' in program_name:
        program_name = '_'.join(program_name.split('.'))

    file_name_parts = [BQ_PARAMS['GDC_RELEASE'], 'clin', program_name]

    # if one-to-many table, append suffix
    bq_table_name = get_bq_name(API_PARAMS, None, table)
    file_name_parts.append(bq_table_name) if bq_table_name else None

    return '_'.join(file_name_parts)


def get_jsonl_filename(program_name, table):
    return generate_long_name(program_name, table) + '.jsonl'


def get_temp_filepath(program_name, table):
    return API_PARAMS['TEMP_PATH'] + '/' + get_jsonl_filename(program_name, table)


def get_table_id(program_name, table):
    return generate_long_name(program_name, table)


def get_table_path(table_name):
    return BQ_PARAMS["WORKING_PROJECT"] + '.' + BQ_PARAMS["TARGET_DATASET"] + '.' + table_name


def get_required_columns(table_key):
    required_columns = list()

    table_id_key = get_table_id_key(table_key)

    required_columns.append(get_bq_name(API_PARAMS, table_key, table_id_key))

    return required_columns


def get_table_id_key(table_key):
    if not API_PARAMS['TABLE_METADATA']:
        has_fatal_error("params['TABLE_METADATA'] not found")
    if 'table_id_key' not in API_PARAMS['TABLE_METADATA'][table_key]:
        has_fatal_error("table_id_key not found in API_PARAMS['TABLE_METADATA']['{}']".format(table_key))
    return API_PARAMS['TABLE_METADATA'][table_key]['table_id_key']


def get_id_column_position(table_key, column_order_dict):
    table_id_key = get_table_id_key(table_key)
    id_column = get_bq_name(API_PARAMS, table_key, table_id_key)
    return column_order_dict[id_column]


def generate_table_paths(program_name, record_counts):
    table_keys = get_tables(record_counts)
    table_paths = dict()

    for table in table_keys:
        table_name = generate_long_name(program_name, table)
        table_paths[table] = get_table_path(table_name)

    return table_paths


##
# Functions which retrieve preliminary information used for creating table schemas / ingesting data
##
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


def build_column_order_dict():
    column_order_dict = dict()
    field_groups = API_PARAMS['TABLE_ORDER']
    max_reference_cols = len(field_groups)

    idx = 0

    for fg in field_groups:
        try:
            column_order_list = API_PARAMS['TABLE_METADATA'][fg]['column_order']
            id_column = API_PARAMS['TABLE_METADATA'][fg]['table_id_key']
            for column in column_order_list:
                bq_column = get_bq_name(API_PARAMS, fg, column)

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
            has_fatal_error("{} found in API_PARAMS['TABLE_ORDER'] "
                            "but not in API_PARAMS['TABLE_METADATA']".format(fg))

    column_order_dict['state'] = idx
    column_order_dict['created_datetime'] = idx + 1
    column_order_dict['updated_datetime'] = idx + 2

    return column_order_dict


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

    for fg in API_PARAMS['EXPAND_FIELD_GROUPS']:
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
        column_type_dict['cases.' + vals[0]] = vals[1]

    single_nested_query_dict = {
        "cases.family_histories": family_histories_query,
        "cases.demographic": demographic_query,
        "cases.exposures": exposures_query
    }

    for key in single_nested_query_dict.keys():
        results = get_query_results(single_nested_query_dict[key])

        for result in results:
            vals = result.values()
            column_type_dict = split_datatype_array(column_type_dict, vals[1], key + '.')

    results = get_query_results(follow_ups_query)

    for result in results:
        vals = result.values()
        split_vals = vals[1].split('molecular_tests ')

        column_type_dict = split_datatype_array(column_type_dict, split_vals[0] + ' ', 'cases.follow_ups.')

        column_type_dict = split_datatype_array(column_type_dict, split_vals[1][:-2], 'cases.follow_ups.molecular_tests.')

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
    column_type_dict = split_datatype_array(column_type_dict, diagnoses, 'cases.diagnoses.')
    column_type_dict = split_datatype_array(column_type_dict, treatments, 'cases.diagnoses.treatments.')
    column_type_dict = split_datatype_array(column_type_dict, annotations, 'cases.diagnoses.annotations.')

    return column_type_dict


def create_schema_dict():
    column_type_dict = lookup_column_types()
    field_mapping_dict = create_mapping_dict(API_PARAMS['ENDPOINT'])

    schema_dict = {}

    for key in column_type_dict:
        try:
            description = field_mapping_dict[key]['description']
        except KeyError:
            # cases.id not returned by mapping endpoint. In such cases, substitute an empty description string.
            description = ""

        field_type = column_type_dict[key]

        # this is the format for bq schema json object entries
        bq_key = get_bq_name(API_PARAMS, 'cases', key)

        schema_dict[key] = {
            "name": bq_key,
            "type": field_type,
            "description": description
        }

    return schema_dict


##
# Functions used to determine a program's table structure(s)
##
def get_excluded_fields(table_key, fatal=False, flattened=False):
    if not API_PARAMS['TABLE_METADATA']:
        has_fatal_error("params['TABLE_METADATA'] not found")

    if 'excluded_fields' not in API_PARAMS['TABLE_METADATA'][table_key]:
        if fatal:
            has_fatal_error("excluded_fields not found in API_PARAMS['TABLE_METADATA']['{}']".format(table_key))
        else:
            return None

    base_column_names = API_PARAMS['TABLE_METADATA'][table_key]['excluded_fields']

    if flattened:
        return set(get_bq_name(API_PARAMS, table_key, column) for column in base_column_names)
    else:
        return base_column_names


def flatten_tables(tables, record_counts):
    def remove_set_fields(record_, table_name):
        excluded_fields = get_excluded_fields(table_name, fatal=True)

        for field_ in record_.copy():
            if field_ in excluded_fields:
                record_.remove(field_)

        return record_

    # record_counts uses fg naming convention
    field_group_counts = dict.fromkeys(record_counts.keys(), 0)

    # sort field group keys by depth
    for fg_key in field_group_counts:
        field_group_counts[fg_key] = len(fg_key.split("."))

    for field_group, depth in sorted(field_group_counts.items(), key=lambda item: item[1], reverse=True):

        tables[field_group] = remove_set_fields(tables[field_group], field_group)

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
                tables[parent_key].add(get_bq_name(API_PARAMS, field_group, field))

        tables.pop(field_group)

    if len(tables.keys()) - 1 != sum(val > 1 for val in record_counts.values()):
        has_fatal_error("Flattened tables dictionary has incorrect number of keys.")
    return tables


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

    print("Record counts for each field group: {}".format(record_counts))

    return table_columns, record_counts


##
# Functions used for schema creation
##
def get_count_column_position(table_key, column_order_dict):
    table_id_key = get_table_id_key(table_key)
    bq_table_id_column_name = get_bq_name(API_PARAMS, table_key, table_id_key)
    id_column_position = column_order_dict[bq_table_id_column_name]

    count_columns_position = id_column_position + len(API_PARAMS['TABLE_ORDER'])

    return count_columns_position


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
            parent_id_column = get_bq_name(API_PARAMS, table_key, parent_id_key)

            # add parent_id to one-to-many table
            schema_dict[parent_id_column] = generate_id_schema_entry(parent_id_column, parent_fg)
            table_columns[table_key].add(parent_id_column)
            column_order_dict[parent_id_column] = reference_col_position

            reference_col_position += 1

        case_id_key = 'case_id'
        case_id_column = get_bq_name(API_PARAMS, table_key, case_id_key)

        # add case_id to one-to-many table
        schema_dict[case_id_column] = generate_id_schema_entry(case_id_key, 'main')
        table_columns[table_key].add(case_id_key)
        column_order_dict[case_id_column] = reference_col_position

        reference_col_position += 1

        parent_table_key = get_parent_table(table_columns.keys(), table_key)
        parent_id_column_position = get_id_column_position(parent_table_key, column_order_dict)
        count_columns_position = parent_id_column_position + len(API_PARAMS['TABLE_ORDER'])
        count_id_key = get_bq_name(API_PARAMS, table_key, 'count')

        # add one-to-many record count column to parent table
        schema_dict[count_id_key] = generate_record_count_schema_entry(count_id_key, parent_table_key)
        table_columns[parent_table_key].add(count_id_key)
        column_order_dict[count_id_key] = count_columns_position

    return schema_dict, table_columns, column_order_dict


def create_schemas(table_columns, schema_dict, column_order_dict):
    table_schema_fields = dict()

    # modify schema dict, add reference columns for this program
    schema_dict, table_columns, column_order_dict = \
        add_reference_columns(table_columns, schema_dict, column_order_dict)

    for table_key in table_columns:
        table_order_dict = dict()

        for column in table_columns[table_key]:
            count_column_position = get_count_column_position(table_key, column_order_dict)
            # don't rename if this is a parent_id column
            if '__' in column:
                column_name = column
            else:
                column_name = get_bq_name(API_PARAMS, table_key, column)

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
            schema_list.append(create_SchemaField(schema_dict, schema_key, required_columns))

        table_schema_fields[table_key] = schema_list

    return table_schema_fields


##
# Functions used for parsing and loading data into BQ tables
##
def flatten_case(case, prefix, flattened_case_dict, table_keys, case_id=None, parent_id=None, parent_id_key=None):
    def remove_dict_fields(record, table_name):
        excluded_fields = get_excluded_fields(table_name, fatal=True, flattened=True)

        for field in record.copy():
            if field in excluded_fields or not record[field]:
                record.pop(field)

        return record

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
                    new_parent_id_key = get_bq_name(API_PARAMS, prefix, entry_id_key)
                    new_parent_id = entry[entry_id_key]

                    flattened_case_dict = flatten_case(entry[key], prefix + '.' + key, flattened_case_dict,
                                                       table_keys, case_id, new_parent_id, new_parent_id_key)
                else:
                    col_name = get_bq_name(API_PARAMS, prefix, key)

                    entry_dict[col_name] = entry[key]

            entry_dict = remove_dict_fields(entry_dict, prefix)
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
                col_name = get_bq_name(API_PARAMS, prefix, key)
                entry_dict[col_name] = case[key]

        if entry_dict:
            entry_dict = remove_dict_fields(entry_dict, prefix)
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
        bq_parent_id_column = get_bq_name(API_PARAMS, parent_table_key, parent_id_key)

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
                record_count_key = get_bq_name(API_PARAMS, field_group_key, 'count')
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
    print("Inserting case records... ")
    table_keys = table_schemas.keys()

    for table in table_keys:
        fp = get_temp_filepath(program_name, table)
        if os.path.exists(fp):
            os.remove(fp)

    for case in cases:
        flattened_case_dict = flatten_case(case, 'cases', dict(), table_keys, case['case_id'], case['case_id'])
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
        upload_to_bucket(BQ_PARAMS, API_PARAMS['TEMP_PATH'], jsonl_file)

        table_id = get_table_id(program_name, table)
        print('\n - for table {}:'.format(table_id))
        create_and_load_table(BQ_PARAMS, jsonl_file, table_schemas[table], table_id)


##
#  Functions for creating documentation
##
def initialize_documentation():
    docs_fp = API_PARAMS['DOCS_PATH'] + '' + API_PARAMS['DOCS_FILE']

    with open(docs_fp, 'w') as doc_file:
        doc_file.write("New BQ Documentation\n")


def generate_documentation(program_name, record_counts):
    docs_fp = API_PARAMS['DOCS_PATH'] + '' + API_PARAMS['DOCS_FILE']

    with open(docs_fp, 'a') as doc_file:
        doc_file.write("{} \n".format(program_name))
        doc_file.write("{}".format(record_counts))


def finalize_documentation():
    upload_to_bucket(BQ_PARAMS, API_PARAMS['DOCS_PATH'], API_PARAMS['DOCS_FILE'])


##
# Functions used for validating inserted data
##
def get_record_count_list(table, table_fg_key, parent_table_id_key):
    table_path = get_table_path(table)
    table_id_key = get_table_id_key(table_fg_key)
    table_id_column = get_bq_name(API_PARAMS, table_fg_key, table_id_key)

    results = get_query_results(
        """
        SELECT distinct({}), count({}) as record_count 
        FROM `{}` 
        GROUP BY {}
        """.format(parent_table_id_key, table_id_column, table_path, parent_table_id_key)
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


def get_main_table_count(program_name, table_id_key, field_name, parent_table_id_key=None, parent_field_name=None):
    table_path = get_table_path(BQ_PARAMS['GDC_RELEASE' + '_clinical_data'])
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


def test_table_output():
    table_ids = get_dataset_table_list(BQ_PARAMS)

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

            full_parent_id_key = get_bq_name(API_PARAMS, parent_table_fg, parent_id_key)

            record_count_list = get_record_count_list(table, table_fg, full_parent_id_key)

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


##
# Script execution
##
def print_final_report(start, steps):
    seconds = time.time() - start
    minutes = math.floor(seconds / 60)
    seconds -= minutes * 60

    print("Script executed in {} minutes, {} seconds".format(minutes, seconds))
    print("Steps completed: ")

    if 'create_and_load_tables' in steps:
        print('\t - created tables and inserted data')
    if 'validate_data' in steps:
        print('\t - validated data (tests not considered exhaustive)')
    if 'generate_documentation' in steps:
        print('\t - generated documentation')


def main(args):
    start = time.time()

    if len(args) != 2:
        has_fatal_error('Usage : {} <configuration_yaml> <column_order_txt>".format(args[0])', ValueError)

    with open(args[1], mode='r') as yaml_file:
        try:
            global API_PARAMS, BQ_PARAMS
            API_PARAMS, BQ_PARAMS, steps = load_config(yaml_file, YAML_HEADERS)
        except ValueError as e:
            has_fatal_error(str(e), ValueError)

    # program_names = get_programs_list()
    program_names = ['HCMI']

    column_order_dict = build_column_order_dict()
    schema_dict = create_schema_dict()
    print(schema_dict)
    return

    if 'generate_documentation' in steps:
        initialize_documentation()

    for program_name in program_names:
        prog_start = time.time()
        print("\n * Executing script for program {}...".format(program_name))

        cases = get_cases_by_program(BQ_PARAMS, program_name)

        if cases:
            table_columns, record_counts = retrieve_program_case_structure(program_name, cases)

            if 'create_and_load_tables' in steps:
                table_schemas = create_schemas(table_columns, schema_dict, column_order_dict.copy())
                create_and_load_tables(program_name, cases, table_schemas)

            if 'generate_documentation' in steps:
                generate_documentation(program_name, record_counts)

        print("... processing completed for program {} in {:.2f} seconds".format(program_name, time.time() - prog_start))

    if 'generate_documentation' in steps:
        finalize_documentation()

    if 'validate_data' in steps:
        test_table_output()

    print_final_report(start, steps)


if __name__ == '__main__':
    main(sys.argv)
