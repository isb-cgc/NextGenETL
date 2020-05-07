from common_etl.utils import create_mapping_dict, get_query_results, has_fatal_error, load_config
from google.cloud import bigquery
from google.api_core import exceptions
import sys
import math

YAML_HEADERS = 'params'
COLUMN_ORDER_DICT = None


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
    return cases


"""
def strip_null_fields(case):
    def strip_null_fields_recursive(sub_case):
        for key in sub_case.copy():
            if not sub_case[key]:
                sub_case.pop(key)

            elif isinstance(sub_case[key], list):
                new_sub_case = []
                for entry in sub_case[key].copy():
                    new_sub_case.append(strip_null_fields_recursive(entry))
                sub_case[key] = new_sub_case

        return sub_case

    return strip_null_fields_recursive(case)
"""


##
#  Functions for creating the BQ table schema dictionary
##
def retrieve_program_case_structure(program_name, cases, params):
    def build_case_structure(tables_, case_, record_counts_, parent_path):
        """
        Recursive function for retrieve_program_data, finds nested fields
        """
        if parent_path not in tables_:
            tables_[parent_path] = set()

        for field_key in case_:
            if not case_[field_key]:
                continue

            if not isinstance(case_[field_key], list) and not isinstance(case_[field_key], dict):
                if parent_path not in record_counts_:
                    record_counts_[parent_path] = 1

                tables_[parent_path].add(field_key)
                continue

            # at this point, the field_key references a dict or list
            nested_path = parent_path + '.' + field_key

            if nested_path not in record_counts_:
                record_counts_[nested_path] = 1

            if isinstance(case_[field_key], dict):
                # is this actually hit? I don't think so
                tables_, record_counts_ = build_case_structure(tables_, case_[field_key], record_counts_, nested_path)
            else:
                record_counts_[nested_path] = max(record_counts_[nested_path], len(case_[field_key]))

                for field_group_entry in case_[field_key]:
                    tables_, record_counts_ = build_case_structure(tables_, field_group_entry, record_counts_,
                                                                   nested_path)

        return tables_, record_counts_

    tables = {}
    record_counts = {}

    null_stripped_cases = []

    for case in cases:
        # case = strip_null_fields(case)
        # null_stripped_cases.append(case)

        tables, record_counts = build_case_structure(tables, case, record_counts, parent_path='cases')
    print('pre flatten_tables')
    print(tables)
    tables = flatten_tables(tables, record_counts, params)

    if not tables:
        has_fatal_error("[ERROR] no case structure returned for program {}".format(program_name))

    return tables, record_counts, null_stripped_cases


def remove_unwanted_fields(record, table_name, params):
    excluded_fields = params["EXCLUDED_FIELDS"][table_name]

    if isinstance(record, dict):
        for field in record.copy():
            if field in excluded_fields or not record[field]:
                record.pop(field)
    elif isinstance(record, set):
        print("From table {}, removed:".format(table_name), end=' ')
        excluded_fields_list = []
        for field in record.copy():
            if field in excluded_fields:
                excluded_fields_list.append(field)
                record.remove(field)
        print(", ".join(excluded_fields_list))
    else:
        print("Wrong type of data structure for remove_unwanted_fields")

    return record


def flatten_tables(tables, record_counts, params):
    print("flatten_tables")
    print(tables)
    """
    Used by retrieve_program_case_structure
    """
    # record_counts uses fg naming convention
    field_group_keys = dict.fromkeys(record_counts.keys(), 0)

    # sort field group keys by depth
    for fg_key in field_group_keys:
        field_group_keys[fg_key] = len(fg_key.split("."))

    for field_group, depth in sorted(field_group_keys.items(), key=lambda item: item[1], reverse=True):
        tables[field_group] = remove_unwanted_fields(tables[field_group], field_group, params)
        print(tables)

        if depth == 1:
            break
        # this fg represents a one-to-many table grouping
        if record_counts[field_group] > 1:
            continue

        split_field_group = field_group.split('.')

        parent_fg_name = split_field_group[-1]

        for field in tables[field_group]:
            prefix = ''
            column_name = parent_fg_name + "__" + field
            parent_key = None

            for i in range(len(split_field_group) - 1, 0, -1):
                parent_key = '.'.join(split_field_group[:i])

                if parent_key not in tables:
                    prefix += split_field_group[i] + '__'

            if not parent_key:
                has_fatal_error("Cases should be the default parent key for any column without another table.")
            else:
                tables[parent_key].add(prefix + column_name)

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
    if not column:
        return None
    elif '.' in column:
        split_name = column.split('.')
        if split_name[0] == 'cases':
            return '__'.join(split_name[1:])
    else:
        return column


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
            count += 1

    return column_dict


def generate_table_name_and_id(params, program_name, table):
    split_table_path = table.split(".")
    # eliminate '.' char from program name if found (which would otherwise create illegal table_id)
    program_name = "_".join(program_name.split('.'))
    base_table_name = [params["GDC_RELEASE"], 'clin', program_name]
    table_name = "_".join(base_table_name)

    if len(split_table_path) > 1:
        table_suffix = "__".join(split_table_path[1:])
        table_name = table_name + '_' + table_suffix

    if not table_name:
        has_fatal_error("generate_table_name returns empty result.")

    table_id = params["WORKING_PROJECT"] + '.' + params["TARGET_DATASET"] + '.' + table_name

    return table_name, table_id


def add_reference_columns(tables_dict, schema_dict, table_keys, table_key):
    def generate_id_schema_entry(parent_table='main', parent_field='case', column_name='case_id'):
        description = "Reference to the {} field of the {} record to which this record belongs. " \
                      "Parent record found in the program's {} table.".format(column_name, parent_field, parent_table)

        return {
            "name": column_name,
            "type": 'STRING',
            "description": description
        }

    """
    def generate_ids_schema_entry(child_table_, child_field_name):
        column_name = "_".join(child_field_name.split(" ")) + '_ids'
        description = "List of {} ids, referencing associated records located in the program's {} " \
                      "table.".format(child_field_name, child_table_)

        return {
            "name": column_name,
            "type": 'STRING',
            "description": description
        }
    """

    if table_key == 'cases.follow_ups':
        # tables_dict['cases'].add('follow_up_ids')
        # schema_dict['follow_up_ids'] = generate_ids_schema_entry('*_follow_ups', 'follow up')

        tables_dict['cases.follow_ups'].add('case_id')
        schema_dict['follow_ups__case_id'] = generate_id_schema_entry()
    elif table_key == 'cases.follow_ups.molecular_tests':
        # tables_dict['cases.follow_ups'].add('molecular_test_ids')
        # schema_dict['follow_ups__molecular_test_ids'] = generate_ids_schema_entry(
        #    '*_follow_ups__molecular_tests', 'molecular test')

        tables_dict['cases.follow_ups.molecular_tests'].add('follow_up_id')
        schema_dict['follow_ups__molecular_tests__follow_up_id'] = generate_id_schema_entry(
            '*_follow_ups', 'follow up', 'follow_up_id')

        tables_dict['cases.follow_ups.molecular_tests'].add('case_id')
        schema_dict['follow_ups__molecular_tests__case_id'] = generate_id_schema_entry()
    elif table_key == 'cases.family_histories':
        # tables_dict['cases'].add('family_history_ids')
        # schema_dict['family_history_ids'] = generate_ids_schema_entry('*_family_histories', 'family history')

        tables_dict['cases.family_histories'].add('case_id')
        schema_dict['family_histories__case_id'] = generate_id_schema_entry()
    elif table_key == 'cases.demographic':
        # tables_dict['cases'].add('demographic_ids')
        # schema_dict['demographic_ids'] = generate_ids_schema_entry('*_demographic', 'demographic')

        tables_dict['cases.demographic'].add('case_id')
        schema_dict['demographic__case_id'] = generate_id_schema_entry()
    elif table_key == 'cases.exposures':
        tables_dict['cases.exposures'].add('case_id')
        schema_dict['exposures__case_id'] = generate_id_schema_entry()

        # tables_dict['cases'].add('exposure_ids')
        # schema_dict['exposure_ids'] = generate_ids_schema_entry('*_exposures', 'exposure')
    elif table_key == 'cases.diagnoses':
        # tables_dict['cases'].add('diagnosis_ids')
        # schema_dict['diagnosis_ids'] = generate_ids_schema_entry('*_diagnoses', 'diagnosis')

        tables_dict['cases.diagnoses'].add('case_id')
        schema_dict['diagnoses__case_id'] = generate_id_schema_entry()
    elif table_key == 'cases.diagnoses.treatments':
        ancestor_table = '*_diagnoses' if 'case.diagnoses' in table_keys else 'main'
        """
        child_table = ancestor_table + '__treatments' if 'case.diagnoses' in table_keys else ancestor_table

        if 'cases.diagnoses' in table_keys:
            tables_dict['cases.diagnoses'].add('treatment_ids')
        else:
            tables_dict['cases'].add('diagnoses__treatment_ids')
        schema_dict['diagnoses__treatment_ids'] = generate_ids_schema_entry(child_table, 'treatment')
        """

        tables_dict['cases.diagnoses.treatments'].add('diagnosis_id')
        schema_dict['diagnoses__treatments__diagnosis_id'] = generate_id_schema_entry(
            ancestor_table, 'diagnosis', 'diagnosis_id')
        tables_dict['cases.diagnoses.treatments'].add('case_id')
        schema_dict['diagnoses__treatments__case_id'] = generate_id_schema_entry()
    elif table_key == 'cases.diagnoses.annotations':
        ancestor_table = '*_diagnoses' if 'case.diagnoses' in table_keys else 'main'
        """
        child_table = ancestor_table + '__annotations' if 'case.diagnoses' in table_keys else ancestor_table
        
        if 'cases.diagnoses' in table_keys:
            tables_dict['cases.diagnoses'].add('annotation_ids')
        else:
            tables_dict['cases'].add('diagnoses__annotation_ids')
        schema_dict['diagnoses__annotation_ids'] = generate_ids_schema_entry(child_table, 'annotation')
        """

        tables_dict['cases.diagnoses.annotations'].add('diagnosis_id')
        schema_dict['diagnoses__annotations__diagnosis_id'] = generate_id_schema_entry(
            ancestor_table, 'diagnosis', 'diagnosis_id')
        tables_dict['cases.diagnoses.annotations'].add('case_id')
        schema_dict['diagnoses__annotations__case_id'] = generate_id_schema_entry()

    return tables_dict, schema_dict


def create_bq_tables(program_name, params, tables_dict, record_counts):
    schema_dict = create_schema_dict(params)

    table_ids = dict()
    documentation_dict = dict()
    documentation_dict['table_schemas'] = dict()

    table_keys = get_tables(record_counts)

    schema_field_set = set()

    for table_key in table_keys:
        tables_dict, schema_dict = add_reference_columns(tables_dict, schema_dict, table_keys, table_key)
        table_order_dict = dict()
        schema_field_keys = []

        table_name, table_id = generate_table_name_and_id(params, program_name, table_key)

        table_ids[table_key] = table_id

        documentation_dict['table_schemas'][table_key] = dict()
        documentation_dict['table_schemas'][table_key]['table_id'] = table_id
        documentation_dict['table_schemas'][table_key]['table_schema'] = list()

        # lookup column position indexes in master list, used to order schema
        for column in tables_dict[table_key]:
            full_column_name = get_bq_name(table_key + '.' + column)

            try:
                table_order_dict[full_column_name] = COLUMN_ORDER_DICT[full_column_name]
            except KeyError:
                has_fatal_error('{} not in COLUMN_ORDER_DICT!'.format(full_column_name))

        for column, value in sorted(table_order_dict.items(), key=lambda x: x[1]):
            ''' 
            fg_name_types: (cases.diagnoses.annotations): tables_dict, record_counts keys 
            bq_name_types: (diagnoses__annotations__case_id): schema_dict, column_order_dict keys, flattened_case_dict
            '''
            schema_field_keys.append(column)

        schema_list = []

        if 'created_datetime' not in schema_field_keys:
            print("No created datetime in create_bq tables")

        for schema_key in schema_field_keys:
            schema_list.append(bigquery.SchemaField(
                schema_key, schema_dict[schema_key]['type'], "NULLABLE", schema_dict[schema_key]['description'], ()))
        try:
            client = bigquery.Client()
            client.delete_table(table_id, not_found_ok=True)
            table = bigquery.Table(table_id, schema=schema_list)
            client.create_table(table)
            schema_field_set.add(table_key)
        except exceptions.BadRequest as err:
            has_fatal_error("Fatal error for table_id: {}\n{}\n{}".format(table_id, err, schema_list))

        documentation_dict['table_schemas'][table_key]['table_schema'].append(schema_list)

    return documentation_dict, table_ids


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


def flatten_case(case, prefix, case_list_dict, params, case_id=None, parent_id=None, parent_id_key=None):
    if isinstance(case, list):
        entry_list = []

        for entry in case:
            entry_dict = dict()
            if case_id != parent_id:
                entry_dict['case_id'] = case_id
                entry_dict[parent_id_key] = parent_id
            else:
                entry_dict[parent_id_key] = parent_id

            for key in entry:
                if isinstance(entry[key], list):
                    # note -- If you're here because you've added a new doubly-nested field group,
                    # this is where you'll want to capture the parent field group's id.
                    if prefix == 'cases.diagnoses':
                        new_parent_id = entry['diagnosis_id']
                        new_parent_id_key = 'diagnosis_id'
                    elif prefix == 'cases.follow_ups':
                        new_parent_id = entry['follow_up_id']
                        new_parent_id_key = 'follow_up_id'
                    else:
                        new_parent_id = parent_id
                        new_parent_id_key = parent_id_key

                    case_list_dict = flatten_case(entry[key], prefix + '.' + key, case_list_dict, params, case_id,
                                                  new_parent_id, new_parent_id_key)
                else:
                    entry_dict[key] = entry[key]

            entry_dict = remove_unwanted_fields(entry_dict, prefix, params)
            entry_list.append(entry_dict)
        if prefix in case_list_dict:
            case_list_dict[prefix] = case_list_dict[prefix] + entry_list
        else:
            if entry_list:
                case_list_dict[prefix] = entry_list
    else:
        entry_list = []
        entry_dict = dict()
        if prefix not in case_list_dict:
            case_list_dict[prefix] = []

        parent_id = case_id = case['case_id']
        parent_id_key = 'case_id'

        for key in case:
            if isinstance(case[key], list):
                case_list_dict = flatten_case(case[key], prefix + '.' + key, case_list_dict, params,
                                              case_id, parent_id, parent_id_key)
            else:
                entry_dict[key] = case[key]
        if entry_dict:
            entry_dict = remove_unwanted_fields(entry_dict, prefix, params)
            entry_list.append(entry_dict)
            case_list_dict[prefix] = entry_list
    return case_list_dict


def merge_single_entry_field_groups(flattened_case_dict, table_keys):
    for field_group_key in flattened_case_dict.copy():
        if field_group_key in table_keys:
            # this group is meant to be a one-to-many table, don't merge
            continue

        for entry in flattened_case_dict[field_group_key].copy():
            # don't need multiple case_id keys in the same table
            entry.pop('case_id')
            # avoids name collisions by specifying source field group
            prefix = "__".join(field_group_key.split(".")[1:]) + "__"

            for key in entry:
                flattened_case_dict['cases'][0][prefix + key] = entry[key]

        flattened_case_dict.pop(field_group_key)

    return flattened_case_dict


"""
def create_child_table_id_list(flattened_case_dict, parent_fg, child_fg):
    def create_id_key(field_name):
        if field_name == 'diagnoses':
            id_key = 'diagnosis_id'
        elif field_name == 'family_histories':
            id_key = 'family_history_id'
        elif field_name[-1] == 's':
            # remove pluralization from field group name to make id keys
            id_key = field_name[:-1] + '_id'
        else:
            id_key = field_name + '_id'
        return id_key

    child_table_name = parent_fg + '.' + child_fg

    parent_id_key = create_id_key(parent_fg.split(".")[-1])
    child_id_key = create_id_key(child_fg)
    child_id_list_key = child_id_key + 's'

    if child_table_name not in flattened_case_dict:
        has_fatal_error("{} does not appear to be a table. There shouldn't be a situation in which the child_fg"
                        "passed doesn't exist in the flat case dictionary.".format(child_table_name))

    while parent_fg not in flattened_case_dict:
        split_parent = parent_fg.split('.')
        parent_fg = '.'.join(split_parent[:-1])
        child_id_list_key = split_parent[-1] + '__' + child_id_list_key

        if len(split_parent) == 1 and parent_fg not in flattened_case_dict:
            return flattened_case_dict

    child_ids_list = []
    parent_id = ""

    for child_record in flattened_case_dict[child_table_name]:
        parent_id = child_record[parent_id_key]
        child_id = child_record[child_id_key]
        child_ids_list.append(child_id)

    child_ids_list.sort()

    parent_fg_entries = flattened_case_dict.pop(parent_fg)

    entry_list = []

    for entry in parent_fg_entries.copy():
        if parent_id_key in entry:
            if entry[parent_id_key] == parent_id:
                entry[child_id_list_key] = ", ".join(child_ids_list)
        entry_list.append(entry)

    flattened_case_dict[parent_fg] = entry_list

    return flattened_case_dict
"""


def insert_case_data(cases, record_counts, tables_dict, params):
    table_keys = get_tables(record_counts)

    insert_lists = dict()

    for case in cases:
        flattened_case_dict = flatten_case(case, 'cases', dict(), params)
        flattened_case_dict = merge_single_entry_field_groups(flattened_case_dict, table_keys)

        if isinstance(flattened_case_dict['cases'], dict):
            flattened_case_dict['cases'] = [flattened_case_dict['cases']]
        for table in flattened_case_dict.keys():
            if table not in insert_lists:
                insert_lists[table] = []

            split_table = table.split('.')

            if len(split_table) > 3:
                has_fatal_error("Expand field group list contains a nested field group with depth > 3.", ValueError)

            """
            parent_fg = ".".join(split_table[:-1])
            child_fg = split_table[-1]
            
            if parent_fg:
                flattened_case_dict = create_child_table_id_list(flattened_case_dict, parent_fg, child_fg)
            """

            insert_lists[table] = insert_lists[table] + flattened_case_dict[table]

    for table in insert_lists:
        table_id = tables_dict[table]

        # todo insert by batch size sys.getsizeof(insert_lists) / (1024*1024)

        table_bytes = sys.getsizeof(insert_lists[table])
        table_mb = table_bytes / (1024 * 1024)
        table_len = len(insert_lists[table])
        batch_size = params["INSERT_BATCH_SIZE"]
        pages = math.ceil(table_len / batch_size)
        page_size = table_mb / pages

        if page_size > 10:
            print("INSERT_BATCH_SIZE is too large. Batch size should be 10 mb maximum, actual: {}".format(page_size))
            new_page_size = math.floor(table_mb / 10)
            ratio = new_page_size / page_size
            batch_size = math.floor(batch_size * ratio)

        try:
            print("Inserting rows into {}".format(table_id))
            client = bigquery.Client()
            bq_table = client.get_table(table_id)

            start_idx = 0
            end_idx = batch_size

            while len(insert_lists[table]) > end_idx:
                client.insert_rows(bq_table, insert_lists[table][start_idx:end_idx])
                print("Successfully inserted records {}->{}".format(
                    start_idx, end_idx))
                start_idx = end_idx
                end_idx += batch_size

            # insert remainder
            client.insert_rows(bq_table, insert_lists[table][start_idx:])
            print("Successfully inserted last {} records\n".format(len(insert_lists[table]) - start_idx))
        except Exception as err:
            print("[ERROR] exception for table: {}, table_id: {}, row count: {}".format(table, table_id,
                                                                                        len(insert_lists[table])))
            has_fatal_error("Fatal error for table: {}\n{}".format(table, err))



"""
def ordered_print(flattened_case_dict):
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
                    field_order_dict[key] = COLUMN_ORDER_DICT[col_order_lookup_key]
                except KeyError:
                    print("ORDERED PRINT -- {} not in column order dict".format(col_order_lookup_key))
                    for k, v in sorted(COLUMN_ORDER_DICT.items(), key=lambda item: item[0]):
                        print("{}: {}".format(k, v))
                    field_order_dict[key] = 0

            for field_key, order in sorted(field_order_dict.items(), key=lambda item: item[1]):
                entry_string += "{}{}: {},\n".format(make_tabs(indent + 2), field_key, entry[field_key])
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
"""


##
#  Functions for creating documentation
##
def generate_documentation(params, program_name, documentation_dict, record_counts):
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


def main(args):
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
        'ENDPOINT': 'https://api.gdc.cancer.gov/cases',
        "DOCS_OUTPUT_FILE": 'docs/documentation.txt',
        "EXPAND_FIELD_GROUPS": 'demographic,diagnoses,diagnoses.treatments,diagnoses.annotations,exposures,'
                               'family_histories,follow_ups,follow_ups.molecular_tests',
        "GDC_RELEASE": 'rel23',
        "WORKING_PROJECT": 'isb-project-zero',
        "TARGET_DATASET": 'GDC_Clinical_Data',
        "PROGRAM_ID_TABLE": 'GDC_metadata.rel23_caseData',
        "EXCLUDE_FIELDS": 'id,aliquot_ids,analyte_ids,case_autocomplete,portion_ids,sample_ids,slide_ids,'
                          'submitter_aliquot_ids,submitter_analyte_ids,submitter_portion_ids,submitter_sample_ids,'
                          'submitter_slide_ids,diagnosis_ids,submitter_diagnosis_ids',
        "EXCLUDED_FIELDS": {
            'cases.diagnoses': {
                "submitter_id"
            },
            'cases.demographic': {
                "submitter_id"
            },
            'cases.diagnoses.treatments': {
                "submitter_id"
            },
            'cases.diagnoses.annotations': {
                "case_submitter_id",
                "entity_submitter_id",
                "submitter_id"
            },
            'cases.exposures': {
                "submitter_id"
            },
            'cases.family_histories': {
                "submitter_id"
            },
            'cases.follow_ups': {
                "submitter_id"
            },
            'cases.follow_ups.molecular_tests': {
                "submitter_id"
            },
            'cases': {
                "aliquot_ids",
                "analyte_ids",
                "case_autocomplete",
                "diagnosis_ids",
                "id",
                "portion_ids",
                "sample_ids",
                "slide_ids",
                "submitter_aliquot_ids",
                "submitter_analyte_ids",
                "submitter_diagnosis_ids",
                "submitter_portion_ids",
                "submitter_sample_ids",
                "submitter_slide_ids"
            }
        },
        # Note: broken pipe/too large at 5000
        "INSERT_BATCH_SIZE": 1000
    }

    # program_names = get_programs_list(params)
    program_names = ['TCGA', 'TARGET']
    # program_names = ['HCMI']

    global COLUMN_ORDER_DICT
    COLUMN_ORDER_DICT = import_column_order(args[2])

    with open(params['DOCS_OUTPUT_FILE'], 'w') as doc_file:
        doc_file.write("New BQ Documentation")

    for program_name in program_names:
        print("\n*** Running script for {} ***".format(program_name))
        print(" - Retrieving cases... ", end='')
        cases = get_cases_by_program(program_name, params)
        if len(cases) == 0:
            print("\nNo cases found for program {}, no tables created.".format(program_name))
            continue

        for case in cases:
            if 'created_datetime' not in case:
                print("NO DATETIME")
            break

        print("\n(Case count = {})...".format(len(cases)))

        print("DONE.\n - Determining program table structure... ")
        tables_dict, record_counts, cases = retrieve_program_case_structure(program_name, cases, params)
        print("\nrecord_counts: {} \n".format(record_counts))

        print("DONE.\n - Creating empty BQ tables... ")
        documentation_dict, table_names_dict = create_bq_tables(program_name, params, tables_dict, record_counts)
        print("\ntable_names: {} \n".format(table_names_dict))

        print("DONE.\n - Inserting case records... ")
        insert_case_data(cases, record_counts, table_names_dict, params)

        # print("DONE.\n - Inserting documentation... ", end='')
        # generate_documentation(params, program_name, documentation_dict, record_counts)
        print("DONE.\n")


if __name__ == '__main__':
    main(sys.argv)
