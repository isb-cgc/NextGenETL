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
    print("\nRetrieving cases... ", end='')

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
    print("DONE. {} cases retrieved.\n".format(len(cases)))
    return cases


##
#  Functions for creating the BQ table schema dictionary
##
def retrieve_program_case_structure(program_name, cases, params, schema_dict):
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

    print("Determining program table structure... ")

    table_columns = {}
    record_counts = {}

    for case in cases:
        table_columns, record_counts = build_case_structure(table_columns, case, record_counts, parent_path='cases')

    table_columns = flatten_tables(table_columns, record_counts, params)

    if not table_columns:
        has_fatal_error("[ERROR] no case structure returned for program {}".format(program_name))

    print("... DONE.\n")
    print("Record counts for each field group: {}\n".format(record_counts))

    table_keys = get_tables(record_counts)

    for table_key in table_keys:
        table_columns, schema_dict = add_reference_columns(table_columns, schema_dict, table_keys, table_key, params)

    return table_columns, record_counts, schema_dict


def remove_unwanted_fields(record, table_name, params):
    excluded_fields = params["EXCLUDED_FIELDS"][table_name]

    if isinstance(record, dict):
        for field in record.copy():
            if field in excluded_fields or not record[field]:
                record.pop(field)
    elif isinstance(record, set):
        excluded_fields_list = []
        for field in record.copy():
            if field in excluded_fields:
                excluded_fields_list.append(field)
                record.remove(field)
        if not excluded_fields_list:
            print("\t- for {}: none removed".format(table_name))
        else:
            print("\t- for {}: removed {}".format(table_name, ", ".join(excluded_fields_list)))
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
        # parent_fg = '.'.join(field_group.split('.')[:-1])
        # if field_group_counts[parent_fg] == 1:

        tables[field_group] = remove_unwanted_fields(tables[field_group], field_group, params)

        if depth == 1:
            break
        # this fg represents a one-to-many table grouping
        if record_counts[field_group] > 1:
            continue

        split_field_group = field_group.split('.')

        parent_fg_name = split_field_group[-1]

        for field in tables[field_group]:
            # check field naming on doubly-nested fields

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
    if not column:
        return None
    elif '.' in column:
        split_name = column.split('.')
        if split_name[0] == 'cases':
            return '__'.join(split_name[1:])
    else:
        return column

def get_parent_table(table_key):
    if not table_key:
        return None

    return '.'.join(table_key.split('.'))[:-1]

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


def add_reference_columns(tables_dict, schema_dict, table_keys, table_key, params):
    def generate_id_schema_entry(parent_table_key_='main', column_name='case_id'):
        if parent_table_key_ in table_keys:
            parent_field_name = get_field_name(parent_table_key_)
            ancestor_table = '*_{}'.format(parent_field_name)
        else:
            ancestor_table = 'main'

        # [:-3] to remove "_id" from key
        parent_fg = column_name[:-3]
        description = "Reference to the {} field of the {} record to which this record belongs. " \
                      "Parent record found in the program's {} table.".format(column_name, parent_fg, ancestor_table)

        return {"name": column_name, "type": 'STRING', "description": description}

    if len(table_key.split('.')) > 1:
        case_id_col_name = get_bq_name(table_key) + 'case_id'
        tables_dict[table_key].add('case_id')
        schema_dict[case_id_col_name] = generate_id_schema_entry()

        if len(table_key.split('.')) > 2:
            # insert parent id into child table
            parent_table_key = get_parent_table(table_key)
            parent_id_key = params['SINGULAR_ID_NAMES'][parent_table_key]

            # insert case_id into child table
            tables_dict[table_key].add(parent_id_key)
            parent_id_col_name = get_bq_name(table_key) + parent_id_key
            schema_dict[parent_id_col_name] = generate_id_schema_entry(parent_table_key, parent_id_key)

    return tables_dict, schema_dict


def create_bq_tables(program_name, params, tables_dict, record_counts, schema_dict):
    print("Adding tables to {}.{} dataset...".format(params['WORKING_PROJECT'], params['TARGET_DATASET']))

    table_ids = dict()
    documentation_dict = dict()
    documentation_dict['table_schemas'] = dict()

    table_keys = get_tables(record_counts)

    for table_key in table_keys:
        # tables_dict, schema_dict = add_reference_columns(tables_dict, schema_dict, table_keys, table_key)
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

        for schema_key in schema_field_keys:
            schema_list.append(bigquery.SchemaField(
                schema_key, schema_dict[schema_key]['type'], "NULLABLE", schema_dict[schema_key]['description'], ()))
        try:
            client = bigquery.Client()
            client.delete_table(table_id, not_found_ok=True)
            table = bigquery.Table(table_id, schema=schema_list)
            client.create_table(table)
            print("\t- {} table added successfully".format(table_id.split('.')[-1]))
        except exceptions.BadRequest as err:
            has_fatal_error("Fatal error for table_id: {}\n{}\n{}".format(table_id, err, schema_list))

        documentation_dict['table_schemas'][table_key]['table_schema'].append(schema_list)
    print("... DONE.\n")
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


def flatten_case(case, prefix, flattened_case_dict, params, table_keys, case_id=None, parent_id=None, parent_id_key=None):
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
                    if prefix in params["SINGULAR_ID_NAMES"]:
                        new_parent_id_key = params['SINGULAR_ID_NAMES'][prefix]
                        new_parent_id = entry[new_parent_id_key]
                    else:
                        new_parent_id = parent_id
                        new_parent_id_key = parent_id_key

                    flattened_case_dict = flatten_case(entry[key], prefix + '.' + key, flattened_case_dict, params, table_keys,
                                                       case_id, new_parent_id, new_parent_id_key)
                else:
                    entry_dict[key] = entry[key]

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
                entry_dict[key] = case[key]
        if entry_dict:
            entry_dict = remove_unwanted_fields(entry_dict, prefix, params)
            entry_list.append(entry_dict)
            flattened_case_dict[prefix] = entry_list

    flattened_case_dict = merge_single_entry_field_groups(flattened_case_dict, table_keys)

    return flattened_case_dict


def merge_single_entry_field_groups(flattened_case_dict, table_keys):
    for field_group_key in flattened_case_dict.copy():
        if field_group_key not in table_keys:
            for entry in flattened_case_dict[field_group_key].copy():
                # don't need multiple case_id keys in the same table
                entry.pop('case_id')
                # avoids name collisions by specifying source field group
                prefix = "__".join(field_group_key.split(".")[1:]) + "__"

                for key in entry:
                    flattened_case_dict['cases'][0][prefix + key] = entry[key]

            flattened_case_dict.pop(field_group_key)

    return flattened_case_dict


def insert_case_data(cases, record_counts, tables_dict, params):
    print("Inserting case records... ")

    table_keys = get_tables(record_counts)

    insert_lists = dict()

    for case in cases:
        flattened_case_dict = flatten_case(case, 'cases', dict(), params, table_keys)

        if isinstance(flattened_case_dict['cases'], dict):
            flattened_case_dict['cases'] = [flattened_case_dict['cases']]
        for table in flattened_case_dict.keys():
            if table not in insert_lists:
                insert_lists[table] = []
            insert_lists[table] = insert_lists[table] + flattened_case_dict[table]

    for table in insert_lists:
        table_id = tables_dict[table]

        table_mb = sys.getsizeof(insert_lists[table]) / (1024 * 1024)
        batch_size = params["INSERT_BATCH_SIZE"]
        pages = math.ceil(len(insert_lists[table]) / batch_size)
        page_size = table_mb / pages

        if page_size > 10:
            print("INSERT_BATCH_SIZE is too large. Batch size should be 10 mb maximum, actual: {}".format(page_size))
            new_page_size = math.floor(table_mb / 10)
            ratio = new_page_size / page_size
            batch_size = math.floor(batch_size * ratio)

        try:
            print("\t- inserting rows into {}... ".format(table_id.split('.')[-1]), end='')
            client = bigquery.Client()
            bq_table = client.get_table(table_id)
            start_idx = 0
            end_idx = batch_size

            while len(insert_lists[table]) > end_idx:
                client.insert_rows(bq_table, insert_lists[table][start_idx:end_idx])
                start_idx = end_idx
                end_idx += batch_size

            # insert the last set of records
            client.insert_rows(bq_table, insert_lists[table][start_idx:])
            print("{} records inserted.".format(len(insert_lists[table]), table))
        except Exception as err:
            has_fatal_error("Failed to insert into {} ({} records)\n{}".format(table, len(insert_lists[table]), err))
    print("... DONE.\n")


def check_data_integrity(params, cases, record_counts, table_columns):
    for case in cases:
        case_id = case['case_id']


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
        "SINGULAR_ID_NAMES": {
            'cases.diagnoses': 'diagnosis_id',
            'cases.demographic': 'demographic_id',
            'cases.exposures': 'exposure_id',
            'cases.family_histories': 'family_history_id',
            'cases.follow_ups': 'follow_up_id'
        },
        "INSERT_BATCH_SIZE": 1000
    }

    program_names = get_programs_list(params)

    global COLUMN_ORDER_DICT
    COLUMN_ORDER_DICT = import_column_order(args[2])

    with open(params['DOCS_OUTPUT_FILE'], 'w') as doc_file:
        doc_file.write("New BQ Documentation")

    for program_name in program_names:
        print("\n\n*** Running script for {} ***".format(program_name))

        cases = get_cases_by_program(program_name, params)

        if len(cases) == 0:
            print("No case records found for {}, skipping.".format(program_name))
            continue

        schema_dict = create_schema_dict(params)

        table_columns, record_counts, schema_dict = retrieve_program_case_structure(
            program_name, cases, params, schema_dict)

        documentation_dict, table_names_dict = create_bq_tables(program_name, params, table_columns, record_counts,
                                                                schema_dict)

        insert_case_data(cases, record_counts, table_names_dict, params)

        generate_documentation(params, program_name, documentation_dict, record_counts)

        check_data_integrity(params, cases, record_counts, table_columns)


if __name__ == '__main__':
    main(sys.argv)
