from common_etl.utils import create_mapping_dict, get_query_results, has_fatal_error, load_config
from google.cloud import bigquery
import sys

YAML_HEADERS = ('api_params', 'bq_params')
COLUMN_ORDER_DICT = dict()


##
#  Functions for retrieving programs and cases
##
def get_programs_list(bq_params):
    programs_table_id = bq_params['WORKING_PROJECT'] + '.' + bq_params['PROGRAM_ID_TABLE']

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


def get_cases_by_program(program_name, bq_params):
    cases = []

    table_name = bq_params["GDC_RELEASE"] + '_clinical_data'
    table_id = bq_params["WORKING_PROJECT"] + '.' + bq_params["TARGET_DATASET"] + '.' + table_name

    results = get_query_results(
        """
        SELECT * 
        FROM `{}`
        WHERE submitter_id 
        IN (SELECT case_barcode
            FROM `isb-project-zero.GDC_metadata.rel22_caseData`
            WHERE program_name = '{}')
        """.format(table_id, program_name)
    )

    for case_row in results:
        case_dict = dict(case_row.items())

        for key in case_dict.copy():
            # note fields with values
            if not case_dict[key]:
                case_dict.pop(key)

        cases.append(case_dict)

    return cases


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


##
#  Functions for creating the BQ table schema dictionary
##
def retrieve_program_case_structure(program_name, cases):
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
        case = strip_null_fields(case)
        null_stripped_cases.append(case)

        tables, record_counts = build_case_structure(tables, case, record_counts, parent_path='cases')

    tables = flatten_tables(tables, record_counts)

    if not tables:
        has_fatal_error("[ERROR] no case structure returned for program {}".format(program_name))

    return tables, record_counts, null_stripped_cases


def flatten_tables(tables, record_counts):
    """
    Used by retrieve_program_case_structure
    """
    field_group_keys = dict.fromkeys(record_counts.keys(), 0)

    # sort field group keys by depth
    for key in field_group_keys:
        field_group_keys[key] = len(key.split("."))

    for key, value in sorted(field_group_keys.items(), key=lambda item: item[1], reverse=True):
        if record_counts[key] > 1:
            continue

        split_key = key.split('.')

        if len(split_key) == 1:
            continue

        field_group_name = split_key[-1]

        for column in tables[key]:
            column_name = field_group_name + "__" + column

            # In the case where a doubly nested field group is also flattened, its direct ancestor won't be a parent.
            #

            end_idx = -1
            parent_table_found = False
            parent_key = ''
            prefix = ''

            while end_idx > (len(split_key) * -1) and not parent_table_found:
                parent_key = ".".join(split_key[:end_idx])

                if parent_key in tables:
                    parent_table_found = True
                else:
                    end_idx -= 1
                    prefix += split_key[end_idx] + "__"

            if not parent_table_found:
                print("[ERROR] Parent table not found in tables dict.")
                print("Key: {}, record count: {}, parent key: {}".format(key, record_counts[key], parent_key))
                print(tables.keys())
            else:
                tables[parent_key].add(prefix + column_name)

        tables.pop(key)

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

    column_type_dict = dict()

    base_query = """
    SELECT column_name, data_type FROM `isb-project-zero.GDC_Clinical_Data.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'rel23_clinical_data' 
    and column_name != 'family_histories' 
    and column_name != 'exposures' 
    and column_name != 'demographic' 
    and column_name != 'diagnoses'
    and column_name != 'follow_ups'
    """

    follow_ups_query = """
    SELECT column_name, data_type FROM `isb-project-zero.GDC_Clinical_Data.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'rel23_clinical_data' and column_name = 'follow_ups'
    """

    exposures_query = """
    SELECT column_name, data_type FROM `isb-project-zero.GDC_Clinical_Data.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'rel23_clinical_data' and column_name = 'exposures'
    """

    demographic_query = """
    SELECT column_name, data_type FROM `isb-project-zero.GDC_Clinical_Data.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'rel23_clinical_data' and column_name = 'demographic'
    """

    diagnoses_query = """
    SELECT column_name, data_type FROM `isb-project-zero.GDC_Clinical_Data.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'rel23_clinical_data' and column_name = 'diagnoses'
    """

    family_histories_query = """
    SELECT column_name, data_type FROM `isb-project-zero.GDC_Clinical_Data.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'rel23_clinical_data' and column_name = 'family_histories'
    """

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

    column_type_dict = split_datatype_array(column_type_dict, diagnoses, 'diagnoses__')
    column_type_dict = split_datatype_array(column_type_dict, treatments, 'diagnoses__treatments__')
    column_type_dict = split_datatype_array(column_type_dict, annotations, 'diagnoses__annotations__')

    return column_type_dict


def create_schema_dict(api_params):
    column_type_dict = lookup_column_types()
    field_mapping_dict = create_mapping_dict(api_params['ENDPOINT'])

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


##
#  Functions for ordering the BQ table schema and creating BQ tables
##
def get_table_names(record_counts):
    table_keys = set()

    for table in record_counts:
        if record_counts[table] > 1 or table == 'cases':
            table_keys.add(table)

    return table_keys


def import_column_order_list(path):
    column_list = []
    with open(path, 'r') as file:
        columns = file.readlines()

        for column in columns:
            column_list.append(column.strip())

    return column_list


def generate_table_name(bq_params, program_name, table):
    split_table_path = table.split(".")

    # eliminate '.' char from program name if found (which would otherwise create illegal table_id)
    program_name = "_".join(program_name.split('.'))

    base_table_name = [bq_params["GDC_RELEASE"], 'clin', program_name]
    table_name = "_".join(base_table_name)

    if len(split_table_path) > 1:
        table_suffix = "__".join(split_table_path[1:])
        table_name = table_name + '_' + table_suffix

    if not table_name:
        has_fatal_error("generate_table_name returns empty result.")

    return table_name


def add_reference_columns(tables_dict, schema_dict, table_keys, table_key):
    def generate_id_schema_entry(parent_table='main', parent_field='case', column_name='case_id'):
        description = "Reference to the {} field of the {} record to which this record belongs. " \
                      "Parent record found in the program's {} table.".format(column_name, parent_field, parent_table)

        return {
            "name": column_name,
            "type": 'STRING',
            "description": description
        }

    def generate_ids_schema_entry(child_table_, child_field_name):
        column_name = "_".join(child_field_name.split(" ")) + '_ids'
        description = "List of {} ids, referencing associated records located in the program's {} " \
                      "table.".format(child_field_name, child_table_)

        return {
            "name": column_name,
            "type": 'STRING',
            "description": description
        }

    if table_key == 'cases.follow_ups':
        tables_dict['cases'].add('follow_up_ids')
        schema_dict['follow_up_ids'] = generate_ids_schema_entry('*_follow_ups', 'follow up')

        tables_dict['cases.follow_ups'].add('case_id')
        schema_dict['follow_ups__case_id'] = generate_id_schema_entry()
    elif table_key == 'cases.follow_ups.molecular_tests':
        tables_dict['cases.follow_ups'].add('molecular_test_ids')
        schema_dict['follow_ups__molecular_test_ids'] = generate_ids_schema_entry(
            '*_follow_ups__molecular_tests', 'molecular test')

        tables_dict['cases.follow_ups.molecular_tests'].add('follow_up_id')
        schema_dict['follow_ups__molecular_tests__follow_up_id'] = generate_id_schema_entry(
            '*_follow_ups', 'follow up', 'follow_up_id')

        tables_dict['cases.follow_ups.molecular_tests'].add('case_id')
        schema_dict['follow_ups__molecular_tests__case_id'] = generate_id_schema_entry()
    elif table_key == 'cases.family_histories':
        tables_dict['cases'].add('family_history_ids')
        schema_dict['family_history_ids'] = generate_ids_schema_entry('*_family_histories', 'family history')

        tables_dict['cases.family_histories'].add('case_id')
        schema_dict['family_histories__case_id'] = generate_id_schema_entry()
    elif table_key == 'cases.demographic':
        tables_dict['cases'].add('demographic_ids')
        schema_dict['demographic_ids'] = generate_ids_schema_entry('*_demographic', 'demographic')

        tables_dict['cases.demographic'].add('case_id')
        schema_dict['demographic__case_id'] = generate_id_schema_entry()
    elif table_key == 'cases.exposures':
        tables_dict['cases.exposures'].add('case_id')
        schema_dict['exposures__case_id'] = generate_id_schema_entry()

        tables_dict['cases'].add('exposure_ids')
        schema_dict['exposure_ids'] = generate_ids_schema_entry('*_exposures', 'exposure')
    elif table_key == 'cases.diagnoses':
        tables_dict['cases'].add('diagnosis_ids')
        schema_dict['diagnosis_ids'] = generate_ids_schema_entry('*_diagnoses', 'diagnosis')

        tables_dict['cases.diagnoses'].add('case_id')
        schema_dict['diagnoses__case_id'] = generate_id_schema_entry()
    elif table_key == 'cases.diagnoses.treatments':
        ancestor_table = '*_diagnoses' if 'case.diagnoses' in table_keys else 'main'
        child_table = ancestor_table + '__treatments' if 'case.diagnoses' in table_keys else ancestor_table

        if 'cases.diagnoses' in table_keys:
            tables_dict['cases.diagnoses'].add('treatment_ids')
        else:
            tables_dict['cases'].add('diagnoses__treatment_ids')
        schema_dict['diagnoses__treatment_ids'] = generate_ids_schema_entry(child_table, 'treatment')

        tables_dict['cases.diagnoses.treatments'].add('diagnosis_id')
        schema_dict['diagnoses__treatments__diagnosis_id'] = generate_id_schema_entry(
            ancestor_table, 'diagnosis', 'diagnosis_id')
        tables_dict['cases.diagnoses.treatments'].add('case_id')
        schema_dict['diagnoses__treatments__case_id'] = generate_id_schema_entry()
    elif table_key == 'cases.diagnoses.annotations':
        ancestor_table = '*_diagnoses' if 'case.diagnoses' in table_keys else 'main'
        child_table = ancestor_table + '__annotations' if 'case.diagnoses' in table_keys else ancestor_table

        if 'cases.diagnoses' in table_keys:
            tables_dict['cases.diagnoses'].add('annotation_ids')
        else:
            tables_dict['cases'].add('diagnoses__annotation_ids')
        schema_dict['diagnoses__annotation_ids'] = generate_ids_schema_entry(child_table, 'annotation')

        tables_dict['cases.diagnoses.annotations'].add('diagnosis_id')
        schema_dict['diagnoses__annotations__diagnosis_id'] = generate_id_schema_entry(
            ancestor_table, 'diagnosis', 'diagnosis_id')
        tables_dict['cases.diagnoses.annotations'].add('case_id')
        schema_dict['diagnoses__annotations__case_id'] = generate_id_schema_entry()

    return tables_dict, schema_dict


def create_bq_tables(program_name, api_params, bq_params, tables_dict, record_counts, column_order_list):
    schema_dict = create_schema_dict(api_params)

    exclude_set = set()

    for field in bq_params["EXCLUDE_FIELDS"].split(','):
        exclude_set.add('cases.' + field)

    table_names_dict = dict()
    documentation_dict = dict()
    documentation_dict['table_schemas'] = dict()

    table_keys = get_table_names(record_counts)

    column_order_dict = {}
    created_table_set = set()

    for table_key in table_keys:
        tables_dict, schema_dict = add_reference_columns(tables_dict, schema_dict, table_keys, table_key)

    for table_key in table_keys:
        schema_list = []

        table_name = generate_table_name(bq_params, program_name, table_key)
        table_id = bq_params["WORKING_PROJECT"] + '.' + bq_params["TARGET_DATASET"] + '.' + table_name
        table_names_dict[table_key] = table_id

        documentation_dict['table_schemas'][table_key] = dict()
        documentation_dict['table_schemas'][table_key]['table_id'] = table_id
        documentation_dict['table_schemas'][table_key]['table_schema'] = list()

        split_prefix = table_key.split('.')
        prefix = ''

        if len(split_prefix) > 1:
            prefix = '__'.join(split_prefix[1:]) + '__'

        # lookup column position indexes in master list, used to order schema
        for column in tables_dict[table_key]:

            full_column_name = prefix + column

            if full_column_name in exclude_set:
                continue

            if full_column_name not in column_order_list:
                has_fatal_error('{} not in column_order_list!'.format(full_column_name))

            column_order_dict[full_column_name] = column_order_list.index(full_column_name)

        # todo: logic for non-nullable fields
        for column in sorted(column_order_dict.items(), key=lambda x: x[1]):
            column_name = column[0]

            schema_field = bigquery.SchemaField(
                column_name,
                schema_dict[column_name]['type'],
                "NULLABLE",
                schema_dict[column_name]['description'],
                ()
            )

            schema_list.append(schema_field)

            documentation_dict['table_schemas'][table_key]['table_schema'].append(schema_dict[column_name])

        client = bigquery.Client()
        table = bigquery.Table(table_id, schema=schema_list)
        client.delete_table(table_id, not_found_ok=True)
        client.create_table(table)
        created_table_set.add(table_key)

    if len(created_table_set) != len(table_names_dict.keys()):
        has_fatal_error("len(created_table_set) = {}, len(table_names_dict) = {}, unequal!\n{}\n{}".format(
            len(created_table_set),
            len(table_names_dict.keys()),
            created_table_set,
            table_names_dict.keys()
        ))

    # make column order dict available for print/case insert functions.
    global COLUMN_ORDER_DICT
    COLUMN_ORDER_DICT = column_order_dict

    return documentation_dict, table_names_dict


##
#  Functions for inserting case entries into BQ tables
##
def create_table_mapping(tables_dict):
    table_mapping_dict = dict()

    for table in tables_dict:
        prefix = "__".join(table.split('.')) + "__"
        prefix = prefix[7:]

        for column in tables_dict[table]:
            table_mapping_dict[prefix + column] = table

    return table_mapping_dict


def flatten_case(case, prefix, case_list_dict=dict(), case_id=None, parent_id=None, parent_id_key=None):
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

                    case_list_dict = flatten_case(entry[key], prefix + '.' + key, case_list_dict, case_id,
                                                  new_parent_id, new_parent_id_key)
                else:
                    entry_dict[key] = entry[key]
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
        for key in case:
            parent_id = case['case_id']
            parent_id_key = 'case_id'
            if isinstance(case[key], list):
                case_list_dict = flatten_case(case[key], prefix + '.' + key, case_list_dict, parent_id,
                                              parent_id, parent_id_key)
            else:
                # case_list_dict[prefix][key] = case_[key]
                entry_dict[key] = case[key]
        if entry_dict:
            entry_list.append(entry_dict)
        if entry_list:
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

    child_ids_dict = dict()

    if parent_fg not in flattened_case_dict:
        print("parent fg not in flattened_case_dict")
        # case: direct ancestor (e.g. cases.diagnoses) was flattened/incorporated into its own ancestor.
        # Therefore, the list of child_ids needs to be included in the distant ancestor's record dictionary,
        # and the direct ancestor's name becomes prefix of the child_id_list_key.
        child_table = parent_fg + '.' + child_fg
        split_parent_fg = parent_fg.split('.')
        parent_fg = split_parent_fg[0]
        child_fg = split_parent_fg[-1]
        parent_id_key = create_id_key(parent_fg)
        child_id_key = create_id_key(child_fg)
        child_id_list_key = split_parent_fg[-1] + '__' + child_id_key + 's'
    else:
        child_table = parent_fg + '.' + child_fg
        parent_id_key = create_id_key(parent_fg.split(".")[-1])
        child_id_key = create_id_key(child_fg)
        child_id_list_key = child_id_key + 's'

    for child_record in flattened_case_dict[child_table]:
        parent_id = child_record[parent_id_key]
        child_id = child_record[child_id_key]

        if parent_id not in child_ids_dict:
            child_ids_dict[parent_id] = []

        child_ids_dict[parent_id].append(child_id)

    if parent_fg == 'cases':
        # todo this might end up being unnecessary
        parent_id = flattened_case_dict[parent_fg][0]['case_id']
        if parent_id in child_ids_dict:
            child_ids = ", ".join(child_ids_dict[parent_id])

            flattened_case_dict[parent_fg][0][child_id_list_key] = child_ids
    else:
        parent_records_list = []

        for parent_record in flattened_case_dict[parent_fg]:
            parent_id = parent_record[parent_id_key]
            if parent_id in child_ids_dict:
                child_ids = ", ".join(child_ids_dict[parent_id])
                parent_record[child_id_list_key] = child_ids
            parent_records_list.append(parent_record)

        flattened_case_dict[parent_fg] = parent_records_list

    return flattened_case_dict


def insert_case_data(cases, record_counts):
    table_keys = get_table_names(record_counts)

    for case in cases[-1:]:
        if 'case_id' in case:
            print("Starting case: {}".format(case['case_id']))
        if 'follow_ups' in case:
            print("1 len(case['follow_ups']) = {}".format(len(case['follow_ups'])))
        flattened_case_dict = flatten_case(case, 'cases')
        if 'cases.follow_ups' in flattened_case_dict:
            print("2 len(flattened_case_dict['cases.follow_ups']) = {}".format(
                len(flattened_case_dict['cases.follow_ups'])))

        flattened_case_dict = merge_single_entry_field_groups(flattened_case_dict, table_keys)

        if 'cases.follow_ups' in flattened_case_dict:
            print("3 len(flattened_case_dict['cases.follow_ups']) = {}".format(
                len(flattened_case_dict['cases.follow_ups'])))
        """
        # cases is dict, the rest are [], todo
        for table in flattened_case_dict.keys():
            if isinstance(flattened_case_dict[table], dict):
                flattened_case_dict[table] = [flattened_case_dict[table]]

            split_table = table.split('.')

            if len(split_table) > 3:
                has_fatal_error("Expand field group list contains a nested field group with depth > 3.", ValueError)

            parent_fg = ".".join(split_table[:-1])
            child_fg = split_table[-1]

            
            if parent_fg:
                flattened_case_dict = create_child_table_id_list(flattened_case_dict, parent_fg, child_fg)
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


##
#  Functions for creating documentation
##
def generate_documentation(api_params, program_name, documentation_dict, record_counts):
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
    with open(api_params['DOCS_OUTPUT_FILE'], 'a') as doc_file:
        doc_file.write("{} \n".format(program_name))
        doc_file.write("{}".format(documentation_dict))
        doc_file.write("{}".format(record_counts))


def main(args):
    """
    if len(args) != 3:
        has_fatal_error('Usage : {} <configuration_yaml> <column_order_txt>".format(args[0])', ValueError)

    with open(args[1], mode='r') as yaml_file:
        try:
            api_params, bq_params, steps = load_config(yaml_file, YAML_HEADERS)
        except ValueError as e:
            has_fatal_error(str(e), ValueError)

    # programs_table_id = bq_params['WORKING_PROJECT'] + '.' + bq_params['PROGRAM_ID_TABLE']
    """

    api_params = {
        'ENDPOINT': 'https://api.gdc.cancer.gov/cases',
        "DOCS_OUTPUT_FILE": 'docs/documentation.txt',
        "EXPAND_FIELD_GROUPS": 'demographic,diagnoses,diagnoses.treatments,diagnoses.annotations,exposures,'
                               'family_histories,follow_ups,follow_ups.molecular_tests'
    }

    bq_params = {
        "GDC_RELEASE": 'rel23',
        "WORKING_PROJECT": 'isb-project-zero',
        "TARGET_DATASET": 'GDC_Clinical_Data',
        "PROGRAM_ID_TABLE": 'GDC_metadata.rel22_caseData',
        "EXCLUDE_FIELDS": 'id,aliquot_ids,analyte_ids,case_autocomplete,portion_ids,sample_ids,slide_ids,'
                          'submitter_aliquot_ids,submitter_analyte_ids,submitter_portion_ids,submitter_sample_ids,'
                          'submitter_slide_ids,diagnosis_ids,submitter_diagnosis_ids'
    }

    # program_names = get_programs_list(bq_params)
    # program_names = ['BEATAML1.0', 'HCMI', 'CTSP']
    program_names = ['HCMI']

    with open(api_params['DOCS_OUTPUT_FILE'], 'w') as doc_file:
        doc_file.write("New BQ Documentation")

    for program_name in program_names:
        print("\n*** Running script for {} ***".format(program_name))

        print(" - Retrieving cases... ", end='')
        cases = get_cases_by_program(program_name, bq_params)

        print("DONE.\n - Determining program table structure... ", end='')
        tables_dict, record_counts, cases = retrieve_program_case_structure(program_name, cases)

        print("DONE.\n - Creating empty BQ tables... ", end='')
        column_order_list = import_column_order_list(args[2])
        documentation_dict, table_names_dict = create_bq_tables(
            program_name, api_params, bq_params, tables_dict, record_counts, column_order_list
        )

        print("DONE.\n - Inserting case records... ", end='')
        insert_case_data(cases, record_counts)

        print("DONE.\n - Inserting documentation... ", end='')
        generate_documentation(api_params, program_name, documentation_dict, record_counts)
        print("DONE.\n")


if __name__ == '__main__':
    main(sys.argv)
