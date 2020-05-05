from common_etl.utils import create_mapping_dict, get_query_results, has_fatal_error, load_config
from google.cloud import bigquery
import sys

YAML_HEADERS = ('api_params', 'bq_params')
COLUMN_ORDER_DICT = None


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


##
#  Functions for creating the BQ table schema dictionary
##
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


def retrieve_program_case_structure(program_name, cases):
    def build_case_structure(tables_, case_, record_counts_, parent_path):
        """
        Recursive fuction for retrieve_program_data, finds nested fields
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

    for key in {k for k, v in sorted(field_group_keys.items(), key=lambda item: item[1], reverse=True)}:
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


def split_datatype_array(col_dict, col_string, name_prefix):
    columns = col_string[13:-2].split(', ')

    for column in columns:
        column_type = column.split(' ')

        column_name = name_prefix + column_type[0]
        col_dict[column_name] = column_type[1].strip(',')

    return col_dict


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


def create_bq_tables(program_name, api_params, bq_params, tables_dict, column_order_list):
    """
    If creating follow_ups table, cases has field with follow_ups_ids string list
    If creating follow_ups__molecular_tests table, follow_ups has field with molecular_tests_ids string list
    If creating diagnoses__treatments table, cases has field with diagnoses__treatments_ids
    If creating diagnoses__annotations table, cases has field with diagnoses__annotations_ids
    If creating family_histories table, cases has field with family_histories_ids
    """
    table_names_dict = dict()
    schema_dict = create_schema_dict(api_params)

    exclude_set = set(bq_params["EXCLUDE_FIELDS"].split(','))

    documentation_dict = dict()
    documentation_dict['table_schemas'] = dict()

    for table_key in tables_dict.keys():
        schema_list = []

        table_name = generate_table_name(bq_params, program_name, table_key)
        table_id = bq_params["WORKING_PROJECT"] + '.' + bq_params["TARGET_DATASET"] + '.' + table_name
        table_names_dict[table_key] = table_id

        split_prefix = table_key.split('.')

        if len(split_prefix) == 1:
            prefix = ''
        else:
            prefix = '__'.join(split_prefix[1:])
            prefix = prefix + '__'

        column_order_dict = {}

        documentation_dict['table_schemas'][table_key] = dict()
        documentation_dict['table_schemas'][table_key]['table_id'] = table_id
        documentation_dict['table_schemas'][table_key]['table_schema'] = list()

        # lookup column position indexes in master list, used to order schema
        for column in tables_dict[table_key]:
            full_column_name = prefix + column

            if full_column_name in exclude_set:
                continue

            column_order_dict[full_column_name] = column_order_list.index(full_column_name)

        # making dict available for print and insert functions
        global COLUMN_ORDER_DICT
        COLUMN_ORDER_DICT = column_order_dict

        # todo: logic for non-nullable fields
        for column in sorted(COLUMN_ORDER_DICT.items(), key=lambda x: x[1]):
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


def flatten_case(case):
    def flatten_case_recursive(case_, case_list_dict, prefix, case_id=None, parent_id=None, parent_id_key=None):
        if isinstance(case_, list):
            entry_list = []

            for entry in case_:
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

                        case_list_dict = flatten_case_recursive(entry[key], case_list_dict, prefix + '.' + key,
                                                                case_id, new_parent_id, new_parent_id_key)
                    else:
                        entry_dict[key] = entry[key]
                entry_list.append(entry_dict)
            if prefix in case_list_dict:
                case_list_dict[prefix] = case_list_dict[prefix] + entry_list
            else:
                if entry_list:
                    case_list_dict[prefix] = entry_list
        else:
            if prefix not in case_list_dict:
                case_list_dict[prefix] = dict()
            for key in case_:
                parent_id = case_['case_id']
                parent_id_key = 'case_id'
                if isinstance(case_[key], list):
                    case_list_dict = flatten_case_recursive(case_[key], case_list_dict, prefix + '.' + key,
                                                            parent_id, parent_id, parent_id_key)
                else:
                    case_list_dict[prefix][key] = case_[key]

        return case_list_dict

    flattened_case_dict = flatten_case_recursive(case, dict(), 'cases')
    return flattened_case_dict


def merge_single_entry_field_groups(flattened_case_dict, table_names_dict):
    for field_group_key in flattened_case_dict.copy():
        if field_group_key not in table_names_dict:
            prefix = "__".join(field_group_key.split(".")[1:])
            prefix = prefix + "__"

            field_group = flattened_case_dict.pop(field_group_key)[0]

            field_group.pop('case_id')

            for key in field_group:
                flattened_case_dict['cases'][prefix + key] = field_group[key]

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


def insert_case_data(cases, table_names_dict):
    """
    table_names_dict = {
        'cases.diagnoses.treatments': table_id,
        'cases': table_id
    }
    """
    """
    case_record = {
        'cases.diagnoses': [
            {}
        ],
        'cases.demographic': [
            {
                'gender': 'female',
                'race': 'white',
                'year_of_birth': 1949,
                'created_datetime': '2018-02-23T13:36:12.278625-06:00',
                'cause_of_death': None,
                'demographic_id': '73d8a76f-7e08-49df-bc28-439716586d69',
                'state': 'released',
                'updated_datetime': '2019-08-19T08:47:10.172187-05:00',
                'occupation_duration_years': None,
                'vital_status': 'Alive',
                'ethnicity': 'not hispanic or latino',
                'premature_at_birth': None,
                'age_at_index': 20380,
                'submitter_id': 'DLBCL11282_1698929-demographic',
                'case_id': '4234a18e-c1ae-4f16-a7c4-259d7db8bab4',
                'days_to_birth': -20380.0,
                'days_to_death': None,
                'weeks_gestation_at_birth': None,
                'cause_of_death_source': None,
                'year_of_death': None,
                'age_is_obfuscated': None
            }
        ],
        'cases': {
            'created_datetime': '2018-02-20T16:11:27.193958-06:00',
            'disease_type': 'Mature B-Cell Lymphomas',
            'state': 'released',
            'updated_datetime': '2019-08-19T08:47:10.172187-05:00',
            'id': '4234a18e-c1ae-4f16-a7c4-259d7db8bab4',
            'index_date': 'Diagnosis',
            'submitter_id': 'CTSP-ACY5',
            'submitter_diagnosis_ids': 'DLBCL11282-diagnosis',
            'case_id': '4234a18e-c1ae-4f16-a7c4-259d7db8bab4',
            'days_to_lost_to_followup': None,
            'lost_to_followup': 'No',
            'diagnosis_ids': '2bd60abb-7ba1-43a5-ab19-89d2de7b50c6',
            'primary_site': 'Unknown'
        }
    }
    """

    # todo: return this to normal
    for case in cases[-4:-3]:
        flattened_case_dict = flatten_case(case)
        flattened_case_dict = merge_single_entry_field_groups(flattened_case_dict, table_names_dict)
        ordered_print(flattened_case_dict)

        # cases is dict, the rest are [], todo
        for table in flattened_case_dict:
            if isinstance(flattened_case_dict[table], dict):
                flattened_case_dict[table] = [flattened_case_dict[table]]

        for field_group in table_names_dict:
            # skip field groups which aren't included in this program's set of one-to-many tables
            if field_group not in flattened_case_dict:
                has_fatal_error("DOES THIS EVER HAPPEN?")

            split_fg = field_group.split('.')

            if len(split_fg) > 3:
                has_fatal_error("The expand field group list contains a field group name with nested depth > 3. "
                                "This script is not set up to handle that.", ValueError)

            parent_fg = ".".join(split_fg[:-1])
            child_fg = split_fg[-1]

            # if not parent_fg, current table iteration is 'cases', the base table
            if parent_fg:
                flattened_case_dict = create_child_table_id_list(flattened_case_dict, parent_fg, child_fg)

        # ordered_print(flattened_case_dict)


def ordered_print(flattened_case_dict):
    print(COLUMN_ORDER_DICT)

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

            if isinstance(entry, str):
                print(flattened_case_dict)
                print(entry)
                return
                entry_string += "{}{}: {},\n".format(make_tabs(indent + 2), entry, flattened_case_dict[table])
            else:
                for key in entry.copy():
                    col_order_lookup_key = prefix + key
                    try:
                        field_order_dict[key] = COLUMN_ORDER_DICT[col_order_lookup_key]
                    except KeyError:
                        print("[ERROR] {} not in column order list".format(col_order_lookup_key))
                        entry.pop(key)
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
        documentation_dict, table_names_dict = create_bq_tables(
            program_name, api_params, bq_params, tables_dict, column_order_list=import_column_order_list(args[2]))

        print("DONE.\n - Inserting case records... ", end='')
        insert_case_data(cases, table_names_dict)

        print("DONE.\n - Inserting documentation... ", end='')
        generate_documentation(api_params, program_name, documentation_dict, record_counts)
        print("DONE.\n")


if __name__ == '__main__':
    main(sys.argv)
