from common_etl.utils import get_cases_by_program, collect_field_values, infer_data_types, create_mapping_dict, \
    get_query_results, has_fatal_error, load_config
from google.cloud import bigquery
import sys

YAML_HEADERS = ('api_params', 'bq_params', 'steps')


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


def retrieve_program_data(program_name, cases):
    tables = {}
    record_counts = {}

    for case in cases:
        tables, record_counts = build_case_structure(tables, case, record_counts, parent_path='cases')

    print("Flattening tables for {}".format(program_name))

    tables = flatten_tables(tables, record_counts)

    if not tables:
        has_fatal_error("[ERROR] no case structure returned for program {}".format(program_name))

    return tables, record_counts


def build_case_structure(tables, case, record_counts, parent_path):
    """
    Recursive fuction for retrieve_program_data, finds nested fields
    """
    if parent_path not in tables:
        tables[parent_path] = set()

    for field_key in case:
        if not case[field_key]:
            continue

        if not isinstance(case[field_key], list) and not isinstance(case[field_key], dict):
            if parent_path not in record_counts:
                record_counts[parent_path] = 1

            tables[parent_path].add(field_key)
            continue

        # at this point, the field_key references a dict or list
        nested_path = parent_path + '.' + field_key

        if nested_path not in record_counts:
            record_counts[nested_path] = 1

        if isinstance(case[field_key], dict):
            tables, record_counts = build_case_structure(tables, case[field_key], record_counts, nested_path)
        else:
            record_counts[nested_path] = max(record_counts[nested_path], len(case[field_key]))

            for field_group_entry in case[field_key]:
                tables, record_counts = build_case_structure(tables, field_group_entry, record_counts, nested_path)

    return tables, record_counts


def flatten_tables(tables, record_counts):
    """
    Used by retrieve_program_data
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

            while end_idx > (len(split_key) * -1) and not parent_table_found:
                parent_key = ".".join(split_key[:end_idx])

                if parent_key in tables:
                    parent_table_found = True
                else:
                    end_idx -= 1

            if not parent_table_found:
                print("[ERROR] Parent table not found in tables dict.")
                print("Key: {}, record count: {}, parent key: {}".format(key, record_counts[key], parent_key))
                print(tables.keys())
            else:
                print("tables[{}] = {}".format(parent_key, column_name))
                tables[parent_key] = column_name

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


def create_bq_tables(program_name, api_params, bq_params, column_order_fp, tables_dict):
    schema_dict = create_schema_dict(api_params)
    column_order_list = import_column_order_list(column_order_fp)

    exclude_set = set(bq_params["EXCLUDE_FIELDS"].split(','))

    documentation_dict = dict()
    documentation_dict['table_schemas'] = dict()

    print(tables_dict)

    for table_key in tables_dict.keys():
        schema_list = []

        table_name = generate_table_name(bq_params, program_name, table_key)
        table_id = 'isb-project-zero.GDC_Clinical_Data.' + table_name

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

            print(full_column_name)
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

    return documentation_dict


def generate_documentation(api_params, program_name, documentation_dict, record_counts):
    print("{} \n".format(program_name))
    print("{}".format(documentation_dict))
    print("{}".format(record_counts))

    with open(api_params['DOCS_OUTPUT_FILE'], 'a') as doc_file:
        doc_file.write("{} \n".format(program_name))
        doc_file.write("{}".format(documentation_dict))
        doc_file.write("{}".format(record_counts))

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


def create_bq_table_and_insert_rows(program_name, cases, schema_field_list, ordered_keys):

    table_id = "isb-project-zero.GDC_Clinical_Data.rel22_clinical_data_{}".format(program_name.lower())
    client = bigquery.Client()

    table = bigquery.Table(table_id, schema=schema_field_list)
    table = client.create_table(table)

    case_tuples = []

    for case in cases:
        case_vals = []
        for key in ordered_keys:
            if key in case:
                case_vals.append(case[key])
            else:
                case_vals.append(None)
        case_tuples.append(tuple(case_vals))

    errors = client.insert_rows(table, case_tuples)

    if not errors:
        print("Rows inserted successfully")
    else:
        print(errors)

"""
todos:
- insert case data into table 
- code commenting
- generate documentation
"""


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
        "DOCS_OUTPUT_FILE": 'docs/documentation.txt'
    }

    bq_params = {
        "GDC_RELEASE": 'rel23',
        "WORKING_PROJECT": 'isb-project-zero',
        "TARGET_DATASET": 'GDC_Clinical_Data',
        "PROGRAM_ID_TABLE": 'GDC_metadata.rel22_caseData',
        "EXCLUDE_FIELDS": 'aliquot_ids,analyte_ids,case_autocomplete,portion_ids,sample_ids,slide_ids,'
                          'submitter_aliquot_ids,submitter_analyte_ids,submitter_portion_ids,submitter_sample_ids,'
                          'submitter_slide_ids,diagnosis_ids,submitter_diagnosis_ids'
    }

    program_names = get_programs_list(bq_params)
    # program_names = ['HCMI', 'CTSP']

    with open(api_params['DOCS_OUTPUT_FILE'], 'w') as doc_file:
        doc_file.write("New BQ Documentation")


    for program_name in program_names:
        cases = get_cases_by_program(program_name)

        tables_dict, record_counts = retrieve_program_data(program_name, cases)

        documentation_dict = create_bq_tables(program_name, api_params, bq_params, args[2], tables_dict)

        generate_documentation(api_params, program_name, documentation_dict, record_counts)


if __name__ == '__main__':
    main(sys.argv)
