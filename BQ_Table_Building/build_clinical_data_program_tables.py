from common_etl.utils import get_cases_by_program, collect_field_values, infer_data_types, create_mapping_dict, \
    get_query_results, has_fatal_error, load_config
from google.cloud import bigquery
import sys

YAML_HEADERS = ('bq_params', 'steps')


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

    tables = flatten_tables(tables, record_counts)

    if not tables:
        has_fatal_error("[ERROR] no case structure returned for program {}".format(program_name))

    return tables


def build_case_structure(tables, case, record_counts, parent_path):
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
            tables, record_counts = build_case_structure(tables, case[field_key],
                                                         record_counts, nested_path)
        else:
            record_counts[nested_path] = max(record_counts[nested_path], len(case[field_key]))

            for field_group_entry in case[field_key]:
                tables, record_counts = build_case_structure(tables, field_group_entry,
                                                             record_counts, nested_path)

    return tables, record_counts


def flatten_tables(tables, record_counts):
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

        parent_key = ".".join(split_key[:-1])
        field_group_name = split_key[-1]

        for column in tables[key]:
            column_name = field_group_name + "__" + column
            try:
                tables[parent_key].add(column_name)
            except KeyError as e:
                print("ERROR ERROR")
                print(e)
                print(tables.keys())

        tables.pop(key)

    if len(tables.keys()) - 1 != sum(val > 1 for val in record_counts.values()):
        has_fatal_error("Flattened tables dictionary has incorrect number of keys.")
    return tables


def generate_table_name(bq_params, program_name, table):
    split_table_path = table.split(".")

    base_table_name = [bq_params["GDC_RELEASE"], 'clin', program_name]
    table_name = "_".join(base_table_name)

    if len(split_table_path) > 1:
        table_suffix = "__".join(split_table_path[1:])
        table_name = table_name + '_' + table_suffix

    if not table_name:
        has_fatal_error("generate_table_name returns empty result.")

    return table_name


"""
def infer_column_types(cases, table_key, columns):
    aggregated_column_vals = dict.fromkeys(columns, set())

    split_table_key = table_key.split('.')


    for case in cases:
        if len(split_table_key) == 1:
            for column in columns:
                if column in case:
                    aggregated_column_vals[column].add(case[column])
        elif len(split_table_key) == 2:
            field_group = case[split_table_key[1]]
        elif len(split_table_key) == 3:
            field_group = case[split_table_key[1]][split_table_key[2]]

            for parent_key in split_table_key[1:]:
"""


def lookup_column_types():
    query = """
    SELECT column_name, data_type FROM `isb-project-zero.GDC_Clinical_Data.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'rel23_clinical_data' and column_name = 'family_histories'
    """

    results = get_query_results(query)

    for result in results:
        print(list(result.data_type))


def create_bq_tables(program_name, bq_params, table_hierarchy, cases):
    for table_key in table_hierarchy.keys():
        table_name = generate_table_name(bq_params, program_name, table_key)

        """
        cases.follow_ups
        {'progression_or_recurrence_anatomic_site', 'height', 'ecog_performance_status', 'follow_up_id', 
        'karnofsky_performance_status', 'submitter_id', 'days_to_follow_up', 'comorbidity', 'updated_datetime', 
        'disease_response', 'reflux_treatment_type', 'risk_factor', 'progression_or_recurrence', 'created_datetime', 
        'bmi', 'weight', 'days_to_progression', 'progression_or_recurrence_type', 'state'}
        """

        column_types = dict.fromkeys(table_hierarchy[table_key], None)

        column_types = infer_column_types(cases, table_key, table_hierarchy[table_key])

    # get table column list
    # infer field type
    #


def main(args):
    """
    if len(args) != 2:
        has_fatal_error('Usage : {} <configuration_yaml>".format(args[0])', ValueError)

    with open(args[1], mode='r') as yaml_file:
        try:
            bq_params, steps = load_config(yaml_file, YAML_HEADERS)
        except ValueError as e:
            has_fatal_error(str(e), ValueError)

    programs_table_id = bq_params['WORKING_PROJECT'] + '.' + bq_params['PROGRAM_ID_TABLE']
    """

    bq_params = {
        "GDC_RELEASE": 'rel23',
        "WORKING_PROJECT": 'isb-project-zero',
        "TARGET_DATASET": 'GDC_Clinical_Data',
        "PROGRAM_ID_TABLE": 'GDC_metadata.rel22_caseData'
    }

    lookup_column_types()
    return

    # program_names = get_programs_list(bq_params)
    program_names = ['HCMI', 'CTSP']

    for program_name in program_names:
        cases = get_cases_by_program(program_name)

        table_hierarchy = retrieve_program_data(program_name, cases)

        create_bq_tables(program_name, bq_params, table_hierarchy, cases)


if __name__ == '__main__':
    main(sys.argv)

"""
no nested keys: FM, NCICCR, CTSP, ORGANOID, CPTAC, WCDT, TARGET, GENIE, BEATAML1.0, OHSU

nested keys:
MMRF: follow_ups, follow_ups.molecular_tests, family_histories, diagnoses__treatments
CGCI: diagnoses__treatments
VAREPOP: family_histories, diagnoses__treatments
HCMI: follow_ups, diagnoses__treatments, follow_ups.molecular_tests
TCGA: diagnoses__treatments

diagnoses__treatments: MMRF, CGCI, VAREPOP, HCMI, TCGA
family_histories: MMRF, VAREPOP
follow_ups: MMRF, HCMI
follow_ups.molecular_tests: MMRF, HCMI
"""

'''
def get_field_data_types(cases):
    field_dict = dict()

    for case in cases:
        for key in case:
            field_dict = collect_field_values(field_dict, key, case, 'cases.')

    field_type_dict = infer_data_types(field_dict)

    return field_type_dict


def create_field_records_dict(field_mapping_dict, field_data_type_dict):
    """
    Generate flat dict containing schema metadata object with fields 'name', 'type', 'description'
    :param field_mapping_dict:
    :param field_data_type_dict:
    :return: schema fields object dict
    """
    schema_dict = {}

    for key in field_data_type_dict:
        column_name = "__".join(key.split(".")[1:])
        mapping_key = ".".join(key.split("__"))

        try:
            description = field_mapping_dict[mapping_key]['description']
        except KeyError:
            # cases.id not returned by mapping endpoint. In such cases, substitute an empty description string.
            description = ""

        if field_data_type_dict[key]:
            # if script was able to infer a data type using field's values, default to using that type
            field_type = field_data_type_dict[key]
        elif key in field_mapping_dict:
            # otherwise, include type from _mapping endpoint
            field_type = field_mapping_dict[key]['type']
        else:
            # this could happen in the case where a field was added to the cases endpoint with only null values,
            # and no entry for the field exists in mapping
            print("[INFO] Not adding field {} because no type found".format(key))
            continue

        # this is the format for bq schema json object entries
        schema_dict[key] = {
            "name": column_name,
            "type": field_type,
            "description": description
        }

    return schema_dict


def create_bq_schema_list(field_data_type_dict, nested_keys):
    mapping_dict = create_mapping_dict("https://api.gdc.cancer.gov/cases")

    schema_parent_field_list = []
    schema_child_field_list = []
    ordered_parent_keys = []
    ordered_child_keys = []

    for key in sorted(field_data_type_dict.keys()):
        split_name = key.split('.')

        col_name = "__".join(split_name[1:])
        col_type = field_data_type_dict[key]

        if key in mapping_dict:
            description = mapping_dict[key]['description']
        else:
            description = ""

        schema_field = bigquery.SchemaField(col_name, col_type, "NULLABLE", description, ())

        if len(split_name) == 2:
            schema_parent_field_list.append(schema_field)
            ordered_parent_keys.append(".".join(split_name[1:]))
        else:
            schema_child_field_list.append(schema_field)
            ordered_child_keys.append(".".join(split_name[1:]))

    schema_field_list = schema_parent_field_list + schema_child_field_list
    ordered_keys = ordered_parent_keys + ordered_child_keys

    return schema_field_list, ordered_keys


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
'''
