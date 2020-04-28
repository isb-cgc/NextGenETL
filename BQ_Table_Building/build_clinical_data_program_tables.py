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


def split_datatype_array(col_dict, col_string, name_prefix):

    columns = col_string[13:-2].split(', ')

    for column in columns:
        column_type = column.split(' ')

        column_name = name_prefix + column_type[0]
        col_dict[column_name] = column_type[1].strip(',')

    return col_dict


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


def create_bq_tables(program_name, bq_params, table_hierarchy, cases, schema_dict):

    exclude_set = set(bq_params["EXCLUDE_FIELDS"].split(','))

    for table_key in table_hierarchy.keys():
        table_id = generate_table_name(bq_params, program_name, table_key)
        schema_list = []
        split_prefix = table_key.split('.')

        if len(split_prefix) == 1:
            prefix = ''
        else:
            prefix = '__'.join(split_prefix[1:])
            prefix = prefix + '__'

        for column in table_hierarchy[table_key]:
            if column in exclude_set:
                continue

            column_name = prefix + column

            schema_field = bigquery.SchemaField(column, schema_dict[column_name]['type'],
                                                "NULLABLE", schema_dict[column_name]['description'], ())
            schema_list.append(schema_field)

        table_id = 'isb-project-zero.GDC_Clinical_Data.' + table_id

        client = bigquery.Client()

        table = bigquery.Table(table_id, schema=schema_list)
        table = client.create_table(table)


def create_schema_dict(field_mapping_dict, column_type_dict):
    """
    Generate flat dict containing schema metadata object with fields 'name', 'type', 'description'
    :param field_mapping_dict:
    :param column_type_dict:
    :return: schema fields object dict
    """
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
    api_params = {
        'ENDPOINT': 'https://api.gdc.cancer.gov/cases'
    }

    bq_params = {
        "GDC_RELEASE": 'rel23',
        "WORKING_PROJECT": 'isb-project-zero',
        "TARGET_DATASET": 'GDC_Clinical_Data',
        "PROGRAM_ID_TABLE": 'GDC_metadata.rel22_caseData',
        "EXCLUDE_FIELDS": 'aliquot_ids,analyte_ids,case_autocomplete,portion_ids,sample_ids,slide_ids,'
                          'submitter_aliquot_ids,submitter_analyte_ids,submitter_portion_ids,submitter_sample_ids,'
                          'submitter_slide_ids,diagnosis_ids'
    }

    column_type_dict = lookup_column_types()

    field_mapping_dict = create_mapping_dict(api_params['ENDPOINT'])

    schema_dict = create_schema_dict(field_mapping_dict, column_type_dict)

    # program_names = get_programs_list(bq_params)
    program_names = ['HCMI', 'CTSP']

    for program_name in program_names:
        cases = get_cases_by_program(program_name)

        table_hierarchy = retrieve_program_data(program_name, cases)

        create_bq_tables(program_name, bq_params, table_hierarchy, cases, schema_dict)



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
