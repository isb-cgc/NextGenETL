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
    print("\n Processing {}\n".format(program_name))

    tables = {}
    record_counts = {}

    for case in cases:
        tables, record_counts = build_case_structure(tables, case, record_counts, parent_path='cases')

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
    """
    If creating follow_ups table, cases has field with follow_ups_ids string list
    If creating follow_ups__molecular_tests table, follow_ups has field with molecular_tests_ids string list
    If creating diagnoses__treatments table, cases has field with diagnoses__treatments_ids
    If creating diagnoses__annotations table, cases has field with diagnoses__annotations_ids
    If creating family_histories table, cases has field with family_histories_ids


    """
    schema_dict = create_schema_dict(api_params)
    column_order_list = import_column_order_list(column_order_fp)

    exclude_set = set(bq_params["EXCLUDE_FIELDS"].split(','))

    documentation_dict = dict()
    documentation_dict['table_schemas'] = dict()

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


def create_table_mapping(tables_dict):
    table_mapping_dict = dict()

    for table in tables_dict:
        prefix = "__".join(table.split('.')) + "__"
        prefix = prefix[7:]

        for column in tables_dict[table]:
            table_mapping_dict[prefix + column] = table

    return table_mapping_dict


def flatten_case(case):
    """
    {
        'diagnosis_ids': '78f75aac-8d68-4525-a68e-0336f44737f6',
        'submitter_diagnosis_ids': 'HCM-CSHL-0063-C18_diagnosis',
        'exposures': [
            {
                'tobacco_smoking_onset_year': None,
                'environmental_tobacco_smoke_exposure': None,
                'updated_datetime': '2019-05-24T12:11:41.511797-05:00',
                'state': 'released',
                'pack_years_smoked': None,
                'bmi': None,
                'tobacco_smoking_status': 1,
                'cigarettes_per_day': None,
                'created_datetime': '2019-05-15T14:59:45.171654-05:00',
                'smoking_frequency': None,
                'tobacco_smoking_quit_year': None,
                'asbestos_exposure': None,
                'alcohol_days_per_week': None,
                'alcohol_intensity': None,
                'weight': None,
                'type_of_smoke_exposure': None,
                'coal_dust_exposure': None,
                'height': None,
                'type_of_tobacco_used': None,
                'radon_exposure': None,
                'time_between_waking_and_first_smoke': None,
                'years_smoked': None,
                'alcohol_drinks_per_day': None,
                'submitter_id': 'HCM-CSHL-0063-C18_exposure',
                'exposure_id': 'd12e9c38-9d56-462c-a876-ba8e416fe4be',
                'alcohol_history': None,
                'respirable_crystalline_silica_exposure': None
            }
        ],
        'id': 'e802e579-5293-465c-a867-74e290268299',
        'updated_datetime': '2019-09-20T15:18:28.629227-05:00',
        'demographic': [
            {
                'ethnicity': 'not hispanic or latino',
                'year_of_birth': 1942,
                'occupation_duration_years': None,
                'updated_datetime': '2019-05-24T12:11:41.511797-05:00',
                'age_is_obfuscated': None,
                'cause_of_death': 'Not Reported',
                'state': 'released',
                'weeks_gestation_at_birth': None,
                'gender': 'male',
                'race': 'black or african american',
                'year_of_death': None,
                'cause_of_death_source': None,
                'premature_at_birth': None,
                'days_to_birth': None,
                'vital_status': 'Not Reported',
                'submitter_id': 'HCM-CSHL-0063-C18_demographic',
                'days_to_death': None,
                'demographic_id': '3b1f3a97-094b-4389-b71b-c6575ea2158c',
                'age_at_index': 27438,
                'created_datetime': '2019-05-15T14:59:45.171654-05:00'
            }
        ],
        'state': 'released',
        'index_date': 'Diagnosis',
        'case_id': 'e802e579-5293-465c-a867-74e290268299',
        'disease_type': 'Adenomas and Adenocarcinomas',
        'created_datetime': '2018-10-02T15:55:34.328011-05:00',
        'lost_to_followup': None,
        'diagnoses': [
            {
                'morphology': '8140/3',
                'vascular_invasion_type': None,
                'mitotic_count': None,
                'treatments': [
                    {
                        'regimen_or_line_of_therapy': None,
                        'treatment_outcome': None,
                        'updated_datetime': '2019-05-24T12:11:41.511797-05:00',
                        'treatment_or_therapy': 'no',
                        'therapeutic_agents': None,
                        'state': 'released',
                        'treatment_anatomic_site': None,
                        'initial_disease_status': None,
                        'treatment_intent_type': 'Neoadjuvant',
                        'created_datetime': '2019-05-15T14:59:45.171654-05:00',
                        'treatment_id': 'ca2e0503-f096-456e-b6ea-7f6743d8707d',
                        'days_to_treatment_start': None,
                        'submitter_id': 'HCM-CSHL-0063-C18_treatment',
                        'treatment_effect': None,
                        'days_to_treatment_end': None,
                        'treatment_type': None
                    }
                ],
                'vascular_invasion_present': 'No',
                'ovarian_surface_involvement': None
            }
        ],
        'follow_ups': [
            {
                'barretts_esophagus_goblet_cells_present': None,
                'molecular_tests': [
                    {
                        'aa_change': None,
                        'cell_count': None,
                        'variant_origin': None,
                        'submitter_id': 'HCM-CSHL-0063-C18_molecular_test5',
                        'second_exon': None,
                        'state': 'released',
                        'test_result': 'Negative',
                        'test_value': None,
                        'laboratory_test': None,
                        'molecular_consequence': None,
                        'created_datetime': '2019-05-15T14:59:45.171654-05:00',
                        'variant_type': None,
                        'exon': None,
                        'copy_number': None,
                        'locus': None,
                        'transcript': None,
                        'biospecimen_type': None,
                        'molecular_analysis_method': 'Not Reported',
                        'molecular_test_id': '7b078ef1-140e-4490-90a5-108fc9c79a6e',
                        'gene_symbol': 'Not Applicable',
                        'zygosity': None,
                        'loci_abnormal_count': None,
                        'loci_count': None,
                        'updated_datetime': '2019-05-24T12:11:41.511797-05:00',
                        'ploidy': None,
                        'test_units': None,
                        'intron': None,
                        'cytoband': None,
                        'histone_variant': None,
                        'mismatch_repair_mutation': 'Yes',
                        'blood_test_normal_range_lower': None,
                        'second_gene_symbol': None,
                        'test_analyte_type': None,
                        'chromosome': None,
                        'antigen': None,
                        'histone_family': None,
                        'specialized_molecular_test': None,
                        'blood_test_normal_range_upper': None
                    },
                    {
                        'aa_change': None,
                        'cell_count': None,
                        'variant_origin': None,
                        'submitter_id': 'HCM-CSHL-0063-C18_molecular_test6',
                        'second_exon': None,
                        'state': 'released',
                        'test_result': 'Negative',
                        'test_value': None,
                        'laboratory_test': None,
                        'molecular_consequence': None,
                        'created_datetime': '2019-05-15T14:59:45.171654-05:00',
                        'variant_type': None,
                        'exon': None,
                        'copy_number': None,
                        'locus': None,
                        'transcript': None,
                        'biospecimen_type': None,
                        'molecular_analysis_method': 'IHC',
                        'molecular_test_id': 'be89c31f-7ef2-4673-9005-cf3beedc7159',
                        'gene_symbol': 'MLH1',
                        'zygosity': None,
                        'loci_abnormal_count': None,
                        'loci_count': None,
                        'updated_datetime': '2019-05-24T12:11:41.511797-05:00',
                        'ploidy': None,
                        'test_units': None,
                        'intron': None,
                        'cytoband': None,
                        'histone_variant': None,
                        'mismatch_repair_mutation': None,
                        'blood_test_normal_range_lower': None,
                        'second_gene_symbol': None,
                        'test_analyte_type': None,
                        'chromosome': None,
                        'antigen': None,
                        'histone_family': None,
                        'specialized_molecular_test': None,
                        'blood_test_normal_range_upper': None
                    },
                    {
                        'aa_change': None,
                        'cell_count': None,
                        'variant_origin': None,
                        'submitter_id': 'HCM-CSHL-0063-C18_molecular_test',
                        'second_exon': None,
                        'state': 'released',
                        'test_result': 'Negative',
                        'test_value': None,
                        'laboratory_test': None,
                        'molecular_consequence': None,
                        'created_datetime': '2019-05-15T14:59:45.171654-05:00',
                        'variant_type': None,
                        'exon': None,
                        'copy_number': None,
                        'locus': None,
                        'transcript': None,
                        'biospecimen_type': None,
                        'molecular_analysis_method': 'Not Reported',
                        'molecular_test_id': '54e8627d-d911-40fa-a748-18186458f164',
                        'gene_symbol': 'BRAF',
                        'zygosity': None,
                        'loci_abnormal_count': None,
                        'loci_count': None,
                        'updated_datetime': '2019-05-24T12:11:41.511797-05:00',
                        'ploidy': None,
                        'test_units': None,
                        'intron': None,
                        'cytoband': None,
                        'histone_variant': None,
                        'mismatch_repair_mutation': None,
                        'blood_test_normal_range_lower': None,
                        'second_gene_symbol': None,
                        'test_analyte_type': None,
                        'chromosome': None,
                        'antigen': None,
                        'histone_family': None,
                        'specialized_molecular_test': None,
                        'blood_test_normal_range_upper': None
                    }
                ],
                'progression_or_recurrence': None,
                'days_to_progression_free': None
            },
            {
                'diabetes_treatment_type': None,
                'days_to_adverse_event': None,
                'fev1_fvc_post_bronch_percent': None,
                'fev1_fvc_pre_bronch_percent': None,
                'updated_datetime': '2019-05-24T12:11:41.511797-05:00',
                'days_to_recurrence': None,
                'cause_of_response': None,
                'state': 'released',
                'days_to_progression': None,
                'adverse_event': None,
                'created_datetime': '2019-05-15T14:59:45.171654-05:00',
                'menopause_status': None,
                'comorbidity': None,
                'viral_hepatitis_serologies': None,
                'dlco_ref_predictive_percent': None,
                'fev1_ref_post_bronch_percent': None,
                'bmi': None,
                'progression_or_recurrence_type': 'Not Reported',
                'height': None,
                'weight': None,
                'reflux_treatment_type': None,
                'pancreatitis_onset_year': None,
                'days_to_follow_up': 0.0,
                'karnofsky_performance_status': None,
                'comorbidity_method_of_diagnosis': None,
                'progression_or_recurrence_anatomic_site': None,
                'fev1_ref_pre_bronch_percent': None,
                'ecog_performance_status': None,
                'disease_response': None,
                'days_to_comorbidity': None,
                'hepatitis_sustained_virological_response': None,
                'submitter_id': 'HCM-CSHL-0063-C18_follow_up2',
                'follow_up_id': 'cd4800c3-bdb1-487a-b3bf-95d49297686c',
                'risk_factor': None,
                'risk_factor_treatment': None,
                'hpv_positive_type': None,
                'barretts_esophagus_goblet_cells_present': None,
                'molecular_tests': [],
                'progression_or_recurrence': None,
                'days_to_progression_free': None
            }
        ],
        'submitter_id': 'HCM-CSHL-0063-C18',
        'primary_site': 'Colon',
        'days_to_lost_to_followup': None,
        'family_histories': [
            {
                'created_datetime': '2019-05-15T14:59:45.171654-05:00',
                'relationship_age_at_diagnosis': None,
                'relative_with_cancer_history': 'no',
                'relationship_primary_diagnosis': None,
                'submitter_id': 'HCM-CSHL-0063-C18_family_history',
                'relationship_type': None,
                'relationship_gender': None,
                'updated_datetime': '2019-05-24T12:11:41.511797-05:00',
                'family_history_id': '42382b47-ed18-4d5c-bfa9-0f82632a0dc0',
                'state': 'released'
            }
        ]
    }
    """


def insert_case_data(program_name, cases, tables_dict):
    table_mapping_dict = create_table_mapping(tables_dict)


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
    # program_names = ['BEATAML1.0', 'HCMI', 'CTSP']

    with open(api_params['DOCS_OUTPUT_FILE'], 'w') as doc_file:
        doc_file.write("New BQ Documentation")

    for program_name in program_names:
        cases = get_cases_by_program(program_name)

        tables_dict, record_counts = retrieve_program_data(program_name, cases)

        documentation_dict = create_bq_tables(program_name, api_params, bq_params, args[2], tables_dict)

        generate_documentation(api_params, program_name, documentation_dict, record_counts)

        insert_case_data(program_name, cases, tables_dict)


if __name__ == '__main__':
    main(sys.argv)
