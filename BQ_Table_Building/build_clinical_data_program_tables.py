from common_etl.utils import get_cases_by_program, collect_field_values, infer_data_types, create_mapping_dict, get_query_results
from google.cloud import bigquery


def build_case_structure(structure_dict, parent_path, prefix, case):
    for field_key in case:
        if not case[field_key]:
            continue
        elif isinstance(case[field_key], list):
            if len(case[field_key]) > 1:
                new_path = parent_path + '.' + field_key
                new_prefix = ''
            else:
                new_path = parent_path
                new_prefix = prefix + field_key + '__'
            for field_group_entry in case[field_key]:
                structure_dict = build_case_structure(structure_dict, new_path, new_prefix, field_group_entry)
        elif isinstance(case[field_key], dict):
            new_prefix = prefix + field_key + '__'
            structure_dict = build_case_structure(structure_dict, parent_path, new_prefix, case[field_key])
        else:
            if parent_path not in structure_dict:
                structure_dict[parent_path] = set()
            structure_dict[parent_path].add(prefix + field_key)
    return structure_dict


def retrieve_program_data(program_name):
    cases = get_cases_by_program(program_name)

    structure_dict = dict()

    for case in cases:
        structure_dict = build_case_structure(structure_dict, 'cases', '', case)

    return structure_dict

    """
    for case in cases:
        for key in case.copy():
            if key in null_fields:
                case.pop(key)
                continue
            elif key in nested_key_set:
                continue
            elif isinstance(case[key], list):
                nested_field_group = case.pop(key)

                if not nested_field_group:
                    continue

                nested_field_group = nested_field_group[0]

                for n_key in nested_field_group:
                    flat_key = key + "__" + n_key
                    case[flat_key] = nested_field_group[n_key]

    return cases, nested_key_set
    """


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


def get_programs_list():
    programs = set()
    results = get_query_results(
        """
        SELECT distinct(program_name)
        FROM `isb-project-zero.GDC_metadata.rel22_caseData`
        """
    )

    for result in results:
        programs.add(result.program_name)

    return programs


def main():
    program_names = get_programs_list()

    for program_name in program_names:

        structure_dict = retrieve_program_data(program_name)

        if not structure_dict:
            print("[ERROR] no case structure returned for program {}".format(program_name))
            return
        else:
            print(program_name)
            print(structure_dict)
            print()
            continue

    return
    """

        record_fieldset = set()

        total_cases = len(cases)

        for case in cases:
            for nested_type in nested_types.copy().keys():
                if nested_type in case.keys() and case[nested_type]:
                    nested_types[nested_type] += 1

        nested_types['follow_ups.molecular_tests'] = 0

        for case in cases:
            if 'follow_ups' in cases[case]:
                if 'molecular_tests' in cases[case]['follow_ups']:
                    print("t2")
                    if cases[case]['follow_ups']['molecular_tests']:
                        print("t3")
                        nested_types['follow_ups.molecular_tests'] += 1

        print()
        print(program_name)
        print("total cases: {}".format(total_cases))
        print(nested_types)
        print()


    return
    '''
    if nested_name not in nested_key_set:
        print("{} not nested in program {}".format(nested_name, program_name))
        return

    for case in cases:
        if nested_name in case.keys():
            for record in case[nested_name]:
                for record_key in record.keys():
                    if record[record_key]:
                        record_fieldset.add(record_key)

    for case in cases:
        if nested_name in case.keys():
            for record in case[nested_name]:
                if 'molecular_tests' in record:
                    for mt_record in record['molecular_tests']:
                        for record_key in mt_record.keys():
                            if mt_record[record_key]:
                                record_fieldset.add(record_key)
    '''
    print(program_name)
    for field in sorted(record_fieldset):
        print(field)
    return

    field_data_type_dict = get_field_data_types(cases)

    mapping_dict = create_mapping_dict("https://api.gdc.cancer.gov/cases")

    schema_dict = create_field_records_dict(mapping_dict, field_data_type_dict)

    divided_schema_dict = dict()

    depth_ordered_nested_key_list = []

    for nested_key in nested_key_set:
        split_key = nested_key.split('.')
        if len(split_key) > 2:
            print("[ERROR] One of the nested keys has a depth > 2, is there a 3rd degree of nesting?")
        elif len(split_key) == 2:
            depth_ordered_nested_key_list.insert(0, nested_key)
        else:
            depth_ordered_nested_key_list.append(nested_key)

    for nested_key in depth_ordered_nested_key_list:
        divided_schema_dict[nested_key] = dict()

        long_key = 'cases.' + nested_key

        for field in schema_dict.copy().keys():
            if field.startswith(long_key):
                divided_schema_dict[nested_key][field] = schema_dict.pop(field)

    divided_schema_dict["non_nested"] = schema_dict

    print(program_name)
    for key in sorted(divided_schema_dict[nested_name].keys()):
        child_key = key.split(".")[-1]
        print(child_key)
    print()
    # schema_field_list, ordered_keys = create_bq_schema_list(field_data_type_dict, nested_key_set)
    return
    create_bq_table_and_insert_rows(program_name, cases, schema_field_list, ordered_keys)
    """



    """
    no nested keys: FM, NCICCR, CTSP, ORGANOID, CPTAC, WCDT, TARGET, GENIE
    nested keys:
    BEATAML1.0: diagnoses__annotations
    MMRF: follow_ups, follow_ups.molecular_tests, family_histories, diagnoses__treatments
    OHSU: diagnoses__annotations
    CGCI: diagnoses__treatments
    VAREPOP: family_histories, diagnoses__treatments
    HCMI: follow_ups, diagnoses__treatments, follow_ups.molecular_tests
    TCGA: diagnoses__treatments
    
    diagnoses__annotations: BEATAML1.0, OHSU
    diagnoses__treatments: MMRF, CGCI, VAREPOP, HCMI, TCGA
    family_histories: MMRF, VAREPOP
    follow_ups: MMRF, HCMI
    follow_ups.molecular_tests: MMRF, HCMI
    
    """


if __name__ == '__main__':
    main()
