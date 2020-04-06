from common_etl.utils import get_cases_by_program, collect_field_values, infer_data_types, create_mapping_dict
from google.cloud import bigquery


def flatten_case_json(program_name):
    cases, nested_key_set = get_cases_by_program(program_name)

    print(nested_key_set)
    return

    for case in cases:
        for key in case.copy():
            if isinstance(case[key], dict):
                for d_key in case[key].copy():
                    if case[key][d_key]:
                        flat_key = key + "." + d_key
                        case[flat_key] = case[key][d_key]

                    case[key].pop(d_key)
                case.pop(key)

    return cases


def get_field_data_types(cases):
    field_dict = dict()
    array_fields = set()

    for case in cases:
        for key in case:
            field_dict, array_fields = collect_field_values(field_dict, key, case, 'cases.', array_fields)

    return infer_data_types(field_dict)


def create_bq_schema_list(field_data_type_dict):
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

        # todo: this will only work for non-nested
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

    print(case_tuples)

    errors = client.insert_rows(table, case_tuples)

    if not errors:
        print("Rows inserted successfully")
    else:
        print(errors)


def main():
    program_name = "HCMI"

    cases, nested_key_list = flatten_case_json(program_name)
    return

    field_data_type_dict = get_field_data_types(cases)

    schema_field_list, ordered_keys = create_bq_schema_list(field_data_type_dict)

    create_bq_table_and_insert_rows(program_name, cases, schema_field_list, ordered_keys)


if __name__ == '__main__':
    main()
