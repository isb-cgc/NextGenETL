from common_etl.utils import get_cases_by_program, collect_field_values, infer_data_types, create_mapping_dict
from google.cloud import bigquery


def flatten_case_json(program_name):
    cases, nested_key_set = get_cases_by_program(program_name)

    for case in cases:
        for key in case.copy():
            if isinstance(case[key], dict):
                for d_key in case[key].copy():
                    if case[key][d_key]:
                        flat_key = key + "__" + d_key
                        case[flat_key] = case[key][d_key]

                    case[key].pop(d_key)
                case.pop(key)
            elif isinstance(case[key], list):
                nested_key_set.add(key)

    # catches child-level nested fields for newly-flattened level
    for case in cases:
        for key in case:
            if isinstance(case[key], list):
                nested_key_set.add(key)
                for i in range(len(case[key])):
                    for n_key in case[key][i]:
                        if isinstance(case[key][i][n_key], list):
                            nested_key_set.add(key + "." + n_key)

    return cases, nested_key_set


def get_field_data_types(cases):
    field_dict = dict()
    array_fields = set()

    for case in cases:
        for key in case:
            field_dict, array_fields = collect_field_values(field_dict, key, case, 'cases.', array_fields)

    field_type_dict = infer_data_types(field_dict)

    return field_type_dict


def generate_bq_schema(schema_dict, record_type, nested_fields):
    # add field group names to a list, in order to generate a dict representing nested fields
    field_group_names = [record_type]
    nested_depth = 0

    for nested_field in nested_fields:
        nested_field_name = record_type + '.' + nested_field
        nested_depth = max(nested_depth, len(nested_field_name.split('.')))
        field_group_names.append(nested_field_name)

    record_lists_dict = {fg_name:[] for fg_name in field_group_names}
    # add field to correct field grouping list based on full field name

    for field in schema_dict:
        print(field)
        continue
        # record_lists_dict key is equal to the parent field components of full field name
        json_obj_key = '.'.join(field.split('.')[:-1])
        record_lists_dict[json_obj_key].append(schema_dict[field])
    return
    temp_schema_field_dict = {}

    while nested_depth >= 1:
        for field_group_name in record_lists_dict:
            split_group_name = field_group_name.split('.')

            # building from max depth inward, to avoid iterating through entire schema object in order to append
            # child field groupings. Therefore, skip any field groupings at a shallower depth.
            if len(split_group_name) != nested_depth:
                continue

            schema_field_sublist = []

            for record in record_lists_dict[field_group_name]:
                schema_field_sublist.append(
                    bigquery.SchemaField(record['name'], record['type'], 'NULLABLE', record['description'], ())
                )

            parent_name = '.'.join(split_group_name[:-1])
            field_name = split_group_name[-1]

            if field_group_name in temp_schema_field_dict:
                schema_field_sublist += temp_schema_field_dict[field_group_name]

            if parent_name:
                if parent_name not in temp_schema_field_dict:
                    temp_schema_field_dict[parent_name] = list()

                temp_schema_field_dict[parent_name].append(
                    bigquery.SchemaField(field_name, 'RECORD', 'REPEATED', '', tuple(schema_field_sublist))
                )
            else:
                if nested_depth > 1:
                    has_fatal_error("Empty parent_name at level {}".format(nested_depth), ValueError)
                return schema_field_sublist

        nested_depth -= 1
    return None


def create_bq_schema_list(field_data_type_dict, nested_keys):
    mapping_dict = create_mapping_dict("https://api.gdc.cancer.gov/cases")

    schema_parent_field_list = []
    schema_child_field_list = []
    ordered_parent_keys = []
    ordered_child_keys = []

    print(field_data_type_dict)
    print(nested_keys)
    return

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

    errors = client.insert_rows(table, case_tuples)

    if not errors:
        print("Rows inserted successfully")
    else:
        print(errors)


def main():
    program_name = "HCMI"

    cases, nested_key_set = flatten_case_json(program_name)

    field_data_type_dict = get_field_data_types(cases)

    # schema_field_list, ordered_keys = create_bq_schema_list(field_data_type_dict, nested_key_set)

    generate_bq_schema(field_data_type_dict, 'cases', nested_key_set)

    return

    create_bq_table_and_insert_rows(program_name, cases, schema_field_list, ordered_keys)


if __name__ == '__main__':
    main()
