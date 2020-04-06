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
        try:
            column_name = field_mapping_dict[key]['name'].split('.')[-1]
            description = field_mapping_dict[key]['description']
        except KeyError:
            # cases.id not returned by mapping endpoint. In such cases, substitute an empty description string.
            column_name = key.split(".")[-1]
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

        # Note: I could likely go back use ARRAY as a column type. It wasn't working before, and I believe the issue
        # was that I'd set the FieldSchema object's mode to NULLABLE, which I later read is invalid for ARRAY types.
        # But, that'll mean more unnesting for the users. So for now, I've converted these lists of ids into
        # comma-delineated strings of ids.
        # if key in array_fields:
        #    field_type = "ARRAY<" + field_type + ">"

        # this is the format for bq schema json object entries
        schema_dict[key] = {
            "name": column_name,
            "type": field_type,
            "description": description
        }

    return schema_dict


def create_field_records_dict(field_mapping_dict, field_data_type_dict):
    """
    Generate flat dict containing schema metadata object with fields 'name', 'type', 'description'
    :param field_mapping_dict:
    :param field_data_type_dict:
    :return: schema fields object dict
    """
    schema_dict = {}

    for key in field_data_type_dict:

        try:
            column_name = field_mapping_dict[key]['name'].split('.')[-1]
            description = field_mapping_dict[key]['description']
        except KeyError:
            # cases.id not returned by mapping endpoint. In such cases, substitute an empty description string.
            column_name = "__".join(key.split(".")[1:])
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

    print(schema_dict)

    return schema_dict


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

    mapping_dict = create_mapping_dict("https://api.gdc.cancer.gov/cases")

    schema_dict = create_field_records_dict(mapping_dict, field_data_type_dict)

    # schema_field_list, ordered_keys = create_bq_schema_list(field_data_type_dict, nested_key_set)

    return

    create_bq_table_and_insert_rows(program_name, cases, schema_field_list, ordered_keys)


if __name__ == '__main__':
    main()
