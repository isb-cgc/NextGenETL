from common_etl.utils import get_cases_by_program, collect_field_values


def main():
    cases = get_cases_by_program("ORGANOID")

    for case in cases:
        for key in case.copy():
            if isinstance(case[key], dict):
                for d_key in case[key].copy():
                    if case[key][d_key]:
                        flat_key = key + "." + d_key
                        case[flat_key] = case[key][d_key]

                    case[key].pop(d_key)
                case.pop(key)

    count_of_case_keys = dict()

    for case in cases:
        for key in case:
            if key not in count_of_case_keys:
                count_of_case_keys[key] = 1
            else:
                count_of_case_keys[key] += 1

    total_count = count_of_case_keys['id']

    list_of_partial_null_keys = []

    for key in count_of_case_keys.keys():
        if count_of_case_keys[key] < total_count:
            list_of_partial_null_keys.append(key)

    case_columns = count_of_case_keys.keys()

    field_dict = dict()
    array_fields = set()

    for case in cases:
        for key in case:
            field_dict, array_fields = collect_field_values(field_dict, key, case, 'cases.', array_fields)

    print(field_dict)


if __name__ == '__main__':
    main()
