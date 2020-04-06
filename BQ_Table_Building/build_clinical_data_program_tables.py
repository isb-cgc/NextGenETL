from common_etl.utils import get_cases_by_program


def main():
    cases = get_cases_by_program("ORGANOID")

    for case in cases:
        for key in case.copy():
            if isinstance(case[key], dict):
                for d_key in case[key].copy():
                    if case[key][d_key]:
                        flat_key = key + "__" + d_key
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

    print(list_of_partial_null_keys)


if __name__ == '__main__':
    main()
