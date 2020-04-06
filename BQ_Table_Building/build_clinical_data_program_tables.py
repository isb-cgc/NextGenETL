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

    print(sorted(cases))


if __name__ == '__main__':
    main()
