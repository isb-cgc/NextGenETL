"""
Copyright 2020, Institute for Systems Biology

Permission is hereby granted, free of charge, to any person obtaining a copy of this
software and associated documentation files (the "Software"), to deal in the Software
without restriction, including without limitation the rights to use, copy, modify,
merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies
or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING
BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""
import sys
import json
from common_etl.utils import get_programs_from_bq, load_config, has_fatal_error


def output_clinical_data_stats(clinical_data_fp, api_params):
    counts = {
        'total': 0,
        'no_clinical_fgs': 0
    }

    programs_with_field_group = {
        'none': set()
    }

    no_fg_case_barcodes = {}

    field_groups = api_params['EXPAND_FIELD_GROUPS'].split(',')

    for fg in field_groups:
        counts[fg] = 0
        programs_with_field_group[fg] = set()

    program_lookup_dict = get_programs_from_bq()
    print(program_lookup_dict)

    with open(clinical_data_fp, 'r') as file:
        for line in file:
            if counts['total'] % 100 == 0:
                print(counts['total'])
            counts['total'] += 1

            json_line = json.loads(line)
            program_name = program_lookup_dict[json_line['submitter_id']]

            if 'demographic' in json_line:
                counts['demographic'] += 1
                programs_with_field_group['demographic'].add(program_name)
            if 'diagnoses' in json_line:
                diagnoses = json_line['diagnoses'][0]
                counts['diagnoses'] += 1
                programs_with_field_group['diagnoses'].add(program_name)
                if 'annotations' in diagnoses:
                    counts['diagnoses.annotations'] += 1
                    programs_with_field_group['diagnoses.annotations'].add(program_name)
                if 'treatments' in diagnoses.keys():
                    counts['diagnoses.treatments'] += 1
                    programs_with_field_group['diagnoses.treatments'].add(program_name)
            if 'exposures' in json_line:
                counts['exposures'] += 1
                programs_with_field_group['exposures'].add(program_name)
            if 'family_histories' in json_line:
                counts['family_histories'] += 1
                programs_with_field_group['family_histories'].add(program_name)
            if 'follow_ups' in json_line:
                counts['follow_ups'] += 1
                programs_with_field_group['follow_ups'].add(program_name)
                if 'molecular_tests' in json_line['follow_ups'][0]:
                    programs_with_field_group['follow_ups.molecular_tests'].add(program_name)

            # Case has no clinical data field groups in API
            if 'demographic' not in json_line and 'family_histories' not in json_line \
                    and 'exposures' not in json_line and 'diagnoses' not in json_line \
                    and 'follow_ups' not in json_line:
                programs_with_field_group['none'].add(program_name)
                counts['no_clinical_fgs'] += 1

                if program_name not in no_fg_case_barcodes:
                    no_fg_case_barcodes[program_name] = set()
                no_fg_case_barcodes[program_name].add(json_line['submitter_id'])

        # OUTPUT RESULTS
        for fg in field_groups:
            print_field_group_check(fg, counts, programs_with_field_group)

        print("\nPrograms with no clinical data:")

        for program in no_fg_case_barcodes:
            no_fg_case_count = len(no_fg_case_barcodes[program])
            print('\n{} has {} cases with no clinical data.'.format(program, str(no_fg_case_count)))
            print('submitter_id (case_barcode) list:')
            print(no_fg_case_barcodes[program])


def print_field_group_check(fg_name, counts, fg_program_list):
    fg_pct = counts[fg_name] / (counts['total'] * 1.0) * 100

    print('For {}:'.format(fg_name))
    print('\tfound in {:.2f}% of cases'.format(fg_pct))
    print('\tprograms with {} field_group: {}'.format(fg_name, str(fg_program_list[fg_name])))


def check_gdc_webapp_data(gdc_dict, api_fp):
    row_match_count = 0
    row_not_match_count = 0
    with open(api_fp, 'r') as api_file:
        for row in api_file:
            row_match = True
            api_case_json = json.loads(row)
            case_id = api_case_json['case_id']

            gdc_case_json = gdc_dict[case_id]

            for fg in gdc_case_json.keys():
                if fg in api_case_json and gdc_case_json[fg] == api_case_json[fg]:
                    continue

                if fg not in api_case_json:
                    print("case_id {}: {} not in api case record".format(case_id, fg))
                    row_match = False
                    continue

                gdc_fg = gdc_case_json[fg]
                api_fg = api_case_json[fg]
                # find mis-matched values
                if isinstance(gdc_fg, list):
                    gdc_fg = gdc_fg[0]
                    api_fg = api_fg[0]

                for fg_key in gdc_fg.keys():
                    if fg_key not in api_fg:
                        row_match = False
                        print("case_id {}: API case version does not contain field {} in {}"
                              .format(case_id, fg_key, fg))
                    elif api_fg[fg_key] != gdc_fg[fg_key]:
                        row_match = False
                        print("case_id {}: field values mismatch for field {} in {}".format(case_id, fg_key, fg))
                        print("api: {}, webapp: {}".format(api_fg[fg_key], gdc_fg[fg_key]))
            if row_match:
                row_match_count += 1
            else:
                row_not_match_count += 1

    print("GDC Clinical file download (from webapp) results")
    print("Matching case count: {}".format(row_match_count))
    print("Non-matching case count: {}".format(row_not_match_count))


def create_gdc_cases_dict(gdc_fp):
    """
    Transform into a dict with case_ids as key
    :param gdc_fp:
    :return:
    """
    gdc_dict = {}
    with open(gdc_fp, 'r') as gdc_file:
        gdc_cases = json.load(gdc_file)

        for case in gdc_cases:
            case_id = case.pop('case_id')
            gdc_dict[case_id] = case

    return gdc_dict


def main(args):
    webapp_data_fp = '../temp/clinical.cases_selection.2020-03-26.json'
    api_data_fp = '../temp/clinical_data.jsonl'

    yaml_headers = ('api_and_file_params', 'bq_params')

    # Load the YAML config file
    with open(args[1], mode='r') as yaml_file:
        try:
            api_params, bq_params = load_config(yaml_file, yaml_headers)
        except ValueError as e:
            has_fatal_error(str(e), ValueError)

    output_clinical_data_stats(api_data_fp, api_params)

    gdc_dict = create_gdc_cases_dict(webapp_data_fp)

    check_gdc_webapp_data(gdc_dict, api_data_fp)


if __name__ == '__main__':
    main(sys.argv)
