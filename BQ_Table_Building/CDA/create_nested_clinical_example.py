"""
Copyright 2023, Institute for Systems Biology

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""
import sys

from common_etl.utils import load_config, has_fatal_error, normalize_value
from common_etl.support import bq_harness_with_result

API_PARAMS = dict()
BQ_PARAMS = dict()
YAML_HEADERS = ('api_params', 'bq_params', 'steps')


def get_cgci_nested_clinical_result():
    def make_cgci_clinical_query():
        return f"""
        SELECT * 
        FROM `{BQ_PARAMS['WORKING_PROJECT']}.{BQ_PARAMS['WORKING_DATASET']}.{BQ_PARAMS['RELEASE']}_clinical`,
        UNNEST(project) as proj
        WHERE proj.project_id LIKE 'CGCI%'
        """

    result = bq_harness_with_result(sql=make_cgci_clinical_query(), do_batch=False, verbose=False)

    cases = list()

    for row in result:
        record = dict(row.items())
        cases.append(record)

    return cases


def recursively_normalize_field_values(json_records, is_single_record=False):
    """
    Recursively explores and normalizes a list of json objects. Useful when there's arbitrary nesting of dicts and
    lists with varying depths.
    :param json_records: list of json objects
    :param is_single_record: If true, json_records contains a single json object,
    otherwise contains a list of json objects
    :return: if is_single_record, returns normalized copy of the json object.
    if multiple records, returns a list of json objects.
    """
    def recursively_normalize_field_value(_obj, _data_set_dict):
        """
        Recursively explore a part of the supplied object. Traverses parent nodes, replicating existing data structures
        and normalizing values when reaching a "leaf" node.
        :param _obj: object in current location of recursion
        :param _data_set_dict: dict of fields and type sets
        """
        for key, value in _obj.items():
            if isinstance(_obj[key], dict):
                if key not in _data_set_dict:
                    # this is a dict, so use dict to nest values
                    _data_set_dict[key] = dict()

                recursively_normalize_field_value(_obj[key], _data_set_dict[key])
            elif isinstance(_obj[key], list) and len(_obj[key]) > 0 and isinstance(_obj[key][0], dict):
                if key not in _data_set_dict:
                    _data_set_dict[key] = list()

                idx = 0
                for _record in _obj[key]:
                    _data_set_dict[key].append(dict())
                    recursively_normalize_field_value(_record, _data_set_dict[key][idx])
                    idx += 1
            elif not isinstance(_obj[key], list) or (isinstance(_obj[key], list) and len(_obj[key]) > 0):
                if key not in _data_set_dict:
                    _data_set_dict[key] = dict()

                value = normalize_value(value)
                _data_set_dict[key] = value

    if is_single_record:
        record_dict = dict()
        recursively_normalize_field_value(json_records, record_dict)
        return record_dict
    else:
        new_record_jsonl_list = list()

        for record in json_records:
            record_dict = dict()
            recursively_normalize_field_value(record, record_dict)

            new_record_jsonl_list.append(record_dict)

        return new_record_jsonl_list


def main(args):
    try:
        global API_PARAMS, BQ_PARAMS
        API_PARAMS, BQ_PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        has_fatal_error(err, ValueError)

    if 'get_cgci_clinical_result' in steps:
        cases = get_cgci_nested_clinical_result()

        print(cases[0])

        norm_cases = recursively_normalize_field_values(cases)

        print(norm_cases[0])


if __name__ == "__main__":
    main(sys.argv)
