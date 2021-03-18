"""
Copyright 2020-2021, Institute for Systems Biology

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

import time
import sys

from common_etl.utils import (format_seconds, has_fatal_error, load_config)

from common_etl.pdc_utils import (build_jsonl_from_pdc_api, build_table_from_jsonl, get_pdc_study_ids)

API_PARAMS = dict()
BQ_PARAMS = dict()
YAML_HEADERS = ('api_params', 'bq_params', 'steps')


def make_biospecimen_per_study_query(pdc_study_id):
    """
    Creates a graphQL string for querying the PDC API's biospecimenPerStudy endpoint.
    :return: GraphQL query string
    """
    return '''
        {{ biospecimenPerStudy( pdc_study_id: \"{}\" acceptDUA: true) {{
            aliquot_id 
            sample_id 
            case_id 
            aliquot_submitter_id 
            sample_submitter_id 
            case_submitter_id 
            aliquot_status 
            case_status 
            sample_status 
            project_name 
            sample_type 
            disease_type 
            primary_site 
            pool 
            taxon
        }}
    }}'''.format(pdc_study_id)


def alter_biospecimen_per_study_obj(json_obj_list, pdc_study_id):
    """
    This function is passed as a parameter to build_jsonl_from_pdc_api(). It allows for the json object to be mutated
    prior to writing it to a file.
    :param json_obj_list: list of json objects to mutate
    :param pdc_study_id: pdc study id for this set of json objects
    """
    for case in json_obj_list:
        case['pdc_study_id'] = pdc_study_id


def main(args):
    start_time = time.time()
    print("PDC script started at {}".format(time.strftime("%x %X", time.localtime())))

    steps = None

    try:
        global API_PARAMS, BQ_PARAMS
        API_PARAMS, BQ_PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        has_fatal_error(err, ValueError)

    all_pdc_study_ids = get_pdc_study_ids(API_PARAMS, BQ_PARAMS, include_embargoed_studies=True)

    if 'build_biospecimen_jsonl' in steps:
        build_jsonl_from_pdc_api(API_PARAMS, BQ_PARAMS,
                                 endpoint="biospecimenPerStudy",
                                 request_function=make_biospecimen_per_study_query,
                                 alter_json_function=alter_biospecimen_per_study_obj,
                                 ids=all_pdc_study_ids,
                                 insert_id=True)

    if 'build_biospecimen_table' in steps:
        build_table_from_jsonl(API_PARAMS, BQ_PARAMS,
                               endpoint="biospecimenPerStudy",
                               infer_schema=True)

    end = time.time() - start_time
    print("Finished program execution in {}!\n".format(format_seconds(end)))


if __name__ == '__main__':
    main(sys.argv)
