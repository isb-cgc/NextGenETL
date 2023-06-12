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

from common_etl.utils import load_config, has_fatal_error, load_table_from_query

API_PARAMS = dict()
BQ_PARAMS = dict()
YAML_HEADERS = ('api_params', 'bq_params', 'steps')


def create_dev_table_id(table_name) -> str:
    return f"`{BQ_PARAMS['WORKING_PROJECT']}.{BQ_PARAMS['WORKING_DATASET']}.{API_PARAMS['RELEASE']}_{table_name}`"


def make_slide_case_table_sql():
    return f"""
    SELECT 
        cpp.program_name, 
        cpp.project_id,
        cpp.case_gdc_id, 
        cpp.case_barcode,
        s.sample_id AS sample_gdc_id,
        s.submitter_id AS sample_barcode,
        s.sample_type_id AS sample_type,
        s.sample_type AS sample_type_name,
        p.portion_id AS portion_gdc_id,
        p.submitter_id AS portion_barcode,
        sl.slide_id AS slide_gdc_id,
        sl.submitter_id AS slide_barcode
    FROM {create_dev_table_id('slide')} sl
    JOIN {create_dev_table_id('slide_from_portion')} sfp
        ON sfp.slide_id = sl.slide_id
    JOIN {create_dev_table_id('portion')} p 
        ON p.portion_id = sfp.portion_id
    JOIN {create_dev_table_id('portion_from_sample')} pfs  
        ON pfs.portion_id = p.portion_id
    JOIN {create_dev_table_id('sample')} s
        ON s.sample_id = pfs.sample_id
    JOIN {create_dev_table_id('sample_from_case')} sic
        ON sic.sample_id = s.sample_id
    JOIN {create_dev_table_id('case_project_program')} cpp
        ON cpp.case_gdc_id = sic.case_id
    """


def main(args):
    try:
        global API_PARAMS, BQ_PARAMS
        API_PARAMS, BQ_PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        has_fatal_error(err, ValueError)

    if 'create_table_from_query' in steps:
        table_id = f"{BQ_PARAMS['WORKING_PROJECT']}.{BQ_PARAMS['WORKING_DATASET']}.slide_to_case_{API_PARAMS['RELEASE']}"

        load_table_from_query(bq_params=BQ_PARAMS, table_id=table_id, query=make_slide_case_table_sql())


if __name__ == "__main__":
    main(sys.argv)