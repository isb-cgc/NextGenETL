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
import time

from cda_bq_etl.utils import load_config, has_fatal_error, create_dev_table_id, format_seconds
from cda_bq_etl.bq_helpers import load_table_from_query, publish_table, update_table_schema_from_generic

PARAMS = dict()
YAML_HEADERS = ('params', 'steps')


def make_slide_case_table_sql() -> str:
    """
    Make BigQuery sql statement that is used to create the slide to case map table
    :return: slide to case map sql query
    """
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
    FROM `{create_dev_table_id(PARAMS, 'slide')}` sl
    JOIN `{create_dev_table_id(PARAMS, 'slide_from_portion')}` sfp
        ON sfp.slide_id = sl.slide_id
    JOIN `{create_dev_table_id(PARAMS, 'portion')}` p 
        ON p.portion_id = sfp.portion_id
    JOIN `{create_dev_table_id(PARAMS, 'portion_from_sample')}` pfs  
        ON pfs.portion_id = p.portion_id
    JOIN `{create_dev_table_id(PARAMS, 'sample')}` s
        ON s.sample_id = pfs.sample_id
    JOIN `{create_dev_table_id(PARAMS, 'sample_from_case')}` sic
        ON sic.sample_id = s.sample_id
    JOIN `{create_dev_table_id(PARAMS, 'case_project_program')}` cpp
        ON cpp.case_gdc_id = sic.case_id
    """


def main(args):
    try:
        global PARAMS
        PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        has_fatal_error(err, ValueError)

    start_time = time.time()

    dev_table_id = f"{PARAMS['DEV_PROJECT']}.{PARAMS['DEV_METADATA_DATASET']}.{PARAMS['TABLE_NAME']}_{PARAMS['RELEASE']}"

    if 'create_table_from_query' in steps:
        load_table_from_query(params=PARAMS, table_id=dev_table_id, query=make_slide_case_table_sql())

        update_table_schema_from_generic(params=PARAMS, table_id=dev_table_id)

    if 'publish_tables' in steps:
        current_table_name = f"{PARAMS['TABLE_NAME']}_current"
        current_table_id = f"{PARAMS['PROD_PROJECT']}.{PARAMS['PROD_DATASET']}.{current_table_name}"
        versioned_table_name = f"{PARAMS['TABLE_NAME']}_{PARAMS['DC_RELEASE']}"
        versioned_table_id = f"{PARAMS['PROD_PROJECT']}.{PARAMS['PROD_DATASET']}_versioned.{versioned_table_name}"

        publish_table(params=PARAMS,
                      source_table_id=dev_table_id,
                      current_table_id=current_table_id,
                      versioned_table_id=versioned_table_id)

    end_time = time.time()

    print(f"Script completed in: {format_seconds(end_time - start_time)}")


if __name__ == "__main__":
    main(sys.argv)
