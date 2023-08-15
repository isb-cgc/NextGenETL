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


def make_aliquot_case_table_sql(legacy_table_id: str) -> str:
    """
    Make BigQuery sql statement, used to generate the aliquot_to_case_mapping table.
    :return: sql query statement
    """
    return f"""
        (
            SELECT 
                cpp.program_name, 
                cpp.project_id,
                cpp.case_gdc_id, 
                cpp.case_barcode,
                s.sample_id AS sample_gdc_id,
                s.submitter_id AS sample_barcode,
                s.sample_type_id AS sample_type,
                s.sample_type AS sample_type_name,
                s.is_ffpe AS sample_is_ffpe, 
                s.preservation_method AS sample_preservation_method,
                p.portion_id AS portion_gdc_id,
                p.submitter_id AS portion_barcode,
                an.analyte_id AS analyte_gdc_id,
                an.submitter_id AS analyte_barcode,
                al.aliquot_id AS aliquot_gdc_id,
                al.submitter_id AS aliquot_barcode        
            FROM `{create_dev_table_id(PARAMS, 'aliquot')}` al
            JOIN `{create_dev_table_id(PARAMS, 'aliquot_of_analyte')}` aoa
                ON aoa.aliquot_id = al.aliquot_id
            JOIN `{create_dev_table_id(PARAMS, 'analyte')}` an
                ON an.analyte_id = aoa.analyte_id
            JOIN `{create_dev_table_id(PARAMS, 'analyte_from_portion')}` afp
                ON afp.analyte_id = an.analyte_id
            JOIN `{create_dev_table_id(PARAMS, 'portion')}` p 
                ON p.portion_id = afp.portion_id
            JOIN `{create_dev_table_id(PARAMS, 'portion_from_sample')}` pfs
                ON pfs.portion_id = p.portion_id
            JOIN `{create_dev_table_id(PARAMS, 'sample')}` s
                ON s.sample_id = pfs.sample_id
            JOIN `{create_dev_table_id(PARAMS, 'sample_from_case')}` sfc
                ON sfc.sample_id = s.sample_id
            JOIN `{create_dev_table_id(PARAMS, 'case_project_program')}` cpp
                ON sfc.case_id = cpp.case_gdc_id
        ) UNION ALL (
            SELECT * 
            FROM `{legacy_table_id}`
        )
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
        legacy_table_id = PARAMS['LEGACY_TABLE_ID']

        load_table_from_query(params=PARAMS, table_id=dev_table_id, query=make_aliquot_case_table_sql(legacy_table_id))

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
