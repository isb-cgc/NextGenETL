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

from cda_bq_etl.utils import load_config, has_fatal_error, create_dev_table_id
from cda_bq_etl.bq_helpers import load_table_from_query

PARAMS = dict()
YAML_HEADERS = ('params', 'steps')


def make_aliquot_case_table_sql() -> str:
    """
    Make BigQuery sql statement, used to generate the aliquot_to_case_mapping table.
    :return:
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
            FROM {create_dev_table_id('aliquot')} al
            JOIN {create_dev_table_id('aliquot_of_analyte')} aoa
                ON aoa.aliquot_id = al.aliquot_id
            JOIN {create_dev_table_id('analyte')} an
                ON an.analyte_id = aoa.analyte_id
            JOIN {create_dev_table_id('analyte_from_portion')} afp
                ON afp.analyte_id = an.analyte_id
            JOIN {create_dev_table_id('portion')} p 
                ON p.portion_id = afp.portion_id
            JOIN {create_dev_table_id('portion_from_sample')} pfs
                ON pfs.portion_id = p.portion_id
            JOIN {create_dev_table_id('sample')} s
                ON s.sample_id = pfs.sample_id
            JOIN {create_dev_table_id('sample_from_case')} sfc
                ON sfc.sample_id = s.sample_id
            JOIN {create_dev_table_id('case_project_program')} cpp
                ON sfc.case_id = cpp.case_gdc_id
        ) UNION ALL (
            SELECT * 
            FROM `isb-project-zero.cda_gdc_test.r37_aliquot_to_case_legacy` 
        )
    """


def main(args):
    try:
        global PARAMS
        PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        has_fatal_error(err, ValueError)

    if 'create_table_from_query' in steps:
        table_id = f"{PARAMS['WORKING_PROJECT']}.{PARAMS['WORKING_DATASET']}.aliquot_to_case_{PARAMS['RELEASE']}"
        load_table_from_query(params=PARAMS, table_id=table_id, query=make_aliquot_case_table_sql())


if __name__ == "__main__":
    main(sys.argv)
