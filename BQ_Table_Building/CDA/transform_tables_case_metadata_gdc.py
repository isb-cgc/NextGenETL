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


def make_case_metadata_table_sql(legacy_table_id: str) -> str:
    """
    Make BigQuery sql statement, used to generate case metadata table.
    :return: sql string
    """
    return f"""
        WITH counts AS (
            SELECT case_id, COUNT(file_id) AS active_file_count 
            FROM `{create_dev_table_id(PARAMS, 'file_in_case')}`
            GROUP BY case_id
        )
        
        (
            SELECT cpp.case_gdc_id, 
            c.primary_site, 
            cpp.project_dbgap_accession_number, 
            pdt.disease_type as project_disease_type,
            cpp.project_name, 
            cpp.program_dbgap_accession_number,
            cpp.program_name, 
            cpp.project_id, 
            c.submitter_id AS case_barcode,
            r.legacy_file_count,
            counts.active_file_count
            FROM `{create_dev_table_id(PARAMS, 'case_project_program')}` cpp
            JOIN `{create_dev_table_id(PARAMS, 'case')}` c
                ON c.case_id = cpp.case_gdc_id
            LEFT JOIN `{create_dev_table_id(PARAMS, 'project_disease_types_merged', True)}` pdt
                ON pdt.project_id = cpp.project_id
            JOIN `{PARAMS['ARCHIVE_COUNT_TABLE_ID']}` r
                ON cpp.case_gdc_id = r.case_gdc_id
            JOIN counts 
                ON counts.case_id = cpp.case_gdc_id
        ) 
        
        UNION ALL 
        
        (
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

    if 'create_table_from_query' in steps:
        legacy_table_id = 'isb-project-zero.cda_gdc_test.r37_case_metadata_legacy'
        table_id = f"{PARAMS['WORKING_PROJECT']}.{PARAMS['WORKING_DATASET']}.case_metadata_{PARAMS['RELEASE']}"

        load_table_from_query(params=PARAMS, table_id=table_id, query=make_case_metadata_table_sql(legacy_table_id))


if __name__ == "__main__":
    main(sys.argv)
