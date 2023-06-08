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
    working_project: str = BQ_PARAMS['WORKING_PROJECT']
    working_dataset: str = BQ_PARAMS['WORKING_DATASET']
    release: str = API_PARAMS['RELEASE']

    return f"`{working_project}.{working_dataset}.{release}_{table_name}`"


def make_case_metadata_table_sql():
    return f"""
    WITH counts AS (
    SELECT case_id, COUNT(file_id) as active_file_count 
        FROM `{create_dev_table_id('file_in_case')}`
        GROUP BY case_id
    )
    
    SELECT cpp.case_gdc_id, 
    c.primary_site, 
    cpp.project_dbgap_accession_number, 
    psdt.disease_type as project_disease_type,
    cpp.project_name, 
    cpp.program_dbgap_accession_number,
    cpp.program_name, 
    cpp.project_id, 
    c.submitter_id AS case_barcode,
    r.legacy_file_count,
    counts.active_file_count
    FROM `{create_dev_table_id('case_project_program')}` cpp
    JOIN `{create_dev_table_id('case')}` c
        ON c.case_id = cpp.case_gdc_id
    JOIN `{create_dev_table_id('project_studies_disease_type')}` psdt
        ON psdt.project_id = cpp.project_id
    JOIN `{BQ_PARAMS['ARCHIVE_COUNT_TABLE_ID']}` r
        ON cpp.case_gdc_id = r.case_gdc_id
    JOIN counts 
        ON counts.case_id = cpp.case_gdc_id
    """


def main(args):
    try:
        global API_PARAMS, BQ_PARAMS
        API_PARAMS, BQ_PARAMS, steps = load_config(args, YAML_HEADERS)
    except ValueError as err:
        has_fatal_error(err, ValueError)

    if 'create_table_from_query' in steps:
        table_id = f"{BQ_PARAMS['WORKING_PROJECT']}.{BQ_PARAMS['WORKING_DATASET']}.case_metadata_{API_PARAMS['RELEASE']}"
        load_table_from_query(bq_params=BQ_PARAMS, table_id=table_id, query=make_case_metadata_table_sql())


if __name__ == "__main__":
    main(sys.argv)