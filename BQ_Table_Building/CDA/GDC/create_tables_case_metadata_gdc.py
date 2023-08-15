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


def make_case_metadata_table_sql(legacy_table_id: str) -> str:
    """
    Make BigQuery sql statement, used to generate case metadata table.
    :param legacy_table_id: table id for legacy data
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
            LEFT JOIN `{create_dev_table_id(PARAMS, 'project_disease_types_merged')}` pdt
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

    start_time = time.time()

    dev_table_id = f"{PARAMS['DEV_PROJECT']}.{PARAMS['DEV_METADATA_DATASET']}.{PARAMS['TABLE_NAME']}_{PARAMS['RELEASE']}"

    if 'create_table_from_query' in steps:
        legacy_table_id = PARAMS['LEGACY_TABLE_ID']

        load_table_from_query(params=PARAMS, table_id=dev_table_id, query=make_case_metadata_table_sql(legacy_table_id))

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
