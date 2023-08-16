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

from cda_bq_etl.utils import load_config, has_fatal_error, format_seconds, create_dev_table_id
from cda_bq_etl.bq_helpers import load_table_from_query, publish_table, update_table_schema_from_generic

PARAMS = dict()
YAML_HEADERS = ('params', 'steps')


def make_case_metadata_query() -> str:
    """
    Make BigQuery sql statement, used to generate the case_metadata table.
    :return: sql query statement
    """
    return f"""
        WITH file_counts AS (
            SELECT case_id, COUNT(file_id) AS file_count
            FROM `{create_dev_table_id(PARAMS, "file_case_id")}`
            GROUP BY case_id
        )

        SELECT c.case_id,
            c.case_submitter_id,
            c.primary_site,
            c.disease_type,
            proj.name AS project_name,
            prog.name AS program_name,
            proj.project_id,
            fc.file_count
        FROM `{create_dev_table_id(PARAMS, "case")}` c 
        LEFT JOIN `{create_dev_table_id(PARAMS, "case_project_id")}` cp
            ON cp.case_id = c.case_id
        LEFT JOIN `{create_dev_table_id(PARAMS, "project")}` proj
            ON proj.project_id = cp.project_id
        LEFT JOIN `{create_dev_table_id(PARAMS, "program_project_id")}` pp
            ON pp.project_id = proj.project_id
        LEFT JOIN `{create_dev_table_id(PARAMS, "program")}` prog
            ON prog.program_id = pp.program_id
        LEFT JOIN file_counts fc
            ON fc.case_id = c.case_id
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
        load_table_from_query(params=PARAMS, table_id=dev_table_id, query=make_case_metadata_query())

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
